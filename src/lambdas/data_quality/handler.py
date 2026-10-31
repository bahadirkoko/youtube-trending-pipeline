"""Data quality gate for the Silver `trending_videos` table.

Reads the Silver Parquet files straight from S3 (no Athena), runs six checks and
returns a verdict. When anything fails it also emails the details through SNS.

  s3://<silver>/youtube/trending_videos/region=US/part-*.parquet

Checks
  row_count      every region has at least `min_rows` rows (and there is data at all)
  files_readable every Parquet file can be opened
  schema         required columns exist with the right kind of type
  null_rate      critical columns have at most `max_null_percent` % nulls, per region
  value_ranges   no negative counts, sane view counts, like_ratio within 0..1,
                 dates not in the future
  freshness      the newest LIVE (source = 'api') row is at most `max_age_hours` old.
                 Kaggle history is ignored here: it is from 2017-2018 by design.

The function does not raise on failed checks. It returns `passed: false` so Step
Functions can branch on it; a manual invoke then shows the full report.
"""

# pyright: reportAttributeAccessIssue=false
# (pyarrow.compute builds its functions at runtime, so type checkers cannot see them)
from __future__ import annotations

import io
import logging
import os
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

SILVER_PREFIX = "youtube/trending_videos/"
REGION_IN_KEY = re.compile(r"/region=([^/]+)/")

DATE_FLOOR = date(2005, 1, 1)  # YouTube did not exist before this
MAX_VIEWS = 20_000_000_000


def _is_string(t: pa.DataType) -> bool:
    return pa.types.is_string(t) or pa.types.is_large_string(t)


def _is_list(t: pa.DataType) -> bool:
    return pa.types.is_list(t) or pa.types.is_large_list(t)


# Silver columns (except the `region` partition) and the kind of type each must have.
EXPECTED_TYPES: dict[str, tuple[str, Callable[[pa.DataType], bool]]] = {
    "video_id": ("string", _is_string),
    "trending_date": ("date", pa.types.is_date),
    "title": ("string", _is_string),
    "channel_id": ("string", _is_string),
    "channel_title": ("string", _is_string),
    "category_id": ("integer", pa.types.is_integer),
    "publish_time": ("timestamp", pa.types.is_timestamp),
    "tags": ("list", _is_list),
    "duration_seconds": ("integer", pa.types.is_integer),
    "views": ("integer", pa.types.is_integer),
    "likes": ("integer", pa.types.is_integer),
    "dislikes": ("integer", pa.types.is_integer),
    "comment_count": ("integer", pa.types.is_integer),
    "comments_disabled": ("boolean", pa.types.is_boolean),
    "ratings_disabled": ("boolean", pa.types.is_boolean),
    "like_ratio": ("float", pa.types.is_floating),
    "engagement_rate": ("float", pa.types.is_floating),
    "source": ("string", _is_string),
    "ingested_at": ("timestamp", pa.types.is_timestamp),
}
CRITICAL_NULL_COLUMNS = (
    "video_id",
    "trending_date",
    "title",
    "channel_title",
    "views",
    "category_id",
)
NON_NEGATIVE_COLUMNS = ("views", "likes", "dislikes", "comment_count")
READ_COLUMNS = (
    "trending_date",
    "like_ratio",
    "source",
    "ingested_at",
    *CRITICAL_NULL_COLUMNS,
    *NON_NEGATIVE_COLUMNS,
)

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------
DEFAULTS: dict[str, Any] = {
    "min_rows": 10,
    "max_null_percent": 5.0,
    "max_age_hours": 48.0,
    "max_ratio_violation_percent": 1.0,
    "require_live_data": True,
}
ENV_NAMES = {
    "min_rows": "DQ_MIN_ROW_COUNT",
    "max_null_percent": "DQ_MAX_NULL_PERCENT",
    "max_age_hours": "DQ_MAX_AGE_HOURS",
    "max_ratio_violation_percent": "DQ_MAX_RATIO_VIOLATION_PERCENT",
    "require_live_data": "DQ_REQUIRE_LIVE_DATA",
}


class ConfigError(Exception):
    """A threshold is missing, unknown, or has an invalid value."""


@dataclass(frozen=True)
class Config:
    min_rows: int
    max_null_percent: float
    max_age_hours: float
    max_ratio_violation_percent: float
    require_live_data: bool


def _coerce(name: str, raw: Any, default: Any) -> Any:
    try:
        if isinstance(default, bool):
            if isinstance(raw, bool):
                return raw
            text = str(raw).strip().lower()
            if text in ("true", "1", "yes"):
                return True
            if text in ("false", "0", "no"):
                return False
            raise ValueError(text)
        if isinstance(default, int):
            return int(raw)
        return float(raw)
    except (TypeError, ValueError):
        raise ConfigError(f"Invalid value for {name}: {raw!r}") from None


def load_config(event: dict[str, Any] | None) -> Config:
    """Environment variables, overridden by event['thresholds'] (used for testing)."""
    overrides = (event or {}).get("thresholds") or {}
    unknown = sorted(set(overrides) - set(DEFAULTS))
    if unknown:
        raise ConfigError(f"Unknown threshold(s): {unknown}. Allowed: {sorted(DEFAULTS)}")

    values = {}
    for name, default in DEFAULTS.items():
        raw = overrides.get(name, os.environ.get(ENV_NAMES[name], default))
        values[name] = _coerce(name, raw, default)

    if values["min_rows"] < 0 or values["max_age_hours"] <= 0:
        raise ConfigError("min_rows must be >= 0 and max_age_hours must be > 0")
    for name in ("max_null_percent", "max_ratio_violation_percent"):
        if not 0 <= values[name] <= 100:
            raise ConfigError(f"{name} must be between 0 and 100")
    return Config(**values)


# --------------------------------------------------------------------------
# Statistics collected from the Parquet files
# --------------------------------------------------------------------------
@dataclass
class RegionStats:
    files: int = 0
    rows: int = 0
    nulls: dict[str, int] = field(default_factory=dict)
    negatives: dict[str, int] = field(default_factory=dict)
    views_too_high: int = 0
    ratio_values: int = 0
    ratio_violations: int = 0
    dates_out_of_range: int = 0
    api_rows: int = 0
    latest_api_ingest: datetime | None = None
    schema_problems: list[str] = field(default_factory=list)

    def merge(self, other: RegionStats) -> None:
        self.files += other.files
        self.rows += other.rows
        for column, count in other.nulls.items():
            self.nulls[column] = self.nulls.get(column, 0) + count
        for column, count in other.negatives.items():
            self.negatives[column] = self.negatives.get(column, 0) + count
        self.views_too_high += other.views_too_high
        self.ratio_values += other.ratio_values
        self.ratio_violations += other.ratio_violations
        self.dates_out_of_range += other.dates_out_of_range
        self.api_rows += other.api_rows
        if other.latest_api_ingest and (
            self.latest_api_ingest is None or other.latest_api_ingest > self.latest_api_ingest
        ):
            self.latest_api_ingest = other.latest_api_ingest
        for problem in other.schema_problems:
            if problem not in self.schema_problems:
                self.schema_problems.append(problem)


@dataclass
class Summary:
    regions: dict[str, RegionStats] = field(default_factory=dict)
    unreadable: list[str] = field(default_factory=list)
    files: int = 0


def schema_problems(schema: pa.Schema) -> list[str]:
    problems = []
    for column, (kind, is_kind) in EXPECTED_TYPES.items():
        if column not in schema.names:
            problems.append(f"missing column '{column}'")
        elif not is_kind(schema.field(column).type):
            actual = schema.field(column).type
            problems.append(f"column '{column}' is {actual}, expected {kind}")
    return problems


def _usable(table: pa.Table, column: str) -> bool:
    """Column exists and has the right kind of type (so computing on it is safe)."""
    return column in table.column_names and EXPECTED_TYPES[column][1](
        table.schema.field(column).type
    )


def _count_true(mask: pa.ChunkedArray | pa.Array) -> int:
    return int(pc.sum(mask).as_py() or 0)


def stats_for_table(table: pa.Table, now: datetime) -> RegionStats:
    stats = RegionStats(files=1, rows=table.num_rows)

    for column in CRITICAL_NULL_COLUMNS:
        if column in table.column_names:
            stats.nulls[column] = table[column].null_count

    for column in NON_NEGATIVE_COLUMNS:
        if _usable(table, column):
            stats.negatives[column] = _count_true(pc.less(table[column], 0))
    if _usable(table, "views"):
        stats.views_too_high = _count_true(pc.greater(table["views"], MAX_VIEWS))

    if _usable(table, "like_ratio"):
        values = table["like_ratio"].drop_null()
        stats.ratio_values = len(values)
        stats.ratio_violations = _count_true(pc.or_(pc.less(values, 0), pc.greater(values, 1)))

    if _usable(table, "trending_date"):
        latest_allowed = pa.scalar(now.date() + timedelta(days=1), pa.date32())
        too_old = pa.scalar(DATE_FLOOR, pa.date32())
        days = table["trending_date"]
        stats.dates_out_of_range = _count_true(pc.greater(days, latest_allowed)) + _count_true(
            pc.less(days, too_old)
        )

    if _usable(table, "source"):
        api = table.filter(pc.equal(table["source"], "api"))
        stats.api_rows = api.num_rows
        if api.num_rows and _usable(api, "ingested_at"):
            ingested = api["ingested_at"].cast(pa.timestamp("us"), safe=False)
            latest = pc.max(ingested).as_py()
            if latest is not None:
                stats.latest_api_ingest = latest if latest.tzinfo else latest.replace(tzinfo=UTC)
    return stats


def analyse_file(body: bytes, now: datetime) -> RegionStats:
    parquet = pq.ParquetFile(io.BytesIO(body))
    problems = schema_problems(parquet.schema_arrow)
    columns = [c for c in READ_COLUMNS if c in parquet.schema_arrow.names]
    stats = stats_for_table(parquet.read(columns=columns), now)
    stats.schema_problems = problems
    return stats


def load_summary(s3: Any, bucket: str, now: datetime) -> Summary:
    summary = Summary()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=SILVER_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            match = REGION_IN_KEY.search(key)
            region = match.group(1) if match else "UNKNOWN"
            summary.files += 1
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            try:
                stats = analyse_file(body, now)
            except (pa.ArrowException, ValueError) as exc:
                logger.error("Cannot read %s: %s", key, exc)
                summary.unreadable.append(key)
                continue
            summary.regions.setdefault(region, RegionStats()).merge(stats)
    return summary


# --------------------------------------------------------------------------
# The checks (pure functions of the statistics, easy to test)
# --------------------------------------------------------------------------
@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def _result(name: str, problems: list[str], ok_detail: str) -> CheckResult:
    if problems:
        return CheckResult(name, False, "; ".join(problems))
    return CheckResult(name, True, ok_detail)


def check_row_count(summary: Summary, config: Config) -> CheckResult:
    total = sum(s.rows for s in summary.regions.values())
    if total == 0:
        return CheckResult("row_count", False, "no rows found in Silver")
    problems = [
        f"{region}: {stats.rows} rows (minimum {config.min_rows})"
        for region, stats in sorted(summary.regions.items())
        if stats.rows < config.min_rows
    ]
    return _result("row_count", problems, f"{total} rows in {len(summary.regions)} regions")


def check_files_readable(summary: Summary) -> CheckResult:
    problems = [f"cannot read {key}" for key in summary.unreadable]
    return _result("files_readable", problems, f"{summary.files} files readable")


def check_schema(summary: Summary) -> CheckResult:
    problems = [
        f"{region}: {problem}"
        for region, stats in sorted(summary.regions.items())
        for problem in stats.schema_problems
    ]
    return _result("schema", problems, "all required columns present with the right types")


def check_null_rate(summary: Summary, config: Config) -> CheckResult:
    problems = []
    for region, stats in sorted(summary.regions.items()):
        if stats.rows == 0:
            continue
        for column, nulls in sorted(stats.nulls.items()):
            percent = nulls * 100 / stats.rows
            if percent > config.max_null_percent:
                problems.append(
                    f"{region}.{column}: {percent:.1f}% null (limit {config.max_null_percent:g}%)"
                )
    return _result(
        "null_rate", problems, f"critical columns at most {config.max_null_percent:g}% null"
    )


def check_value_ranges(summary: Summary, config: Config) -> CheckResult:
    problems = []
    for region, stats in sorted(summary.regions.items()):
        for column, count in sorted(stats.negatives.items()):
            if count:
                problems.append(f"{region}.{column}: {count} negative values")
        if stats.views_too_high:
            problems.append(f"{region}.views: {stats.views_too_high} values above {MAX_VIEWS:,}")
        if stats.ratio_values:
            percent = stats.ratio_violations * 100 / stats.ratio_values
            if percent > config.max_ratio_violation_percent:
                problems.append(
                    f"{region}.like_ratio: {percent:.1f}% outside 0..1 "
                    f"(limit {config.max_ratio_violation_percent:g}%)"
                )
        if stats.dates_out_of_range:
            problems.append(
                f"{region}.trending_date: {stats.dates_out_of_range} dates before "
                f"{DATE_FLOOR} or in the future"
            )
    return _result("value_ranges", problems, "counts, ratios and dates are within range")


def check_freshness(summary: Summary, config: Config, now: datetime) -> CheckResult:
    api_rows = sum(s.api_rows for s in summary.regions.values())
    if api_rows == 0:
        if config.require_live_data:
            return CheckResult("freshness", False, "no live API rows in Silver")
        return CheckResult("freshness", True, "skipped: no live API rows (not required)")

    latest = max(
        (s.latest_api_ingest for s in summary.regions.values() if s.latest_api_ingest),
        default=None,
    )
    if latest is None:
        return CheckResult("freshness", False, "live API rows have no ingested_at timestamp")

    age_hours = (now - latest).total_seconds() / 3600
    if age_hours > config.max_age_hours:
        return CheckResult(
            "freshness",
            False,
            f"latest live data is {age_hours:.1f}h old (limit {config.max_age_hours:g}h)",
        )
    return CheckResult("freshness", True, f"latest live data is {age_hours:.1f}h old")


def evaluate(summary: Summary, config: Config, now: datetime) -> list[CheckResult]:
    return [
        check_row_count(summary, config),
        check_files_readable(summary),
        check_schema(summary),
        check_null_rate(summary, config),
        check_value_ranges(summary, config),
        check_freshness(summary, config, now),
    ]


# --------------------------------------------------------------------------
# Alerting
# --------------------------------------------------------------------------
def format_alert(
    environment: str, results: list[CheckResult], summary: Summary, checked_at: datetime
) -> tuple[str, str]:
    failed = [r for r in results if not r.passed]
    passed = [r.name for r in results if r.passed]
    subject = (
        f"[{environment}] YouTube pipeline: data quality FAILED "
        f"({len(failed)} of {len(results)} checks)"
    )
    total = sum(s.rows for s in summary.regions.values())
    lines = [
        f"The data quality gate failed at {checked_at.isoformat()}.",
        f"Environment: {environment}",
        "",
        "Failed checks:",
        *[f"  - {r.name}: {r.detail}" for r in failed],
        "",
        f"Passed checks: {', '.join(passed) if passed else 'none'}",
        f"Rows checked: {total} in {len(summary.regions)} regions ({summary.files} files)",
        "",
        "Gold analytics should not run until this is resolved.",
    ]
    return subject[:100], "\n".join(lines)


def send_alert(subject: str, message: str) -> bool | None:
    """True = sent, False = SNS error, None = no topic configured."""
    topic_arn = os.environ.get("SNS_ALERT_TOPIC_ARN")
    if not topic_arn:
        logger.warning("SNS_ALERT_TOPIC_ARN is not set, alert not sent")
        return None
    try:
        boto3.client("sns").publish(TopicArn=topic_arn, Subject=subject, Message=message)
    except ClientError as exc:
        logger.error("Could not publish alert: %s", exc)
        return False
    return True


# --------------------------------------------------------------------------
# Lambda entry point
# --------------------------------------------------------------------------
def _utc_now() -> datetime:
    return datetime.now(UTC)


def lambda_handler(event: dict[str, Any] | None, context: Any) -> dict[str, Any]:
    bucket = os.environ["SILVER_BUCKET"]
    environment = os.environ.get("ENVIRONMENT", "dev")
    config = load_config(event)
    now = _utc_now()

    summary = load_summary(boto3.client("s3"), bucket, now)
    results = evaluate(summary, config, now)
    failed = [r for r in results if not r.passed]

    for result in results:
        level = logging.INFO if result.passed else logging.ERROR
        verdict = "PASS" if result.passed else "FAIL"
        logger.log(level, "%s %s: %s", verdict, result.name, result.detail)

    alert_sent = None
    if failed:
        subject, message = format_alert(environment, results, summary, now)
        alert_sent = send_alert(subject, message)

    return {
        "passed": not failed,
        "status": "PASSED" if not failed else "FAILED",
        "checked_at": now.isoformat(),
        "rows_checked": sum(s.rows for s in summary.regions.values()),
        "regions": sorted(summary.regions),
        "checks": [{"name": r.name, "passed": r.passed, "detail": r.detail} for r in results],
        "failed_checks": [r.name for r in failed],
        "alert_sent": alert_sent,
    }
