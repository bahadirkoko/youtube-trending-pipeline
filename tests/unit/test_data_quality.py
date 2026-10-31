"""Unit tests for the data_quality Lambda (fake S3 + SNS->SQS, no real AWS)."""

import importlib.util
import io
import json
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from moto import mock_aws

HANDLER_PATH = (
    Path(__file__).resolve().parents[2] / "src" / "lambdas" / "data_quality" / "handler.py"
)
SILVER = "test-silver"
FIXED_NOW = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
FRESH_INGEST = FIXED_NOW - timedelta(hours=6)

SCHEMA = pa.schema(
    [
        ("video_id", pa.string()),
        ("trending_date", pa.date32()),
        ("title", pa.string()),
        ("channel_id", pa.string()),
        ("channel_title", pa.string()),
        ("category_id", pa.int32()),
        ("publish_time", pa.timestamp("us")),
        ("tags", pa.list_(pa.string())),
        ("duration_seconds", pa.int32()),
        ("views", pa.int64()),
        ("likes", pa.int64()),
        ("dislikes", pa.int64()),
        ("comment_count", pa.int64()),
        ("comments_disabled", pa.bool_()),
        ("ratings_disabled", pa.bool_()),
        ("like_ratio", pa.float64()),
        ("engagement_rate", pa.float64()),
        ("source", pa.string()),
        ("ingested_at", pa.timestamp("us")),
    ]
)


def silver_table(n=20, source="api", ingested=FRESH_INGEST, **overrides):
    """A valid Silver table of n rows. `overrides` replace whole columns (list of n values)."""
    naive = ingested.replace(tzinfo=None)
    columns = {
        "video_id": [f"vid{i}" for i in range(n)],
        "trending_date": [date(2026, 10, 20)] * n,
        "title": [f"Title {i}" for i in range(n)],
        "channel_id": ["chan"] * n,
        "channel_title": ["Channel"] * n,
        "category_id": [10] * n,
        "publish_time": [datetime(2026, 10, 19, 8, 0)] * n,
        "tags": [["a", "b"]] * n,
        "duration_seconds": [200] * n,
        "views": [1000 + i for i in range(n)],
        "likes": [50] * n,
        "dislikes": [0] * n,
        "comment_count": [5] * n,
        "comments_disabled": [False] * n,
        "ratings_disabled": [False] * n,
        "like_ratio": [0.05] * n,
        "engagement_rate": [0.055] * n,
        "source": [source] * n,
        "ingested_at": [naive] * n,
    }
    columns.update(overrides)
    return pa.table(columns, schema=SCHEMA)


def parquet_bytes(table):
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@pytest.fixture
def handler(monkeypatch):
    spec = importlib.util.spec_from_file_location("data_quality_handler", HANDLER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclasses look the module up in sys.modules while the class bodies run
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "_utc_now", lambda: FIXED_NOW)
    return module


@pytest.fixture
def aws(monkeypatch):
    """Silver bucket + SNS topic wired to an SQS queue so alerts can be read back."""
    for name in (
        "DQ_MIN_ROW_COUNT",
        "DQ_MAX_NULL_PERCENT",
        "DQ_MAX_AGE_HOURS",
        "DQ_MAX_RATIO_VIOLATION_PERCENT",
        "DQ_REQUIRE_LIVE_DATA",
    ):
        monkeypatch.delenv(name, raising=False)
    with mock_aws():
        s3 = boto3.client("s3")
        s3.create_bucket(Bucket=SILVER)
        sns = boto3.client("sns")
        sqs = boto3.client("sqs")
        topic = sns.create_topic(Name="alerts")["TopicArn"]
        queue_url = sqs.create_queue(QueueName="alerts-q")["QueueUrl"]
        queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        sns.subscribe(
            TopicArn=topic,
            Protocol="sqs",
            Endpoint=queue_arn,
            Attributes={"RawMessageDelivery": "true"},
        )
        monkeypatch.setenv("SILVER_BUCKET", SILVER)
        monkeypatch.setenv("SNS_ALERT_TOPIC_ARN", topic)
        monkeypatch.setenv("ENVIRONMENT", "test")

        class Env:
            def put(self, region, table, name="part-0.parquet"):
                key = f"youtube/trending_videos/region={region}/{name}"
                s3.put_object(Bucket=SILVER, Key=key, Body=parquet_bytes(table))
                return key

            def put_raw(self, region, body, name="part-0.parquet"):
                key = f"youtube/trending_videos/region={region}/{name}"
                s3.put_object(Bucket=SILVER, Key=key, Body=body)
                return key

            def alerts(self):
                resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
                return resp.get("Messages", [])

            topic_arn = topic

        yield Env()


def run(handler, event=None):
    return handler.lambda_handler(event or {}, None)


def check(result, name):
    return next(c for c in result["checks"] if c["name"] == name)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------
def test_good_data_passes_without_alert(handler, aws):
    aws.put("US", silver_table())
    aws.put("CA", silver_table())

    result = run(handler)

    assert result["passed"] is True
    assert result["status"] == "PASSED"
    assert result["failed_checks"] == []
    assert result["rows_checked"] == 40
    assert result["regions"] == ["CA", "US"]
    assert result["alert_sent"] is None
    assert aws.alerts() == []


def test_multiple_files_in_one_region_are_aggregated(handler, aws):
    aws.put("US", silver_table(6), name="a.parquet")
    aws.put("US", silver_table(6), name="b.parquet")

    result = run(handler)

    assert result["passed"] is True
    assert result["rows_checked"] == 12


def test_non_parquet_objects_are_ignored(handler, aws):
    aws.put("US", silver_table())
    aws.put_raw("US", b"junk", name="_SUCCESS")

    assert run(handler)["passed"] is True


# ---------------------------------------------------------------------------
# Row count / files
# ---------------------------------------------------------------------------
def test_empty_bucket_fails(handler, aws):
    result = run(handler)

    assert result["passed"] is False
    assert "row_count" in result["failed_checks"]
    assert "no rows" in check(result, "row_count")["detail"]


def test_too_few_rows_fails(handler, aws):
    aws.put("US", silver_table(20))
    aws.put("CA", silver_table(3))

    result = run(handler)

    assert result["failed_checks"] == ["row_count"]
    assert "CA: 3 rows" in check(result, "row_count")["detail"]


def test_unreadable_file_fails(handler, aws):
    aws.put("US", silver_table())
    aws.put_raw("CA", b"this is not parquet")

    result = run(handler)

    assert "files_readable" in result["failed_checks"]
    assert "region=CA" in check(result, "files_readable")["detail"]
    assert result["regions"] == ["US"]


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
def test_missing_column_fails(handler, aws):
    aws.put("US", silver_table().drop_columns(["likes"]))

    result = run(handler)

    assert "schema" in result["failed_checks"]
    assert "missing column 'likes'" in check(result, "schema")["detail"]


def test_wrong_type_fails(handler, aws):
    table = silver_table()
    table = table.set_column(
        table.schema.get_field_index("views"), "views", pa.array(["x"] * 20, pa.string())
    )
    aws.put("US", table)

    result = run(handler)

    assert "schema" in result["failed_checks"]
    assert "column 'views' is string, expected integer" in check(result, "schema")["detail"]
    # a wrongly typed column must not crash the range checks
    assert result["status"] == "FAILED"


# ---------------------------------------------------------------------------
# Nulls
# ---------------------------------------------------------------------------
def test_null_rate_above_threshold_fails(handler, aws):
    titles = [None] * 4 + [f"t{i}" for i in range(16)]  # 20% null
    aws.put("US", silver_table(title=titles))

    result = run(handler)

    assert result["failed_checks"] == ["null_rate"]
    assert "US.title: 20.0% null" in check(result, "null_rate")["detail"]


def test_null_rate_exactly_at_threshold_passes(handler, aws):
    titles = [None] + [f"t{i}" for i in range(19)]  # exactly 5%
    aws.put("US", silver_table(title=titles))

    assert run(handler)["passed"] is True


def test_nulls_in_non_critical_column_are_ignored(handler, aws):
    aws.put("US", silver_table(likes=[None] * 20))

    assert run(handler)["passed"] is True


# ---------------------------------------------------------------------------
# Value ranges
# ---------------------------------------------------------------------------
def test_negative_counts_fail(handler, aws):
    views = [-5] + [100] * 19
    aws.put("US", silver_table(views=views))

    result = run(handler)

    assert result["failed_checks"] == ["value_ranges"]
    assert "US.views: 1 negative" in check(result, "value_ranges")["detail"]


def test_absurd_view_count_fails(handler, aws):
    views = [10**11] + [100] * 19
    aws.put("US", silver_table(views=views))

    assert "value_ranges" in run(handler)["failed_checks"]


def test_like_ratio_violations_above_limit_fail(handler, aws):
    ratios = [1.5, 2.0] + [0.05] * 18  # 10% outside 0..1
    aws.put("US", silver_table(like_ratio=ratios))

    result = run(handler)

    assert result["failed_checks"] == ["value_ranges"]
    assert "like_ratio: 10.0%" in check(result, "value_ranges")["detail"]


def test_like_ratio_nulls_are_ignored(handler, aws):
    aws.put("US", silver_table(like_ratio=[None] * 20))

    assert run(handler)["passed"] is True


def test_future_trending_date_fails(handler, aws):
    dates = [date(2027, 1, 1)] + [date(2026, 10, 20)] * 19
    aws.put("US", silver_table(trending_date=dates))

    result = run(handler)

    assert result["failed_checks"] == ["value_ranges"]
    assert "trending_date" in check(result, "value_ranges")["detail"]


def test_impossibly_old_date_fails(handler, aws):
    dates = [date(1999, 1, 1)] + [date(2026, 10, 20)] * 19
    aws.put("US", silver_table(trending_date=dates))

    assert "value_ranges" in run(handler)["failed_checks"]


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------
def test_stale_live_data_fails(handler, aws):
    aws.put("US", silver_table(ingested=FIXED_NOW - timedelta(hours=100)))

    result = run(handler)

    assert result["failed_checks"] == ["freshness"]
    assert "100.0h old" in check(result, "freshness")["detail"]


def test_kaggle_history_does_not_count_as_fresh(handler, aws):
    aws.put("US", silver_table(source="kaggle", ingested=FIXED_NOW))

    result = run(handler)

    assert result["failed_checks"] == ["freshness"]
    assert "no live API rows" in check(result, "freshness")["detail"]


def test_kaggle_only_passes_when_live_data_not_required(handler, aws):
    aws.put("US", silver_table(source="kaggle"))

    result = run(handler, {"thresholds": {"require_live_data": False}})

    assert result["passed"] is True
    assert "skipped" in check(result, "freshness")["detail"]


def test_old_kaggle_rows_next_to_fresh_api_rows_pass(handler, aws):
    aws.put(
        "US",
        silver_table(source="kaggle", ingested=datetime(2018, 1, 1, tzinfo=UTC)),
        name="k.parquet",
    )
    aws.put("US", silver_table(source="api"), name="a.parquet")

    assert run(handler)["passed"] is True


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
def test_event_override_can_force_failure(handler, aws):
    aws.put("US", silver_table(20))

    result = run(handler, {"thresholds": {"min_rows": 1000}})

    assert result["failed_checks"] == ["row_count"]


def test_env_thresholds_are_used(handler, aws, monkeypatch):
    monkeypatch.setenv("DQ_MAX_AGE_HOURS", "1")
    aws.put("US", silver_table())  # 6h old

    assert run(handler)["failed_checks"] == ["freshness"]


def test_event_override_beats_env(handler, aws, monkeypatch):
    monkeypatch.setenv("DQ_MAX_AGE_HOURS", "1")
    aws.put("US", silver_table())

    assert run(handler, {"thresholds": {"max_age_hours": 24}})["passed"] is True


@pytest.mark.parametrize(
    "thresholds",
    [
        {"nonsense": 1},
        {"min_rows": "many"},
        {"max_null_percent": 150},
        {"max_age_hours": 0},
        {"require_live_data": "maybe"},
    ],
)
def test_invalid_thresholds_raise(handler, aws, thresholds):
    with pytest.raises(handler.ConfigError):
        run(handler, {"thresholds": thresholds})


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------
def test_failure_sends_alert_with_details(handler, aws):
    aws.put("US", silver_table(3))

    result = run(handler)
    messages = aws.alerts()

    assert result["alert_sent"] is True
    assert len(messages) == 1
    body = messages[0]["Body"]
    assert "row_count" in body
    assert "US: 3 rows" in body
    assert "Passed checks:" in body


def test_alert_subject_is_short_and_descriptive(handler):
    summary = handler.Summary()
    results = [
        handler.CheckResult("row_count", False, "x"),
        handler.CheckResult("schema", True, "ok"),
    ]

    subject, message = handler.format_alert("dev", results, summary, FIXED_NOW)

    assert subject == "[dev] YouTube pipeline: data quality FAILED (1 of 2 checks)"
    assert len(subject) <= 100
    assert "schema" in message


def test_alert_failure_does_not_crash(handler, aws, monkeypatch):
    monkeypatch.setenv("SNS_ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789012:missing")
    aws.put("US", silver_table(3))

    result = run(handler)

    assert result["passed"] is False
    assert result["alert_sent"] is False


def test_missing_topic_env_is_reported(handler, aws, monkeypatch):
    monkeypatch.delenv("SNS_ALERT_TOPIC_ARN")
    aws.put("US", silver_table(3))

    result = run(handler)

    assert result["passed"] is False
    assert result["alert_sent"] is None


def test_result_is_json_serialisable(handler, aws):
    aws.put("US", silver_table(3))

    json.dumps(run(handler))
