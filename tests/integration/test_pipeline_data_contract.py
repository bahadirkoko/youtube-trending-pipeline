"""Raw API files -> Silver job -> quality gate -> Gold job, on real Spark.

The unit tests check each stage with hand-made input. This checks the stages against
each other: the Silver job's real output must satisfy the real quality gate, and the
Gold job must read that same output and agree with it on row counts.

Needs Java 17+ and pyspark; skipped otherwise.
"""

import importlib.util
import json
import os
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is not installed", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402

pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
SILVER_JOB = ROOT / "src" / "glue_jobs" / "bronze_to_silver.py"
GOLD_JOB = ROOT / "src" / "glue_jobs" / "silver_to_gold.py"
QUALITY_GATE = ROOT / "src" / "lambdas" / "data_quality" / "handler.py"

SILVER_BUCKET = "contract-silver"
# The API files below are stamped date=2026-10-20 hour=06; the gate runs six hours later.
GATE_TIME = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
ROWS_PER_REGION = {"US": 12, "GB": 11}


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses need the module registered while it loads
    spec.loader.exec_module(module)
    return module


def api_file(region: str, count: int) -> dict:
    """What the ingestion Lambda stores: a YouTube videos.list response."""
    return {
        "items": [
            {
                "id": f"{region}vid{i:02d}",
                "snippet": {
                    "publishedAt": "2026-10-19T08:00:00Z",
                    "channelId": f"chan{i}",
                    "title": f"{region} video {i}",
                    "channelTitle": f"{region} channel {i}",
                    "categoryId": "10",
                    "tags": ["a", "b"],
                },
                "contentDetails": {"duration": "PT4M13S"},
                "statistics": {
                    "viewCount": str(1000 + i * 10),
                    "likeCount": "100",
                    "commentCount": "10",
                },
            }
            for i in range(count)
        ]
    }


@pytest.fixture(scope="module")
def spark():
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.Builder()
        .master("local[2]")
        .appName("pipeline-contract-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def lake(spark, tmp_path_factory):
    """Run the real Silver and Gold jobs once on local folders standing in for the buckets."""
    root = tmp_path_factory.mktemp("lake")
    bronze, silver, gold = root / "bronze", root / "silver", root / "gold"
    for region, count in ROWS_PER_REGION.items():
        folder = bronze / "youtube" / "raw_statistics" / f"region={region}"
        folder = folder / "date=2026-10-20" / "hour=06"
        folder.mkdir(parents=True)
        (folder / "trending_20261020T06.json").write_text(json.dumps(api_file(region, count)))

    silver_job = load(SILVER_JOB, "contract_bronze_to_silver")
    gold_job = load(GOLD_JOB, "contract_silver_to_gold")
    silver_stats = silver_job.run(spark, str(bronze), str(silver))
    gold_stats = gold_job.run(spark, str(silver), str(gold))
    return {"silver": silver, "gold": gold, "silver_stats": silver_stats, "gold_stats": gold_stats}


@pytest.fixture
def gate(monkeypatch):
    """The real quality gate, reading from a fake Silver bucket."""
    module = load(QUALITY_GATE, "contract_quality_gate")
    monkeypatch.setattr(module, "_utc_now", lambda: GATE_TIME)
    monkeypatch.delenv("SNS_ALERT_TOPIC_ARN", raising=False)
    monkeypatch.setenv("SILVER_BUCKET", SILVER_BUCKET)
    monkeypatch.setenv("ENVIRONMENT", "contract-test")
    with mock_aws():
        boto3.client("s3").create_bucket(Bucket=SILVER_BUCKET)
        yield module


def upload_silver(silver: Path) -> int:
    s3 = boto3.client("s3")
    base = silver / "youtube" / "trending_videos"
    uploaded = 0
    for file in base.rglob("*.parquet"):
        key = f"youtube/trending_videos/{file.relative_to(base).as_posix()}"
        s3.put_object(Bucket=SILVER_BUCKET, Key=key, Body=file.read_bytes())
        uploaded += 1
    return uploaded


def test_silver_job_wrote_every_row(lake):
    stats = lake["silver_stats"]

    assert stats["rows"] == sum(ROWS_PER_REGION.values())
    assert stats["by_region"] == ROWS_PER_REGION
    assert stats["by_source"] == {"api": 23}


def test_silver_output_passes_the_quality_gate(lake, gate):
    assert upload_silver(lake["silver"]) >= 2

    result = gate.lambda_handler({}, None)

    failed = [c for c in result["checks"] if not c["passed"]]
    assert failed == []
    assert result["passed"] is True
    assert result["rows_checked"] == 23
    assert result["regions"] == ["GB", "US"]


def test_gold_agrees_with_silver_on_row_counts(lake, spark):
    gold = lake["gold"] / "youtube"

    daily = spark.read.parquet(str(gold / "trending_analytics"))
    history = spark.read.parquet(str(gold / "video_trending_history"))

    assert daily.agg({"videos": "sum"}).collect()[0][0] == 23
    assert history.agg({"days_on_trending": "sum"}).collect()[0][0] == 23
    assert history.count() == 23


def test_gold_tables_all_exist_with_the_expected_row_counts(lake):
    assert lake["gold_stats"]["tables"] == {
        "video_trending_history": 23,
        "trending_analytics": 2,  # one trending day in each of two regions
        "channel_analytics": 23,  # every video has its own channel
        "category_analytics": 2,  # category 10 in each region
    }
