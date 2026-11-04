"""Tests for the silver_to_gold Glue job, run on a real local Spark.

Needs Java 17+ and `pip install pyspark==3.5.4`. If either is missing, every test in
this file is skipped (the rest of the suite still runs).
"""

import importlib.util
import os
import shutil
import sys
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is not installed", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

pytestmark = pytest.mark.spark

JOB_PATH = Path(__file__).resolve().parents[2] / "src" / "glue_jobs" / "silver_to_gold.py"

SILVER_SCHEMA = T.StructType(
    [
        T.StructField("video_id", T.StringType()),
        T.StructField("region", T.StringType()),
        T.StructField("trending_date", T.DateType()),
        T.StructField("title", T.StringType()),
        T.StructField("channel_id", T.StringType()),
        T.StructField("channel_title", T.StringType()),
        T.StructField("category_id", T.IntegerType()),
        T.StructField("views", T.LongType()),
        T.StructField("likes", T.LongType()),
        T.StructField("like_ratio", T.DoubleType()),
        T.StructField("engagement_rate", T.DoubleType()),
        T.StructField("source", T.StringType()),
    ]
)


# --------------------------------------------------------------------------
# Fixtures and helpers
# --------------------------------------------------------------------------
@pytest.fixture(scope="session")
def job():
    spec = importlib.util.spec_from_file_location("silver_to_gold_job", JOB_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def spark(job):
    # Spark workers must use the same Python as this test process (the active .venv)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    session = (
        SparkSession.Builder()
        .master("local[2]")
        .appName("silver-to-gold-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    job.configure_session(session)
    yield session
    session.stop()


@pytest.fixture
def lake(tmp_path):
    silver, gold = tmp_path / "silver", tmp_path / "gold"
    silver.mkdir()
    gold.mkdir()
    return silver, gold


def row(
    video_id,
    day,
    views,
    region="US",
    title=None,
    channel: str | None = "Chan A",
    category=10,
    like_ratio=0.05,
    engagement=0.06,
):
    return (
        video_id,
        region,
        date(2026, 10, day),
        title or f"Title {video_id}",
        None,
        channel,
        category,
        views,
        None,
        like_ratio,
        engagement,
        "api",
    )


def write_silver(spark, silver_dir, rows):
    df = spark.createDataFrame(rows, SILVER_SCHEMA)
    (
        df.write.mode("overwrite")
        .partitionBy("region")
        .parquet(f"{silver_dir}/youtube/trending_videos")
    )


def write_categories(spark, silver_dir, rows):
    df = spark.createDataFrame(rows, ["category_id", "category_name", "assignable", "region"])
    df.write.mode("overwrite").partitionBy("region").parquet(f"{silver_dir}/youtube/categories")


def build(job, spark, silver_dir, rows, categories=None):
    write_silver(spark, silver_dir, rows)
    if categories is not None:
        write_categories(spark, silver_dir, categories)
    silver = job.read_silver(spark, str(silver_dir))
    cats = job.read_categories(spark, str(silver_dir))
    return job.build_gold(silver, cats)


def only(df, **where):
    rows = [r.asDict() for r in df.collect()]
    for key, value in where.items():
        rows = [r for r in rows if r[key] == value]
    assert len(rows) == 1, f"expected exactly one row for {where}, got {len(rows)}"
    return rows[0]


# --------------------------------------------------------------------------
# video_trending_history
# --------------------------------------------------------------------------
def test_history_tracks_first_last_peak(job, spark, lake):
    silver, _ = lake
    gold = build(
        job,
        spark,
        silver,
        [row("v1", 1, 100), row("v1", 2, 500), row("v1", 3, 300), row("v2", 1, 50)],
    )

    v1 = only(gold["video_trending_history"], video_id="v1")

    assert v1["first_trending_date"] == date(2026, 10, 1)
    assert v1["last_trending_date"] == date(2026, 10, 3)
    assert v1["days_on_trending"] == 3
    assert v1["calendar_span_days"] == 3
    assert v1["peak_views"] == 500
    assert v1["peak_views_date"] == date(2026, 10, 2)
    assert v1["first_views"] == 100
    assert v1["last_views"] == 300


def test_history_counts_gaps_in_span_but_not_in_days(job, spark, lake):
    silver, _ = lake
    gold = build(job, spark, silver, [row("v1", 1, 10), row("v1", 6, 20)])

    v1 = only(gold["video_trending_history"], video_id="v1")

    assert v1["days_on_trending"] == 2
    assert v1["calendar_span_days"] == 6


def test_history_uses_latest_title_and_channel(job, spark, lake):
    silver, _ = lake
    gold = build(
        job,
        spark,
        silver,
        [
            row("v1", 1, 10, title="Old title", channel="Old"),
            row("v1", 2, 20, title="New title", channel="New"),
        ],
    )

    v1 = only(gold["video_trending_history"], video_id="v1")

    assert v1["title"] == "New title"
    assert v1["channel_title"] == "New"


def test_same_video_in_two_regions_is_two_rows(job, spark, lake):
    silver, _ = lake
    gold = build(job, spark, silver, [row("v1", 1, 10, region="US"), row("v1", 1, 20, region="GB")])

    history = gold["video_trending_history"]

    assert history.count() == 2
    assert only(history, region="GB")["peak_views"] == 20


# --------------------------------------------------------------------------
# trending_analytics
# --------------------------------------------------------------------------
def test_daily_analytics(job, spark, lake):
    silver, _ = lake
    gold = build(
        job,
        spark,
        silver,
        [
            row("v1", 1, 100, channel="A", category=10),
            row("v2", 1, 300, channel="B", category=20),
            row("v3", 1, 200, channel="B", category=20),
            row("v1", 2, 150, channel="A", category=10),
        ],
    )

    day1 = only(gold["trending_analytics"], trending_date=date(2026, 10, 1))

    assert day1["videos"] == 3
    assert day1["distinct_channels"] == 2
    assert day1["distinct_categories"] == 2
    assert day1["total_views"] == 600
    assert day1["avg_views"] == pytest.approx(200.0)
    assert day1["max_views"] == 300
    assert day1["top_video_id"] == "v2"
    assert day1["top_video_title"] == "Title v2"
    assert only(gold["trending_analytics"], trending_date=date(2026, 10, 2))["videos"] == 1


def test_daily_analytics_ignores_null_views_in_totals(job, spark, lake):
    silver, _ = lake
    gold = build(job, spark, silver, [row("v1", 1, None), row("v2", 1, 40)])

    day = only(gold["trending_analytics"], trending_date=date(2026, 10, 1))

    assert day["videos"] == 2
    assert day["total_views"] == 40


# --------------------------------------------------------------------------
# channel_analytics
# --------------------------------------------------------------------------
def test_channel_totals_use_peak_views_not_daily_sums(job, spark, lake):
    silver, _ = lake
    # one video, views grow 100 -> 200 -> 300 over three days
    gold = build(job, spark, silver, [row("v1", 1, 100), row("v1", 2, 200), row("v1", 3, 300)])

    chan = only(gold["channel_analytics"], channel_title="Chan A")

    assert chan["videos_trended"] == 1
    assert chan["total_trending_days"] == 3
    assert chan["total_peak_views"] == 300  # NOT 600
    assert chan["avg_days_per_video"] == pytest.approx(3.0)


def test_channel_aggregates_several_videos(job, spark, lake):
    silver, _ = lake
    gold = build(
        job,
        spark,
        silver,
        [
            row("v1", 1, 100),
            row("v1", 2, 400),
            row("v2", 5, 1000, title="Best one"),
        ],
    )

    chan = only(gold["channel_analytics"], channel_title="Chan A")

    assert chan["videos_trended"] == 2
    assert chan["total_trending_days"] == 3
    assert chan["total_peak_views"] == 1400
    assert chan["max_peak_views"] == 1000
    assert chan["first_trending_date"] == date(2026, 10, 1)
    assert chan["last_trending_date"] == date(2026, 10, 5)
    assert chan["top_video_title"] == "Best one"


def test_channels_without_a_name_are_excluded(job, spark, lake):
    silver, _ = lake
    gold = build(
        job,
        spark,
        silver,
        [row("v1", 1, 10, channel=None), row("v2", 1, 10, channel="  "), row("v3", 1, 10)],
    )

    channels = gold["channel_analytics"]

    assert [r["channel_title"] for r in channels.collect()] == ["Chan A"]


# --------------------------------------------------------------------------
# category_analytics
# --------------------------------------------------------------------------
def test_category_names_are_joined_and_unknown_falls_back(job, spark, lake):
    silver, _ = lake
    gold = build(
        job,
        spark,
        silver,
        [row("v1", 1, 10, category=10), row("v2", 1, 10, category=99)],
        categories=[(10, "Music", True, "US")],
    )

    cats = gold["category_analytics"]

    assert only(cats, category_id=10)["category_name"] == "Music"
    assert only(cats, category_id=99)["category_name"] == "Unknown (99)"


def test_category_share_adds_up_per_region(job, spark, lake):
    silver, _ = lake
    gold = build(
        job,
        spark,
        silver,
        [
            row("v1", 1, 10, category=10),
            row("v1", 2, 10, category=10),
            row("v1", 3, 10, category=10),
            row("v2", 1, 10, category=20),
            row("g1", 1, 10, category=10, region="GB"),
        ],
        categories=[(10, "Music", True, "US"), (20, "Gaming", True, "US")],
    )

    cats = gold["category_analytics"]

    assert only(cats, region="US", category_id=10)["share_of_trending_days"] == pytest.approx(0.75)
    assert only(cats, region="US", category_id=20)["share_of_trending_days"] == pytest.approx(0.25)
    assert only(cats, region="GB", category_id=10)["share_of_trending_days"] == pytest.approx(1.0)


def test_category_names_are_per_region(job, spark, lake):
    silver, _ = lake
    gold = build(
        job,
        spark,
        silver,
        [row("v1", 1, 10, region="US", category=10), row("v2", 1, 10, region="GB", category=10)],
        categories=[(10, "Music", True, "US"), (10, "Musique GB", True, "GB")],
    )

    cats = gold["category_analytics"]

    assert only(cats, region="GB")["category_name"] == "Musique GB"
    assert only(cats, region="US")["category_name"] == "Music"


# --------------------------------------------------------------------------
# Schemas
# --------------------------------------------------------------------------
def test_every_table_matches_its_declared_schema(job, spark, lake):
    silver, _ = lake
    gold = build(
        job,
        spark,
        silver,
        [row("v1", 1, 100), row("v1", 2, 200)],
        categories=[(10, "Music", True, "US")],
    )

    for name, df in gold.items():
        expected = [("region", "string"), *job.GOLD_SCHEMAS[name]]
        actual = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        assert actual == expected, name


# --------------------------------------------------------------------------
# End to end: run()
# --------------------------------------------------------------------------
def test_run_writes_all_four_tables_partitioned_by_region(job, spark, lake):
    silver, gold = lake
    write_silver(
        spark,
        silver,
        [row("v1", 1, 100), row("v1", 2, 200), row("g1", 1, 50, region="GB")],
    )
    write_categories(spark, silver, [(10, "Music", True, "US"), (10, "Music", True, "GB")])

    stats = job.run(spark, str(silver), str(gold))

    assert stats["tables"] == {
        "video_trending_history": 2,
        "trending_analytics": 3,
        "channel_analytics": 2,
        "category_analytics": 2,
    }
    for table in stats["tables"]:
        assert (gold / "youtube" / table / "region=US").is_dir()
        assert (gold / "youtube" / table / "region=GB").is_dir()


def test_run_is_idempotent(job, spark, lake):
    silver, gold = lake
    write_silver(spark, silver, [row("v1", 1, 100), row("v1", 2, 200)])

    first = job.run(spark, str(silver), str(gold))
    second = job.run(spark, str(silver), str(gold))

    assert first == second
    history = spark.read.parquet(str(gold / "youtube" / "video_trending_history"))
    assert history.count() == 1


def test_run_works_without_categories(job, spark, lake):
    silver, gold = lake
    write_silver(spark, silver, [row("v1", 1, 100)])

    job.run(spark, str(silver), str(gold))

    cats = spark.read.parquet(str(gold / "youtube" / "category_analytics"))
    assert cats.collect()[0]["category_name"] == "Unknown (10)"


def test_run_leaves_regions_it_did_not_see_untouched(job, spark, lake):
    silver, gold = lake
    write_silver(spark, silver, [row("v1", 1, 100)])
    job.run(spark, str(silver), str(gold))
    leftover = gold / "youtube" / "channel_analytics" / "region=ZZ"
    leftover.mkdir()
    (leftover / "keep.txt").write_text("x")

    job.run(spark, str(silver), str(gold))

    assert (leftover / "keep.txt").exists()


def test_run_refuses_when_silver_is_missing(job, spark, lake):
    silver, gold = lake

    with pytest.raises(RuntimeError, match="No Silver"):
        job.run(spark, str(silver), str(gold))
