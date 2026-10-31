"""Tests for the bronze_to_silver Glue job, run on a real local Spark.

Needs Java 17+ and `pip install pyspark==3.5.4`. If either is missing, every test in
this file is skipped (the rest of the suite still runs).
"""

import csv
import importlib.util
import json
import os
import shutil
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("Java is not installed", allow_module_level=True)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

JOB_PATH = Path(__file__).resolve().parents[2] / "src" / "glue_jobs" / "bronze_to_silver.py"


# --------------------------------------------------------------------------
# Fixtures and helpers
# --------------------------------------------------------------------------
@pytest.fixture(scope="session")
def job():
    spec = importlib.util.spec_from_file_location("bronze_to_silver_job", JOB_PATH)
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
        .appName("bronze-to-silver-tests")
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
    """Empty local stand-ins for the Bronze and Silver buckets."""
    bronze, silver = tmp_path / "bronze", tmp_path / "silver"
    bronze.mkdir()
    silver.mkdir()
    return bronze, silver


def api_item(
    video_id: str,
    views: str = "1000",
    likes: str | None = "100",
    comments: str | None = "50",
    **overrides,
):
    """One item shaped like a real YouTube videos.list response."""
    snippet = {
        "publishedAt": "2026-09-25T09:17:35Z",
        "channelId": "UC44awJ49tZNNMliYVPQDdmA",
        "title": f"Title {video_id}",
        "channelTitle": "Some Channel",
        "categoryId": "10",
        "liveBroadcastContent": "none",
        "localized": {"title": "x", "description": "ignored"},
    }
    statistics = {"viewCount": views, "likeCount": likes, "favoriteCount": "0"}
    if comments is not None:
        statistics["commentCount"] = comments
    if likes is None:
        del statistics["likeCount"]
    item = {
        "kind": "youtube#video",
        "id": video_id,
        "snippet": snippet,
        "contentDetails": {"duration": "PT4M2S", "definition": "hd"},
        "statistics": statistics,
    }
    item["snippet"].update(overrides.pop("snippet", {}))
    item.update(overrides)
    return item


def write_api(bronze, region, date, hour, items):
    folder = bronze / "youtube" / "raw_statistics" / f"region={region}" / f"date={date}"
    folder = folder / f"hour={hour}"
    folder.mkdir(parents=True, exist_ok=True)
    payload = {"kind": "youtube#videoListResponse", "items": items}
    (folder / f"trending_{date}_{hour}.json").write_text(json.dumps(payload))  # one line


def kaggle_row(video_id, **overrides):
    row = {
        "video_id": video_id,
        "trending_date": "17.14.11",
        "title": f"Title {video_id}",
        "channel_title": "Kaggle Channel",
        "category_id": "22",
        "publish_time": "2017-11-13T17:13:01.000Z",
        "tags": "tag one|tag two",
        "views": "1000",
        "likes": "100",
        "dislikes": "10",
        "comment_count": "50",
        "thumbnail_link": "https://i.ytimg.com/vi/x/default.jpg",
        "comments_disabled": "False",
        "ratings_disabled": "False",
        "video_error_or_removed": "False",
        "description": "plain description",
    }
    row.update(overrides)
    return row


def write_kaggle(bronze, region, rows):
    folder = bronze / "youtube" / "kaggle_raw" / f"region={region}"
    folder.mkdir(parents=True, exist_ok=True)
    with open(folder / f"{region}videos.csv", "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(kaggle_row("x")), quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)


def run_job(job, spark, lake):
    bronze, silver = lake
    return job.run(spark, str(bronze), str(silver))


def read_silver(spark, lake):
    """Silver rows as dicts. Timestamps and dates become strings (session time zone is UTC)."""
    df = spark.read.parquet(str(lake[1] / "youtube" / "trending_videos"))
    for field in df.schema.fields:
        if field.dataType.simpleString() in ("timestamp", "date"):
            df = df.withColumn(field.name, F.col(field.name).cast("string"))
    return [row.asDict() for row in df.orderBy("region", "video_id", "trending_date").collect()]


def one(rows, video_id, region=None):
    match = [r for r in rows if r["video_id"] == video_id and region in (None, r["region"])]
    assert len(match) == 1, f"expected one row for {video_id}, got {len(match)}"
    return match[0]


# --------------------------------------------------------------------------
# Column helpers
# --------------------------------------------------------------------------
def test_iso_duration_parsing(job, spark):
    values = ["PT4M2S", "PT1H2M3S", "PT45S", "PT2H", "P0D", "P1DT1H", "garbage", None]
    df = spark.createDataFrame([(v,) for v in values], "d string")
    result = [r[0] for r in df.select(job.parse_iso_duration(F.col("d"))).collect()]
    assert result == [242, 3723, 45, 7200, 0, 90000, None, None]


def test_kaggle_tags_parsing(job, spark):
    values = ["[none]", "", None, "a|b", '"x"|"y z"', '"only"', "a||b"]
    df = spark.createDataFrame([(v,) for v in values], "t string")
    result = [r[0] for r in df.select(job.parse_kaggle_tags(F.col("t"))).collect()]
    assert result == [[], [], [], ["a", "b"], ["x", "y z"], ["only"], ["a", "b"]]


def test_metrics_math_and_null_rules(job, spark):
    data = [
        (1000, 100, 50),  # normal
        (0, 5, 5),  # zero views
        (None, 5, 5),  # unknown views
        (1000, None, 50),  # ratings disabled
        (1000, 100, None),  # comments disabled
    ]
    df = spark.createDataFrame(data, "views long, likes long, comment_count long")
    rows = [r.asDict() for r in job.add_metrics(df).collect()]
    assert (rows[0]["like_ratio"], rows[0]["engagement_rate"]) == (0.1, 0.15)
    assert (rows[1]["like_ratio"], rows[1]["engagement_rate"]) == (None, None)
    assert (rows[2]["like_ratio"], rows[2]["engagement_rate"]) == (None, None)
    assert (rows[3]["like_ratio"], rows[3]["engagement_rate"]) == (None, None)
    assert (rows[4]["like_ratio"], rows[4]["engagement_rate"]) == (0.1, 0.1)


# --------------------------------------------------------------------------
# API source
# --------------------------------------------------------------------------
def test_api_item_is_flattened_and_typed(job, spark, lake):
    write_api(lake[0], "US", "2026-10-06", "23", [api_item("vid1")])

    run_job(job, spark, lake)
    row = one(read_silver(spark, lake), "vid1")

    assert row["region"] == "US"
    assert row["trending_date"] == "2026-10-06"
    assert row["title"] == "Title vid1"
    assert row["channel_id"] == "UC44awJ49tZNNMliYVPQDdmA"
    assert row["category_id"] == 10
    assert row["publish_time"] == "2026-09-25 09:17:35"
    assert row["duration_seconds"] == 242
    assert (row["views"], row["likes"], row["comment_count"]) == (1000, 100, 50)
    assert row["dislikes"] is None
    assert row["source"] == "api"
    assert row["ingested_at"] == "2026-10-06 23:00:00"
    assert row["like_ratio"] == pytest.approx(0.1)
    assert row["engagement_rate"] == pytest.approx(0.15)
    assert row["comments_disabled"] is False
    assert row["ratings_disabled"] is False


def test_api_item_without_tags_gets_empty_list(job, spark, lake):
    # the real item you pasted has no `tags` key at all
    write_api(lake[0], "US", "2026-10-06", "23", [api_item("notags")])
    write_api(lake[0], "GB", "2026-10-06", "23", [api_item("tagged", snippet={"tags": ["a", "b"]})])

    run_job(job, spark, lake)
    rows = read_silver(spark, lake)

    assert one(rows, "notags")["tags"] == []
    assert one(rows, "tagged")["tags"] == ["a", "b"]


def test_api_disabled_flags_are_inferred_from_missing_stats(job, spark, lake):
    items = [
        api_item("no_comments", comments=None),
        api_item("no_likes", likes=None),
    ]
    write_api(lake[0], "US", "2026-10-06", "23", items)

    run_job(job, spark, lake)
    rows = read_silver(spark, lake)

    no_comments = one(rows, "no_comments")
    assert no_comments["comments_disabled"] is True
    assert no_comments["engagement_rate"] == pytest.approx(0.1)  # comments count as 0

    no_likes = one(rows, "no_likes")
    assert no_likes["ratings_disabled"] is True
    assert no_likes["like_ratio"] is None
    assert no_likes["engagement_rate"] is None


def test_four_snapshots_in_a_day_collapse_to_the_latest(job, spark, lake):
    for hour, views in [("00", "1000"), ("06", "1800"), ("12", "2500"), ("18", "3100")]:
        write_api(lake[0], "US", "2026-10-15", hour, [api_item("abc", views=views)])
    write_api(lake[0], "US", "2026-10-16", "00", [api_item("abc", views="4000")])

    run_job(job, spark, lake)
    rows = [r for r in read_silver(spark, lake) if r["video_id"] == "abc"]

    assert [(r["trending_date"], r["views"]) for r in rows] == [
        ("2026-10-15", 3100),
        ("2026-10-16", 4000),
    ]
    assert rows[0]["ingested_at"] == "2026-10-15 18:00:00"


def test_same_video_in_two_regions_stays_separate(job, spark, lake):
    write_api(lake[0], "US", "2026-10-15", "06", [api_item("abc", views="1000")])
    write_api(lake[0], "GB", "2026-10-15", "06", [api_item("abc", views="400")])

    run_job(job, spark, lake)
    rows = read_silver(spark, lake)

    assert one(rows, "abc", "US")["views"] == 1000
    assert one(rows, "abc", "GB")["views"] == 400


# --------------------------------------------------------------------------
# Kaggle source
# --------------------------------------------------------------------------
def test_kaggle_row_is_typed_and_date_is_year_day_month(job, spark, lake):
    write_kaggle(lake[0], "US", [kaggle_row("kvid", tags="[none]")])

    run_job(job, spark, lake)
    row = one(read_silver(spark, lake), "kvid")

    assert row["trending_date"] == "2017-11-14"  # '17.14.11' = 14 Nov 2017
    assert row["region"] == "US"
    assert row["category_id"] == 22
    assert row["publish_time"] == "2017-11-13 17:13:01"
    assert row["tags"] == []
    assert (row["views"], row["likes"], row["dislikes"], row["comment_count"]) == (
        1000,
        100,
        10,
        50,
    )
    assert row["channel_id"] is None
    assert row["duration_seconds"] is None
    assert row["ingested_at"] is None
    assert row["source"] == "kaggle"
    assert row["comments_disabled"] is False
    assert row["engagement_rate"] == pytest.approx(0.15)


def test_kaggle_multiline_quoted_description_does_not_break_parsing(job, spark, lake):
    nasty = 'Line one\nLine "two", with a comma\nLine three'
    rows_in = [kaggle_row("first", description=nasty), kaggle_row("second")]
    write_kaggle(lake[0], "CA", rows_in)

    run_job(job, spark, lake)
    rows = read_silver(spark, lake)

    assert sorted(r["video_id"] for r in rows) == ["first", "second"]
    assert one(rows, "first")["title"] == "Title first"


def test_kaggle_duplicates_keep_highest_views(job, spark, lake):
    rows_in = [kaggle_row("dup", views="100"), kaggle_row("dup", views="500")]
    write_kaggle(lake[0], "US", rows_in)

    run_job(job, spark, lake)
    rows = [r for r in read_silver(spark, lake) if r["video_id"] == "dup"]

    assert len(rows) == 1
    assert rows[0]["views"] == 500


def test_kaggle_broken_rows_are_dropped_and_bad_numbers_become_null(job, spark, lake):
    rows_in = [
        kaggle_row("good"),
        kaggle_row("", title="no id"),
        kaggle_row("bad_date", trending_date="not-a-date"),
        kaggle_row("bad_views", views="lots"),
    ]
    write_kaggle(lake[0], "US", rows_in)

    run_job(job, spark, lake)
    rows = read_silver(spark, lake)

    assert sorted(r["video_id"] for r in rows) == ["bad_views", "good"]
    assert one(rows, "bad_views")["views"] is None
    assert one(rows, "bad_views")["like_ratio"] is None


def test_kaggle_tags_with_quotes_are_cleaned(job, spark, lake):
    write_kaggle(lake[0], "US", [kaggle_row("t", tags='"shantell martin"|"art"')])

    run_job(job, spark, lake)

    assert one(read_silver(spark, lake), "t")["tags"] == ["shantell martin", "art"]


# --------------------------------------------------------------------------
# Whole job
# --------------------------------------------------------------------------
def test_both_sources_are_combined_with_the_expected_schema(job, spark, lake):
    write_kaggle(lake[0], "US", [kaggle_row("old")])
    write_api(lake[0], "US", "2026-10-06", "23", [api_item("new")])

    stats = run_job(job, spark, lake)

    assert stats["rows"] == 2
    assert stats["by_source"] == {"kaggle": 1, "api": 1}
    assert stats["by_region"] == {"US": 2}

    df = spark.read.parquet(str(lake[1] / "youtube" / "trending_videos"))
    assert dict(df.dtypes) == {
        "video_id": "string",
        "trending_date": "date",
        "title": "string",
        "channel_id": "string",
        "channel_title": "string",
        "category_id": "int",
        "publish_time": "timestamp",
        "tags": "array<string>",
        "duration_seconds": "int",
        "views": "bigint",
        "likes": "bigint",
        "dislikes": "bigint",
        "comment_count": "bigint",
        "comments_disabled": "boolean",
        "ratings_disabled": "boolean",
        "like_ratio": "double",
        "engagement_rate": "double",
        "source": "string",
        "ingested_at": "timestamp",
        "region": "string",  # comes from the folder name
    }


def test_output_is_partitioned_by_region_and_snappy_compressed(job, spark, lake):
    write_kaggle(lake[0], "US", [kaggle_row("a")])
    write_kaggle(lake[0], "GB", [kaggle_row("b")])

    run_job(job, spark, lake)

    root = lake[1] / "youtube" / "trending_videos"
    assert sorted(p.name for p in root.glob("region=*")) == ["region=GB", "region=US"]
    parquet_files = list(root.glob("region=US/*.parquet"))
    assert parquet_files and all(".snappy." in p.name for p in parquet_files)


def test_rerun_is_idempotent_and_leaves_other_regions_alone(job, spark, lake):
    write_kaggle(lake[0], "US", [kaggle_row("a")])
    # a region this job does not produce this time, already sitting in Silver
    existing = spark.createDataFrame([("keepme", "ZZ")], "video_id string, region string")
    existing.write.partitionBy("region").parquet(str(lake[1] / "youtube" / "trending_videos"))

    run_job(job, spark, lake)
    run_job(job, spark, lake)

    rows = spark.read.parquet(str(lake[1] / "youtube" / "trending_videos"))
    counts = {r["region"]: r["count"] for r in rows.groupBy("region").count().collect()}
    assert counts == {"US": 1, "ZZ": 1}


def test_runs_with_only_kaggle_or_only_api(job, spark, lake, tmp_path):
    write_kaggle(lake[0], "US", [kaggle_row("only_kaggle")])
    assert run_job(job, spark, lake)["by_source"] == {"kaggle": 1}

    other = (tmp_path / "b2", tmp_path / "s2")
    for folder in other:
        folder.mkdir()
    write_api(other[0], "US", "2026-10-06", "23", [api_item("only_api")])
    assert run_job(job, spark, other)["by_source"] == {"api": 1}


def test_empty_bronze_fails_loudly(job, spark, lake):
    with pytest.raises(RuntimeError, match="empty"):
        run_job(job, spark, lake)
