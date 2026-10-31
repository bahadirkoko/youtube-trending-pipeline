"""Glue job: Bronze -> Silver `trending_videos`.

Reads BOTH Bronze sources and writes one clean table:

  api     s3://<bronze>/youtube/raw_statistics/region=US/date=YYYY-MM-DD/hour=HH/*.json
  kaggle  s3://<bronze>/youtube/kaggle_raw/region=US/USvideos.csv

  ->      s3://<silver>/youtube/trending_videos/region=US/part-*.snappy.parquet

Grain: ONE row per (video_id, region, trending_date). The Lambda snapshots 4 times a
day, so the latest snapshot of the day wins (ties: highest views).

Design notes
  * Plain PySpark, no awsglue imports, so the same code runs on Glue and on a laptop.
  * Every run re-reads all of Bronze and replaces the regions it finds (idempotent).
    Dynamic partition overwrite means regions with no input are left untouched.
  * Statistics arrive as strings (API) or CSV text (Kaggle), so we cast ourselves.
    Anything that cannot be cast becomes NULL instead of failing the job.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from pyspark.errors import AnalysisException
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger("bronze_to_silver")

API_SUBPATH = "youtube/raw_statistics"
KAGGLE_SUBPATH = "youtube/kaggle_raw"
SILVER_SUBPATH = "youtube/trending_videos"

# Final column order. `region` is also the partition folder, so it is not stored in
# the Parquet files themselves.
SILVER_COLUMNS = [
    "video_id",
    "region",
    "trending_date",
    "title",
    "channel_id",
    "channel_title",
    "category_id",
    "publish_time",
    "tags",
    "duration_seconds",
    "views",
    "likes",
    "dislikes",
    "comment_count",
    "comments_disabled",
    "ratings_disabled",
    "like_ratio",
    "engagement_rate",
    "source",
    "ingested_at",
]

# --------------------------------------------------------------------------
# Raw schemas (explicit, so Spark never samples files to guess types)
# --------------------------------------------------------------------------
_S = T.StringType()

API_RAW_SCHEMA = T.StructType(
    [
        T.StructField(
            "items",
            T.ArrayType(
                T.StructType(
                    [
                        T.StructField("id", _S),
                        T.StructField(
                            "snippet",
                            T.StructType(
                                [
                                    T.StructField("publishedAt", _S),
                                    T.StructField("channelId", _S),
                                    T.StructField("title", _S),
                                    T.StructField("channelTitle", _S),
                                    T.StructField("categoryId", _S),
                                    T.StructField("tags", T.ArrayType(_S)),
                                ]
                            ),
                        ),
                        T.StructField(
                            "contentDetails", T.StructType([T.StructField("duration", _S)])
                        ),
                        T.StructField(
                            "statistics",
                            T.StructType(
                                [
                                    T.StructField("viewCount", _S),
                                    T.StructField("likeCount", _S),
                                    T.StructField("commentCount", _S),
                                ]
                            ),
                        ),
                    ]
                )
            ),
        )
    ]
)
# Partition folders (region=, date=, hour=) appear as extra string columns on read.
API_FILE_SCHEMA = T.StructType(
    API_RAW_SCHEMA.fields + [T.StructField(n, _S) for n in ("region", "date", "hour")]
)

KAGGLE_COLUMNS = [
    "video_id",
    "trending_date",
    "title",
    "channel_title",
    "category_id",
    "publish_time",
    "tags",
    "views",
    "likes",
    "dislikes",
    "comment_count",
    "thumbnail_link",
    "comments_disabled",
    "ratings_disabled",
    "video_error_or_removed",
    "description",
]
KAGGLE_RAW_SCHEMA = T.StructType([T.StructField(c, _S) for c in KAGGLE_COLUMNS])
KAGGLE_FILE_SCHEMA = T.StructType(KAGGLE_RAW_SCHEMA.fields + [T.StructField("region", _S)])


# --------------------------------------------------------------------------
# Session setup
# --------------------------------------------------------------------------
def configure_session(spark: SparkSession) -> SparkSession:
    # UTC everywhere so dates/timestamps never shift with the machine's time zone
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # unparsable dates become NULL instead of raising an exception
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "CORRECTED")
    # keep region=US, hour=05 as plain strings (no int/date guessing)
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    # only replace the region folders we write, leave the others alone
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


# --------------------------------------------------------------------------
# Column helpers
# --------------------------------------------------------------------------
_DURATION_PATTERN = r"^P(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$"


def parse_iso_duration(col: Column) -> Column:
    """ISO-8601 duration ('PT4M13S', 'PT1H2M', 'P0D') -> seconds. NULL if unparsable."""

    def part(group: int) -> Column:
        text = F.regexp_extract(col, _DURATION_PATTERN, group)
        return F.when(F.length(text) > 0, text.cast("int")).otherwise(F.lit(0))

    seconds = part(1) * 86400 + part(2) * 3600 + part(3) * 60 + part(4)
    return F.when(col.rlike(_DURATION_PATTERN), seconds)


def parse_kaggle_tags(col: Column) -> Column:
    """'"a"|"b"' -> ['a', 'b'];  '[none]', '' or NULL -> []."""
    empty = F.array().cast("array<string>")
    split = F.split(col, r"\|")
    cleaned = F.transform(split, lambda t: F.trim(F.regexp_replace(t, '^"+|"+$', "")))
    non_empty = F.filter(cleaned, lambda t: t != "")
    return F.when(col.isNull() | (F.trim(col) == "") | (col == "[none]"), empty).otherwise(
        non_empty
    )


def add_metrics(df: DataFrame) -> DataFrame:
    """like_ratio = likes/views, engagement_rate = (likes+comments)/views.

    Both are NULL when views is NULL or 0. Missing likes (ratings disabled) make both
    NULL; missing comments (comments disabled) count as 0.
    """
    views, likes = F.col("views"), F.col("likes")
    comments = F.coalesce(F.col("comment_count"), F.lit(0))
    return df.withColumn("like_ratio", F.when(views > 0, likes / views)).withColumn(
        "engagement_rate", F.when(views > 0, (likes + comments) / views)
    )


# --------------------------------------------------------------------------
# Source-specific transforms (raw -> common Silver columns, without metrics)
# --------------------------------------------------------------------------
def transform_api(raw: DataFrame) -> DataFrame:
    items = raw.select("region", "date", "hour", F.explode("items").alias("item"))
    snippet, stats = F.col("item.snippet"), F.col("item.statistics")
    return items.select(
        F.col("item.id").alias("video_id"),
        F.col("region"),
        F.to_date("date", "yyyy-MM-dd").alias("trending_date"),
        snippet["title"].alias("title"),
        snippet["channelId"].alias("channel_id"),
        snippet["channelTitle"].alias("channel_title"),
        snippet["categoryId"].cast("int").alias("category_id"),
        F.to_timestamp(snippet["publishedAt"]).alias("publish_time"),
        F.coalesce(snippet["tags"], F.array().cast("array<string>")).alias("tags"),
        parse_iso_duration(F.col("item.contentDetails.duration")).alias("duration_seconds"),
        stats["viewCount"].cast("long").alias("views"),
        stats["likeCount"].cast("long").alias("likes"),
        F.lit(None).cast("long").alias("dislikes"),  # YouTube hides dislikes since 2021
        stats["commentCount"].cast("long").alias("comment_count"),
        stats["commentCount"].isNull().alias("comments_disabled"),  # inferred
        stats["likeCount"].isNull().alias("ratings_disabled"),  # inferred
        F.lit("api").alias("source"),
        F.to_timestamp(
            F.concat_ws(" ", F.col("date"), F.lpad(F.col("hour"), 2, "0")), "yyyy-MM-dd HH"
        ).alias("ingested_at"),
    )


def transform_kaggle(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("video_id"),
        F.col("region"),
        # Kaggle dates are yy.dd.MM: '17.14.11' means 14 November 2017
        F.to_date("trending_date", "yy.dd.MM").alias("trending_date"),
        F.col("title"),
        F.lit(None).cast("string").alias("channel_id"),
        F.col("channel_title"),
        F.col("category_id").cast("int").alias("category_id"),
        F.to_timestamp("publish_time").alias("publish_time"),
        parse_kaggle_tags(F.col("tags")).alias("tags"),
        F.lit(None).cast("int").alias("duration_seconds"),
        F.col("views").cast("long").alias("views"),
        F.col("likes").cast("long").alias("likes"),
        F.col("dislikes").cast("long").alias("dislikes"),
        F.col("comment_count").cast("long").alias("comment_count"),
        F.col("comments_disabled").cast("boolean").alias("comments_disabled"),
        F.col("ratings_disabled").cast("boolean").alias("ratings_disabled"),
        F.lit("kaggle").alias("source"),
        F.lit(None).cast("timestamp").alias("ingested_at"),
    )


# --------------------------------------------------------------------------
# Combine, clean, dedupe
# --------------------------------------------------------------------------
def clean(df: DataFrame) -> DataFrame:
    """Drop rows that cannot be identified (broken CSV lines, unparsable dates)."""
    return df.filter(
        F.col("video_id").isNotNull()
        & (F.trim(F.col("video_id")) != "")
        & F.col("trending_date").isNotNull()
        & F.col("region").isNotNull()
    )


def dedupe(df: DataFrame) -> DataFrame:
    """Keep one row per (video_id, region, trending_date): latest snapshot, then most views."""
    window = Window.partitionBy("video_id", "region", "trending_date").orderBy(
        F.col("ingested_at").desc_nulls_last(), F.col("views").desc_nulls_last()
    )
    return (
        df.withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def build_silver(api_raw: DataFrame, kaggle_raw: DataFrame) -> DataFrame:
    combined = transform_api(api_raw).unionByName(transform_kaggle(kaggle_raw))
    return add_metrics(dedupe(clean(combined))).select(*SILVER_COLUMNS)


# --------------------------------------------------------------------------
# I/O
# --------------------------------------------------------------------------
def _is_missing_path(exc: AnalysisException) -> bool:
    text = str(exc)
    return "PATH_NOT_FOUND" in text or "Path does not exist" in text


def read_api(spark: SparkSession, bronze_root: str) -> DataFrame:
    try:
        return (
            spark.read.schema(API_FILE_SCHEMA)
            .option("pathGlobFilter", "*.json")
            .json(f"{bronze_root}/{API_SUBPATH}")
        )
    except AnalysisException as exc:
        if not _is_missing_path(exc):
            raise
        logger.warning("No API data under %s/%s", bronze_root, API_SUBPATH)
        return spark.createDataFrame([], API_FILE_SCHEMA)


def read_kaggle(spark: SparkSession, bronze_root: str) -> DataFrame:
    try:
        return (
            spark.read.schema(KAGGLE_FILE_SCHEMA)
            .option("header", "true")
            .option("multiLine", "true")  # descriptions contain line breaks
            .option("quote", '"')
            .option("escape", '"')
            .option("pathGlobFilter", "*.csv")
            .csv(f"{bronze_root}/{KAGGLE_SUBPATH}")
        )
    except AnalysisException as exc:
        if not _is_missing_path(exc):
            raise
        logger.warning("No Kaggle data under %s/%s", bronze_root, KAGGLE_SUBPATH)
        return spark.createDataFrame([], KAGGLE_FILE_SCHEMA)


def run(spark: SparkSession, bronze_root: str, silver_root: str) -> dict[str, Any]:
    configure_session(spark)

    silver = build_silver(read_api(spark, bronze_root), read_kaggle(spark, bronze_root)).cache()
    total = silver.count()
    by_source = {r["source"]: r["count"] for r in silver.groupBy("source").count().collect()}
    by_region = {r["region"]: r["count"] for r in silver.groupBy("region").count().collect()}

    if total == 0:
        raise RuntimeError("Silver would be empty: no usable rows found in Bronze.")

    (
        silver.repartition("region")
        .write.mode("overwrite")
        .partitionBy("region")
        .option("compression", "snappy")
        .parquet(f"{silver_root}/{SILVER_SUBPATH}")
    )
    silver.unpersist()

    stats = {"rows": total, "by_source": by_source, "by_region": by_region}
    logger.info("Wrote Silver trending_videos: %s", stats)
    return stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze_bucket", required=True)
    parser.add_argument("--silver_bucket", required=True)
    # Glue also passes --JOB_NAME and friends, which we ignore
    args, _ = parser.parse_known_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    spark = SparkSession.Builder().appName("bronze_to_silver").getOrCreate()
    stats = run(spark, f"s3://{args.bronze_bucket}", f"s3://{args.silver_bucket}")
    print(f"bronze_to_silver finished: {stats}")


if __name__ == "__main__":
    main()