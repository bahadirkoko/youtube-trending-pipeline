"""Glue job: Silver -> Gold analytics tables.

Reads the Silver `trending_videos` (and `categories`) tables and rebuilds four Gold
tables, each partitioned by region:

  s3://<gold>/youtube/video_trending_history/region=US/   one row per video
  s3://<gold>/youtube/trending_analytics/region=US/       one row per trending day
  s3://<gold>/youtube/channel_analytics/region=US/        one row per channel
  s3://<gold>/youtube/category_analytics/region=US/       one row per category

Design notes
  * Views are CUMULATIVE: a video's view count on day 3 already includes day 1 and 2.
    So channel and category totals are built from each video's PEAK views, never by
    summing the daily rows (that would count the same views many times).
  * Channels are keyed by channel_title because Kaggle rows have no channel_id.
  * Plain PySpark, no awsglue imports. Every run rebuilds everything from Silver and
    replaces the regions it finds (idempotent, dynamic partition overwrite).
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Any

from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

logger = logging.getLogger("silver_to_gold")

SILVER_TRENDING_SUBPATH = "youtube/trending_videos"
SILVER_CATEGORIES_SUBPATH = "youtube/categories"
GOLD_SUBPATH = "youtube"

# Output columns (the `region` partition column is added in front when writing).
# These drive the final select/cast, and must match the Glue tables in Terraform.
GOLD_SCHEMAS: dict[str, list[tuple[str, str]]] = {
    "video_trending_history": [
        ("video_id", "string"),
        ("title", "string"),
        ("channel_title", "string"),
        ("category_id", "int"),
        ("first_trending_date", "date"),
        ("last_trending_date", "date"),
        ("days_on_trending", "int"),
        ("calendar_span_days", "int"),
        ("peak_views", "bigint"),
        ("peak_views_date", "date"),
        ("first_views", "bigint"),
        ("last_views", "bigint"),
        ("avg_like_ratio", "double"),
        ("avg_engagement_rate", "double"),
    ],
    "trending_analytics": [
        ("trending_date", "date"),
        ("videos", "int"),
        ("distinct_channels", "int"),
        ("distinct_categories", "int"),
        ("total_views", "bigint"),
        ("avg_views", "double"),
        ("max_views", "bigint"),
        ("avg_like_ratio", "double"),
        ("avg_engagement_rate", "double"),
        ("top_video_id", "string"),
        ("top_video_title", "string"),
    ],
    "channel_analytics": [
        ("channel_title", "string"),
        ("videos_trended", "int"),
        ("total_trending_days", "int"),
        ("avg_days_per_video", "double"),
        ("total_peak_views", "bigint"),
        ("avg_peak_views", "double"),
        ("max_peak_views", "bigint"),
        ("avg_like_ratio", "double"),
        ("first_trending_date", "date"),
        ("last_trending_date", "date"),
        ("top_video_title", "string"),
    ],
    "category_analytics": [
        ("category_id", "int"),
        ("category_name", "string"),
        ("videos_trended", "int"),
        ("total_trending_days", "int"),
        ("share_of_trending_days", "double"),
        ("avg_days_per_video", "double"),
        ("total_peak_views", "bigint"),
        ("avg_peak_views", "double"),
        ("avg_like_ratio", "double"),
        ("avg_engagement_rate", "double"),
        ("distinct_channels", "int"),
    ],
}

CATEGORIES_SCHEMA = T.StructType(
    [
        T.StructField("category_id", T.IntegerType()),
        T.StructField("category_name", T.StringType()),
        T.StructField("assignable", T.BooleanType()),
        T.StructField("region", T.StringType()),
    ]
)


def configure_session(spark: SparkSession) -> SparkSession:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    # keep region=US as a plain string
    spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    # only replace the region folders we write, leave the others alone
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    return spark


def conform(df: DataFrame, table: str) -> DataFrame:
    """Select exactly the table's columns, in order, with the declared types."""
    columns = [F.col(name).cast(dtype).alias(name) for name, dtype in GOLD_SCHEMAS[table]]
    return df.select("region", *columns)


# --------------------------------------------------------------------------
# The four Gold tables
# --------------------------------------------------------------------------
def build_video_history(silver: DataFrame) -> DataFrame:
    grouped = silver.groupBy("region", "video_id").agg(
        F.max_by("title", "trending_date").alias("title"),  # latest title wins
        F.max_by("channel_title", "trending_date").alias("channel_title"),
        F.max_by("category_id", "trending_date").alias("category_id"),
        F.min("trending_date").alias("first_trending_date"),
        F.max("trending_date").alias("last_trending_date"),
        F.count(F.lit(1)).alias("days_on_trending"),  # Silver has one row per video-day
        F.max("views").alias("peak_views"),
        F.max_by("trending_date", "views").alias("peak_views_date"),
        F.min_by("views", "trending_date").alias("first_views"),
        F.max_by("views", "trending_date").alias("last_views"),
        F.avg("like_ratio").alias("avg_like_ratio"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
    )
    spanned = grouped.withColumn(
        "calendar_span_days",
        F.datediff("last_trending_date", "first_trending_date") + 1,
    )
    return conform(spanned, "video_trending_history")


def build_trending_analytics(silver: DataFrame) -> DataFrame:
    grouped = silver.groupBy("region", "trending_date").agg(
        F.count(F.lit(1)).alias("videos"),
        F.countDistinct("channel_title").alias("distinct_channels"),
        F.countDistinct("category_id").alias("distinct_categories"),
        F.sum("views").alias("total_views"),
        F.avg("views").alias("avg_views"),
        F.max("views").alias("max_views"),
        F.avg("like_ratio").alias("avg_like_ratio"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.max_by("video_id", "views").alias("top_video_id"),
        F.max_by("title", "views").alias("top_video_title"),
    )
    return conform(grouped, "trending_analytics")


def build_channel_analytics(history: DataFrame) -> DataFrame:
    named = history.filter(F.col("channel_title").isNotNull() & (F.trim("channel_title") != ""))
    grouped = named.groupBy("region", "channel_title").agg(
        F.count(F.lit(1)).alias("videos_trended"),
        F.sum("days_on_trending").alias("total_trending_days"),
        F.avg("days_on_trending").alias("avg_days_per_video"),
        F.sum("peak_views").alias("total_peak_views"),
        F.avg("peak_views").alias("avg_peak_views"),
        F.max("peak_views").alias("max_peak_views"),
        F.avg("avg_like_ratio").alias("avg_like_ratio"),
        F.min("first_trending_date").alias("first_trending_date"),
        F.max("last_trending_date").alias("last_trending_date"),
        F.max_by("title", "peak_views").alias("top_video_title"),
    )
    return conform(grouped, "channel_analytics")


def build_category_analytics(history: DataFrame, categories: DataFrame) -> DataFrame:
    grouped = history.groupBy("region", "category_id").agg(
        F.count(F.lit(1)).alias("videos_trended"),
        F.sum("days_on_trending").alias("total_trending_days"),
        F.avg("days_on_trending").alias("avg_days_per_video"),
        F.sum("peak_views").alias("total_peak_views"),
        F.avg("peak_views").alias("avg_peak_views"),
        F.avg("avg_like_ratio").alias("avg_like_ratio"),
        F.avg("avg_engagement_rate").alias("avg_engagement_rate"),
        F.countDistinct("channel_title").alias("distinct_channels"),
    )
    names = categories.select("region", "category_id", "category_name").dropDuplicates(
        ["region", "category_id"]
    )
    joined = grouped.join(names, on=["region", "category_id"], how="left")
    joined = joined.withColumn(
        "category_name",
        F.coalesce(
            F.col("category_name"),
            F.concat(F.lit("Unknown ("), F.col("category_id").cast("string"), F.lit(")")),
        ),
    )
    by_region = Window.partitionBy("region")
    shared = joined.withColumn(
        "share_of_trending_days",
        F.col("total_trending_days") / F.sum("total_trending_days").over(by_region),
    )
    return conform(shared, "category_analytics")


def build_gold(silver: DataFrame, categories: DataFrame) -> dict[str, DataFrame]:
    history = build_video_history(silver)
    return {
        "video_trending_history": history,
        "trending_analytics": build_trending_analytics(silver),
        "channel_analytics": build_channel_analytics(history),
        "category_analytics": build_category_analytics(history, categories),
    }


# --------------------------------------------------------------------------
# I/O
# --------------------------------------------------------------------------
def _is_missing(exc: AnalysisException) -> bool:
    text = str(exc)
    return (
        "PATH_NOT_FOUND" in text
        or "Path does not exist" in text
        or "UNABLE_TO_INFER_SCHEMA" in text
        or "Unable to infer schema" in text
    )


def read_silver(spark: SparkSession, silver_root: str) -> DataFrame:
    try:
        return spark.read.parquet(f"{silver_root}/{SILVER_TRENDING_SUBPATH}")
    except AnalysisException as exc:
        if not _is_missing(exc):
            raise
        raise RuntimeError(f"No Silver trending_videos found under {silver_root}") from exc


def read_categories(spark: SparkSession, silver_root: str) -> DataFrame:
    try:
        return spark.read.parquet(f"{silver_root}/{SILVER_CATEGORIES_SUBPATH}")
    except AnalysisException as exc:
        if not _is_missing(exc):
            raise
        logger.warning("No categories under %s, names will be 'Unknown'", silver_root)
        return spark.createDataFrame([], CATEGORIES_SCHEMA)


def run(spark: SparkSession, silver_root: str, gold_root: str) -> dict[str, Any]:
    configure_session(spark)

    silver = read_silver(spark, silver_root).cache()
    if silver.count() == 0:
        raise RuntimeError("Silver trending_videos is empty, refusing to rebuild Gold.")

    tables = build_gold(silver, read_categories(spark, silver_root))

    counts: dict[str, int] = {}
    for name, df in tables.items():
        df = df.cache()
        counts[name] = df.count()
        (
            df.repartition("region")
            .write.mode("overwrite")
            .partitionBy("region")
            .option("compression", "snappy")
            .parquet(f"{gold_root}/{GOLD_SUBPATH}/{name}")
        )
        df.unpersist()
    silver.unpersist()

    logger.info("Wrote Gold tables: %s", counts)
    return {"tables": counts}


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver_bucket", required=True)
    parser.add_argument("--gold_bucket", required=True)
    # Glue also passes --JOB_NAME and friends, which we ignore
    args, _ = parser.parse_known_args(argv)

    logging.basicConfig(level=logging.INFO, stream=sys.stdout)
    spark = SparkSession.Builder().appName("silver_to_gold").getOrCreate()
    stats = run(spark, f"s3://{args.silver_bucket}", f"s3://{args.gold_bucket}")
    print(f"silver_to_gold finished: {stats}")


if __name__ == "__main__":
    main()
