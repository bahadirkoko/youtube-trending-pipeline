locals {
  parquet_input  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  parquet_serde  = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"

  # Written by the silver_to_gold Glue job:
  #   s3://<gold>/youtube/<table>/region=US/part-*.snappy.parquet
  # The column lists must match GOLD_SCHEMAS in src/glue_jobs/silver_to_gold.py.
  tables = {
    video_trending_history = {
      description = "One row per video and region: first/last trending date, days on trending, peak views"
      columns = [
        { name = "video_id", type = "string" },
        { name = "title", type = "string" },
        { name = "channel_title", type = "string" },
        { name = "category_id", type = "int" },
        { name = "first_trending_date", type = "date" },
        { name = "last_trending_date", type = "date" },
        { name = "days_on_trending", type = "int" },
        { name = "calendar_span_days", type = "int" },
        { name = "peak_views", type = "bigint" },
        { name = "peak_views_date", type = "date" },
        { name = "first_views", type = "bigint" },
        { name = "last_views", type = "bigint" },
        { name = "avg_like_ratio", type = "double" },
        { name = "avg_engagement_rate", type = "double" },
      ]
    }
    trending_analytics = {
      description = "One row per region and trending day: volume, views and the top video of the day"
      columns = [
        { name = "trending_date", type = "date" },
        { name = "videos", type = "int" },
        { name = "distinct_channels", type = "int" },
        { name = "distinct_categories", type = "int" },
        { name = "total_views", type = "bigint" },
        { name = "avg_views", type = "double" },
        { name = "max_views", type = "bigint" },
        { name = "avg_like_ratio", type = "double" },
        { name = "avg_engagement_rate", type = "double" },
        { name = "top_video_id", type = "string" },
        { name = "top_video_title", type = "string" },
      ]
    }
    channel_analytics = {
      description = "One row per channel and region (channel_title, Kaggle has no channel id)"
      columns = [
        { name = "channel_title", type = "string" },
        { name = "videos_trended", type = "int" },
        { name = "total_trending_days", type = "int" },
        { name = "avg_days_per_video", type = "double" },
        { name = "total_peak_views", type = "bigint" },
        { name = "avg_peak_views", type = "double" },
        { name = "max_peak_views", type = "bigint" },
        { name = "avg_like_ratio", type = "double" },
        { name = "first_trending_date", type = "date" },
        { name = "last_trending_date", type = "date" },
        { name = "top_video_title", type = "string" },
      ]
    }
    category_analytics = {
      description = "One row per category and region, with category names and share of trending days"
      columns = [
        { name = "category_id", type = "int" },
        { name = "category_name", type = "string" },
        { name = "videos_trended", type = "int" },
        { name = "total_trending_days", type = "int" },
        { name = "share_of_trending_days", type = "double" },
        { name = "avg_days_per_video", type = "double" },
        { name = "total_peak_views", type = "bigint" },
        { name = "avg_peak_views", type = "double" },
        { name = "avg_like_ratio", type = "double" },
        { name = "avg_engagement_rate", type = "double" },
        { name = "distinct_channels", type = "int" },
      ]
    }
  }
}

# Partition projection: Athena works out the region=XX folders from the list in
# var.regions, so there is nothing to register by hand (no crawler, no MSCK REPAIR).
resource "aws_glue_catalog_table" "gold" {
  for_each = local.tables

  name          = each.key
  database_name = var.database_name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"            = "parquet"
    "parquet.compression"       = "SNAPPY"
    "projection.enabled"        = "true"
    "projection.region.type"    = "enum"
    "projection.region.values"  = join(",", var.regions)
    "storage.location.template" = "s3://${var.gold_bucket_name}/youtube/${each.key}/region=$${region}/"
  }

  partition_keys {
    name = "region"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${var.gold_bucket_name}/youtube/${each.key}/"
    input_format  = local.parquet_input
    output_format = local.parquet_output

    ser_de_info {
      serialization_library = local.parquet_serde
      parameters = {
        "serialization.format" = "1"
      }
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}
