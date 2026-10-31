locals {
  parquet_input  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
  parquet_output = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
  parquet_serde  = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
}

# Written by the json_to_parquet Lambda:
#   s3://<silver>/youtube/categories/region=US/categories.parquet
#
# Partition projection: Athena works out the region=XX folders from the list
# below, so there is nothing to register by hand (no crawler, no MSCK REPAIR).
resource "aws_glue_catalog_table" "categories" {
  name          = "categories"
  database_name = var.database_name
  description   = "YouTube video category id -> name, one file per region"
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    "classification"            = "parquet"
    "parquet.compression"       = "SNAPPY"
    "projection.enabled"        = "true"
    "projection.region.type"    = "enum"
    "projection.region.values"  = join(",", var.regions)
    "storage.location.template" = "s3://${var.silver_bucket_name}/youtube/categories/region=$${region}/"
  }

  partition_keys {
    name = "region"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${var.silver_bucket_name}/youtube/categories/"
    input_format  = local.parquet_input
    output_format = local.parquet_output

    ser_de_info {
      serialization_library = local.parquet_serde
      parameters = {
        "serialization.format" = "1"
      }
    }

    columns {
      name = "category_id"
      type = "int"
    }

    columns {
      name = "category_name"
      type = "string"
    }

    columns {
      name = "assignable"
      type = "boolean"
    }
  }
}