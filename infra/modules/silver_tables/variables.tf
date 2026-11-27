variable "database_name" {
  description = "Silver Glue database (from the glue_catalog module)"
  type        = string
}

variable "silver_bucket_name" {
  type = string
}

variable "regions" {
  description = "Region codes Athena projects as region=XX partitions. Pass the environment's youtube_regions, so every ingested region is queryable."
  type        = list(string)
}
