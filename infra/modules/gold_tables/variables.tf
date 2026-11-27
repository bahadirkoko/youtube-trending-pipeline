variable "database_name" {
  description = "Gold Glue database (from the glue_catalog module)"
  type        = string
}

variable "gold_bucket_name" {
  type = string
}

variable "regions" {
  description = "Region codes Athena projects as region=XX partitions. Pass the environment's youtube_regions, so every ingested region is queryable."
  type        = list(string)
}
