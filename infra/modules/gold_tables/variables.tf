variable "database_name" {
  description = "Gold Glue database (from the glue_catalog module)"
  type        = string
}

variable "gold_bucket_name" {
  type = string
}

variable "regions" {
  description = "Region codes Athena should look for. Add new regions here to make them queryable."
  type        = list(string)
  default     = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]
}