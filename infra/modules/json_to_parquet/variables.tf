variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "lambda_role_arn" {
  description = "Execution role for the function (from the iam module)"
  type        = string
}

variable "bronze_bucket_name" {
  description = "Bucket the category JSON is read from"
  type        = string
}

variable "silver_bucket_name" {
  description = "Bucket the Parquet files are written to"
  type        = string
}

variable "source_dir" {
  description = "Folder containing handler.py"
  type        = string
}

variable "pandas_layer_arn" {
  description = "AWS SDK for pandas managed layer (provides pyarrow). The ARN differs per region and Python version."
  type        = string
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "timeout_seconds" {
  type    = number
  default = 60
}

variable "memory_mb" {
  description = "pyarrow is a large import, so this is higher than the ingestion Lambda"
  type        = number
  default     = 512
}

variable "log_level" {
  description = "Lambda LOG_LEVEL (INFO or DEBUG)"
  type        = string
  default     = "INFO"
}