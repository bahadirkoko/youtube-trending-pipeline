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

variable "silver_bucket_name" {
  description = "Bucket the Silver Parquet files are read from"
  type        = string
}

variable "alerts_topic_arn" {
  description = "SNS topic that failure emails are published to"
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
  default = 180
}

variable "memory_mb" {
  description = "Each Parquet file is read fully into memory, so this is generous"
  type        = number
  default     = 1024
}

variable "log_level" {
  description = "Lambda LOG_LEVEL (INFO or DEBUG)"
  type        = string
  default     = "INFO"
}

# ---- data quality thresholds (can also be overridden per invoke) ----
variable "min_row_count" {
  type    = number
  default = 10
}

variable "max_null_percent" {
  type    = number
  default = 5
}

variable "max_age_hours" {
  description = "Newest live (API) data must be younger than this"
  type        = number
  default     = 48
}

variable "max_ratio_violation_percent" {
  type    = number
  default = 1
}

variable "require_live_data" {
  description = "Fail the gate when Silver has no live API rows (only Kaggle history)"
  type        = bool
  default     = true
}