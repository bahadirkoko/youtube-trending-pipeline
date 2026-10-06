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
  description = "Bucket the raw JSON is written to"
  type        = string
}

variable "regions" {
  description = "Region codes fetched by default, e.g. [\"US\", \"GB\"]"
  type        = list(string)
}

variable "source_dir" {
  description = "Folder containing handler.py"
  type        = string
}

variable "secret_recovery_window_days" {
  description = "Days a deleted secret is recoverable. 0 = delete immediately (use in dev so destroy/apply cycles work)."
  type        = number
  default     = 30
}

variable "log_retention_days" {
  type    = number
  default = 14
}

variable "timeout_seconds" {
  type    = number
  default = 120
}

variable "memory_mb" {
  type    = number
  default = 256
}

variable "log_level" {
  description = "Lambda LOG_LEVEL (INFO or DEBUG)"
  type        = string
  default     = "INFO"
}