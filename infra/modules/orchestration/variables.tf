variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "region" {
  description = "Used for the execution links in notification emails"
  type        = string
}

variable "sfn_role_arn" {
  description = "Execution role for the state machine (from the iam module)"
  type        = string
}

variable "events_role_arn" {
  description = "Role EventBridge uses to start executions (from the iam module)"
  type        = string
}

variable "alerts_topic_arn" {
  description = "SNS topic for success and failure notifications"
  type        = string
}

variable "ingestion_function_name" {
  type = string
}

variable "json_to_parquet_function_name" {
  type = string
}

variable "data_quality_function_name" {
  type = string
}

variable "bronze_to_silver_job_name" {
  type = string
}

variable "silver_to_gold_job_name" {
  type = string
}

variable "definition_path" {
  description = "Local path of orchestration/pipeline.asl.json.tpl"
  type        = string
}

variable "schedule_expression" {
  description = "EventBridge schedule. Default: every day at 11:00 UTC."
  type        = string
  default     = "cron(0 11 * * ? *)"
}

variable "schedule_enabled" {
  description = "false = the rule exists but never fires; run the pipeline by hand first"
  type        = bool
  default     = false
}

variable "log_level" {
  description = "Step Functions execution logging: ALL, ERROR, FATAL or OFF"
  type        = string
  default     = "ALL"
}

variable "log_retention_days" {
  type    = number
  default = 14
}
