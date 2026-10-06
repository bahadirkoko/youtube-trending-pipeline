variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "account_id" {
  type = string
}

variable "region" {
  type = string
}

variable "bucket_arns" {
  description = "Map of layer => bucket ARN (from the data_lake module)"
  type        = map(string)
}

variable "sns_topic_arn" {
  type = string
}
