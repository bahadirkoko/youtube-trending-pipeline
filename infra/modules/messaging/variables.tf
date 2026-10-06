variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "alert_email" {
  description = "Email subscribed to pipeline alerts. Leave empty to skip the subscription."
  type        = string
  default     = ""
}
