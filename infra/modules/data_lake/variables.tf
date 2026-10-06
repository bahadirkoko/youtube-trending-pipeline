variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "account_id" {
  description = "AWS account ID, included in bucket names to keep them globally unique"
  type        = string
}

variable "region" {
  type = string
}

variable "force_destroy" {
  description = "Allow destroy to delete non-empty buckets (dev only)"
  type        = bool
  default     = false
}

variable "layers" {
  description = "One bucket is created per layer"
  type        = list(string)
  default     = ["bronze", "silver", "gold", "scripts", "athena"]
}
