variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix used in all resource names"
  type        = string
  default     = "yt-data-pipeline"
}

variable "alert_email" {
  description = "Email subscribed to pipeline alerts (empty = no subscription)"
  type        = string
  default     = ""
}

variable "force_destroy_buckets" {
  description = "Let terraform destroy empty and delete buckets (fine for dev)"
  type        = bool
  default     = true
}

variable "youtube_regions" {
  description = "Region codes the ingestion Lambda fetches by default"
  type        = list(string)
  default     = ["US", "GB"]
}
