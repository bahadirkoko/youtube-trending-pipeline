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
  description = "Region codes the pipeline ingests and Athena can query (Silver and Gold tables)"
  type        = list(string)
  default     = ["CA", "DE", "FR", "GB", "IN", "JP", "KR", "MX", "RU", "US"]

  validation {
    condition     = length(var.youtube_regions) > 0 && alltrue([for r in var.youtube_regions : can(regex("^[A-Z]{2}$", r))])
    error_message = "youtube_regions must be a non-empty list of two-letter uppercase codes, e.g. [\"US\", \"GB\"]."
  }
}


variable "pipeline_schedule_enabled" {
  description = "Whether the EventBridge schedule fires. Keep false until a manual run is green."
  type        = bool
  default     = true
}
