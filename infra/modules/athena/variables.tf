variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "results_bucket_name" {
  description = "Bucket that stores query results (the athena bucket)"
  type        = string
}

variable "bytes_scanned_cutoff" {
  description = "A query that would scan more than this is cancelled (cost safety net). Default 1 GB."
  type        = number
  default     = 1073741824
}

variable "force_destroy" {
  description = "Allow destroy even if the workgroup has saved queries (dev only)"
  type        = bool
  default     = false
}
