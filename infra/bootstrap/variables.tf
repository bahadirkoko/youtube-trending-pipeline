variable "aws_region" {
  description = "Region for the Terraform state bucket"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix used in resource names"
  type        = string
  default     = "yt-data-pipeline"
}
