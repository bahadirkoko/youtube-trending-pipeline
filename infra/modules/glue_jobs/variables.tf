variable "project_name" {
  type = string
}

variable "environment" {
  type = string
}

variable "glue_role_arn" {
  description = "Execution role for Glue jobs (from the iam module)"
  type        = string
}

variable "scripts_bucket_name" {
  description = "Bucket the job script is uploaded to"
  type        = string
}

variable "bronze_bucket_name" {
  type = string
}

variable "silver_bucket_name" {
  type = string
}

variable "script_path" {
  description = "Local path of bronze_to_silver.py"
  type        = string
}

variable "glue_version" {
  description = "5.0 = Spark 3.5.4, Python 3.11, Java 17"
  type        = string
  default     = "5.0"
}

variable "worker_type" {
  type    = string
  default = "G.1X"
}

variable "number_of_workers" {
  description = "Glue needs at least 2. Each G.1X worker is 1 DPU ($0.44 per DPU-hour)."
  type        = number
  default     = 2
}

variable "timeout_minutes" {
  description = "The run is stopped (and stops billing) after this long"
  type        = number
  default     = 30
}


variable "gold_bucket_name" {
  type = string
}

variable "gold_script_path" {
  description = "Local path of silver_to_gold.py"
  type        = string
}
