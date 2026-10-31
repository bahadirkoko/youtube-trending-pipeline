locals {
  job_name   = "${var.project_name}-bronze-to-silver-${var.environment}"
  script_key = "glue_jobs/bronze_to_silver.py"
}

# Terraform uploads the script. When the file changes, the etag changes and the
# next `terraform apply` uploads the new version (the job reads it at start-up).
resource "aws_s3_object" "bronze_to_silver_script" {
  bucket = var.scripts_bucket_name
  key    = local.script_key
  source = var.script_path
  etag   = filemd5(var.script_path)
}

# The name must match <project>-*-<env>: the Step Functions role in the iam
# module only allows starting jobs with that pattern.
resource "aws_glue_job" "bronze_to_silver" {
  name        = local.job_name
  description = "Bronze (API JSON + Kaggle CSV) to Silver trending_videos Parquet"
  role_arn    = var.glue_role_arn

  glue_version      = var.glue_version
  worker_type       = var.worker_type
  number_of_workers = var.number_of_workers
  timeout           = var.timeout_minutes
  max_retries       = 0 # a failed run should be looked at, not silently re-billed

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_object.bronze_to_silver_script.bucket}/${aws_s3_object.bronze_to_silver_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--bronze_bucket" = var.bronze_bucket_name
    "--silver_bucket" = var.silver_bucket_name

    "--job-language"                     = "python"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}