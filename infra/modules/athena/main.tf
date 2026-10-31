resource "aws_athena_workgroup" "this" {
  name          = "${var.project_name}-${var.environment}"
  description   = "Queries for the YouTube trending pipeline (${var.environment})"
  force_destroy = var.force_destroy

  configuration {
    # Users cannot override the output location or the scan limit below
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.results_bucket_name}/results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}