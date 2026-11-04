locals {
  function_name = "${var.project_name}-data-quality-${var.environment}"
}

# Terraform zips the source folder itself; no manual packaging step.
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${path.module}/builds/${local.function_name}.zip"
  excludes    = ["__pycache__", "requirements.txt", ".gitkeep"]
}

resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "data_quality" {
  function_name = local.function_name
  description   = "Data quality gate for Silver trending_videos; emails via SNS on failure"

  role    = var.lambda_role_arn
  runtime = "python3.12"
  handler = "handler.lambda_handler"

  # pyarrow comes from this managed layer
  layers = [var.pandas_layer_arn]

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  timeout     = var.timeout_seconds
  memory_size = var.memory_mb

  environment {
    variables = {
      SILVER_BUCKET                  = var.silver_bucket_name
      SNS_ALERT_TOPIC_ARN            = var.alerts_topic_arn
      ENVIRONMENT                    = var.environment
      LOG_LEVEL                      = var.log_level
      DQ_MIN_ROW_COUNT               = tostring(var.min_row_count)
      DQ_MAX_NULL_PERCENT            = tostring(var.max_null_percent)
      DQ_MAX_AGE_HOURS               = tostring(var.max_age_hours)
      DQ_MAX_RATIO_VIOLATION_PERCENT = tostring(var.max_ratio_violation_percent)
      DQ_REQUIRE_LIVE_DATA           = tostring(var.require_live_data)
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
}
