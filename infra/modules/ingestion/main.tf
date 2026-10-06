locals {
  function_name = "${var.project_name}-youtube-ingestion-${var.environment}"
  secret_name   = "${var.project_name}/${var.environment}/youtube-api-key"
}

# The secret container is created here; its VALUE is set out-of-band with the
# AWS CLI so the API key never appears in Terraform code, plan output or state.
resource "aws_secretsmanager_secret" "youtube_api_key" {
  name                    = local.secret_name
  description             = "YouTube Data API v3 key (value set manually, not by Terraform)"
  recovery_window_in_days = var.secret_recovery_window_days
}

# Terraform zips the source folder itself; no manual packaging step.
data "archive_file" "lambda" {
  type        = "zip"
  source_dir  = var.source_dir
  output_path = "${path.module}/builds/${local.function_name}.zip"
  excludes    = ["__pycache__", "requirements.txt", ".gitkeep"]
}

# Own the log group so retention is set and `terraform destroy` removes the logs.
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "ingestion" {
  function_name = local.function_name
  description   = "Fetches YouTube trending videos + categories into the Bronze bucket"

  role    = var.lambda_role_arn
  runtime = "python3.12"
  handler = "handler.lambda_handler"

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  timeout     = var.timeout_seconds
  memory_size = var.memory_mb

  environment {
    variables = {
      BRONZE_BUCKET      = var.bronze_bucket_name
      YOUTUBE_SECRET_ARN = aws_secretsmanager_secret.youtube_api_key.arn
      YOUTUBE_REGIONS    = join(",", var.regions)
      LOG_LEVEL          = var.log_level
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
}