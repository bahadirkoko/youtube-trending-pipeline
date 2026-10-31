locals {
  function_name = "${var.project_name}-json-to-parquet-${var.environment}"
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

resource "aws_lambda_function" "json_to_parquet" {
  function_name = local.function_name
  description   = "Converts category reference JSON (Bronze) to Parquet (Silver)"

  role    = var.lambda_role_arn
  runtime = "python3.12"
  handler = "handler.lambda_handler"

  # pyarrow comes from this managed layer, so nothing heavy is in our zip
  layers = [var.pandas_layer_arn]

  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256

  timeout     = var.timeout_seconds
  memory_size = var.memory_mb

  environment {
    variables = {
      BRONZE_BUCKET = var.bronze_bucket_name
      SILVER_BUCKET = var.silver_bucket_name
      LOG_LEVEL     = var.log_level
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda]
}