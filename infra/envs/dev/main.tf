data "aws_caller_identity" "current" {}

locals {
  environment = "dev"
  account_id  = data.aws_caller_identity.current.account_id
}

# S3 buckets: bronze / silver / gold / scripts / athena
module "data_lake" {
  source = "../../modules/data_lake"

  project_name  = var.project_name
  environment   = local.environment
  account_id    = local.account_id
  region        = var.aws_region
  force_destroy = var.force_destroy_buckets
}

# Glue Data Catalog databases
module "glue_catalog" {
  source = "../../modules/glue_catalog"

  environment = local.environment
}

# SNS topic for pipeline and data quality alerts
module "messaging" {
  source = "../../modules/messaging"

  project_name = var.project_name
  environment  = local.environment
  alert_email  = var.alert_email
}

# IAM roles for Lambda, Glue, Step Functions, EventBridge
module "iam" {
  source = "../../modules/iam"

  project_name  = var.project_name
  environment   = local.environment
  account_id    = local.account_id
  region        = var.aws_region
  bucket_arns   = module.data_lake.bucket_arns
  sns_topic_arn = module.messaging.alerts_topic_arn
}

# Phase 2: YouTube ingestion Lambda + Secrets Manager secret (Bronze layer)
module "ingestion" {
  source = "../../modules/ingestion"

  project_name       = var.project_name
  environment        = local.environment
  lambda_role_arn    = module.iam.role_arns["lambda"]
  bronze_bucket_name = module.data_lake.bucket_names["bronze"]
  regions            = var.youtube_regions
  source_dir         = "${path.root}/../../../src/lambdas/youtube_ingestion"

  # dev only: delete the secret immediately on destroy so apply/destroy cycles work
  secret_recovery_window_days = 0
}

# Phase 3: category reference data, JSON (Bronze) -> Parquet (Silver)
module "json_to_parquet" {
  source = "../../modules/json_to_parquet"

  project_name       = var.project_name
  environment        = local.environment
  lambda_role_arn    = module.iam.role_arns["lambda"]
  bronze_bucket_name = module.data_lake.bucket_names["bronze"]
  silver_bucket_name = module.data_lake.bucket_names["silver"]
  source_dir         = "${path.root}/../../../src/lambdas/json_to_parquet"

  # AWS SDK for pandas (library 3.18.0), Python 3.12, x86_64, us-east-1.
  # This ARN is specific to the region and Python version. See:
  # https://aws-sdk-pandas.readthedocs.io/en/stable/layers.html
  pandas_layer_arn = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:32"
}