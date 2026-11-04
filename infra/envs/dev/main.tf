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

module "data_quality" {
  source = "../../modules/data_quality"

  project_name       = var.project_name
  environment        = local.environment
  lambda_role_arn    = module.iam.role_arns["lambda"]
  silver_bucket_name = module.data_lake.bucket_names["silver"]
  alerts_topic_arn   = module.messaging.alerts_topic_arn
  source_dir         = "${path.root}/../../../src/lambdas/data_quality"

  # Same AWS SDK for pandas layer as json_to_parquet (provides pyarrow).
  pandas_layer_arn = "arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:32"
}

# Phase 3: Silver tables in the Glue catalog (queryable from Athena)
module "silver_tables" {
  source = "../../modules/silver_tables"

  database_name      = module.glue_catalog.database_names["silver"]
  silver_bucket_name = module.data_lake.bucket_names["silver"]
}

module "gold_tables" {
  source = "../../modules/gold_tables"

  database_name    = module.glue_catalog.database_names["gold"]
  gold_bucket_name = module.data_lake.bucket_names["gold"]
}

# Phase 3: Athena workgroup (results location + scan limit)
module "athena" {
  source = "../../modules/athena"

  project_name        = var.project_name
  environment         = local.environment
  results_bucket_name = module.data_lake.bucket_names["athena"]

  # dev only: destroy even if the workgroup has saved queries
  force_destroy = true
}

# Phase 3: Glue job, Bronze (API JSON + Kaggle CSV) -> Silver trending_videos
module "glue_jobs" {
  source = "../../modules/glue_jobs"

  project_name        = var.project_name
  environment         = local.environment
  glue_role_arn       = module.iam.role_arns["glue"]
  scripts_bucket_name = module.data_lake.bucket_names["scripts"]
  bronze_bucket_name  = module.data_lake.bucket_names["bronze"]
  silver_bucket_name  = module.data_lake.bucket_names["silver"]
  script_path         = "${path.root}/../../../src/glue_jobs/bronze_to_silver.py"

  gold_bucket_name = module.data_lake.bucket_names["gold"]
  gold_script_path = "${path.root}/../../../src/glue_jobs/silver_to_gold.py"
}


# Phase 6: Step Functions state machine + EventBridge schedule
module "orchestration" {
  source = "../../modules/orchestration"

  project_name = var.project_name
  environment  = local.environment
  region       = var.aws_region

  sfn_role_arn     = module.iam.role_arns["sfn"]
  events_role_arn  = module.iam.role_arns["events"]
  alerts_topic_arn = module.messaging.alerts_topic_arn

  ingestion_function_name       = module.ingestion.function_name
  json_to_parquet_function_name = module.json_to_parquet.function_name
  data_quality_function_name    = module.data_quality.function_name
  bronze_to_silver_job_name     = module.glue_jobs.bronze_to_silver_job_name
  silver_to_gold_job_name       = module.glue_jobs.silver_to_gold_job_name

  definition_path  = "${path.root}/../../../orchestration/pipeline.asl.json.tpl"
  schedule_enabled = var.pipeline_schedule_enabled
}

# Phase 7: alarms and dashboard
module "monitoring" {
  source = "../../modules/monitoring"

  project_name      = var.project_name
  environment       = local.environment
  region            = var.aws_region
  alerts_topic_arn  = module.messaging.alerts_topic_arn
  state_machine_arn = module.orchestration.state_machine_arn
  schedule_enabled  = var.pipeline_schedule_enabled

  lambda_function_names = {
    ingestion       = module.ingestion.function_name
    json-to-parquet = module.json_to_parquet.function_name
    data-quality    = module.data_quality.function_name
  }
}