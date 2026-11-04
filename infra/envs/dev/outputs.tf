output "bucket_names" {
  description = "S3 bucket names by layer"
  value       = module.data_lake.bucket_names
}

output "glue_databases" {
  description = "Glue database names by layer"
  value       = module.glue_catalog.database_names
}

output "alerts_topic_arn" {
  value = module.messaging.alerts_topic_arn
}

output "role_arns" {
  description = "IAM role ARNs by service"
  value       = module.iam.role_arns
}

output "ingestion_function_name" {
  value = module.ingestion.function_name
}

output "youtube_secret_name" {
  value = module.ingestion.secret_name
}

output "json_to_parquet_function_name" {
  value = module.json_to_parquet.function_name
}

output "athena_workgroup" {
  value = module.athena.workgroup_name
}

output "bronze_to_silver_job_name" {
  value = module.glue_jobs.bronze_to_silver_job_name
}

output "data_quality_function_name" {
  value = module.data_quality.function_name
}

output "silver_to_gold_job_name" {
  value = module.glue_jobs.silver_to_gold_job_name
}

output "gold_tables" {
  value = module.gold_tables.table_names
}