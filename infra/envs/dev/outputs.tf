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