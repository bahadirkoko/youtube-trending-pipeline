output "function_name" {
  value = aws_lambda_function.ingestion.function_name
}

output "function_arn" {
  value = aws_lambda_function.ingestion.arn
}

output "secret_name" {
  description = "Use with: aws secretsmanager put-secret-value --secret-id <this>"
  value       = aws_secretsmanager_secret.youtube_api_key.name
}