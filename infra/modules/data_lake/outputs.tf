output "bucket_names" {
  description = "Map of layer => bucket name"
  value       = { for layer, b in aws_s3_bucket.this : layer => b.id }
}

output "bucket_arns" {
  description = "Map of layer => bucket ARN"
  value       = { for layer, b in aws_s3_bucket.this : layer => b.arn }
}
