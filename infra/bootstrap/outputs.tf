output "state_bucket_name" {
  description = "Paste this into infra/envs/<env>/backend.tf"
  value       = aws_s3_bucket.tfstate.id
}

output "backend_snippet" {
  description = "Ready-to-paste backend block for the dev environment"
  value       = <<-EOT
    backend "s3" {
      bucket       = "${aws_s3_bucket.tfstate.id}"
      key          = "dev/terraform.tfstate"
      region       = "${var.aws_region}"
      encrypt      = true
      use_lockfile = true
    }
  EOT
}
