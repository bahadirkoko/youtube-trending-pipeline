# Remote state in S3 (created by infra/bootstrap).
# Replace the bucket value with the `state_bucket_name` output from bootstrap.
terraform {
  backend "s3" {
    bucket       = "yt-data-pipeline-tfstate-743116070631-us-east-1"
    key          = "dev/terraform.tfstate"
    region       = "us-east-1"
    encrypt      = true
    use_lockfile = true
  }
}
