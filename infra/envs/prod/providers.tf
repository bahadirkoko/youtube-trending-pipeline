provider "aws" {
  region = var.aws_region

  # Applied to every taggable resource automatically
  default_tags {
    tags = {
      Project     = var.project_name
      Environment = local.environment
      ManagedBy   = "terraform"
    }
  }
}
