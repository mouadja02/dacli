terraform {
  required_version = ">= 1.6.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
    }
    random = {
      source  = "hashicorp/random"
    }
    null = {
      source  = "hashicorp/null"
    }
  }

  # Remote state in S3 (bucket created separately or via bootstrap)
  backend "s3" {
    bucket         = "dacli-terraform-state"
    key            = "agentcore/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "dacli-terraform-locks"
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "dacli"
      Environment = var.environment
      ManagedBy   = "terraform"
      Owner       = "mouadja02"
    }
  }
}
