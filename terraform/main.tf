terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.24"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }

  backend "s3" {
    bucket  = "aws-docs-rag-terraform-state"
    key     = "terraform.tfstate"
    region  = "us-east-1"
    profile = "aws-docs-rag-dev"
  }
}

provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

# Random suffix for globally unique bucket names
resource "random_id" "suffix" {
  byte_length = 4
}
