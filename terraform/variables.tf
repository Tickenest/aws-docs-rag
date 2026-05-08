variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS CLI profile to use"
  type        = string
  default     = "aws-docs-rag-dev"
}

variable "aws_account_id" {
  description = "AWS account ID - set in terraform.tfvars (not committed to git)"
  type        = string
}

variable "project" {
  description = "Project name used for resource naming and tagging"
  type        = string
  default     = "aws-docs-rag"
}

variable "vector_index_name" {
  description = "Name of the S3 Vectors index"
  type        = string
  default     = "docs-rag-index"
}

variable "vector_dimensions" {
  description = "Dimensions for Titan Text Embeddings V2"
  type        = number
  default     = 1024
}

variable "embedding_model_id" {
  description = "Bedrock model ID for embeddings"
  type        = string
  default     = "amazon.titan-embed-text-v2:0"
}

variable "generation_model_id" {
  description = "Bedrock model ID for answer generation"
  type        = string
  default     = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
}

variable "refresh_batch_size" {
  description = "Number of URLs to process per refresh Lambda invocation"
  type        = number
  default     = 50
}

variable "refresh_crawl_delay_seconds" {
  description = "Delay in seconds between HTTP requests during refresh"
  type        = number
  default     = 2
}

variable "query_top_k" {
  description = "Number of chunks to retrieve from S3 Vectors per query"
  type        = number
  default     = 10
}
