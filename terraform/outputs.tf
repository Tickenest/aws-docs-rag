output "data_bucket_name" {
  description = "S3 data bucket name (stores chunk text)"
  value       = aws_s3_bucket.data.bucket
}

output "frontend_url" {
  description = "S3 static website URL for the React frontend"
  value       = "http://${aws_s3_bucket_website_configuration.frontend.website_endpoint}"
}

output "api_url" {
  description = "API Gateway invoke URL"
  value       = "${aws_api_gateway_stage.prod.invoke_url}/query"
}

output "api_key_id" {
  description = "API Gateway API key ID (retrieve value with: aws apigateway get-api-key --api-key <id> --include-value --profile aws-docs-rag-dev)"
  value       = aws_api_gateway_api_key.main.id
}

output "vector_bucket_name" {
  description = "S3 Vectors bucket name"
  value       = aws_s3vectors_vector_bucket.main.vector_bucket_name
}

output "vector_index_arn" {
  description = "S3 Vectors index ARN"
  value       = aws_s3vectors_index.main.index_arn
}

output "frontend_bucket_name" {
  description = "S3 frontend bucket name"
  value       = aws_s3_bucket.frontend.bucket
}

output "url_table_name" {
  description = "DynamoDB URL refresh queue table name"
  value       = aws_dynamodb_table.url_queue.name
}
