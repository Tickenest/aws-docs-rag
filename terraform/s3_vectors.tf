resource "aws_s3vectors_vector_bucket" "main" {
  vector_bucket_name = "docs-rag-vectors-${random_id.suffix.hex}"

  tags = {
    Project = var.project
  }
}

resource "aws_s3vectors_index" "main" {
  vector_bucket_name = aws_s3vectors_vector_bucket.main.vector_bucket_name
  index_name         = var.vector_index_name
  data_type          = "float32"
  dimension          = var.vector_dimensions
  distance_metric    = "cosine"
}
