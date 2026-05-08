# ---------------------------------------------------------------------------
# Discovery Lambda
# Runs daily, fetches sitemaps, syncs URL table
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "discovery" {
  name              = "/aws/lambda/${var.project}-discovery"
  retention_in_days = 14

  tags = {
    Project = var.project
  }
}

data "archive_file" "discovery_placeholder" {
  type        = "zip"
  output_path = "${path.module}/placeholder_discovery.zip"

  source {
    content  = "# placeholder - deploy with deploy_discovery.ps1"
    filename = "lambda_function.py"
  }
}

resource "aws_lambda_function" "discovery" {
  function_name    = "${var.project}-discovery"
  role             = aws_iam_role.discovery.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.discovery_placeholder.output_path
  source_code_hash = data.archive_file.discovery_placeholder.output_base64sha256
  timeout          = 300
  memory_size      = 256

  environment {
    variables = {
      URL_TABLE_NAME     = aws_dynamodb_table.url_queue.name
      VECTOR_BUCKET_NAME = aws_s3vectors_vector_bucket.main.vector_bucket_name
      VECTOR_INDEX_NAME  = var.vector_index_name
    }
  }

  depends_on = [aws_cloudwatch_log_group.discovery]

  tags = {
    Project = var.project
  }
}

# ---------------------------------------------------------------------------
# Refresh Lambda
# Runs every 6 hours, processes 50 URLs, updates S3 Vectors
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "refresh" {
  name              = "/aws/lambda/${var.project}-refresh"
  retention_in_days = 14

  tags = {
    Project = var.project
  }
}

data "archive_file" "refresh_placeholder" {
  type        = "zip"
  output_path = "${path.module}/placeholder_refresh.zip"

  source {
    content  = "# placeholder - deploy with deploy_refresh.ps1"
    filename = "lambda_function.py"
  }
}

resource "aws_lambda_function" "refresh" {
  function_name    = "${var.project}-refresh"
  role             = aws_iam_role.refresh.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.refresh_placeholder.output_path
  source_code_hash = data.archive_file.refresh_placeholder.output_base64sha256
  timeout          = 900
  memory_size      = 512

  environment {
    variables = {
      URL_TABLE_NAME              = aws_dynamodb_table.url_queue.name
      VECTOR_BUCKET_NAME          = aws_s3vectors_vector_bucket.main.vector_bucket_name
      VECTOR_INDEX_NAME           = var.vector_index_name
      EMBEDDING_MODEL_ID          = var.embedding_model_id
      BATCH_SIZE                  = tostring(var.refresh_batch_size)
      CRAWL_DELAY_SECONDS         = tostring(var.refresh_crawl_delay_seconds)
      CHUNK_BUCKET_NAME           = aws_s3_bucket.data.bucket
    }
  }

  depends_on = [aws_cloudwatch_log_group.refresh]

  tags = {
    Project = var.project
  }
}

# ---------------------------------------------------------------------------
# Query Lambda
# Invoked by API Gateway, embeds query, searches S3 Vectors, calls Claude
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "query" {
  name              = "/aws/lambda/${var.project}-query"
  retention_in_days = 14

  tags = {
    Project = var.project
  }
}

data "archive_file" "query_placeholder" {
  type        = "zip"
  output_path = "${path.module}/placeholder_query.zip"

  source {
    content  = "# placeholder - deploy with deploy_query.ps1"
    filename = "lambda_function.py"
  }
}

resource "aws_lambda_function" "query" {
  function_name    = "${var.project}-query"
  role             = aws_iam_role.query.arn
  handler          = "lambda_function.lambda_handler"
  runtime          = "python3.12"
  filename         = data.archive_file.query_placeholder.output_path
  source_code_hash = data.archive_file.query_placeholder.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      VECTOR_BUCKET_NAME  = aws_s3vectors_vector_bucket.main.vector_bucket_name
      VECTOR_INDEX_NAME   = var.vector_index_name
      EMBEDDING_MODEL_ID  = var.embedding_model_id
      GENERATION_MODEL_ID = var.generation_model_id
      TOP_K               = tostring(var.query_top_k)
      CHUNK_BUCKET_NAME   = aws_s3_bucket.data.bucket
    }
  }

  depends_on = [aws_cloudwatch_log_group.query]

  tags = {
    Project = var.project
  }
}

# Allow API Gateway to invoke the query Lambda
resource "aws_lambda_permission" "api_gateway_query" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.query.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.main.execution_arn}/*/*"
}

# Allow EventBridge to invoke the discovery Lambda
resource "aws_lambda_permission" "eventbridge_discovery" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.discovery.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.discovery.arn
}

# Allow EventBridge to invoke the refresh Lambda
resource "aws_lambda_permission" "eventbridge_refresh" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.refresh.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.refresh.arn
}
