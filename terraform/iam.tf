# ---------------------------------------------------------------------------
# Common assume role policy for Lambda
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

# ---------------------------------------------------------------------------
# Discovery Lambda role
# Needs: DynamoDB read/write (url queue), S3 Vectors read (list vectors by URL)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "discovery" {
  name               = "${var.project}-discovery-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = {
    Project = var.project
  }
}

resource "aws_iam_role_policy" "discovery" {
  name = "${var.project}-discovery-policy"
  role = aws_iam_role.discovery.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/${var.project}-discovery:*"
      },
      {
        Sid    = "DynamoDB"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:DeleteItem",
          "dynamodb:Query",
          "dynamodb:Scan"
        ]
        Resource = [
          aws_dynamodb_table.url_queue.arn,
          "${aws_dynamodb_table.url_queue.arn}/index/*"
        ]
      },
      {
        Sid    = "S3VectorsRead"
        Effect = "Allow"
        Action = [
          "s3vectors:ListVectors",
          "s3vectors:GetVectors"
        ]
        Resource = aws_s3vectors_index.main.index_arn
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Refresh Lambda role
# Needs: DynamoDB read/write, S3 Vectors read/write, Bedrock embed
# ---------------------------------------------------------------------------

resource "aws_iam_role" "refresh" {
  name               = "${var.project}-refresh-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = {
    Project = var.project
  }
}

resource "aws_iam_role_policy" "refresh" {
  name = "${var.project}-refresh-policy"
  role = aws_iam_role.refresh.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/${var.project}-refresh:*"
      },
      {
        Sid    = "DynamoDB"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:UpdateItem",
          "dynamodb:Query"
        ]
        Resource = [
          aws_dynamodb_table.url_queue.arn,
          "${aws_dynamodb_table.url_queue.arn}/index/*"
        ]
      },
      {
        Sid    = "S3ChunkReadWrite"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.data.arn}/chunks/*"
      },
      {
        Sid    = "S3VectorsReadWrite"
        Effect = "Allow"
        Action = [
          "s3vectors:PutVectors",
          "s3vectors:DeleteVectors",
          "s3vectors:GetVectors",
          "s3vectors:ListVectors",
          "s3vectors:QueryVectors"
        ]
        Resource = aws_s3vectors_index.main.index_arn
      },
      {
        Sid    = "BedrockEmbed"
        Effect = "Allow"
        Action = [
          "bedrock:InvokeModel"
        ]
        Resource = "*"
      }
    ]
  })
}

# ---------------------------------------------------------------------------
# Query Lambda role
# Needs: S3 Vectors read/query, Bedrock embed + generate
# ---------------------------------------------------------------------------

resource "aws_iam_role" "query" {
  name               = "${var.project}-query-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume_role.json

  tags = {
    Project = var.project
  }
}

resource "aws_iam_role_policy" "query" {
  name = "${var.project}-query-policy"
  role = aws_iam_role.query.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "CloudWatchLogs"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:${var.aws_region}:${var.aws_account_id}:log-group:/aws/lambda/${var.project}-query:*"
      },
      {
        Sid    = "S3ChunkRead"
        Effect = "Allow"
        Action = [
          "s3:GetObject"
        ]
        Resource = "${aws_s3_bucket.data.arn}/chunks/*"
      },
      {
        Sid    = "S3VectorsQuery"
        Effect = "Allow"
        Action = [
          "s3vectors:QueryVectors",
          "s3vectors:GetVectors"
        ]
        Resource = aws_s3vectors_index.main.index_arn
      },
      {
        Sid    = "BedrockInvoke"
        Effect = "Allow"
        Action = [
          "bedrock:InvokeModel"
        ]
        Resource = "*"
      }
    ]
  })
}
