resource "aws_dynamodb_table" "url_queue" {
  name         = "${var.project}-url-queue"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "url"

  attribute {
    name = "url"
    type = "S"
  }

  # GSI to query by next_check timestamp for efficient batch selection.
  # The refresh Lambda queries: service = <service> AND next_check <= <now>
  # sorted by next_check ascending to process most overdue URLs first.
  attribute {
    name = "service"
    type = "S"
  }

  attribute {
    name = "next_check"
    type = "S"
  }

  global_secondary_index {
    name            = "service-next_check-index"
    hash_key        = "service"
    range_key       = "next_check"
    projection_type = "ALL"
  }

  tags = {
    Project = var.project
  }
}
