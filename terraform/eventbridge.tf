# ---------------------------------------------------------------------------
# Discovery Lambda - runs daily at 6 AM UTC
# Fetches sitemaps, adds new URLs to DynamoDB, removes deleted URLs
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "discovery" {
  name                = "${var.project}-discovery"
  description         = "Trigger discovery Lambda daily to sync sitemap URLs"
  schedule_expression = "cron(0 6 * * ? *)"

  tags = {
    Project = var.project
  }
}

resource "aws_cloudwatch_event_target" "discovery" {
  rule      = aws_cloudwatch_event_rule.discovery.name
  target_id = "DiscoveryLambda"
  arn       = aws_lambda_function.discovery.arn
}

# ---------------------------------------------------------------------------
# Refresh Lambda - runs every 6 hours
# Processes a batch of 50 URLs, conditionally re-embeds changed pages
# ---------------------------------------------------------------------------

resource "aws_cloudwatch_event_rule" "refresh" {
  name                = "${var.project}-refresh"
  description         = "Trigger refresh Lambda every 6 hours to update changed doc pages"
  schedule_expression = "rate(6 hours)"

  tags = {
    Project = var.project
  }
}

resource "aws_cloudwatch_event_target" "refresh" {
  rule      = aws_cloudwatch_event_rule.refresh.name
  target_id = "RefreshLambda"
  arn       = aws_lambda_function.refresh.arn
}
