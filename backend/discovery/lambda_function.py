"""
Discovery Lambda - AWS Docs RAG
================================
Runs daily via EventBridge. Fetches sitemaps for all 8 services, compares
against the DynamoDB URL queue table, and syncs the two:

  - New URLs (in sitemap but not in table): inserted with next_check = now
    so the Refresh Lambda picks them up immediately.
  - Removed URLs (in table but not in sitemap): deleted from the table
    and their vectors deleted from S3 Vectors.

Note: S3 chunk text files are NOT deleted for removed URLs. Chunk IDs may
be shared across multiple URLs (identical content), so deletion requires
reference counting which we intentionally avoid. Orphaned text files cost
fractions of a cent and are acceptable.

IMPORTANT: If ANY sitemap fails to fetch, the Lambda aborts without making
any changes. This prevents accidentally treating all URLs for a failed
service as "removed" and deleting them.

Does NOT embed or rechunk anything — that is the Refresh Lambda's job.

Environment variables (set by Terraform):
    URL_TABLE_NAME     - DynamoDB table name
    VECTOR_BUCKET_NAME - S3 Vectors bucket name
    VECTOR_INDEX_NAME  - S3 Vectors index name
"""

import logging
import os
from datetime import datetime, timezone
from xml.etree import ElementTree
import urllib.request

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

URL_TABLE_NAME     = os.environ["URL_TABLE_NAME"]
VECTOR_BUCKET_NAME = os.environ["VECTOR_BUCKET_NAME"]
VECTOR_INDEX_NAME  = os.environ["VECTOR_INDEX_NAME"]

SERVICES = {
    "lambda": {
        "sitemap": "https://docs.aws.amazon.com/lambda/latest/dg/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/lambda/latest/dg/",
    },
    "s3": {
        "sitemap": "https://docs.aws.amazon.com/AmazonS3/latest/userguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/AmazonS3/latest/userguide/",
    },
    "dynamodb": {
        "sitemap": "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/",
    },
    "apigateway": {
        "sitemap": "https://docs.aws.amazon.com/apigateway/latest/developerguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/apigateway/latest/developerguide/",
    },
    "bedrock": {
        "sitemap": "https://docs.aws.amazon.com/bedrock/latest/userguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/bedrock/latest/userguide/",
    },
    "eventbridge": {
        "sitemap": "https://docs.aws.amazon.com/eventbridge/latest/userguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/eventbridge/latest/userguide/",
    },
    "iam": {
        "sitemap": "https://docs.aws.amazon.com/IAM/latest/UserGuide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/IAM/latest/UserGuide/",
    },
    "ec2": {
        "sitemap": "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/",
    },
}

# Must match scrape_docs.py exactly
EXCLUDE_PATTERNS = [
    "doc-history", "document-history", "release-notes",
    "release-history", "/index.html",
]

EC2_EXTRA_EXCLUDES = ["instance-types", "instance_types"]

# S3 Vectors DeleteVectors limit
DELETE_VECTORS_BATCH_SIZE = 500

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
s3v      = boto3.client("s3vectors",  region_name="us-east-1")
table    = dynamodb.Table(URL_TABLE_NAME)

# ---------------------------------------------------------------------------
# Sitemap fetching
# ---------------------------------------------------------------------------

def fetch_sitemap_urls(service_key: str) -> set:
    """
    Fetch sitemap and return set of filtered page URLs.
    Raises RuntimeError if the sitemap cannot be fetched or parsed,
    so the caller can abort the entire run safely.
    """
    config = SERVICES[service_key]
    try:
        with urllib.request.urlopen(config["sitemap"], timeout=30) as resp:
            content = resp.read()
    except Exception as e:
        raise RuntimeError(f"Failed to fetch sitemap for {service_key}: {e}")

    try:
        root = ElementTree.fromstring(content)
    except ElementTree.ParseError as e:
        raise RuntimeError(f"Failed to parse sitemap for {service_key}: {e}")

    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}")[0] + "}"

    urls = set()
    for url_el in root.iter(f"{namespace}loc"):
        url = url_el.text.strip()
        if url.startswith(config["prefix"]) and url.endswith(".html"):
            if not should_exclude(url, service_key):
                urls.add(url)

    logger.info(f"{service_key}: {len(urls)} URLs in sitemap")
    return urls


def should_exclude(url: str, service_key: str) -> bool:
    patterns = EXCLUDE_PATTERNS[:]
    if service_key == "ec2":
        patterns.extend(EC2_EXTRA_EXCLUDES)
    return any(p in url for p in patterns)


# ---------------------------------------------------------------------------
# DynamoDB operations
# ---------------------------------------------------------------------------

def get_all_table_urls() -> dict:
    """
    Scan the URL table and return a dict of {url: service} for all items.
    Only fetches the url and service attributes to minimise read cost.
    """
    urls   = {}
    kwargs = {
        "ProjectionExpression":     "#u, service",
        "ExpressionAttributeNames": {"#u": "url"},
    }
    while True:
        resp = table.scan(**kwargs)
        for item in resp.get("Items", []):
            urls[item["url"]] = item.get("service", "")
        if "LastEvaluatedKey" not in resp:
            break
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

    logger.info(f"Found {len(urls)} URLs in DynamoDB table")
    return urls


def insert_new_url(url: str, service: str, now: str) -> None:
    """
    Insert a new URL with next_check = now so the Refresh Lambda
    processes it on its next run.

    chunk_ids is intentionally omitted — DynamoDB rejects empty String
    Sets. The Refresh Lambda writes chunk_ids after processing the page.

    Uses attribute_not_exists condition so re-runs are safe.
    """
    try:
        table.put_item(
            Item={
                "url":           url,
                "service":       service,
                "next_check":    now,
                "last_checked":  "",
                "last_modified": "",
                "etag":          "",
            },
            ConditionExpression="attribute_not_exists(#u)",
            ExpressionAttributeNames={"#u": "url"},
        )
    except Exception as e:
        if "ConditionalCheckFailedException" not in str(e):
            logger.error(f"Failed to insert {url}: {e}")


def delete_url_from_table(url: str) -> set:
    """
    Delete a URL from the table and return its chunk_ids set.
    Returns empty set if the item has no chunk_ids or is not found.
    """
    try:
        resp = table.delete_item(
            Key={"url": url},
            ReturnValues="ALL_OLD",
        )
        return resp.get("Attributes", {}).get("chunk_ids", set())
    except Exception as e:
        logger.error(f"Failed to delete {url} from table: {e}")
        return set()


# ---------------------------------------------------------------------------
# S3 Vectors cleanup
# ---------------------------------------------------------------------------

def delete_vectors(chunk_ids: set) -> None:
    """
    Delete vectors from S3 Vectors for the given chunk IDs.
    Batches into groups of DELETE_VECTORS_BATCH_SIZE.
    Logs but does not raise on failure — a stale vector is harmless.
    """
    if not chunk_ids:
        return

    chunk_id_list = list(chunk_ids)
    for i in range(0, len(chunk_id_list), DELETE_VECTORS_BATCH_SIZE):
        batch = chunk_id_list[i:i + DELETE_VECTORS_BATCH_SIZE]
        try:
            s3v.delete_vectors(
                vectorBucketName=VECTOR_BUCKET_NAME,
                indexName=VECTOR_INDEX_NAME,
                keys=batch,
            )
            logger.info(f"Deleted {len(batch)} vectors from S3 Vectors")
        except Exception as e:
            logger.error(f"Failed to delete vector batch: {e}")


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    logger.info("Discovery Lambda started")
    now = datetime.now(timezone.utc).isoformat()

    # Step 1: Fetch all sitemap URLs for all services.
    # Abort entirely if any sitemap fails — proceeding with incomplete
    # sitemap data would cause valid URLs to be incorrectly treated as
    # removed and deleted.
    sitemap_urls = {}  # url -> service_key
    for service_key in SERVICES:
        try:
            for url in fetch_sitemap_urls(service_key):
                sitemap_urls[url] = service_key
        except RuntimeError as e:
            logger.error(str(e))
            logger.error(
                "Aborting discovery run — incomplete sitemap data could "
                "cause valid URLs to be incorrectly deleted."
            )
            return {
                "statusCode": 500,
                "error": str(e),
            }

    logger.info(f"Total sitemap URLs across all services: {len(sitemap_urls)}")

    # Step 2: Get all URLs currently in the DynamoDB table
    table_urls = get_all_table_urls()

    # Step 3: Diff
    new_urls = {
        url: service
        for url, service in sitemap_urls.items()
        if url not in table_urls
    }
    removed_urls = [
        url
        for url in table_urls
        if url not in sitemap_urls
    ]

    logger.info(f"New URLs to add:     {len(new_urls)}")
    logger.info(f"Removed URLs to delete: {len(removed_urls)}")

    # Step 4: Insert new URLs
    for url, service in new_urls.items():
        insert_new_url(url, service, now)

    # Step 5: Delete removed URLs — table first, then S3 Vectors
    for url in removed_urls:
        chunk_ids = delete_url_from_table(url)
        if chunk_ids:
            delete_vectors(chunk_ids)

    logger.info(
        f"Discovery complete. Added: {len(new_urls)}, "
        f"Removed: {len(removed_urls)}"
    )

    return {
        "statusCode":        200,
        "added":             len(new_urls),
        "removed":           len(removed_urls),
        "total_sitemap_urls": len(sitemap_urls),
        "total_table_urls":   len(table_urls),
    }
