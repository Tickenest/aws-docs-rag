"""
Refresh Lambda - AWS Docs RAG
==============================
Runs every 6 hours via EventBridge. Processes a batch of the most overdue
URLs from the DynamoDB URL queue, making conditional HTTP requests to check
for changes. Only changed pages are rechunked and re-embedded.

Workflow per URL:
  1. Query DynamoDB GSI for the BATCH_SIZE most overdue URLs across all services
  2. For each URL:
     a. Make a conditional GET (If-Modified-Since / If-None-Match)
     b. 304 Not Modified → update last_checked, advance next_check, done
     c. 200 OK → rechunk page, diff against old chunk_ids, delete stale vectors
        from S3 Vectors, embed and write new chunks, update DynamoDB

Note on first-run behaviour: URLs seeded with empty last_modified/etag will
always return 200 on the first fetch (no conditional headers). This means
the first full cycle through the corpus re-embeds every page regardless of
whether it changed. This is unavoidable given the seeding approach and is
acceptable — it only happens once per URL.

Note: S3 chunk text files for removed chunks are NOT deleted. Chunk IDs may
be shared across URLs, so deletion requires reference counting which we
intentionally avoid. Orphaned text files cost fractions of a cent.

Environment variables (set by Terraform):
    URL_TABLE_NAME      - DynamoDB table name
    VECTOR_BUCKET_NAME  - S3 Vectors bucket name
    VECTOR_INDEX_NAME   - S3 Vectors index name
    CHUNK_BUCKET_NAME   - S3 bucket storing chunk text at chunks/{chunk_id}.txt
    EMBEDDING_MODEL_ID  - Bedrock embedding model ID
    BATCH_SIZE          - Number of URLs to process per invocation (default: 50)
    CRAWL_DELAY_SECONDS - Delay between HTTP requests (default: 2)
"""

import hashlib
import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Key
from bs4 import BeautifulSoup

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

URL_TABLE_NAME     = os.environ["URL_TABLE_NAME"]
VECTOR_BUCKET_NAME = os.environ["VECTOR_BUCKET_NAME"]
VECTOR_INDEX_NAME  = os.environ["VECTOR_INDEX_NAME"]
CHUNK_BUCKET_NAME  = os.environ["CHUNK_BUCKET_NAME"]
EMBEDDING_MODEL_ID = os.environ["EMBEDDING_MODEL_ID"]
BATCH_SIZE         = int(os.environ.get("BATCH_SIZE", "50"))
CRAWL_DELAY        = int(os.environ.get("CRAWL_DELAY_SECONDS", "2"))

# How far into the future to set next_check after processing a URL.
# 35 days × 50 URLs × 4 runs/day = ~7,000 URLs/day coverage.
# At 4,583 URLs this cycles the full corpus in under a day when running
# normally, giving us far more frequent refreshes than the 35-day interval
# suggests. The interval just controls when each individual URL is next due.
REFRESH_INTERVAL_DAYS = 35

# Chunking settings — must match scrape_docs.py exactly
MAX_TOKENS     = 512
OVERLAP_TOKENS = 50
MIN_WORDS      = 100

# All services we track — must match SERVICES in scrape_docs.py
SERVICES = [
    "lambda", "s3", "dynamodb", "apigateway",
    "bedrock", "eventbridge", "iam", "ec2",
]

# Stop processing new URLs if fewer than this many seconds remain in the
# Lambda execution. Prevents being killed mid-page-processing.
TIMEOUT_BUFFER_SECONDS = 30

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
s3v      = boto3.client("s3vectors",      region_name="us-east-1")
s3       = boto3.client("s3",             region_name="us-east-1")
bedrock  = boto3.client("bedrock-runtime", region_name="us-east-1")
table    = dynamodb.Table(URL_TABLE_NAME)

# ---------------------------------------------------------------------------
# DynamoDB — fetch overdue URLs
# ---------------------------------------------------------------------------

def get_overdue_urls(batch_size: int) -> list:
    """
    Query the GSI for each service to find overdue URLs (next_check <= now),
    merge results, and return the BATCH_SIZE most overdue overall.

    The GSI has 'service' as partition key and 'next_check' as range key,
    so we must query each service partition separately and merge.
    """
    now        = datetime.now(timezone.utc).isoformat()
    candidates = []

    for service in SERVICES:
        try:
            resp = table.query(
                IndexName="service-next_check-index",
                KeyConditionExpression=(
                    Key("service").eq(service) &
                    Key("next_check").lte(now)
                ),
                Limit=batch_size,
            )
            candidates.extend(resp.get("Items", []))
        except Exception as e:
            logger.error(f"Failed to query overdue URLs for {service}: {e}")

    # Sort by next_check ascending (most overdue first), take top batch_size
    candidates.sort(key=lambda x: x.get("next_check", ""))
    selected = candidates[:batch_size]
    logger.info(f"Found {len(selected)} overdue URLs to process")
    return selected


# ---------------------------------------------------------------------------
# DynamoDB — update after processing
# ---------------------------------------------------------------------------

def update_url_unchanged(url: str, last_checked: str, next_check: str) -> None:
    """Update timestamps after a 304 response — content unchanged."""
    try:
        table.update_item(
            Key={"url": url},
            UpdateExpression="SET last_checked = :lc, next_check = :nc",
            ExpressionAttributeValues={":lc": last_checked, ":nc": next_check},
        )
    except Exception as e:
        logger.error(f"Failed to update unchanged URL {url}: {e}")


def update_url_changed(
    url: str,
    last_checked: str,
    next_check: str,
    last_modified: str,
    etag: str,
    chunk_ids: set,
) -> None:
    """Update all fields after a 200 response — content changed."""
    try:
        if chunk_ids:
            table.update_item(
                Key={"url": url},
                UpdateExpression=(
                    "SET last_checked = :lc, next_check = :nc, "
                    "last_modified = :lm, etag = :et, chunk_ids = :ci"
                ),
                ExpressionAttributeValues={
                    ":lc": last_checked,
                    ":nc": next_check,
                    ":lm": last_modified,
                    ":et": etag,
                    ":ci": chunk_ids,
                },
            )
        else:
            # DynamoDB rejects empty String Sets — remove the attribute
            table.update_item(
                Key={"url": url},
                UpdateExpression=(
                    "SET last_checked = :lc, next_check = :nc, "
                    "last_modified = :lm, etag = :et "
                    "REMOVE chunk_ids"
                ),
                ExpressionAttributeValues={
                    ":lc": last_checked,
                    ":nc": next_check,
                    ":lm": last_modified,
                    ":et": etag,
                },
            )
    except Exception as e:
        logger.error(f"Failed to update changed URL {url}: {e}")


# ---------------------------------------------------------------------------
# HTTP fetch with conditional request
# ---------------------------------------------------------------------------

def fetch_page(url: str, last_modified: str, etag: str) -> tuple:
    """
    Fetch a page with conditional headers.
    Returns (status_code, html, last_modified, etag).
    html is None for non-200 responses.
    status_code is 0 on network error.
    """
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
        }
    )

    if last_modified:
        req.add_header("If-Modified-Since", last_modified)
    if etag:
        req.add_header("If-None-Match", etag)

    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            html          = resp.read().decode("utf-8", errors="replace")
            resp_modified = resp.headers.get("Last-Modified", "")
            resp_etag     = resp.headers.get("ETag", "")
            return 200, html, resp_modified, resp_etag
    except urllib.error.HTTPError as e:
        if e.code == 304:
            return 304, None, last_modified, etag
        logger.warning(f"HTTP {e.code} for {url}")
        return e.code, None, last_modified, etag
    except Exception as e:
        logger.warning(f"Network error fetching {url}: {e}")
        return 0, None, last_modified, etag


# ---------------------------------------------------------------------------
# Content extraction — mirrors scrape_docs.py exactly
# ---------------------------------------------------------------------------

def extract_content(html: str, url: str):
    """
    Extract (page_title, [(heading, text), ...]) from HTML.
    Returns None if the page is too short to be useful.
    Logic must match scrape_docs.py exactly to produce identical chunks.
    """
    soup = BeautifulSoup(html, "html.parser")

    title_tag  = soup.find("title")
    page_title = title_tag.get_text(strip=True) if title_tag else url

    for selector in [
        "nav", "header", "footer",
        ".breadcrumb", ".awsdocs-breadcrumb",
        ".feedback", ".page-feedback",
        "#feedback", "#awsdocs-body-feedback",
        ".awsdocs-sidebar", "#left-column",
        ".awsdocs-header", "#awsdocs-header",
        "#aws-nav", "#aws-page-header",
        ".warning", "script", "style",
        "[data-testid='feedback']",
    ]:
        for el in soup.select(selector):
            el.decompose()

    for el in soup.find_all(["pre", "code"]):
        el.decompose()

    main = (
        soup.find("main") or
        soup.find(id="main-content") or
        soup.find(id="main") or
        soup.find(class_="main-content") or
        soup.find("article") or
        soup.find("body")
    )

    if not main:
        return None

    sections        = []
    current_heading = page_title
    current_lines   = []

    for el in main.find_all(["h1", "h2", "h3", "p", "li", "td"]):
        if el.name in ("h1", "h2", "h3"):
            text = "\n".join(current_lines).strip()
            if text:
                sections.append((current_heading, text))
            current_heading = el.get_text(strip=True)
            current_lines   = []
        else:
            text = el.get_text(separator=" ", strip=True)
            if text:
                current_lines.append(text)

    text = "\n".join(current_lines).strip()
    if text:
        sections.append((current_heading, text))

    sections = [(h, t) for h, t in sections if len(t.split()) >= 20]

    if not sections:
        return None

    if sum(len(t.split()) for _, t in sections) < MIN_WORDS:
        return None

    return page_title, sections


# ---------------------------------------------------------------------------
# Chunking — mirrors scrape_docs.py exactly
# ---------------------------------------------------------------------------

def count_tokens(text: str) -> int:
    return len(text) // 4


def chunk_section(
    heading: str, text: str, service: str, source_url: str, page_title: str
) -> list:
    """
    Split a section into chunks. Chunk IDs are MD5 hashes of chunk text,
    matching the approach used in scrape_docs.py and embed_and_ingest.py.
    """
    chunks        = []
    prefix        = f"{heading}\n\n"
    max_chars     = MAX_TOKENS * 4
    overlap_chars = OVERLAP_TOKENS * 4
    effective_max = max_chars - len(prefix)

    if not text:
        return []

    if len(text) <= effective_max:
        chunk_text = prefix + text
        chunk_hash = hashlib.md5(chunk_text.encode()).hexdigest()
        chunks.append({
            "chunk_id":   chunk_hash,
            "chunk_hash": chunk_hash,
            "service":    service,
            "source_url": source_url,
            "page_title": page_title,
            "heading":    heading,
            "text":       chunk_text,
        })
        return chunks

    words      = text.split()
    start_word = 0

    while start_word < len(words):
        end_word      = start_word
        current_chars = 0
        while end_word < len(words):
            word_chars = len(words[end_word]) + 1
            if current_chars + word_chars > effective_max:
                break
            current_chars += word_chars
            end_word      += 1

        if end_word == start_word:
            end_word = start_word + 1

        chunk_text = prefix + " ".join(words[start_word:end_word])
        chunk_hash = hashlib.md5(chunk_text.encode()).hexdigest()
        chunks.append({
            "chunk_id":   chunk_hash,
            "chunk_hash": chunk_hash,
            "service":    service,
            "source_url": source_url,
            "page_title": page_title,
            "heading":    heading,
            "text":       chunk_text,
        })

        chunk_word_count = end_word - start_word
        avg_word_chars   = max(1, current_chars // max(1, chunk_word_count))
        overlap_words    = overlap_chars // max(1, avg_word_chars)
        overlap_words    = min(overlap_words, chunk_word_count // 4)
        advance          = max(1, chunk_word_count - overlap_words)
        start_word      += advance

    return chunks


def chunks_for_page(url: str, service: str, html: str) -> list:
    """
    Extract, chunk, and deduplicate all chunks for a page.
    Deduplication by chunk_id prevents duplicate key errors in S3 Vectors.
    """
    result = extract_content(html, url)
    if result is None:
        return []

    page_title, sections = result
    all_chunks = []
    for heading, text in sections:
        all_chunks.extend(chunk_section(heading, text, service, url, page_title))

    # Deduplicate by chunk_id — keep last occurrence (matching ingest behaviour)
    seen = {}
    for chunk in all_chunks:
        seen[chunk["chunk_id"]] = chunk
    return list(seen.values())


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_text(text: str) -> list:
    """Embed text using Bedrock Titan Text Embeddings V2."""
    resp = bedrock.invoke_model(
        modelId=EMBEDDING_MODEL_ID,
        body=json.dumps({"inputText": text}),
        contentType="application/json",
        accept="application/json",
    )
    return json.loads(resp["body"].read())["embedding"]


# ---------------------------------------------------------------------------
# S3 Vectors operations
# ---------------------------------------------------------------------------

def put_vectors(chunks: list) -> None:
    """
    Write chunks to S3 Vectors. Deduplicates by key before writing to avoid
    ValidationException on duplicate keys.
    """
    if not chunks:
        return

    # Deduplicate by key (should already be done upstream, but be safe)
    seen = {}
    for c in chunks:
        seen[c["chunk_id"]] = c
    deduped = list(seen.values())

    vectors = [
        {
            "key":  c["chunk_id"],
            "data": {"float32": c["embedding"]},
            "metadata": {
                "service":    c["service"],
                "source_url": c["source_url"],
                "heading":    c["heading"],
                "chunk_hash": c["chunk_hash"],
                "page_title": c["page_title"],
            },
        }
        for c in deduped
    ]
    s3v.put_vectors(
        vectorBucketName=VECTOR_BUCKET_NAME,
        indexName=VECTOR_INDEX_NAME,
        vectors=vectors,
    )


def delete_vectors(chunk_ids: set) -> None:
    """Delete vectors from S3 Vectors. Logs but does not raise on failure."""
    if not chunk_ids:
        return
    try:
        s3v.delete_vectors(
            vectorBucketName=VECTOR_BUCKET_NAME,
            indexName=VECTOR_INDEX_NAME,
            keys=list(chunk_ids),
        )
        logger.info(f"Deleted {len(chunk_ids)} stale vectors")
    except Exception as e:
        logger.error(f"Failed to delete vectors: {e}")


# ---------------------------------------------------------------------------
# S3 chunk text
# ---------------------------------------------------------------------------

def write_chunk_text(chunk_id: str, text: str) -> None:
    """Write chunk text to S3 data bucket."""
    try:
        s3.put_object(
            Bucket=CHUNK_BUCKET_NAME,
            Key=f"chunks/{chunk_id}.txt",
            Body=text.encode("utf-8"),
            ContentType="text/plain; charset=utf-8",
        )
    except Exception as e:
        logger.error(f"Failed to write chunk text {chunk_id}: {e}")


# ---------------------------------------------------------------------------
# Process a single changed page
# ---------------------------------------------------------------------------

def process_changed_page(item: dict, html: str) -> set:
    """
    Rechunk a changed page, diff against old chunk_ids, update S3 Vectors
    and S3 chunk text. Returns the new set of chunk_ids.
    """
    url     = item["url"]
    service = item.get("service", "")
    old_ids = set(item.get("chunk_ids", set()))

    new_chunks = chunks_for_page(url, service, html)
    new_ids    = {c["chunk_id"] for c in new_chunks}

    to_delete = old_ids - new_ids
    to_add    = [c for c in new_chunks if c["chunk_id"] not in old_ids]

    logger.info(
        f"  Chunks: {len(new_chunks)} total, "
        f"{len(to_add)} new, {len(to_delete)} stale"
    )

    # Delete stale vectors
    if to_delete:
        delete_vectors(to_delete)

    # Embed and write new chunks
    if to_add:
        embeddable = []
        for chunk in to_add:
            try:
                chunk["embedding"] = embed_text(chunk["text"])
                embeddable.append(chunk)
            except Exception as e:
                logger.error(
                    f"Failed to embed chunk {chunk['chunk_id']}: {e}"
                )

        if embeddable:
            try:
                put_vectors(embeddable)
            except Exception as e:
                logger.error(f"Failed to put vectors for {url}: {e}")

            for chunk in embeddable:
                write_chunk_text(chunk["chunk_id"], chunk["text"])

    return new_ids


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    logger.info(f"Refresh Lambda started. Batch size: {BATCH_SIZE}")

    now        = datetime.now(timezone.utc)
    next_check = (now + timedelta(days=REFRESH_INTERVAL_DAYS)).isoformat()
    now_str    = now.isoformat()

    items = get_overdue_urls(BATCH_SIZE)

    stats = {"unchanged": 0, "changed": 0, "errors": 0, "skipped": 0}

    for i, item in enumerate(items):
        # Check remaining execution time before processing each URL
        remaining_ms = context.get_remaining_time_in_millis()
        if remaining_ms < TIMEOUT_BUFFER_SECONDS * 1000:
            logger.warning(
                f"Only {remaining_ms}ms remaining — stopping early after "
                f"{i} of {len(items)} URLs to avoid timeout"
            )
            break

        url           = item["url"]
        last_modified = item.get("last_modified", "")
        etag          = item.get("etag", "")

        logger.info(f"[{i+1}/{len(items)}] {url}")

        status, html, resp_modified, resp_etag = fetch_page(url, last_modified, etag)

        if status == 304:
            update_url_unchanged(url, now_str, next_check)
            stats["unchanged"] += 1

        elif status == 200:
            try:
                new_chunk_ids = process_changed_page(item, html)
                update_url_changed(
                    url, now_str, next_check,
                    resp_modified, resp_etag, new_chunk_ids,
                )
                stats["changed"] += 1
            except Exception as e:
                logger.error(f"Failed to process {url}: {e}")
                stats["errors"] += 1

        elif status == 0:
            # Network error — leave next_check unchanged so it retries
            logger.warning(f"Network error for {url}, will retry next run")
            stats["errors"] += 1

        else:
            # Unexpected status (404, 403, etc.) — advance to avoid hammering
            logger.warning(f"HTTP {status} for {url}, advancing schedule")
            update_url_unchanged(url, now_str, next_check)
            stats["skipped"] += 1

        if i < len(items) - 1:
            time.sleep(CRAWL_DELAY)

    logger.info(
        f"Refresh complete. Unchanged: {stats['unchanged']}, "
        f"Changed: {stats['changed']}, Errors: {stats['errors']}, "
        f"Skipped: {stats['skipped']}"
    )

    return {
        "statusCode": 200,
        "processed":  sum(stats.values()),
        **stats,
    }
