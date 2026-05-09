"""
Seed URL Table
===============
One-time script that reads the scraped chunk JSON files and the ingest
progress file to populate the DynamoDB URL queue table with one item per
unique source URL.

Uses ingest_progress.json as the source of truth for which chunk IDs are
actually in S3 Vectors. This is important because the ingest script
deduplicates chunk IDs before writing — some chunk IDs in the JSON files
were never written to S3 Vectors. Building chunk_ids from ingest_progress.json
ensures the DynamoDB records accurately reflect what's in S3 Vectors.

Each item contains:
    url           - the source URL (partition key)
    service       - the AWS service name
    chunk_ids     - String Set of chunk IDs actually in S3 Vectors for this URL
    next_check    - ISO timestamp set to now (triggers immediate refresh eligibility)
    last_checked  - empty string (never checked)
    last_modified - empty string (no Last-Modified header known yet)
    etag          - empty string (no ETag known yet)

Idempotent by default: URLs already in the table are skipped, preserving
any last_modified/etag values the Refresh Lambda has already populated.
Use --overwrite to reset all items.

Usage:
    python seed_url_table.py --input-dir test_output

    # Force overwrite of all existing items:
    python seed_url_table.py --input-dir test_output --overwrite

Requirements:
    pip install boto3
"""

import argparse
import json
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TABLE_NAME         = "aws-docs-rag-url-queue"
AWS_PROFILE        = "aws-docs-rag-dev"
AWS_REGION         = "us-east-1"
PROGRESS_FILE      = "ingest_progress.json"

PROGRESS_INTERVAL  = 100


# ---------------------------------------------------------------------------
# Load ingested chunk IDs from progress file
# ---------------------------------------------------------------------------

def load_ingested_ids(progress_file: str) -> set:
    """
    Load the set of chunk IDs that were actually written to S3 Vectors
    from the ingest progress file.
    """
    if not os.path.exists(progress_file):
        print(f"[ERROR] Progress file not found: {progress_file}")
        print("Make sure ingest_progress.json exists before running this script.")
        return set()

    with open(progress_file, "r") as f:
        data = json.load(f)

    ingested = set(data.get("ingested_chunk_ids", []))
    print(f"  Loaded {len(ingested):,} ingested chunk IDs from {progress_file}")
    return ingested


# ---------------------------------------------------------------------------
# Load chunks and build URL -> chunk_ids mapping
# ---------------------------------------------------------------------------

def load_url_chunk_map(input_dir: str, ingested_ids: set) -> dict:
    """
    Read all *_chunks.json files and build a mapping of:
        source_url -> {
            "service":   str,
            "chunk_ids": set of str  (only IDs confirmed in S3 Vectors)
        }

    Only includes chunk IDs that are in ingested_ids, ensuring the
    DynamoDB records accurately reflect what's actually in S3 Vectors.
    """
    files = [
        os.path.join(input_dir, f)
        for f in sorted(os.listdir(input_dir))
        if f.endswith("_chunks.json")
    ]

    if not files:
        print(f"[ERROR] No *_chunks.json files found in {input_dir}")
        return {}

    url_map      = defaultdict(lambda: {"service": "", "chunk_ids": set()})
    total_chunks = 0
    included     = 0
    excluded     = 0

    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        chunks  = data.get("chunks", [])
        service = data.get("metadata", {}).get("services", ["unknown"])[0]
        print(f"  {path}: {len(chunks):,} chunks (service: {service})")
        total_chunks += len(chunks)

        for chunk in chunks:
            url      = chunk.get("source_url", "")
            chunk_id = chunk.get("chunk_id", "")
            if not url or not chunk_id:
                continue

            if chunk_id in ingested_ids:
                url_map[url]["service"] = chunk.get("service", service)
                url_map[url]["chunk_ids"].add(chunk_id)
                included += 1
            else:
                excluded += 1

    # Remove any URLs with empty chunk_id sets (DynamoDB rejects empty String Sets)
    # This shouldn't happen in practice but guard against it
    empty_urls = [url for url, info in url_map.items() if not info["chunk_ids"]]
    if empty_urls:
        print(f"  [WARN] Dropping {len(empty_urls)} URLs with no ingested chunk IDs")
        for url in empty_urls:
            del url_map[url]

    print(f"\n  Total chunks in JSON files:     {total_chunks:,}")
    print(f"  Chunks confirmed in S3 Vectors: {included:,}")
    print(f"  Chunks excluded (not ingested): {excluded:,} "
          f"(duplicates collapsed during ingest)")

    return dict(url_map)


# ---------------------------------------------------------------------------
# Write to DynamoDB
# ---------------------------------------------------------------------------

def seed_table(url_map: dict, table, overwrite: bool) -> tuple:
    """
    Write one DynamoDB item per URL.

    overwrite=False (default): skips URLs already in the table using
        attribute_not_exists condition, preserving last_modified/etag.
    overwrite=True: unconditionally replaces all items.

    Returns (written, skipped, errors).
    """
    now     = datetime.now(timezone.utc).isoformat()
    urls    = list(url_map.items())
    written = 0
    skipped = 0
    errors  = 0

    mode = "overwrite all" if overwrite else "skip existing"
    print(f"\nWriting {len(urls):,} URLs to '{TABLE_NAME}' (mode: {mode})...")

    for i, (url, info) in enumerate(urls, 1):
        item = {
            "url":           url,
            "service":       info["service"],
            "chunk_ids":     info["chunk_ids"],
            "next_check":    now,
            "last_checked":  "",
            "last_modified": "",
            "etag":          "",
        }

        try:
            if overwrite:
                table.put_item(Item=item)
            else:
                table.put_item(
                    Item=item,
                    ConditionExpression="attribute_not_exists(#u)",
                    ExpressionAttributeNames={"#u": "url"},
                )
            written += 1
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                skipped += 1
            else:
                print(f"  [ERROR] Failed to write {url}: {e}")
                errors += 1

        if i % PROGRESS_INTERVAL == 0:
            print(f"  [{i:>6}/{len(urls):,}] processed "
                  f"(written: {written}, skipped: {skipped}, errors: {errors})")

    return written, skipped, errors


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Seed DynamoDB URL queue table from chunk JSON files"
    )
    parser.add_argument(
        "--input-dir",
        default="test_output",
        help="Directory containing *_chunks.json files (default: test_output)",
    )
    parser.add_argument(
        "--progress-file",
        default=PROGRESS_FILE,
        help=f"Path to ingest progress file (default: {PROGRESS_FILE})",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Overwrite existing items, resetting last_modified and etag. "
            "Default behaviour skips URLs already in the table."
        ),
    )
    args = parser.parse_args()

    # Load ingested chunk IDs from progress file
    print(f"Loading ingest progress from {args.progress_file}...")
    ingested_ids = load_ingested_ids(args.progress_file)
    if not ingested_ids:
        return

    # Build URL -> chunk_ids map using only confirmed ingested IDs
    print(f"\nLoading chunks from {args.input_dir}/...")
    url_map = load_url_chunk_map(args.input_dir, ingested_ids)
    if not url_map:
        return

    total_urls   = len(url_map)
    total_chunks = sum(len(v["chunk_ids"]) for v in url_map.values())
    print(f"\nUnique URLs:              {total_urls:,}")
    print(f"Total chunk ID references: {total_chunks:,}")

    service_counts = Counter(v["service"] for v in url_map.values())
    print("\nURL count by service:")
    for service, count in sorted(service_counts.items()):
        print(f"  {service:<15} {count:>5,} URLs")

    # Connect to DynamoDB
    session  = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    dynamodb = session.resource("dynamodb")
    table    = dynamodb.Table(TABLE_NAME)

    started = datetime.now()
    written, skipped, errors = seed_table(url_map, table, args.overwrite)
    elapsed = datetime.now() - started

    print(f"\n{'='*60}")
    print(f"COMPLETE")
    print(f"{'='*60}")
    print(f"Written:  {written:,}")
    print(f"Skipped:  {skipped:,} (already existed, preserved last_modified/etag)")
    print(f"Errors:   {errors}")
    print(f"Elapsed:  {elapsed}")

    if errors == 0:
        print(f"\nDynamoDB URL queue is ready.")
        if skipped > 0:
            print(f"Re-run with --overwrite to reset all items if needed.")


if __name__ == "__main__":
    main()
