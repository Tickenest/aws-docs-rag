"""
Upload Chunks to S3
====================
One-time script that reads the scraped chunk JSON files and uploads each
chunk's text to S3 at chunks/{chunk_id}.txt.

This is needed because the initial ingest wrote vectors to S3 Vectors but
did not write chunk text to S3. The Query Lambda needs the chunk text to
provide context to Claude Haiku for answer generation.

Future ingest runs (embed_and_ingest.py) will handle this automatically.

Usage:
    python upload_chunks_to_s3.py --input-dir test_output

Requirements:
    pip install boto3
"""

import argparse
import json
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_BUCKET_NAME = "aws-docs-rag-data-9cfae7c1"
CHUNK_PREFIX     = "chunks/"
AWS_PROFILE      = "aws-docs-rag-dev"
AWS_REGION       = "us-east-1"

# Number of chunks to upload before printing progress
PROGRESS_INTERVAL = 500


# ---------------------------------------------------------------------------
# Load chunks
# ---------------------------------------------------------------------------

def load_chunks(input_dir: str) -> list:
    """Load all chunks from *_chunks.json files in input_dir."""
    all_chunks = []
    files = [
        os.path.join(input_dir, f)
        for f in sorted(os.listdir(input_dir))
        if f.endswith("_chunks.json")
    ]
    if not files:
        print(f"[ERROR] No *_chunks.json files found in {input_dir}")
        return []

    for path in files:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        chunks = data.get("chunks", [])
        service = data.get("metadata", {}).get("services", ["unknown"])
        print(f"  {path}: {len(chunks):,} chunks ({service})")
        all_chunks.extend(chunks)

    return all_chunks


# ---------------------------------------------------------------------------
# Upload
# ---------------------------------------------------------------------------

def upload_chunks(chunks: list, s3_client) -> tuple:
    """
    Upload each chunk's text to S3 at chunks/{chunk_id}.txt.
    Deduplicates by chunk_id — identical chunks share the same key so only
    one upload is needed.
    Returns (uploaded, skipped, errors).
    """
    # Deduplicate by chunk_id
    seen = {}
    for chunk in chunks:
        seen[chunk["chunk_id"]] = chunk
    unique_chunks = list(seen.values())
    duplicates = len(chunks) - len(unique_chunks)

    print(f"\nTotal chunks:     {len(chunks):,}")
    print(f"Unique chunk IDs: {len(unique_chunks):,} ({duplicates:,} duplicates skipped)")
    print(f"Uploading to:     s3://{DATA_BUCKET_NAME}/{CHUNK_PREFIX}")
    print()

    uploaded = 0
    errors   = 0
    started  = datetime.now()

    for i, chunk in enumerate(unique_chunks, 1):
        key  = f"{CHUNK_PREFIX}{chunk['chunk_id']}.txt"
        text = chunk["text"]

        try:
            s3_client.put_object(
                Bucket=DATA_BUCKET_NAME,
                Key=key,
                Body=text.encode("utf-8"),
                ContentType="text/plain; charset=utf-8",
            )
            uploaded += 1
        except ClientError as e:
            print(f"  [ERROR] Failed to upload {key}: {e}")
            errors += 1

        if i % PROGRESS_INTERVAL == 0:
            elapsed = (datetime.now() - started).total_seconds()
            rate    = i / elapsed if elapsed > 0 else 0
            eta     = (len(unique_chunks) - i) / rate if rate > 0 else 0
            print(f"  [{i:>6}/{len(unique_chunks):,}] uploaded | "
                  f"{rate:.0f} chunks/s | ETA {eta:.0f}s")

    elapsed = datetime.now() - started
    return uploaded, duplicates, errors, elapsed


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Upload chunk text to S3 data bucket"
    )
    parser.add_argument(
        "--input-dir",
        default="test_output",
        help="Directory containing *_chunks.json files (default: test_output)",
    )
    args = parser.parse_args()

    print(f"Loading chunks from {args.input_dir}/...")
    chunks = load_chunks(args.input_dir)
    if not chunks:
        return

    print(f"Total chunks loaded: {len(chunks):,}\n")

    session  = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    s3_client = session.client("s3")

    uploaded, duplicates, errors, elapsed = upload_chunks(chunks, s3_client)

    print(f"\n{'='*60}")
    print(f"COMPLETE")
    print(f"{'='*60}")
    print(f"Uploaded:   {uploaded:,}")
    print(f"Duplicates: {duplicates:,} (skipped - same content already uploaded)")
    print(f"Errors:     {errors}")
    print(f"Elapsed:    {elapsed}")
    print(f"\nChunk text is now available at s3://{DATA_BUCKET_NAME}/{CHUNK_PREFIX}")


if __name__ == "__main__":
    main()
