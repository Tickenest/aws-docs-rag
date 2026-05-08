"""
Embed and Ingest Script
========================
Reads scraped chunk JSON files, generates embeddings using Amazon Bedrock
Titan Text Embeddings V2, and writes vectors to S3 Vectors.

Features:
- Resume support: tracks ingested chunk IDs in a local progress file so the
  script can be restarted after a failure without re-embedding already-written
  chunks or incurring duplicate Bedrock costs.
- Consecutive error abort: exits cleanly after MAX_CONSECUTIVE_ERRORS in a row
  rather than silently continuing with gaps in the ingested data.
- Failed chunks file: records the chunk ID and error for every chunk that fails
  so they can be retried later with --retry-failed.
- Final verification: after ingest completes, compares the ingested count
  against the expected total and reports any discrepancy clearly.
- Dry run: use --dry-run to see chunk counts and estimated cost without
  calling Bedrock or writing to S3 Vectors.
- Batch writes: vectors are written to S3 Vectors in batches of 100.

Usage:
    # Ingest all services
    python embed_and_ingest.py --input-dir test_output

    # Ingest a single service
    python embed_and_ingest.py --input-files test_output/lambda_chunks.json

    # Dry run - show counts and estimated cost only
    python embed_and_ingest.py --input-dir test_output --dry-run

    # Resume after failure (automatic - just re-run the same command)
    python embed_and_ingest.py --input-dir test_output

    # Retry only previously failed chunks
    python embed_and_ingest.py --input-dir test_output --retry-failed

Requirements:
    pip install boto3
"""

import argparse
import json
import os
import time
from collections import Counter
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration — update these to match your Terraform outputs
# ---------------------------------------------------------------------------

VECTOR_BUCKET_NAME = "docs-rag-vectors-9cfae7c1"
VECTOR_INDEX_NAME  = "docs-rag-index"
EMBEDDING_MODEL_ID = "amazon.titan-embed-text-v2:0"
AWS_PROFILE        = "aws-docs-rag-dev"
AWS_REGION         = "us-east-1"

# Batch size for PutVectors API calls (max 500, we use 100 for safety)
PUT_BATCH_SIZE = 100

# Delay between Bedrock embedding calls (seconds) to avoid throttling
EMBED_DELAY = 0.1

# Abort after this many consecutive errors (embed or put)
MAX_CONSECUTIVE_ERRORS = 5

# Default file paths
PROGRESS_FILE      = "ingest_progress.json"
FAILED_CHUNKS_FILE = "ingest_failed.json"

# Estimated cost per 1000 tokens for Titan Text Embeddings V2
COST_PER_1K_TOKENS = 0.00002


# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------

def get_clients():
    session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)
    bedrock = session.client("bedrock-runtime")
    s3v     = session.client("s3vectors")
    return bedrock, s3v


# ---------------------------------------------------------------------------
# Progress tracking
# ---------------------------------------------------------------------------

def load_progress(progress_file: str = PROGRESS_FILE) -> set:
    """Load set of already-ingested chunk IDs from progress file."""
    if not os.path.exists(progress_file):
        return set()
    with open(progress_file, "r") as f:
        data = json.load(f)
    ingested = set(data.get("ingested_chunk_ids", []))
    print(f"Resuming: {len(ingested):,} chunks already ingested (loaded from {progress_file})")
    return ingested


def save_progress(ingested: set, progress_file: str = PROGRESS_FILE) -> None:
    """Persist set of ingested chunk IDs to progress file."""
    with open(progress_file, "w") as f:
        json.dump({
            "ingested_chunk_ids": list(ingested),
            "count": len(ingested),
            "updated_at": datetime.now().isoformat(),
        }, f)


# ---------------------------------------------------------------------------
# Failed chunks tracking
# ---------------------------------------------------------------------------

def load_failed(failed_file: str = FAILED_CHUNKS_FILE) -> dict:
    """Load dict of {chunk_id: error_message} for previously failed chunks."""
    if not os.path.exists(failed_file):
        return {}
    with open(failed_file, "r") as f:
        data = json.load(f)
    return data.get("failed", {})


def save_failed(failed: dict, failed_file: str = FAILED_CHUNKS_FILE) -> None:
    """Persist failed chunk IDs and their error messages."""
    with open(failed_file, "w") as f:
        json.dump({
            "failed": failed,
            "count": len(failed),
            "updated_at": datetime.now().isoformat(),
        }, f, indent=2)


# ---------------------------------------------------------------------------
# Load chunks from JSON files
# ---------------------------------------------------------------------------

def load_chunks(input_files: list) -> list:
    """Load and return all chunks from the given JSON files."""
    all_chunks = []
    for path in input_files:
        if not os.path.exists(path):
            print(f"  [WARN] File not found: {path}")
            continue
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        chunks = data.get("chunks", [])
        service = data.get("metadata", {}).get("services", ["unknown"])
        print(f"  {path}: {len(chunks):,} chunks ({service})")
        all_chunks.extend(chunks)
    return all_chunks


def resolve_input_files(input_dir: str, input_files: list) -> list:
    """Return list of chunk JSON file paths to process."""
    if input_files:
        return input_files
    files = [
        os.path.join(input_dir, f)
        for f in sorted(os.listdir(input_dir))
        if f.endswith("_chunks.json")
    ]
    if not files:
        print(f"[ERROR] No *_chunks.json files found in {input_dir}")
    return files


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_text(bedrock_client, text: str) -> list:
    """
    Call Bedrock Titan Text Embeddings V2 and return the embedding vector.
    Returns a list of 1024 floats.
    """
    body = json.dumps({"inputText": text})
    response = bedrock_client.invoke_model(
        modelId=EMBEDDING_MODEL_ID,
        body=body,
        contentType="application/json",
        accept="application/json",
    )
    result = json.loads(response["body"].read())
    return result["embedding"]


# ---------------------------------------------------------------------------
# S3 Vectors write
# ---------------------------------------------------------------------------

def put_vectors_batch(s3v_client, vectors: list) -> list:
    """
    Write a batch of vectors to S3 Vectors.
    Each item in vectors is a dict with keys: key, data (list of floats), metadata.

    Deduplicates by key before writing — S3 Vectors rejects batches with
    duplicate keys even when the vectors are identical. Duplicates arise when
    two chunks have identical text and therefore the same content-hash chunk_id.
    We keep the last occurrence, which is fine since the content is identical.

    Returns the list of keys actually written (after deduplication).
    """
    # Deduplicate by key, keeping last occurrence
    seen = {}
    for v in vectors:
        seen[v["key"]] = v
    deduped = list(seen.values())

    if len(deduped) < len(vectors):
        print(f"    [INFO] Deduplicated {len(vectors) - len(deduped)} duplicate key(s) in batch")

    formatted = [
        {
            "key":      v["key"],
            "data":     {"float32": v["data"]},
            "metadata": v["metadata"],
        }
        for v in deduped
    ]
    s3v_client.put_vectors(
        vectorBucketName=VECTOR_BUCKET_NAME,
        indexName=VECTOR_INDEX_NAME,
        vectors=formatted,
    )
    return [v["key"] for v in deduped]


# ---------------------------------------------------------------------------
# Main ingest loop
# ---------------------------------------------------------------------------

def ingest(
    chunks: list,
    ingested: set,
    bedrock,
    s3v,
    progress_file: str = PROGRESS_FILE,
    failed_file: str = FAILED_CHUNKS_FILE,
) -> tuple:
    """
    Embed and ingest chunks that haven't been ingested yet.

    Returns (ingested: set, failed: dict) where failed maps chunk_id to error message.

    Aborts cleanly after MAX_CONSECUTIVE_ERRORS consecutive failures to avoid
    silently producing a swiss-cheese index.
    """
    pending = [c for c in chunks if c["chunk_id"] not in ingested]
    total   = len(pending)

    if total == 0:
        print("All chunks already ingested. Nothing to do.")
        return ingested, {}

    # Load any previously failed chunks so we can update the record
    failed = load_failed(failed_file)

    print(f"\nChunks to ingest: {total:,}")
    print(f"Estimated Bedrock cost: ${total * 250 * COST_PER_1K_TOKENS / 1000:.4f} "
          f"(assuming ~250 tokens/chunk average)")
    print(f"Abort threshold: {MAX_CONSECUTIVE_ERRORS} consecutive errors")
    print()

    batch               = []
    batch_chunk_ids     = []  # track chunk IDs corresponding to current batch
    total_errors        = 0
    total_deduped       = 0   # chunks dropped as exact duplicates
    consecutive_errors  = 0
    start               = datetime.now()
    aborted             = False

    for i, chunk in enumerate(pending, 1):
        # Embed
        try:
            vector = embed_text(bedrock, chunk["text"])
            consecutive_errors = 0  # reset on success
        except ClientError as e:
            error_msg = str(e)
            print(f"  [{i:>6}/{total}] EMBED ERROR: {error_msg}")
            failed[chunk["chunk_id"]] = f"embed: {error_msg}"
            total_errors += 1
            consecutive_errors += 1
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                print(f"\n[ABORT] {MAX_CONSECUTIVE_ERRORS} consecutive errors — stopping to avoid "
                      f"data gaps. Fix the issue and re-run to resume.")
                aborted = True
                break
            time.sleep(2)  # back off on error
            continue

        batch.append({
            "key":  chunk["chunk_id"],
            "data": vector,
            "metadata": {
                "service":    chunk["service"],
                "source_url": chunk["source_url"],
                "heading":    chunk["heading"],
                "chunk_hash": chunk["chunk_hash"],
                "page_title": chunk["page_title"],
            },
        })
        batch_chunk_ids.append(chunk["chunk_id"])

        # Flush batch when full
        if len(batch) >= PUT_BATCH_SIZE:
            try:
                written_keys = put_vectors_batch(s3v, batch)
                deduped_count = len(batch_chunk_ids) - len(written_keys)
                total_deduped += deduped_count
                for cid in written_keys:
                    ingested.add(cid)
                    failed.pop(cid, None)
                # Also mark deduplicated chunks as ingested since their
                # identical content exists in the index under the surviving key
                written_set = set(written_keys)
                for cid in batch_chunk_ids:
                    if cid not in written_set:
                        ingested.add(cid)
                save_progress(ingested, progress_file)
                save_failed(failed, failed_file)
                consecutive_errors = 0
                elapsed = (datetime.now() - start).total_seconds()
                rate    = i / elapsed if elapsed > 0 else 0
                eta     = (total - i) / rate if rate > 0 else 0
                print(f"  [{i:>6}/{total}] batch written | "
                      f"{rate:.1f} chunks/s | ETA {eta/60:.1f}m")
            except ClientError as e:
                error_msg = str(e)
                print(f"  [{i:>6}/{total}] PUT ERROR: {error_msg}")
                for cid in batch_chunk_ids:
                    failed[cid] = f"put: {error_msg}"
                save_failed(failed, failed_file)
                total_errors += 1
                consecutive_errors += 1
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    print(f"\n[ABORT] {MAX_CONSECUTIVE_ERRORS} consecutive errors — stopping to avoid "
                          f"data gaps. Fix the issue and re-run to resume.")
                    aborted = True
                    batch = []
                    batch_chunk_ids = []
                    break

            batch = []
            batch_chunk_ids = []

        time.sleep(EMBED_DELAY)

    # Flush remaining batch (only if not aborted)
    if batch and not aborted:
        try:
            written_keys = put_vectors_batch(s3v, batch)
            deduped_count = len(batch_chunk_ids) - len(written_keys)
            total_deduped += deduped_count
            written_set = set(written_keys)
            for cid in written_keys:
                ingested.add(cid)
                failed.pop(cid, None)
            for cid in batch_chunk_ids:
                if cid not in written_set:
                    ingested.add(cid)
            save_progress(ingested, progress_file)
            save_failed(failed, failed_file)
            print(f"  [{total:>6}/{total}] final batch written")
        except ClientError as e:
            error_msg = str(e)
            print(f"  Final batch PUT ERROR: {error_msg}")
            for cid in batch_chunk_ids:
                failed[cid] = f"put: {error_msg}"
            save_failed(failed, failed_file)
            total_errors += 1

    elapsed = datetime.now() - start
    status  = "ABORTED" if aborted else "DONE"
    print(f"\n{status}. {len(ingested):,} ingested | "
          f"{total_errors} errors | {len(failed):,} failed chunks | "
          f"{total_deduped} exact duplicates dropped | elapsed {elapsed}")

    return ingested, failed


# ---------------------------------------------------------------------------
# Final verification
# ---------------------------------------------------------------------------

def verify(chunks: list, ingested: set, failed: dict) -> bool:
    """
    Compare ingested count against total expected and report discrepancies.
    Uses unique chunk IDs since duplicate chunks are intentionally collapsed.
    Returns True if everything looks clean, False if there are gaps.
    """
    unique_ids     = set(c["chunk_id"] for c in chunks)
    total_unique   = len(unique_ids)
    total_raw      = len(chunks)
    duplicates     = total_raw - total_unique
    total_ingested = len(ingested)
    total_failed   = len(failed)
    total_missing  = total_unique - total_ingested - total_failed

    print(f"\n{'='*60}")
    print(f"VERIFICATION")
    print(f"{'='*60}")
    print(f"Total chunks (raw):     {total_raw:,}")
    print(f"Unique chunk IDs:       {total_unique:,}  ({duplicates:,} exact duplicates collapsed)")
    print(f"Successfully ingested:  {total_ingested:,}")
    print(f"Failed (see {FAILED_CHUNKS_FILE}): {total_failed:,}")
    print(f"Unaccounted for:        {total_missing:,}")

    if total_failed > 0:
        print(f"\n[WARNING] {total_failed:,} chunks failed. Re-run with --retry-failed to retry them.")
        error_types = Counter(
            "embed error" if v.startswith("embed:") else "put error"
            for v in failed.values()
        )
        for error_type, count in error_types.items():
            print(f"  {error_type}: {count}")

    if total_missing > 0:
        print(f"\n[WARNING] {total_missing:,} chunks are unaccounted for — "
              f"neither ingested nor recorded as failed. This should not happen. "
              f"Check the script output for errors.")

    if total_failed == 0 and total_missing == 0:
        print(f"\n[OK] All {total_unique:,} unique chunks successfully ingested "
              f"({duplicates:,} exact duplicates collapsed).")
        return True

    return False


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def dry_run(chunks: list, ingested: set) -> None:
    pending      = [c for c in chunks if c["chunk_id"] not in ingested]
    total_tokens = sum(c.get("token_count", 0) for c in pending)
    est_cost     = total_tokens * COST_PER_1K_TOKENS / 1000

    print(f"\n{'='*60}")
    print(f"DRY RUN — no Bedrock calls, no S3 Vectors writes")
    print(f"{'='*60}")
    print(f"Total chunks loaded:     {len(chunks):,}")
    print(f"Already ingested:        {len(ingested):,}")
    print(f"Pending ingestion:       {len(pending):,}")
    print(f"Estimated total tokens:  {total_tokens:,}")
    print(f"Estimated Bedrock cost:  ${est_cost:.4f}")
    print(f"Batch size:              {PUT_BATCH_SIZE}")
    print(f"PutVectors calls needed: {len(pending) // PUT_BATCH_SIZE + 1}")
    print(f"Abort threshold:         {MAX_CONSECUTIVE_ERRORS} consecutive errors")
    print()

    service_counts = Counter(c["service"] for c in pending)
    print("Pending by service:")
    for service, count in sorted(service_counts.items()):
        print(f"  {service:<15} {count:>6,} chunks")
    print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Embed chunks with Bedrock Titan and ingest into S3 Vectors"
    )
    parser.add_argument(
        "--input-dir",
        default="test_output",
        help="Directory containing *_chunks.json files (default: test_output)",
    )
    parser.add_argument(
        "--input-files",
        nargs="+",
        help="Specific chunk JSON file(s) to ingest (overrides --input-dir)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show chunk counts and estimated cost without calling AWS",
    )
    parser.add_argument(
        "--progress-file",
        default=PROGRESS_FILE,
        help=f"Path to progress tracking file (default: {PROGRESS_FILE})",
    )
    parser.add_argument(
        "--failed-file",
        default=FAILED_CHUNKS_FILE,
        help=f"Path to failed chunks file (default: {FAILED_CHUNKS_FILE})",
    )
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry only chunks that previously failed (recorded in failed-file)",
    )
    args = parser.parse_args()

    progress_file = args.progress_file
    failed_file   = args.failed_file

    # Resolve input files
    input_files = resolve_input_files(args.input_dir, args.input_files)
    if not input_files:
        return

    print(f"Loading chunks from {len(input_files)} file(s)...")
    all_chunks = load_chunks(input_files)
    print(f"Total chunks loaded: {len(all_chunks):,}\n")

    if not all_chunks:
        print("[ERROR] No chunks found. Check your input files.")
        return

    # Load progress
    ingested = load_progress(progress_file)

    # If retrying failed chunks, restrict to just those
    if args.retry_failed:
        failed_ids = set(load_failed(failed_file).keys())
        if not failed_ids:
            print("No failed chunks to retry.")
            return
        chunks = [c for c in all_chunks if c["chunk_id"] in failed_ids]
        print(f"Retrying {len(chunks):,} previously failed chunks.")
    else:
        chunks = all_chunks

    if args.dry_run:
        dry_run(chunks, ingested)
        return

    # Run ingest
    bedrock, s3v = get_clients()
    print(f"Vector bucket:   {VECTOR_BUCKET_NAME}")
    print(f"Vector index:    {VECTOR_INDEX_NAME}")
    print(f"Embedding model: {EMBEDDING_MODEL_ID}")
    print()

    ingested, failed = ingest(
        chunks, ingested, bedrock, s3v, progress_file, failed_file
    )

    # Save final progress
    save_progress(ingested, progress_file)

    # Verify
    clean = verify(all_chunks, ingested, failed)

    if clean:
        print(f"\nProgress saved to {progress_file}")
        print("Next step: run a test query, then deploy Lambda functions.")
    else:
        print(f"\nProgress saved to {progress_file}")
        print(f"Failed chunks saved to {failed_file}")
        print("Re-run with --retry-failed to retry failed chunks.")


if __name__ == "__main__":
    main()
