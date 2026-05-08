"""
Query Lambda - AWS Docs RAG
============================
Receives a question from the frontend via API Gateway, embeds it using
Amazon Bedrock Titan Text Embeddings V2, searches S3 Vectors for the most
relevant chunks, fetches the chunk text from S3, and passes everything to
Claude Haiku to generate a grounded answer with citations.

Request body (JSON):
    {
        "question": "How do I configure a Lambda function URL?",
        "service":  "lambda"   # optional - omit or pass "all" to search everything
    }

Response body (JSON):
    {
        "answer":  "...",
        "sources": [
            {
                "service":    "lambda",
                "page_title": "...",
                "heading":    "...",
                "source_url": "https://docs.aws.amazon.com/...",
                "distance":   0.12
            },
            ...
        ]
    }

Environment variables (set by Terraform):
    VECTOR_BUCKET_NAME  - S3 Vectors bucket name
    VECTOR_INDEX_NAME   - S3 Vectors index name
    CHUNK_BUCKET_NAME   - S3 bucket storing chunk text at chunks/{chunk_id}.txt
    EMBEDDING_MODEL_ID  - Bedrock embedding model ID
    GENERATION_MODEL_ID - Bedrock generation model ID
    TOP_K               - Number of chunks to retrieve (default: 10)
"""

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Configuration from environment variables
# ---------------------------------------------------------------------------

VECTOR_BUCKET_NAME  = os.environ["VECTOR_BUCKET_NAME"]
VECTOR_INDEX_NAME   = os.environ["VECTOR_INDEX_NAME"]
CHUNK_BUCKET_NAME   = os.environ["CHUNK_BUCKET_NAME"]
EMBEDDING_MODEL_ID  = os.environ["EMBEDDING_MODEL_ID"]
GENERATION_MODEL_ID = os.environ["GENERATION_MODEL_ID"]
TOP_K               = int(os.environ.get("TOP_K", "10"))

# Valid service names for metadata filtering
VALID_SERVICES = {
    "lambda", "s3", "dynamodb", "apigateway",
    "bedrock", "eventbridge", "iam", "ec2"
}

# ---------------------------------------------------------------------------
# AWS clients (initialised outside handler for reuse across invocations)
# ---------------------------------------------------------------------------

bedrock   = boto3.client("bedrock-runtime", region_name="us-east-1")
s3v       = boto3.client("s3vectors",       region_name="us-east-1")
s3        = boto3.client("s3",              region_name="us-east-1")

# ---------------------------------------------------------------------------
# CORS headers
# ---------------------------------------------------------------------------

CORS_HEADERS = {
    "Content-Type":                 "application/json",
    "Access-Control-Allow-Origin":  "*",
    "Access-Control-Allow-Headers": "Content-Type,X-Api-Key",
    "Access-Control-Allow-Methods": "OPTIONS,POST",
}


def response(status_code: int, body: dict) -> dict:
    return {
        "statusCode": status_code,
        "headers":    CORS_HEADERS,
        "body":       json.dumps(body),
    }


# ---------------------------------------------------------------------------
# Embedding
# ---------------------------------------------------------------------------

def embed_query(question: str) -> list:
    """Embed the user's question using Titan Text Embeddings V2."""
    resp = bedrock.invoke_model(
        modelId=EMBEDDING_MODEL_ID,
        body=json.dumps({"inputText": question}),
        contentType="application/json",
        accept="application/json",
    )
    return json.loads(resp["body"].read())["embedding"]


# ---------------------------------------------------------------------------
# Vector search
# ---------------------------------------------------------------------------

def search_vectors(embedding: list, service: str) -> list:
    """
    Search S3 Vectors for the top-K most relevant chunks.
    Applies a service metadata filter when a specific service is requested.
    Returns a list of result dicts with metadata and distance.
    """
    kwargs = {
        "vectorBucketName": VECTOR_BUCKET_NAME,
        "indexName":        VECTOR_INDEX_NAME,
        "queryVector":      {"float32": embedding},
        "topK":             TOP_K,
        "returnMetadata":   True,
        "returnDistance":   True,
    }

    if service and service.lower() in VALID_SERVICES:
        kwargs["filter"] = {"service": {"$eq": service.lower()}}
        logger.info(f"Filtering by service: {service}")
    else:
        logger.info("Searching across all services")

    result = s3v.query_vectors(**kwargs)
    return result.get("vectors", [])


# ---------------------------------------------------------------------------
# Chunk text retrieval from S3
# ---------------------------------------------------------------------------

def fetch_chunk_text(chunk_id: str) -> str:
    """Fetch a single chunk's text from S3. Returns empty string on error."""
    try:
        obj = s3.get_object(
            Bucket=CHUNK_BUCKET_NAME,
            Key=f"chunks/{chunk_id}.txt",
        )
        return obj["Body"].read().decode("utf-8")
    except Exception as e:
        logger.warning(f"Failed to fetch chunk {chunk_id}: {e}")
        return ""


def fetch_chunk_texts(chunk_ids: list) -> dict:
    """
    Fetch chunk texts from S3 in parallel using a thread pool.
    Returns a dict mapping chunk_id to text.
    """
    texts = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_id = {
            executor.submit(fetch_chunk_text, cid): cid
            for cid in chunk_ids
        }
        for future in as_completed(future_to_id):
            cid = future_to_id[future]
            texts[cid] = future.result()
    return texts


# ---------------------------------------------------------------------------
# Answer generation
# ---------------------------------------------------------------------------

def build_prompt(question: str, chunks: list, chunk_texts: dict) -> str:
    """
    Build the prompt for Claude Haiku with retrieved chunk texts as context.
    Each chunk includes its source URL and heading for citation.
    """
    context_parts = []
    for i, chunk in enumerate(chunks, 1):
        meta     = chunk.get("metadata", {})
        chunk_id = chunk.get("key", "")
        heading  = meta.get("heading", "")
        url      = meta.get("source_url", "")
        service  = meta.get("service", "").upper()
        text     = chunk_texts.get(chunk_id, "").strip()

        if not text:
            continue

        context_parts.append(
            f"[Source {i}] {service} — {heading}\n"
            f"URL: {url}\n\n"
            f"{text}"
        )

    if not context_parts:
        return None

    context = "\n\n---\n\n".join(context_parts)

    return f"""You are an expert AWS documentation assistant. Answer the user's question based solely on the provided AWS documentation excerpts below.

Guidelines:
- Base your answer strictly on the provided documentation excerpts
- Be concise but thorough — cover the key points the user needs
- Cite sources using [Source N] notation where relevant
- If multiple sources contribute to the answer, reference each one
- If the documentation doesn't fully answer the question, say so clearly rather than guessing
- Use bullet points or numbered steps where appropriate for clarity

AWS Documentation:
{context}

Question: {question}

Answer:"""


def generate_answer(question: str, chunks: list, chunk_texts: dict) -> str:
    """Call Claude Haiku to generate a grounded answer from retrieved chunks."""
    prompt = build_prompt(question, chunks, chunk_texts)

    if not prompt:
        return ("I found relevant documentation but couldn't retrieve the content. "
                "Please try again.")

    resp = bedrock.invoke_model(
        modelId=GENERATION_MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens":        1024,
            "messages": [
                {"role": "user", "content": prompt}
            ],
        }),
        contentType="application/json",
        accept="application/json",
    )

    result = json.loads(resp["body"].read())
    return result["content"][0]["text"]


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------

def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    # Handle CORS preflight
    if event.get("httpMethod") == "OPTIONS":
        return response(200, {})

    # Parse request body - handle both API Gateway events and direct invocations
    try:
        if "body" in event:
            # API Gateway invocation - body is a JSON string
            body = json.loads(event.get("body") or "{}")
        else:
            # Direct Lambda invocation - event is the body
            body = event
        question = body.get("question", "").strip()
        service  = body.get("service", "all").strip().lower()
    except (json.JSONDecodeError, AttributeError) as e:
        return response(400, {"error": f"Invalid request body: {e}"})

    if not question:
        return response(400, {"error": "question is required"})

    if len(question) > 2000:
        return response(400, {"error": "question must be 2000 characters or fewer"})

    logger.info(f"Question: {question} | Service filter: {service}")

    try:
        # Step 1: Embed the question
        embedding = embed_query(question)
        logger.info("Query embedded successfully")

        # Step 2: Search S3 Vectors for relevant chunks
        raw_chunks = search_vectors(embedding, service)
        logger.info(f"Retrieved {len(raw_chunks)} chunks from S3 Vectors")

        if not raw_chunks:
            return response(200, {
                "answer":  ("I couldn't find any relevant documentation for your question. "
                            "Try rephrasing or selecting a different service filter."),
                "sources": [],
            })

        # Step 3: Fetch chunk texts from S3 in parallel
        chunk_ids  = [chunk.get("key", "") for chunk in raw_chunks]
        chunk_texts = fetch_chunk_texts(chunk_ids)
        logger.info(f"Fetched {len(chunk_texts)} chunk texts from S3")

        # Step 4: Generate answer with Claude Haiku
        answer = generate_answer(question, raw_chunks, chunk_texts)
        logger.info("Answer generated successfully")

        # Step 5: Build deduplicated sources list for the frontend
        sources   = []
        seen_urls = set()
        for chunk in raw_chunks:
            meta = chunk.get("metadata", {})
            url  = meta.get("source_url", "")
            if url and url not in seen_urls:
                seen_urls.add(url)
                sources.append({
                    "service":    meta.get("service", ""),
                    "page_title": meta.get("page_title", ""),
                    "heading":    meta.get("heading", ""),
                    "source_url": url,
                    "distance":   round(chunk.get("distance", 0), 4),
                })

        return response(200, {
            "answer":  answer,
            "sources": sources,
        })

    except Exception as e:
        logger.error(f"Error processing query: {e}", exc_info=True)
        return response(500, {"error": "Internal server error. Please try again."})
