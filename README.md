# aws-docs-rag

A production-grade Retrieval-Augmented Generation (RAG) system over AWS documentation, built on Amazon Bedrock, S3 Vectors, and Claude Haiku. Ask natural language questions and get grounded, cited answers drawn from 38,000+ chunks of live AWS documentation across 8 services.

\---

## What It Does

Type a question like *"How do I configure a Lambda function URL?"* or *"What is the difference between a DynamoDB GSI and LSI?"* and the system:

1. Embeds your question using Amazon Titan Text Embeddings V2
2. Searches 38,143 vector embeddings in S3 Vectors for the most relevant documentation chunks
3. Fetches the chunk text from S3
4. Passes the retrieved context to Claude Haiku on Bedrock to generate a grounded answer with source citations
5. Returns the answer alongside links to the original AWS documentation pages

You can filter by service (Lambda, S3, DynamoDB, API Gateway, Bedrock, EventBridge, IAM, EC2) or search across all services at once.

\---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Ingestion (one-time, local)                                    │
│                                                                 │
│  docs.aws.amazon.com ──► scrape\_docs.py ──► chunk JSON files   │
│                                    ▼                            │
│                         embed\_and\_ingest.py                     │
│                           ├── Bedrock Titan (embed)             │
│                           ├── S3 Vectors (store vectors)        │
│                           └── S3 data bucket (store text)       │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Automated Refresh                                              │
│                                                                 │
│  EventBridge (daily)  ──► Discovery Lambda                      │
│                              └── Syncs new/removed URLs         │
│                                  in DynamoDB URL queue          │
│                                                                 │
│  EventBridge (6-hourly) ──► Refresh Lambda                      │
│                               └── Conditional HTTP fetch        │
│                                   ├── 304: advance schedule     │
│                                   └── 200: rechunk, re-embed,   │
│                                       update S3 Vectors + S3    │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Query                                                          │
│                                                                 │
│  React Frontend ──► API Gateway ──► Query Lambda                │
│                                       ├── Titan embed query     │
│                                       ├── S3 Vectors search     │
│                                       ├── S3 fetch chunk text   │
│                                       └── Claude Haiku generate │
└─────────────────────────────────────────────────────────────────┘
```

\---

## Tech Stack

|Layer|Technology|
|-|-|
|Embeddings|Amazon Titan Text Embeddings V2 (1,024-dim, cosine)|
|Vector store|Amazon S3 Vectors|
|Generation|Claude Haiku on Amazon Bedrock|
|Chunk text storage|Amazon S3|
|Refresh queue|Amazon DynamoDB|
|Compute|AWS Lambda (Python 3.12)|
|API|Amazon API Gateway|
|Scheduling|Amazon EventBridge|
|Frontend|React, hosted on S3|
|Infrastructure|Terraform|

\---

## Corpus

|Service|Pages|Chunks|
|-|-|-|
|Lambda|450|\~4,300|
|S3|774|\~7,600|
|DynamoDB|600|\~4,600|
|API Gateway|385|\~2,500|
|Bedrock|888|\~6,000|
|EventBridge|196|\~1,500|
|IAM|581|\~5,700|
|EC2|710|\~7,400|
|**Total**|**4,584**|**38,143**|

Chunks are \~512 tokens each with 50-token overlap. Code blocks are excluded from chunking — only prose is indexed, which produces cleaner retrieval results.

\---

## Refresh Pipeline

The system keeps documentation current without re-scraping everything on a schedule:

**Discovery Lambda** (daily): Fetches all 8 service sitemaps and compares against the DynamoDB URL queue. New pages are inserted immediately; removed pages are deleted from the queue and their vectors purged from S3 Vectors. If any sitemap fails to fetch, the run aborts entirely rather than risk treating a whole service's pages as removed.

**Refresh Lambda** (every 6 hours): Queries DynamoDB for the 50 most overdue URLs, makes conditional HTTP requests (`If-Modified-Since` / `If-None-Match`), and only re-embeds pages that have actually changed (HTTP 200). Unchanged pages (HTTP 304) just get their schedule advanced. This means embedding costs on a typical refresh run are near zero.

Each URL stores its current chunk IDs in DynamoDB. When a page changes, the Lambda diffs the old and new chunk sets, deletes stale vectors from S3 Vectors, and embeds only the new or changed chunks.

\---

## Project Structure

```
aws-docs-rag/
├── backend/
│   ├── discovery/
│   │   └── lambda\_function.py    # Daily sitemap sync
│   ├── query/
│   │   └── lambda\_function.py    # RAG query handler
│   └── refresh/
│       └── lambda\_function.py    # Incremental doc refresh
├── frontend/
│   ├── public/
│   └── src/
│       ├── App.js
│       ├── App.css
│       └── components/
├── terraform/                    # All AWS infrastructure
├── scrape\_docs.py                # Local doc scraper
├── embed\_and\_ingest.py           # Embedding and S3 Vectors ingest
├── seed\_url\_table.py             # One-time DynamoDB seeding
├── upload\_chunks\_to\_s3.py        # One-time S3 chunk text upload
├── deploy\_query.ps1
├── deploy\_discovery.ps1
└── deploy\_refresh.ps1
```

\---

## Setup

### Prerequisites

* AWS account with Bedrock model access enabled for Titan Text Embeddings V2 and Claude Haiku
* AWS CLI configured with a named profile
* Python 3.12+, Node.js 18+, Terraform 1.0+

### Infrastructure

```powershell
# Create Terraform state bucket manually first
aws s3 mb s3://aws-docs-rag-terraform-state --region us-east-1 --profile <your-profile>
aws s3api put-bucket-versioning --bucket aws-docs-rag-terraform-state \\
    --versioning-configuration Status=Enabled --profile <your-profile>

# Create terraform.tfvars with your account ID
echo 'aws\_account\_id = "your-account-id"' > terraform/terraform.tfvars

cd terraform
terraform init
terraform apply
```

### Ingestion

```powershell
# Install Python dependencies
pip install requests beautifulsoup4 boto3

# Scrape docs (one service at a time recommended)
python scrape\_docs.py --services lambda --output test\_output/lambda\_chunks.json
# ... repeat for each service

# Embed and ingest into S3 Vectors
python embed\_and\_ingest.py --input-dir test\_output

# Upload chunk text to S3
python upload\_chunks\_to\_s3.py --input-dir test\_output

# Seed DynamoDB URL queue
python seed\_url\_table.py --input-dir test\_output
```

### Deploy

```powershell
.\\deploy\_query.ps1
.\\deploy\_discovery.ps1
.\\deploy\_refresh.ps1

cd frontend
npm install
npm run build
cd ..
.\\deploy\_frontend.ps1
```

\---

## Cost

At portfolio usage this system costs approximately **$1-2/month** to run:

* S3 Vectors storage: \~$0.50/month for 38,143 vectors
* Bedrock (query): pennies per month at low query volume
* Bedrock (refresh): near zero — most pages return 304 and are not re-embedded
* Lambda, API Gateway, DynamoDB, S3: all within free tier at this scale
* One-time ingest cost: \~$0.18 in Bedrock embedding calls

