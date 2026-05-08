"""
AWS Documentation Scraper
==========================
Scrapes AWS service documentation from docs.aws.amazon.com using sitemaps.
Chunks content by heading, hashes each chunk, and saves to a local JSON file.
Does NOT call Bedrock or write to S3 Vectors - this is the scrape-only phase.

HTML cache
----------
Each fetched page is saved to html_cache/<service>/<filename>.html so that
subsequent runs (e.g. to tune chunking) read from disk instead of hitting the
network. The crawl delay is skipped for cache hits, so rechunking runs instantly.

Use --no-cache to force fresh fetches even when cached HTML exists.
Use --cache-dir to override the default cache location (default: html_cache).

Dry run
-------
Use --dry-run to fetch sitemaps and report page counts without scraping anything.
Shows total pages per service, how many are already cached, and an estimated
crawl time for uncached pages. Useful for deciding whether to proceed before
committing to a long scrape.

Saves the output JSON incrementally after every page so progress is never lost.

Usage (dry run - check page counts before scraping):
    python scrape_docs.py --services all --dry-run

Usage (test run - Lambda only):
    python scrape_docs.py --services lambda --output test_output/lambda_chunks.json

Usage (full run - all services):
    python scrape_docs.py --services all --output test_output/all_chunks.json

Usage (rechunk from cache with no network calls):
    python scrape_docs.py --services lambda --output test_output/lambda_chunks.json
    (cache hits are automatic - just run the same command again)

Requirements:
    pip install requests beautifulsoup4
"""

import argparse
import hashlib
import json
import os
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Optional
from xml.etree import ElementTree

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Service configuration
# ---------------------------------------------------------------------------

SERVICES = {
    "lambda": {
        "sitemap": "https://docs.aws.amazon.com/lambda/latest/dg/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/lambda/latest/dg/",
        "name":    "lambda",
    },
    "s3": {
        "sitemap": "https://docs.aws.amazon.com/AmazonS3/latest/userguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/AmazonS3/latest/userguide/",
        "name":    "s3",
    },
    "dynamodb": {
        "sitemap": "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/",
        "name":    "dynamodb",
    },
    "apigateway": {
        "sitemap": "https://docs.aws.amazon.com/apigateway/latest/developerguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/apigateway/latest/developerguide/",
        "name":    "apigateway",
    },
    "bedrock": {
        "sitemap": "https://docs.aws.amazon.com/bedrock/latest/userguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/bedrock/latest/userguide/",
        "name":    "bedrock",
    },
    "eventbridge": {
        "sitemap": "https://docs.aws.amazon.com/eventbridge/latest/userguide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/eventbridge/latest/userguide/",
        "name":    "eventbridge",
    },
    "iam": {
        "sitemap": "https://docs.aws.amazon.com/IAM/latest/UserGuide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/IAM/latest/UserGuide/",
        "name":    "iam",
    },
    "ec2": {
        "sitemap": "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/sitemap.xml",
        "prefix":  "https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/",
        "name":    "ec2",
    },
}

# ---------------------------------------------------------------------------
# URL exclusion patterns - pages that produce poor chunks
# ---------------------------------------------------------------------------

EXCLUDE_PATTERNS = [
    r"/api/",
    r"-api-reference",
    r"doc-history",
    r"document-history",
    r"release-notes",
    r"release-history",
    r"/index\.html$",
]

EC2_EXTRA_EXCLUDES = [
    r"instance-types",
    r"instance_types",
    r"/ec2/.*pricing",
]

# Minimum word count to bother chunking a page
MIN_WORDS = 100

# Chunking settings
MAX_TOKENS = 512
OVERLAP_TOKENS = 50

# Crawl delay in seconds - applied only on actual network fetches, not cache hits
CRAWL_DELAY = 4

# Default HTML cache directory
DEFAULT_CACHE_DIR = "html_cache"


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Chunk:
    chunk_id: str          # md5 of (service + source_url + heading + start position)
    service: str
    source_url: str
    page_title: str
    heading: str
    text: str
    chunk_hash: str        # md5 of text only (used for refresh diffing)
    token_count: int


# ---------------------------------------------------------------------------
# Token counting (~4 characters per token approximation)
# ---------------------------------------------------------------------------

def count_tokens(text: str) -> int:
    return len(text) // 4


# ---------------------------------------------------------------------------
# HTTP session with browser-like headers
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})


def fetch_url(url: str, timeout: tuple = (10, 20)) -> Optional[requests.Response]:
    """
    Fetch a URL over the network. Returns Response or None on error.
    timeout is a (connect_timeout, read_timeout) tuple.
    """
    try:
        resp = SESSION.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        print(f"    [WARN] Failed to fetch {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# HTML cache
# ---------------------------------------------------------------------------

def cache_path_for(url: str, service_name: str, cache_dir: str) -> str:
    """
    Return the local file path where this URL's HTML should be cached.

    Structure: <cache_dir>/<service_name>/<filename>.html
    e.g. html_cache/lambda/welcome.html

    The filename is taken directly from the URL's last path segment.
    This is safe because all filenames within a service's doc set are unique
    (they come from the sitemap which already deduplicates them).
    """
    filename = url.rstrip("/").split("/")[-1]
    if not filename.endswith(".html"):
        filename += ".html"
    return os.path.join(cache_dir, service_name, filename)


def get_html(
    url: str,
    service_name: str,
    cache_dir: str,
    use_cache: bool,
    crawl_delay: int,
    is_last: bool,
) -> tuple:
    """
    Return (html_string, from_cache) for the given URL.

    If use_cache is True and the file exists on disk, read it directly.
    Otherwise fetch over the network, save to cache, and apply the crawl delay.
    is_last controls whether the crawl delay is applied after the fetch
    (we skip the delay after the final page of a service).

    Returns (None, False) if the network fetch fails.
    """
    path = cache_path_for(url, service_name, cache_dir)

    if use_cache and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return f.read(), True

    # Network fetch
    resp = fetch_url(url)
    if resp is None:
        return None, False

    html = resp.text

    # Save to cache
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)

    # Polite delay after network fetch (skip after last page)
    if not is_last:
        time.sleep(crawl_delay)

    return html, False


def count_cached_pages(urls: list, service_name: str, cache_dir: str) -> int:
    """Return how many of the given URLs are already cached on disk."""
    return sum(
        1 for url in urls
        if os.path.exists(cache_path_for(url, service_name, cache_dir))
    )


# ---------------------------------------------------------------------------
# Sitemap parsing
# ---------------------------------------------------------------------------

def fetch_sitemap_urls(sitemap_url: str, prefix: str, service: str) -> list:
    """Fetch sitemap XML over the network, return filtered list of page URLs."""
    print(f"  Fetching sitemap: {sitemap_url}")
    resp = fetch_url(sitemap_url, timeout=(10, 30))
    if resp is None:
        print(f"  [ERROR] Could not fetch sitemap for {service}. Check URL.")
        return []

    try:
        root = ElementTree.fromstring(resp.content)
    except ElementTree.ParseError as e:
        print(f"  [ERROR] Failed to parse sitemap XML: {e}")
        return []

    namespace = ""
    if root.tag.startswith("{"):
        namespace = root.tag.split("}")[0] + "}"

    urls = []
    for url_el in root.iter(f"{namespace}loc"):
        url = url_el.text.strip()
        if url.startswith(prefix) and url.endswith(".html"):
            if not should_exclude(url, service):
                urls.append(url)

    print(f"  Found {len(urls)} pages after filtering")
    return urls


def should_exclude(url: str, service: str) -> bool:
    """Return True if this URL should be skipped."""
    patterns = EXCLUDE_PATTERNS[:]
    if service == "ec2":
        patterns.extend(EC2_EXTRA_EXCLUDES)
    for pattern in patterns:
        if re.search(pattern, url, re.IGNORECASE):
            return True
    return False


# ---------------------------------------------------------------------------
# HTML content extraction
# ---------------------------------------------------------------------------

def extract_content(html: str, url: str) -> Optional[tuple]:
    """
    Parse HTML, extract page title and list of (heading, text) sections.
    Returns (page_title, [(heading, section_text), ...]) or None if too short.

    Code blocks (<pre>, <code>) are intentionally excluded. They don't chunk
    well (short tokens, no natural sentence boundaries) and are not useful for
    natural language retrieval. The prose surrounding code is what we want.
    """
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("title")
    page_title = title_tag.get_text(strip=True) if title_tag else url

    # Remove boilerplate elements before extracting content
    for selector in [
        "nav", "header", "footer",
        ".breadcrumb", ".awsdocs-breadcrumb",
        ".feedback", ".page-feedback",
        "#feedback", "#awsdocs-body-feedback",
        ".awsdocs-sidebar", "#left-column",
        ".awsdocs-header", "#awsdocs-header",
        "#aws-nav", "#aws-page-header",
        ".warning",
        "script", "style",
        "[data-testid='feedback']",
    ]:
        for el in soup.select(selector):
            el.decompose()

    # Remove all <pre> and <code> blocks before traversal so their content
    # doesn't appear inside <p> or <li> elements either.
    for el in soup.find_all(["pre", "code"]):
        el.decompose()

    # Find main content area - try specific containers before falling back to body
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

    # Walk the content, splitting on headings to build sections.
    # We use p, li, and td only - no pre/code (already removed above).
    sections = []
    current_heading = page_title
    current_lines = []

    for el in main.find_all(["h1", "h2", "h3", "p", "li", "td"]):
        if el.name in ("h1", "h2", "h3"):
            # Save the accumulated text under the previous heading
            text = "\n".join(current_lines).strip()
            if text:
                sections.append((current_heading, text))
            current_heading = el.get_text(strip=True)
            current_lines = []
        else:
            text = el.get_text(separator=" ", strip=True)
            if text:
                current_lines.append(text)

    # Don't forget the final section
    text = "\n".join(current_lines).strip()
    if text:
        sections.append((current_heading, text))

    # Drop sections that are too short to be meaningful
    sections = [(h, t) for h, t in sections if len(t.split()) >= 20]

    if not sections:
        return None

    total_words = sum(len(t.split()) for _, t in sections)
    if total_words < MIN_WORDS:
        return None

    return page_title, sections


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def chunk_section(
    heading: str,
    text: str,
    service: str,
    source_url: str,
    page_title: str,
) -> list:
    """
    Split a section into token-bounded chunks with overlap.

    Works in characters throughout (4 chars ~ 1 token) to avoid per-word
    token counting issues.

    If the entire section fits within MAX_TOKENS, we emit exactly one chunk.
    Only sections that exceed MAX_TOKENS are split, with overlap between splits
    to preserve context at chunk boundaries.

    The overlap is capped at 25% of the chunk word count to guarantee forward
    progress regardless of section length or word size.
    """
    chunks = []
    prefix = f"{heading}\n\n"
    max_chars = MAX_TOKENS * 4
    overlap_chars = OVERLAP_TOKENS * 4
    effective_max_chars = max_chars - len(prefix)

    if not text:
        return []

    # Short section: emit as a single chunk and return immediately
    if len(text) <= effective_max_chars:
        chunk_text = prefix + text
        chunk_hash = hashlib.md5(chunk_text.encode()).hexdigest()
        chunks.append(Chunk(
            chunk_id=chunk_hash,
            service=service,
            source_url=source_url,
            page_title=page_title,
            heading=heading,
            text=chunk_text,
            chunk_hash=chunk_hash,
            token_count=count_tokens(chunk_text),
        ))
        return chunks

    # Long section: split at word boundaries with overlap
    words = text.split()

    start_word = 0
    while start_word < len(words):
        # Accumulate words until we hit the character budget
        end_word = start_word
        current_chars = 0
        while end_word < len(words):
            word_chars = len(words[end_word]) + 1  # +1 for space
            if current_chars + word_chars > effective_max_chars:
                break
            current_chars += word_chars
            end_word += 1

        # Safety: always include at least one word
        if end_word == start_word:
            end_word = start_word + 1

        chunk_text = prefix + " ".join(words[start_word:end_word])
        chunk_hash = hashlib.md5(chunk_text.encode()).hexdigest()
        chunks.append(Chunk(
            chunk_id=chunk_hash,
            service=service,
            source_url=source_url,
            page_title=page_title,
            heading=heading,
            text=chunk_text,
            chunk_hash=chunk_hash,
            token_count=count_tokens(chunk_text),
        ))

        # Overlap words derived from character budget, capped at 25% of chunk
        # size so we always make meaningful forward progress.
        chunk_word_count = end_word - start_word
        avg_word_chars = max(1, current_chars // max(1, chunk_word_count))
        overlap_words = overlap_chars // max(1, avg_word_chars)
        overlap_words = min(overlap_words, chunk_word_count // 4)
        advance = max(1, chunk_word_count - overlap_words)
        start_word += advance

    return chunks


# ---------------------------------------------------------------------------
# Incremental JSON save
# ---------------------------------------------------------------------------

def save_chunks(
    all_chunks: list,
    output_path: str,
    started: datetime,
    services: list,
    crawl_delay: int,
    status: str = "in_progress",
) -> None:
    """Write current chunks to output JSON file."""
    os.makedirs(
        os.path.dirname(output_path) if os.path.dirname(output_path) else ".",
        exist_ok=True,
    )
    elapsed = datetime.now() - started
    output = {
        "metadata": {
            "scraped_at": started.isoformat(),
            "elapsed_seconds": int(elapsed.total_seconds()),
            "services": services,
            "total_chunks": len(all_chunks),
            "crawl_delay_seconds": crawl_delay,
            "status": status,
        },
        "chunks": [asdict(c) for c in all_chunks],
    }
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Main scraping logic
# ---------------------------------------------------------------------------

def scrape_service(
    service_key: str,
    crawl_delay: int = CRAWL_DELAY,
    cache_dir: str = DEFAULT_CACHE_DIR,
    use_cache: bool = True,
    output_path: str = None,
    chunks_so_far: list = None,
    started: datetime = None,
    valid_services: list = None,
) -> list:
    """Scrape one service's documentation. Returns list of Chunk objects."""
    config = SERVICES[service_key]
    service_name = config["name"]
    print(f"\n{'='*60}")
    print(f"Scraping: {service_name.upper()}")
    print(f"{'='*60}")

    urls = fetch_sitemap_urls(config["sitemap"], config["prefix"], service_key)
    if not urls:
        return []

    all_chunks = []
    skipped = 0
    errors = 0

    for i, url in enumerate(urls, 1):
        is_last = (i == len(urls))
        filename = url.split("/")[-1]

        # Check cache status for display before fetching
        cached_path = cache_path_for(url, service_name, cache_dir)
        from_cache_label = " [cache]" if (use_cache and os.path.exists(cached_path)) else ""
        print(f"  [{i}/{len(urls)}] {filename}{from_cache_label} ... ", end="", flush=True)

        html, from_cache = get_html(url, service_name, cache_dir, use_cache, crawl_delay, is_last)

        if html is None:
            errors += 1
            print("[ERROR]")
            if not is_last:
                time.sleep(crawl_delay)
            continue

        result = extract_content(html, url)
        if result is None:
            skipped += 1
            print("[SKIP - too short]")
            continue

        page_title, sections = result
        page_chunks = []
        for heading, text in sections:
            page_chunks.extend(
                chunk_section(heading, text, service_name, url, page_title)
            )

        all_chunks.extend(page_chunks)
        source = "cached" if from_cache else "fetched"
        print(f"{len(sections)} sections, {len(page_chunks)} chunks ({source})")

        # Save incrementally after every page
        if output_path and started:
            combined = (chunks_so_far or []) + all_chunks
            save_chunks(
                combined, output_path, started,
                valid_services or [service_key], crawl_delay,
            )

    print(f"\n  {service_name.upper()} summary:")
    print(f"    Pages processed: {len(urls) - skipped - errors}")
    print(f"    Pages skipped (too short): {skipped}")
    print(f"    Pages errored: {errors}")
    print(f"    Total chunks: {len(all_chunks)}")

    return all_chunks


# ---------------------------------------------------------------------------
# Dry run
# ---------------------------------------------------------------------------

def dry_run(valid_services: list, cache_dir: str, crawl_delay: int) -> None:
    """
    Fetch sitemaps and report page counts, cache status, and estimated crawl
    time for each service. Does not fetch any doc pages or produce any output.
    """
    print(f"\n{'='*60}")
    print(f"DRY RUN — page counts only, no scraping")
    print(f"{'='*60}\n")

    col_w = 14
    print(
        f"  {'Service':<{col_w}} {'Pages':>6}  {'Cached':>6}  {'Uncached':>8}  {'Est. time':>10}"
    )
    print(f"  {'-'*col_w} {'------':>6}  {'------':>6}  {'--------':>8}  {'----------':>10}")

    total_pages = 0
    total_cached = 0
    total_uncached = 0

    for key in valid_services:
        config = SERVICES[key]
        urls = fetch_sitemap_urls(config["sitemap"], config["prefix"], key)
        if not urls:
            print(f"  {key:<{col_w}} {'ERROR':>6}")
            continue

        cached = count_cached_pages(urls, config["name"], cache_dir)
        uncached = len(urls) - cached
        est_seconds = uncached * crawl_delay
        est_str = _format_duration(est_seconds)

        print(
            f"  {key:<{col_w}} {len(urls):>6}  {cached:>6}  {uncached:>8}  {est_str:>10}"
        )

        total_pages += len(urls)
        total_cached += cached
        total_uncached += uncached

    print(f"  {'-'*col_w} {'------':>6}  {'------':>6}  {'--------':>8}  {'----------':>10}")
    total_est = _format_duration(total_uncached * crawl_delay)
    print(
        f"  {'TOTAL':<{col_w}} {total_pages:>6}  {total_cached:>6}  {total_uncached:>8}  {total_est:>10}"
    )
    print(f"\n  Crawl delay: {crawl_delay}s per uncached page")
    print(f"  Cache dir:   {cache_dir}/")
    print()


def _format_duration(seconds: int) -> str:
    """Format a duration in seconds as Xh Ym or Xm or Xs."""
    if seconds >= 3600:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}m"
    elif seconds >= 60:
        m = seconds // 60
        return f"{m}m"
    else:
        return f"{seconds}s"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Scrape AWS docs to local JSON chunks")
    parser.add_argument(
        "--services",
        default="lambda",
        help=(
            "Comma-separated list of services to scrape, or 'all'. "
            f"Available: {', '.join(SERVICES.keys())}"
        ),
    )
    parser.add_argument(
        "--output",
        default="test_output/chunks.json",
        help="Output JSON file path (default: test_output/chunks.json)",
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=CRAWL_DELAY,
        help=f"Seconds between network fetches (default: {CRAWL_DELAY}). Ignored for cache hits.",
    )
    parser.add_argument(
        "--cache-dir",
        default=DEFAULT_CACHE_DIR,
        help=f"Directory for cached HTML files (default: {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Ignore cached HTML and fetch all pages from the network",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Fetch sitemaps and report page counts, cache status, and estimated "
            "crawl time. Does not scrape any pages or produce output files."
        ),
    )
    args = parser.parse_args()

    use_cache = not args.no_cache

    if args.services.strip().lower() == "all":
        service_keys = list(SERVICES.keys())
    else:
        service_keys = [s.strip().lower() for s in args.services.split(",")]
        invalid = [s for s in service_keys if s not in SERVICES]
        if invalid:
            print(f"[ERROR] Unknown services: {invalid}")
            print(f"Valid options: {list(SERVICES.keys())}")
            return

    if args.dry_run:
        print(f"Services:      {service_keys}")
        print(f"Cache dir:     {args.cache_dir}")
        print(f"Crawl delay:   {args.delay}s")
    else:
        print(f"Services to scrape: {service_keys}")
        print(f"Crawl delay:        {args.delay}s (network fetches only)")
        print(f"Cache directory:    {args.cache_dir}")
        print(f"Cache enabled:      {use_cache}")
        print(f"Output file:        {args.output}")

    print("\nValidating sitemap URLs...")
    valid_services = []
    for key in service_keys:
        config = SERVICES[key]
        resp = fetch_url(config["sitemap"])
        if resp and resp.status_code == 200:
            print(f"  ✓ {key}: {config['sitemap']}")
            valid_services.append(key)
        else:
            print(f"  ✗ {key}: FAILED - {config['sitemap']}")
            print(f"    This service will be skipped. Update the sitemap URL in SERVICES config.")

    if not valid_services:
        print("\n[ERROR] No valid sitemaps found. Exiting.")
        return

    # Dry run: report page counts and exit
    if args.dry_run:
        dry_run(valid_services, args.cache_dir, args.delay)
        return

    # Full scrape
    started = datetime.now()
    all_chunks = []

    for key in valid_services:
        chunks = scrape_service(
            key,
            crawl_delay=args.delay,
            cache_dir=args.cache_dir,
            use_cache=use_cache,
            output_path=args.output,
            chunks_so_far=all_chunks,
            started=started,
            valid_services=valid_services,
        )
        all_chunks.extend(chunks)

    # Final save with completed status
    save_chunks(
        all_chunks, args.output, started, valid_services, args.delay, status="complete"
    )

    elapsed = datetime.now() - started
    print(f"\n{'='*60}")
    print(f"COMPLETE")
    print(f"{'='*60}")
    print(f"Total chunks:   {len(all_chunks)}")
    print(f"Total tokens:   {sum(c.token_count for c in all_chunks):,}")
    print(f"Elapsed:        {elapsed}")
    print(f"Output:         {args.output}")
    print(f"HTML cache:     {args.cache_dir}/")
    print(f"\nNext step: inspect {args.output}, then run embed_and_ingest.py")


if __name__ == "__main__":
    main()
