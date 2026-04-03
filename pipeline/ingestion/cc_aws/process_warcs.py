"""Phase 2: Fetch WARC records and extract toponym matches.

Reads candidate URLs from Athena results (CSV on S3), fetches the actual
WARC records, scans for toponym mentions, and pushes matches to BigQuery.

Designed to run on an EC2 spot instance in us-east-1 for free S3 access,
but also works locally (slower due to cross-region S3 reads).

Usage:
    python -m pipeline.ingestion.cc_aws.process_warcs --input s3://kyivnotkiev-cc-results/query-id.csv
    python -m pipeline.ingestion.cc_aws.process_warcs --input candidates.csv  # local file

Prerequisites:
    - pip install boto3 warcio requests google-cloud-bigquery
    - AWS credentials for S3 access (or run on EC2 with IAM role)
    - GCP credentials for BigQuery writes
"""

import argparse
import csv
import io
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# BigQuery destination
BQ_PROJECT = "kyivnotkiev-research"
BQ_TABLE = f"{BQ_PROJECT}.kyivnotkiev.raw_common_crawl"

# Rate limiting for WARC fetches
MAX_CONCURRENT_FETCHES = 10
FETCH_TIMEOUT = 30


def load_pairs() -> list[dict]:
    """Load enabled pairs from config."""
    config_path = Path(__file__).resolve().parent.parent.parent.parent / "config" / "pairs.yaml"
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    return [p for p in cfg["pairs"] if p.get("enabled", True) and not p.get("is_control", False)]


def build_patterns(pairs: list[dict]) -> list[tuple]:
    """Build compiled regex patterns for all pairs."""
    patterns = []
    for p in pairs:
        ru_re = re.compile(rf"\b{re.escape(p['russian'])}\b", re.IGNORECASE)
        uk_re = re.compile(rf"\b{re.escape(p['ukrainian'])}\b", re.IGNORECASE)
        patterns.append((p["id"], p["russian"], p["ukrainian"], ru_re, uk_re))
    return patterns


def fetch_warc_record(warc_filename: str, offset: int, length: int) -> str | None:
    """Fetch a single WARC record from S3 via HTTP range request."""
    url = f"https://data.commoncrawl.org/{warc_filename}"
    headers = {"Range": f"bytes={offset}-{offset + length - 1}"}
    try:
        resp = requests.get(url, headers=headers, timeout=FETCH_TIMEOUT)
        if resp.status_code not in (200, 206):
            return None
        # Parse WARC record
        from warcio.archiveiterator import ArchiveIterator
        stream = io.BytesIO(resp.content)
        for record in ArchiveIterator(stream):
            if record.rec_type == "response":
                content = record.content_stream().read()
                return content.decode("utf-8", errors="ignore")
    except Exception:
        return None
    return None


def extract_matches(text: str, url: str, crawl_id: str, domain: str,
                    tld: str, patterns: list[tuple]) -> list[dict]:
    """Scan text for toponym matches, return list of match dicts."""
    if not text:
        return []

    now = datetime.now(timezone.utc)
    # Extract crawl date from ID (e.g., CC-MAIN-2024-10 -> 2024-03)
    parts = crawl_id.replace("CC-MAIN-", "").split("-")
    year = int(parts[0])
    week = int(parts[1])
    # Approximate: week number to month
    month = min(12, max(1, (week * 7) // 30 + 1))
    crawl_date = f"{year}-{month:02d}-01"

    results = []
    for pair_id, russian_term, ukrainian_term, ru_re, uk_re in patterns:
        for match in ru_re.finditer(text):
            start = max(0, match.start() - 50)
            end = min(len(text), match.end() + 50)
            snippet = text[start:end].replace("\n", " ").strip()[:200]
            results.append({
                "pair_id": pair_id,
                "url": url[:500],
                "domain": domain,
                "tld": tld,
                "matched_term": russian_term,
                "variant": "russian",
                "context_snippet": snippet,
                "crawl_id": crawl_id,
                "crawl_date": crawl_date,
                "content_language": "eng",
                "ingested_at": now.isoformat(),
            })

        for match in uk_re.finditer(text):
            start = max(0, match.start() - 50)
            end = min(len(text), match.end() + 50)
            snippet = text[start:end].replace("\n", " ").strip()[:200]
            results.append({
                "pair_id": pair_id,
                "url": url[:500],
                "domain": domain,
                "tld": tld,
                "matched_term": ukrainian_term,
                "variant": "ukrainian",
                "context_snippet": snippet,
                "crawl_id": crawl_id,
                "crawl_date": crawl_date,
                "content_language": "eng",
                "ingested_at": now.isoformat(),
            })

    return results


def process_candidates(candidates: list[dict], patterns: list[tuple],
                       batch_size: int = 1000) -> list[dict]:
    """Process a list of candidate URLs: fetch WARC records and extract matches."""
    all_matches = []
    total = len(candidates)
    processed = 0
    fetched = 0

    log.info(f"Processing {total:,} candidate URLs...")

    for batch_start in range(0, total, batch_size):
        batch = candidates[batch_start:batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_FETCHES) as pool:
            futures = {}
            for row in batch:
                future = pool.submit(
                    fetch_warc_record,
                    row["warc_filename"],
                    int(row["warc_record_offset"]),
                    int(row["warc_record_length"]),
                )
                futures[future] = row

            for future in as_completed(futures):
                row = futures[future]
                processed += 1
                text = future.result()
                if text:
                    fetched += 1
                    matches = extract_matches(
                        text, row["url"], row["crawl"],
                        row["domain"], row.get("tld", ""),
                        patterns,
                    )
                    all_matches.extend(matches)

        log.info(
            f"  {processed:,}/{total:,} processed, "
            f"{fetched:,} fetched, {len(all_matches):,} matches"
        )

    return all_matches


def push_to_bigquery(matches: list[dict]):
    """Push matches to BigQuery."""
    if not matches:
        log.info("No matches to push")
        return

    from google.cloud import bigquery

    client = bigquery.Client(project=BQ_PROJECT)

    # Convert to BQ-compatible format
    for m in matches:
        m["crawl_date"] = m["crawl_date"]  # already a string
        m["ingested_at"] = m["ingested_at"]

    errors = client.insert_rows_json(BQ_TABLE, matches)
    if errors:
        log.error(f"BQ insert errors: {errors[:3]}")
    else:
        log.info(f"Pushed {len(matches):,} matches to BigQuery")


def load_candidates(input_path: str) -> list[dict]:
    """Load candidate URLs from CSV (local file or S3 path)."""
    if input_path.startswith("s3://"):
        import boto3
        s3 = boto3.client("s3")
        bucket, key = input_path[5:].split("/", 1)
        obj = s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(content))
    else:
        reader = csv.DictReader(open(input_path))

    return list(reader)


def main():
    parser = argparse.ArgumentParser(description="Process WARC records for toponym matches")
    parser.add_argument("--input", required=True, help="CSV file with candidate URLs (local or s3://)")
    parser.add_argument("--limit", type=int, help="Limit number of URLs to process")
    parser.add_argument("--dry-run", action="store_true", help="Process but don't push to BQ")
    args = parser.parse_args()

    pairs = load_pairs()
    patterns = build_patterns(pairs)
    log.info(f"Loaded {len(pairs)} pairs with {len(patterns)} patterns")

    candidates = load_candidates(args.input)
    if args.limit:
        candidates = candidates[:args.limit]
    log.info(f"Loaded {len(candidates):,} candidate URLs")

    matches = process_candidates(candidates, patterns)
    log.info(f"Total matches: {len(matches):,}")

    if matches and not args.dry_run:
        push_to_bigquery(matches)
    elif matches:
        log.info(f"Dry run — would push {len(matches):,} matches to BigQuery")
        # Show sample
        for m in matches[:5]:
            log.info(f"  {m['crawl_id']} | {m['pair_id']} | {m['variant']} | {m['domain']} | {m['matched_term']}")


if __name__ == "__main__":
    main()
