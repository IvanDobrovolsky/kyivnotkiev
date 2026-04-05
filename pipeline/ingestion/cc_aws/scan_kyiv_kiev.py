"""Scan Common Crawl for Kyiv/Kiev URLs across all crawls 2013-2026.

One comprehensive Athena query per crawl. Finds all HTML pages with
'kyiv' or 'kiev' in the URL. Then fetches WARC records and scans
the actual page content for both variants.

Cost: ~$0.34 per crawl × 28 = ~$10 total.

Usage:
    python -m pipeline.ingestion.cc_aws.scan_kyiv_kiev
"""

import csv
import io
import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
ATHENA_DB = "ccindex"
S3_OUTPUT = "s3://kyivnotkiev-cc-results/athena/"
BQ_PROJECT = "kyivnotkiev-research"
BQ_TABLE = f"{BQ_PROJECT}.kyivnotkiev.raw_common_crawl"

athena = boto3.client("athena", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)

# All crawls 2013-2026 (2 per year)
CRAWLS = [
    "CC-MAIN-2013-20", "CC-MAIN-2013-48",
    "CC-MAIN-2014-15", "CC-MAIN-2014-42",
    "CC-MAIN-2015-14", "CC-MAIN-2015-40",
    "CC-MAIN-2016-18", "CC-MAIN-2016-44",
    "CC-MAIN-2017-13", "CC-MAIN-2017-43",
    "CC-MAIN-2018-09", "CC-MAIN-2018-43",
    "CC-MAIN-2019-09", "CC-MAIN-2019-43",
    "CC-MAIN-2020-10", "CC-MAIN-2020-45",
    "CC-MAIN-2021-10", "CC-MAIN-2021-43",
    "CC-MAIN-2022-05", "CC-MAIN-2022-40",
    "CC-MAIN-2023-06", "CC-MAIN-2023-40",
    "CC-MAIN-2024-10", "CC-MAIN-2024-42",
    "CC-MAIN-2025-05", "CC-MAIN-2025-26",
    "CC-MAIN-2026-04", "CC-MAIN-2026-12",
]

KYIV_RE = re.compile(r"\bKyiv\b", re.IGNORECASE)
KIEV_RE = re.compile(r"\bKiev\b", re.IGNORECASE)
# Exclude: Chicken Kiev, Kiev cake, Dynamo Kiev, Kievan Rus (separate pairs)
EXCLUDE_RE = re.compile(r"chicken\s+kiev|kiev\s+cake|dynamo\s+kiev|kievan\s+rus", re.IGNORECASE)


def run_athena(sql: str) -> str:
    """Submit query, wait, return execution ID."""
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DB},
        WorkGroup="primary",
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )
    qid = resp["QueryExecutionId"]

    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)
        state = r["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(5)

    if state != "SUCCEEDED":
        reason = r["QueryExecution"]["Status"].get("StateChangeReason", "")
        log.error(f"  Query failed: {reason}")
        return ""

    stats = r["QueryExecution"]["Statistics"]
    gb = stats["DataScannedInBytes"] / 1024**3
    sec = stats["EngineExecutionTimeInMillis"] / 1000
    log.info(f"  Scanned {gb:.1f} GB (${gb * 5 / 1024:.2f}), {sec:.0f}s")
    return qid


def get_result_s3_path(qid: str) -> str:
    """Get the S3 path of query results CSV."""
    r = athena.get_query_execution(QueryExecutionId=qid)
    return r["QueryExecution"]["ResultConfiguration"]["OutputLocation"]


def download_results(s3_path: str) -> list[dict]:
    """Download Athena CSV results from S3."""
    # s3://bucket/key -> bucket, key
    parts = s3_path.replace("s3://", "").split("/", 1)
    bucket, key = parts[0], parts[1]
    obj = s3.get_object(Bucket=bucket, Key=key)
    content = obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(content))
    return list(reader)


def fetch_warc_text(filename: str, offset: int, length: int) -> str | None:
    """Fetch and extract text from a WARC record."""
    url = f"https://data.commoncrawl.org/{filename}"
    try:
        resp = requests.get(url, headers={"Range": f"bytes={offset}-{offset + length - 1}"}, timeout=30)
        if resp.status_code not in (200, 206):
            return None
        from warcio.archiveiterator import ArchiveIterator
        for record in ArchiveIterator(io.BytesIO(resp.content)):
            if record.rec_type == "response":
                return record.content_stream().read().decode("utf-8", errors="ignore")
    except Exception:
        return None
    return None


def scan_text(text: str, url: str, crawl_id: str, domain: str) -> list[dict]:
    """Scan page text for Kyiv/Kiev mentions. Return match records."""
    if not text:
        return []

    # Skip excluded compound terms
    clean = EXCLUDE_RE.sub("", text)

    now = datetime.now(timezone.utc)
    parts = crawl_id.replace("CC-MAIN-", "").split("-")
    year = int(parts[0])
    week = int(parts[1])
    month = min(12, max(1, (week * 7) // 30 + 1))
    crawl_date = f"{year}-{month:02d}-01"

    results = []
    for match in KYIV_RE.finditer(clean):
        start = max(0, match.start() - 50)
        end = min(len(clean), match.end() + 50)
        results.append({
            "pair_id": 1, "url": url[:500], "domain": domain, "tld": domain.split(".")[-1],
            "matched_term": "Kyiv", "variant": "ukrainian",
            "context_snippet": clean[start:end].replace("\n", " ")[:200],
            "crawl_id": crawl_id, "crawl_date": crawl_date,
            "content_language": "eng", "ingested_at": now.isoformat(),
        })

    for match in KIEV_RE.finditer(clean):
        start = max(0, match.start() - 50)
        end = min(len(clean), match.end() + 50)
        results.append({
            "pair_id": 1, "url": url[:500], "domain": domain, "tld": domain.split(".")[-1],
            "matched_term": "Kiev", "variant": "russian",
            "context_snippet": clean[start:end].replace("\n", " ")[:200],
            "crawl_id": crawl_id, "crawl_date": crawl_date,
            "content_language": "eng", "ingested_at": now.isoformat(),
        })

    return results


def process_crawl(crawl_id: str) -> list[dict]:
    """Query index + fetch + scan for one crawl."""
    log.info(f"Processing {crawl_id}...")

    # Phase 1: Athena query
    sql = f"""
        SELECT url, url_host_registered_domain as domain,
            warc_filename, warc_record_offset as offset, warc_record_length as length
        FROM ccindex.ccindex
        WHERE crawl = '{crawl_id}' AND subset = 'warc'
          AND fetch_status = 200 AND content_mime_detected = 'text/html'
          AND (lower(url) LIKE '%kyiv%' OR lower(url) LIKE '%kiev%')
    """
    qid = run_athena(sql)
    if not qid:
        return []

    # Download results
    s3_path = get_result_s3_path(qid)
    candidates = download_results(s3_path)
    log.info(f"  {crawl_id}: {len(candidates)} candidate URLs")

    if not candidates:
        return []

    # Phase 2: Fetch WARC records and scan (parallel, max 10)
    all_matches = []
    processed = 0

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {}
        for row in candidates:
            f = pool.submit(fetch_warc_text, row["warc_filename"],
                            int(row["offset"]), int(row["length"]))
            futures[f] = row

        for future in as_completed(futures):
            row = futures[future]
            processed += 1
            text = future.result()
            if text:
                matches = scan_text(text, row["url"], crawl_id, row["domain"])
                all_matches.extend(matches)

            if processed % 500 == 0:
                log.info(f"  {crawl_id}: {processed}/{len(candidates)} processed, {len(all_matches)} matches")

    log.info(f"  {crawl_id}: DONE — {len(all_matches)} matches from {len(candidates)} URLs")
    return all_matches


def push_to_bigquery(matches: list[dict]):
    """Push matches to BQ."""
    if not matches:
        return
    from google.cloud import bigquery
    client = bigquery.Client(project=BQ_PROJECT)
    errors = client.insert_rows_json(BQ_TABLE, matches)
    if errors:
        log.error(f"BQ errors: {errors[:3]}")
    else:
        log.info(f"Pushed {len(matches):,} matches to BigQuery")


def main():
    log.info("=" * 60)
    log.info("Common Crawl: Kyiv/Kiev comprehensive scan")
    log.info(f"Crawls: {len(CRAWLS)} (2013-2026)")
    log.info(f"Estimated cost: ~${len(CRAWLS) * 0.34:.0f}")
    log.info("=" * 60)

    all_matches = []
    summary = []

    for crawl_id in CRAWLS:
        matches = process_crawl(crawl_id)
        all_matches.extend(matches)

        kyiv_count = sum(1 for m in matches if m["variant"] == "ukrainian")
        kiev_count = sum(1 for m in matches if m["variant"] == "russian")
        summary.append({"crawl": crawl_id, "kyiv": kyiv_count, "kiev": kiev_count,
                         "total": len(matches)})
        log.info(f"  Running total: {len(all_matches):,} matches")

        # Push per-crawl to avoid losing data
        if matches:
            push_to_bigquery(matches)

    # Print summary
    log.info("\n" + "=" * 60)
    log.info("SUMMARY")
    log.info("=" * 60)
    for s in summary:
        total = s["total"]
        pct = round(s["kyiv"] / total * 100, 1) if total > 0 else 0
        log.info(f"  {s['crawl']}: {s['total']:>6} matches | Kyiv {s['kyiv']:>5} ({pct}%) | Kiev {s['kiev']:>5}")

    total = len(all_matches)
    kyiv_total = sum(1 for m in all_matches if m["variant"] == "ukrainian")
    log.info(f"\n  TOTAL: {total:,} matches | Kyiv: {kyiv_total:,} ({kyiv_total/total*100:.1f}%)" if total else "  No matches")

    # Save local summary
    out = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "common_crawl"
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "kyiv_kiev_summary.json", "w") as f:
        json.dump(summary, f, indent=2)
    log.info(f"  Summary saved: {out / 'kyiv_kiev_summary.json'}")


if __name__ == "__main__":
    main()
