"""Phase 1: Query Common Crawl index via Athena to find candidate URLs.

For each crawl, queries the CC columnar index filtered by our target domains
and English language. Saves candidate URLs (with WARC file locations) to S3.

Usage:
    python -m pipeline.ingestion.cc_aws.query_index [--crawl CC-MAIN-2024-10]

Prerequisites:
    1. AWS account with Athena access
    2. Run setup_athena.sql to create the external table
    3. Create S3 bucket for results (see config.py)
    4. Set AWS credentials: aws configure
"""

import argparse
import logging
import time

import boto3

from pipeline.ingestion.cc_aws.config import (
    AWS_REGION, ATHENA_DATABASE, ATHENA_OUTPUT_BUCKET,
    CRAWL_IDS, NEWS_DOMAINS, NEWS_TLDS,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

athena = boto3.client("athena", region_name=AWS_REGION)


def build_domain_filter() -> str:
    """Build SQL WHERE clause for domain filtering."""
    # Exact domain matches
    domain_list = ", ".join(f"'{d}'" for d in NEWS_DOMAINS)
    # TLD matches
    tld_list = ", ".join(f"'{t}'" for t in NEWS_TLDS)
    return f"""(
        url_host_registered_domain IN ({domain_list})
        OR url_host_tld IN ({tld_list})
    )"""


def run_athena_query(sql: str, description: str = "") -> str:
    """Submit Athena query and wait for completion. Returns query execution ID."""
    log.info(f"  Athena query: {description}")

    response = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_BUCKET},
    )
    query_id = response["QueryExecutionId"]

    # Poll for completion
    while True:
        result = athena.get_query_execution(QueryExecutionId=query_id)
        state = result["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = result["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
        log.error(f"  Query failed: {reason}")
        return ""

    stats = result["QueryExecution"]["Statistics"]
    scanned_mb = stats.get("DataScannedInBytes", 0) / 1024 / 1024
    runtime_s = stats.get("EngineExecutionTimeInMillis", 0) / 1000
    log.info(f"  Done: {scanned_mb:.0f} MB scanned, {runtime_s:.1f}s, cost ~${scanned_mb / 1024 * 5:.2f}")

    return query_id


def get_query_results(query_id: str) -> list[dict]:
    """Fetch results from a completed Athena query."""
    paginator = athena.get_paginator("get_query_results")
    rows = []
    for page in paginator.paginate(QueryExecutionId=query_id):
        columns = [c["Name"] for c in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
        for row in page["ResultSet"]["Rows"][1:]:  # skip header
            values = [d.get("VarCharValue", "") for d in row["Data"]]
            rows.append(dict(zip(columns, values)))
    return rows


def query_crawl(crawl_id: str) -> int:
    """Query CC index for candidate URLs in one crawl.

    Results are saved to S3 by Athena (CTAS or direct output).
    Returns number of candidate URLs found.
    """
    log.info(f"Querying crawl: {crawl_id}")

    domain_filter = build_domain_filter()

    # Query: get WARC locations for English pages on target domains
    sql = f"""
        SELECT
            url,
            url_host_registered_domain as domain,
            url_host_tld as tld,
            content_languages,
            warc_filename,
            warc_record_offset,
            warc_record_length,
            crawl
        FROM ccindex.ccindex
        WHERE crawl = '{crawl_id}'
          AND subset = 'warc'
          AND fetch_status = 200
          AND content_mime_detected = 'text/html'
          AND (content_languages LIKE '%eng%' OR url_host_tld = 'ua')
          AND {domain_filter}
    """

    query_id = run_athena_query(sql, f"candidates for {crawl_id}")
    if not query_id:
        return 0

    # Get count
    count_sql = f"""
        SELECT COUNT(*) as cnt
        FROM ccindex.ccindex
        WHERE crawl = '{crawl_id}'
          AND subset = 'warc'
          AND fetch_status = 200
          AND content_mime_detected = 'text/html'
          AND (content_languages LIKE '%eng%' OR url_host_tld = 'ua')
          AND {domain_filter}
    """
    count_id = run_athena_query(count_sql, f"count for {crawl_id}")
    if count_id:
        results = get_query_results(count_id)
        if results:
            count = int(results[0]["cnt"])
            log.info(f"  {crawl_id}: {count:,} candidate URLs")
            return count

    return 0


def main():
    parser = argparse.ArgumentParser(description="Query CC index via Athena")
    parser.add_argument("--crawl", type=str, help="Process specific crawl ID")
    parser.add_argument("--dry-run", action="store_true", help="Show SQL without running")
    parser.add_argument("--list-crawls", action="store_true", help="List configured crawls")
    args = parser.parse_args()

    if args.list_crawls:
        for c in CRAWL_IDS:
            print(c)
        return

    crawls = [args.crawl] if args.crawl else CRAWL_IDS

    if args.dry_run:
        domain_filter = build_domain_filter()
        print(f"Domain filter covers {len(NEWS_DOMAINS)} domains + TLDs {NEWS_TLDS}")
        print(f"Crawls to process: {len(crawls)}")
        print(f"\nSample query for {crawls[0]}:")
        print(f"""
SELECT url, url_host_registered_domain, warc_filename, warc_record_offset, warc_record_length
FROM ccindex.ccindex
WHERE crawl = '{crawls[0]}'
  AND subset = 'warc'
  AND fetch_status = 200
  AND content_mime_detected = 'text/html'
  AND (content_languages LIKE '%eng%' OR url_host_tld = 'ua')
  AND {domain_filter}
LIMIT 10;
        """)
        return

    total_urls = 0
    for crawl_id in crawls:
        count = query_crawl(crawl_id)
        total_urls += count

    log.info(f"Total candidate URLs across {len(crawls)} crawls: {total_urls:,}")


if __name__ == "__main__":
    main()
