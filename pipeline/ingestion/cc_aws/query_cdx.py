"""Query Common Crawl's CDX API (FREE) to find candidate URLs by domain.

This replaces the expensive Athena approach. The CDX API lets us search
the CC index by domain for free, returning WARC file locations.

Usage:
    python -m pipeline.ingestion.cc_aws.query_cdx --crawl CC-MAIN-2024-10
    python -m pipeline.ingestion.cc_aws.query_cdx --all
"""

import argparse
import csv
import json
import logging
import time
from pathlib import Path

import requests

from pipeline.ingestion.cc_aws.config import CRAWL_IDS, NEWS_DOMAINS

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CDX_BASE = "https://index.commoncrawl.org"
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "common_crawl"


def query_domain(crawl_id: str, domain: str, limit: int = 10000) -> list[dict]:
    """Query CDX API for all English HTML pages from a domain in a crawl."""
    index_name = f"{crawl_id}-index"
    url = f"{CDX_BASE}/{index_name}"
    params = {
        "url": f"{domain}/*",
        "output": "json",
        "limit": limit,
        "filter": "=status:200",
        "fl": "url,filename,offset,length,languages,timestamp",
    }

    try:
        resp = requests.get(url, params=params, timeout=60)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()

        results = []
        for line in resp.text.strip().split("\n"):
            if not line:
                continue
            try:
                record = json.loads(line)
                # Filter to English pages
                langs = record.get("languages", "")
                if "eng" in langs or domain.endswith(".ua"):
                    results.append(record)
            except json.JSONDecodeError:
                continue
        return results
    except requests.RequestException as e:
        log.warning(f"  CDX query failed for {domain}: {e}")
        return []


def query_crawl(crawl_id: str) -> list[dict]:
    """Query all target domains for one crawl. Returns candidate URLs."""
    log.info(f"Querying crawl: {crawl_id}")
    all_candidates = []

    for i, domain in enumerate(NEWS_DOMAINS):
        results = query_domain(crawl_id, domain)
        if results:
            for r in results:
                r["domain"] = domain
                r["crawl"] = crawl_id
            all_candidates.extend(results)
            log.info(f"  [{i+1}/{len(NEWS_DOMAINS)}] {domain}: {len(results)} pages")
        else:
            log.debug(f"  [{i+1}/{len(NEWS_DOMAINS)}] {domain}: 0 pages")

        # Be polite to CDX API
        time.sleep(0.5)

    # Also query .ua domains (top Ukrainian news)
    ua_domains = [
        "pravda.com.ua", "ukrinform.ua", "unian.net", "24tv.ua",
        "liga.net", "interfax.com.ua", "korrespondent.net",
    ]
    for domain in ua_domains:
        results = query_domain(crawl_id, domain, limit=5000)
        if results:
            for r in results:
                r["domain"] = domain
                r["crawl"] = crawl_id
            all_candidates.extend(results)
            log.info(f"  [UA] {domain}: {len(results)} pages")
        time.sleep(0.5)

    log.info(f"  {crawl_id}: {len(all_candidates):,} total candidate URLs")
    return all_candidates


def save_candidates(crawl_id: str, candidates: list[dict]):
    """Save candidates to CSV for processing."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / f"candidates_{crawl_id}.csv"

    fieldnames = ["url", "domain", "crawl", "filename", "offset", "length", "languages", "timestamp"]
    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(candidates)

    log.info(f"  Saved: {out_path} ({len(candidates)} rows)")
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Query CC CDX API (free)")
    parser.add_argument("--crawl", type=str, help="Process specific crawl ID")
    parser.add_argument("--all", action="store_true", help="Process all configured crawls")
    parser.add_argument("--list-crawls", action="store_true")
    args = parser.parse_args()

    if args.list_crawls:
        for c in CRAWL_IDS:
            print(c)
        return

    crawls = CRAWL_IDS if args.all else ([args.crawl] if args.crawl else CRAWL_IDS[:1])

    total = 0
    for crawl_id in crawls:
        candidates = query_crawl(crawl_id)
        if candidates:
            save_candidates(crawl_id, candidates)
            total += len(candidates)

    log.info(f"Total across {len(crawls)} crawls: {total:,} candidate URLs")


if __name__ == "__main__":
    main()
