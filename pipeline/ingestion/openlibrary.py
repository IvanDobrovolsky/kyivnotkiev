"""Open Library book title adoption — Internet Archive's open book catalog.

Counts how many published books use each toponym variant in their title per year.
Extends Google Ngrams coverage beyond 2019 to present.

Usage:
    python -m pipeline.ingestion.openlibrary [--start-year 2010] [--end-year 2025]
"""

import argparse
import json
import logging
import time
from pathlib import Path

import requests
from google.cloud import bigquery

from pipeline.config import load_pairs, DATA_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

API_URL = "https://openlibrary.org/search.json"
REQUEST_DELAY = 0.3  # Be polite to Internet Archive
RAW_DIR = DATA_DIR / "raw" / "openlibrary"
BQ_TABLE = "kyivnotkiev.raw_openlibrary"


def search_books(term: str, year: int) -> int:
    """Count books with term in title published in a given year."""
    params = {
        "q": term,
        "first_publish_year": year,
        "limit": 1,
    }
    try:
        resp = requests.get(API_URL, params=params, timeout=15)
        if resp.status_code == 200:
            return resp.json().get("numFound", 0)
        else:
            log.warning(f"  HTTP {resp.status_code} for {term} {year}")
            return 0
    except Exception as e:
        log.warning(f"  Error for {term} {year}: {e}")
        return 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=2010)
    parser.add_argument("--end-year", type=int, default=2025)
    args = parser.parse_args()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_pairs()
    pairs = [(p["id"], p["russian"], p["ukrainian"])
             for p in cfg["pairs"]
             if p.get("enabled", True) and not p.get("is_control", False)]

    log.info(f"Open Library ingestion: {len(pairs)} pairs, {args.start_year}-{args.end_year}")

    results = []
    years = list(range(args.start_year, args.end_year + 1))

    for pair_id, russian, ukrainian in pairs:
        log.info(f"  Pair {pair_id}: {russian} / {ukrainian}")

        for term, variant in [(russian, "russian"), (ukrainian, "ukrainian")]:
            for year in years:
                count = search_books(term, year)
                results.append({
                    "pair_id": pair_id,
                    "year": year,
                    "term": term,
                    "variant": variant,
                    "book_count": count,
                })
                time.sleep(REQUEST_DELAY)

        # Log progress for this pair
        pair_results = [r for r in results if r["pair_id"] == pair_id]
        ru_total = sum(r["book_count"] for r in pair_results if r["variant"] == "russian")
        ua_total = sum(r["book_count"] for r in pair_results if r["variant"] == "ukrainian")
        total = ru_total + ua_total
        adopt = ua_total / total * 100 if total > 0 else 0
        log.info(f"    RU={ru_total} UA={ua_total} adopt={adopt:.0f}%")

    # Save locally
    out_path = RAW_DIR / "openlibrary_results.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    log.info(f"Saved {len(results)} records to {out_path}")

    # Upload to BigQuery
    try:
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("pair_id", "INT64"),
                bigquery.SchemaField("year", "INT64"),
                bigquery.SchemaField("term", "STRING"),
                bigquery.SchemaField("variant", "STRING"),
                bigquery.SchemaField("book_count", "INT64"),
            ],
            write_disposition="WRITE_TRUNCATE",
        )
        job = client.load_table_from_json(results, BQ_TABLE, job_config=job_config)
        job.result()
        log.info(f"Uploaded {len(results)} rows to {BQ_TABLE}")
    except Exception as e:
        log.warning(f"BQ upload failed: {e}")

    # Summary
    log.info("\nSUMMARY (2018 vs 2023):")
    for pair_id, russian, ukrainian in pairs[:10]:
        pr = [r for r in results if r["pair_id"] == pair_id]
        for year in [2018, 2023]:
            ru = sum(r["book_count"] for r in pr if r["variant"] == "russian" and r["year"] == year)
            ua = sum(r["book_count"] for r in pr if r["variant"] == "ukrainian" and r["year"] == year)
            total = ru + ua
            adopt = ua / total * 100 if total > 0 else 0
            log.info(f"  {year} {russian:>15}/{ukrainian:<15}: RU={ru:3d} UA={ua:3d} ({adopt:.0f}%)")


if __name__ == "__main__":
    main()
