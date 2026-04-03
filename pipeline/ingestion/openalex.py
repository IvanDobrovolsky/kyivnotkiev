"""Collect academic paper data from OpenAlex for toponym adoption analysis.

OpenAlex (openalex.org) indexes 250M+ academic works. We search for
each toponym variant in paper titles and count occurrences per year.
Free API, no auth needed, polite rate limiting.

Replaces Common Crawl as the 7th data source — adds academic writing
as a distinct domain of language production.

Usage:
    python -m pipeline.ingestion.openalex [--pair-ids 1,2,3]
"""

import argparse
import json
import logging
import time
from pathlib import Path

import requests

from pipeline.config import DATA_DIR, PROCESSED_DIR, ensure_dirs, load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OPENALEX_API = "https://api.openalex.org/works"
RAW_DIR = DATA_DIR / "raw" / "openalex"
REQUEST_DELAY = 0.2  # OpenAlex is generous: 10 req/s for polite pool
MAILTO = "ivan@kyivnotkiev.org"  # Gets us into the polite pool (faster)

# Year range
START_YEAR = 2010  # Pre-2010 data too sparse/noisy for meaningful adoption signal
END_YEAR = 2026


def count_papers_by_year(term: str) -> dict[int, int]:
    """Count papers with term in title, grouped by publication year."""
    params = {
        "filter": f"title.search:{term}",
        "group_by": "publication_year",
        "per_page": 50,
        "mailto": MAILTO,
    }
    try:
        resp = requests.get(OPENALEX_API, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return {
            int(g["key"]): g["count"]
            for g in data.get("group_by", [])
            if START_YEAR <= int(g["key"]) <= END_YEAR
        }
    except (requests.RequestException, ValueError, KeyError) as e:
        log.warning(f"  OpenAlex query failed for '{term}': {e}")
        return {}


def collect_pair(pair: dict) -> dict | None:
    """Collect OpenAlex data for one pair."""
    pid = pair["id"]
    russian = pair["russian"]
    ukrainian = pair["ukrainian"]

    if pair.get("is_control", False):
        return None

    log.info(f"  Pair {pid}: '{russian}' vs '{ukrainian}'")

    russian_counts = count_papers_by_year(russian)
    time.sleep(REQUEST_DELAY)
    ukrainian_counts = count_papers_by_year(ukrainian)
    time.sleep(REQUEST_DELAY)

    if not russian_counts and not ukrainian_counts:
        log.info(f"    No data")
        return None

    # Merge years
    all_years = sorted(set(russian_counts.keys()) | set(ukrainian_counts.keys()))
    rows = []
    for year in all_years:
        ru = russian_counts.get(year, 0)
        uk = ukrainian_counts.get(year, 0)
        rows.append({
            "year": year,
            "russian_count": ru,
            "ukrainian_count": uk,
            "total": ru + uk,
        })

    total_ru = sum(r["russian_count"] for r in rows)
    total_uk = sum(r["ukrainian_count"] for r in rows)
    log.info(f"    {total_ru + total_uk} papers ({total_uk} Ukrainian, {total_ru} Russian)")

    return {
        "pair_id": pid,
        "russian_term": russian,
        "ukrainian_term": ukrainian,
        "category": pair["category"],
        "yearly": rows,
        "total_russian": total_ru,
        "total_ukrainian": total_uk,
    }


def collect_all(pair_ids: list[int] | None = None):
    """Collect OpenAlex data for all enabled pairs."""
    ensure_dirs()
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_pairs()
    pairs = [p for p in cfg["pairs"] if p.get("enabled", True)]
    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    log.info(f"Collecting OpenAlex data for {len(pairs)} pairs...")

    results = []
    for pair in pairs:
        data = collect_pair(pair)
        if data:
            results.append(data)

    # Save raw JSON
    out_path = RAW_DIR / "openalex_all_pairs.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    log.info(f"Saved: {out_path} ({len(results)} pairs)")

    # Save summary CSV
    import csv
    csv_path = PROCESSED_DIR / "openalex_summary.csv"
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["pair_id", "category", "russian_term", "ukrainian_term",
                         "year", "russian_count", "ukrainian_count", "total", "adoption_ratio"])
        for r in results:
            for yr in r["yearly"]:
                total = yr["total"]
                ratio = yr["ukrainian_count"] / total if total > 0 else None
                writer.writerow([
                    r["pair_id"], r["category"], r["russian_term"], r["ukrainian_term"],
                    yr["year"], yr["russian_count"], yr["ukrainian_count"], yr["total"],
                    round(ratio, 4) if ratio is not None else "",
                ])
    log.info(f"Saved: {csv_path}")

    # Print summary
    log.info(f"\n{'Pair':<30} {'Russian':>8} {'Ukrainian':>10} {'Total':>8} {'Adoption':>10}")
    log.info("-" * 70)
    for r in sorted(results, key=lambda x: x["total_ukrainian"] + x["total_russian"], reverse=True):
        total = r["total_russian"] + r["total_ukrainian"]
        pct = round(r["total_ukrainian"] / total * 100, 1) if total > 0 else 0
        log.info(f"  {r['russian_term']}/{r['ukrainian_term']:<24} {r['total_russian']:>8} {r['total_ukrainian']:>10} {total:>8} {pct:>9.1f}%")

    return results


def run(pair_ids: list[int] | None = None):
    """Entry point for orchestrator."""
    return collect_all(pair_ids)


def main():
    parser = argparse.ArgumentParser(description="Collect OpenAlex academic paper data")
    parser.add_argument("--pair-ids", type=str, default=None)
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    collect_all(pair_ids=pair_ids)


if __name__ == "__main__":
    main()
