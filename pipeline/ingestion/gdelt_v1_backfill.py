"""Backfill GDELT data for 2010-2014 using v1 events_partitioned.

GDELT v2 GKG starts Feb 2015. This script fills the gap using v1 events,
matching toponyms against SOURCEURL (consistent with v2 methodology).

Note: v1 SOURCEURL matching undercounts vs v2 GKG AllNames, but is the
most honest approach. See data/audit/data_quality_findings.json.

Usage:
    python -m pipeline.ingestion.gdelt_v1_backfill
"""

import json
import logging
import re
from pathlib import Path

import pandas as pd
import yaml
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
GCP_PROJECT = "kyivnotkiev-research"
OUT_PATH = ROOT / "data" / "raw" / "gdelt" / "v1_backfill.parquet"


def load_pairs():
    with open(ROOT / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)
    return [p for p in cfg["pairs"] if p.get("enabled", True)]


def build_query(pairs):
    """Build BQ query matching SOURCEURL against all pair terms."""
    url_conditions = []
    for p in pairs:
        for term in [p["russian"], p["ukrainian"]]:
            escaped = term.replace("'", "\\'").lower()
            url_conditions.append(f"LOWER(SOURCEURL) LIKE '%{escaped}%'")
            if " " in term:
                hyphenated = escaped.replace(" ", "-")
                url_conditions.append(f"LOWER(SOURCEURL) LIKE '%{hyphenated}%'")

    where = " OR ".join(url_conditions)

    return f"""
    SELECT
        SourceURL,
        FORMAT_DATE('%Y-%m', CAST(_PARTITIONTIME AS DATE)) AS month,
        EXTRACT(YEAR FROM _PARTITIONTIME) AS year
    FROM `gdelt-bq.full.events_partitioned`
    WHERE _PARTITIONTIME >= '2010-01-01'
      AND _PARTITIONTIME < '2015-02-19'
      AND ({where})
    """


def classify_matches(df, pairs):
    """Assign pair_slug and variant to each URL."""
    pair_patterns = []
    for p in pairs:
        negatives = [re.compile(f, re.IGNORECASE) for f in p.get("homonym_filters", [])]
        pair_patterns.append({
            "slug": p["slug"],
            "russian": p["russian"],
            "ukrainian": p["ukrainian"],
            "ru_re": re.compile(re.escape(p["russian"]), re.IGNORECASE),
            "ua_re": re.compile(re.escape(p["ukrainian"]), re.IGNORECASE),
            "negatives": negatives,
        })

    results = []
    for _, row in df.iterrows():
        url = str(row.get("SourceURL", "")).lower()
        month = row.get("month", "")

        for pp in pair_patterns:
            if any(neg.search(url) for neg in pp["negatives"]):
                continue

            has_ru = pp["ru_re"].search(url) is not None
            has_ua = pp["ua_re"].search(url) is not None

            if has_ua:
                results.append({
                    "date": f"{month}-01",
                    "source_domain": "gdelt_v1",
                    "matched_term": pp["ukrainian"],
                    "variant": "ukrainian",
                    "count": 1,
                    "pair_slug": pp["slug"],
                })
            elif has_ru:
                results.append({
                    "date": f"{month}-01",
                    "source_domain": "gdelt_v1",
                    "matched_term": pp["russian"],
                    "variant": "russian",
                    "count": 1,
                    "pair_slug": pp["slug"],
                })

    return pd.DataFrame(results)


def main():
    pairs = load_pairs()
    client = bigquery.Client(project=GCP_PROJECT)

    log.info(f"Building query for {len(pairs)} pairs...")
    query = build_query(pairs)

    log.info("Running BigQuery (v1 events_partitioned, 2010-01 to 2015-02)...")
    df = client.query(query).to_dataframe()
    log.info(f"Raw results: {len(df):,} rows")

    if df.empty:
        log.warning("No results!")
        return

    # Deduplicate by URL + month (same article appears in multiple events)
    before = len(df)
    df = df.drop_duplicates(subset=["SourceURL", "month"])
    log.info(f"After URL dedup: {len(df):,} (removed {before - len(df):,} duplicate events)")

    log.info("Classifying matches...")
    matched = classify_matches(df, pairs)
    log.info(f"Matched: {len(matched):,} pair-variant assignments")

    # Aggregate by pair_slug × month × variant
    agg = matched.groupby(["pair_slug", "date", "variant", "matched_term", "source_domain"])["count"].sum().reset_index()
    log.info(f"Aggregated: {len(agg):,} rows")

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(OUT_PATH, index=False)
    log.info(f"Saved: {OUT_PATH}")

    # Summary
    log.info(f"\nSummary:")
    for slug in sorted(agg["pair_slug"].unique()):
        sub = agg[agg["pair_slug"] == slug]
        total = sub["count"].sum()
        variants = sub.groupby("variant")["count"].sum().to_dict()
        log.info(f"  {slug}: {total:,} — {variants}")


if __name__ == "__main__":
    main()
