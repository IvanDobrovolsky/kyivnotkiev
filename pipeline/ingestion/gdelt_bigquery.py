"""Fetch GDELT data via BigQuery public dataset.

Scans URL + entity names for all enabled pair terms.
Partition-pruned to date range — typically ~$3 for full 2015–2025.

Requires: google-cloud-bigquery, GCP project with billing enabled.

Usage:
    python -m pipeline.ingestion.gdelt_bigquery [--start 2015-02-01 --end 2025-12-31]
"""

import argparse
import logging
import re
from pathlib import Path

import pandas as pd
import yaml
from google.cloud import bigquery

from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

GCP_PROJECT = "kyivnotkiev-research"
OUT_DIR = ROOT_DIR / "data" / "raw" / "gdelt"


def load_pairs():
    with open(ROOT_DIR / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)
    return [p for p in cfg["pairs"] if p.get("enabled", True)]


def build_query(pairs, start_date, end_date):
    conditions = []
    for p in pairs:
        for term in [p["russian"], p["ukrainian"]]:
            escaped = term.replace("'", "\\'").lower()
            conditions.append(f"LOWER(DocumentIdentifier) LIKE '%{escaped}%'")
            conditions.append(f"LOWER(IFNULL(AllNames,'')) LIKE '%{escaped}%'")
            if " " in term:
                hyphenated = escaped.replace(" ", "-")
                conditions.append(f"LOWER(DocumentIdentifier) LIKE '%{hyphenated}%'")

    where = " OR ".join(conditions)

    return f"""
    SELECT
        SourceCommonName AS domain,
        DocumentIdentifier AS url,
        FORMAT_DATE('%Y%m%d', CAST(_PARTITIONTIME AS DATE)) AS gkg_date,
        LOWER(IFNULL(AllNames,'')) AS allnames
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE _PARTITIONTIME >= '{start_date}'
      AND _PARTITIONTIME < DATE_ADD(DATE '{end_date}', INTERVAL 1 DAY)
      AND ({where})
    """


def classify_matches(df, pairs):
    """Assign pair_id and variant to each row based on term matching."""
    pair_patterns = []
    for p in pairs:
        pair_patterns.append({
            "id": p["id"],
            "russian": p["russian"],
            "ukrainian": p["ukrainian"],
            "ru_re": re.compile(re.escape(p["russian"]), re.IGNORECASE),
            "ua_re": re.compile(re.escape(p["ukrainian"]), re.IGNORECASE),
        })

    results = []
    for _, row in df.iterrows():
        text = str(row.get("url", "")) + " " + str(row.get("allnames", ""))
        for pp in pair_patterns:
            if pp["ru_re"].search(text):
                results.append({
                    "pair_id": pp["id"],
                    "date": str(row.get("gkg_date", "")),
                    "source_domain": str(row.get("domain", "")),
                    "matched_term": pp["russian"],
                    "variant": "russian",
                    "count": 1,
                })
            if pp["ua_re"].search(text):
                results.append({
                    "pair_id": pp["id"],
                    "date": str(row.get("gkg_date", "")),
                    "source_domain": str(row.get("domain", "")),
                    "matched_term": pp["ukrainian"],
                    "variant": "ukrainian",
                    "count": 1,
                })
    return pd.DataFrame(results)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2015-02-01", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default="2025-12-31", help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--dry-run", action="store_true", help="Show cost estimate only")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pairs = load_pairs()
    sql = build_query(pairs, args.start, args.end)
    log.info(f"Pairs: {len(pairs)}, SQL: {len(sql):,} chars")

    client = bigquery.Client(project=GCP_PROJECT)

    if args.dry_run:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = client.query(sql, job_config=job_config)
        gb = job.total_bytes_processed / 1e9
        cost = job.total_bytes_processed / 1e12 * 5
        log.info(f"Dry run: {gb:.1f} GB, ${cost:.2f}")
        return

    log.info("Running query...")
    job = client.query(sql)
    log.info(f"Job: {job.job_id}")

    results = job.result()
    gb = job.total_bytes_processed / 1e9
    cost = job.total_bytes_processed / 1e12 * 5
    log.info(f"Scanned: {gb:.1f} GB, Cost: ${cost:.2f}")
    log.info(f"Raw rows: {results.total_rows:,}")

    log.info("Downloading results...")
    raw_df = results.to_dataframe()

    # Save raw BQ output
    raw_path = OUT_DIR / "gdelt_bq_raw.parquet"
    raw_df.to_parquet(raw_path, index=False)
    log.info(f"Saved raw: {raw_path} ({len(raw_df):,} rows)")
    log.info(f"Date range: {raw_df['gkg_date'].min()} to {raw_df['gkg_date'].max()}")

    # Classify into pair matches
    log.info("Classifying matches...")
    classified = classify_matches(raw_df, pairs)
    log.info(f"Classified: {len(classified):,} matches")

    # Aggregate: monthly counts per pair × domain × variant
    classified["date"] = pd.to_datetime(classified["date"], format="%Y%m%d", errors="coerce")
    classified = classified.dropna(subset=["date"])
    classified["month"] = classified["date"].dt.strftime("%Y-%m-01")

    agg = (classified.groupby(["pair_id", "month", "source_domain", "matched_term", "variant"])["count"]
           .sum().reset_index())
    agg = agg.rename(columns={"month": "date"})

    out_path = OUT_DIR / "gdelt_bq_classified.parquet"
    agg.to_parquet(out_path, index=False)
    log.info(f"Saved classified: {out_path} ({len(agg):,} rows)")

    # Per-pair summary
    for pid, grp in agg.groupby("pair_id"):
        p = next((x for x in pairs if x["id"] == pid), {})
        total = grp["count"].sum()
        ru = grp[grp["variant"] == "russian"]["count"].sum()
        ua = grp[grp["variant"] == "ukrainian"]["count"].sum()
        dates = grp["date"]
        log.info(f"  Pair {pid:3d} ({p.get('russian',''):20s}/{p.get('ukrainian',''):20s}): "
                 f"{total:>8,} total (RU:{ru:,} UA:{ua:,}) {dates.min()} to {dates.max()}")


if __name__ == "__main__":
    main()
