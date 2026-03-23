"""Collect toponym mention counts from GDELT via Google BigQuery.

Queries the GDELT Global Knowledge Graph (GKG) for article counts mentioning
each spelling variant, aggregated by week and source country.

Usage:
    python -m src.pipeline.collect_gdelt [--pair-ids 1,2,3] [--dry-run]
"""

import argparse
import logging
import time

import pandas as pd
from google.cloud import bigquery

from src.config import (
    BQ_COST_LIMIT_TB,
    END_DATE,
    GDELT_RAW_DIR,
    START_DATE,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Estimated bytes per full-table scan of GKG partitioned (for cost tracking)
BYTES_PER_TB = 1_099_511_627_776


def build_query(russian: str, ukrainian: str, start: str, end: str) -> str:
    """Build a BigQuery SQL query to count mentions of both spelling variants.

    Searches the GKG V2ExtrasXML and Extras fields for exact term matches,
    aggregated by ISO week and source country.
    """
    # Escape single quotes for SQL
    rus = russian.replace("'", "\\'")
    ukr = ukrainian.replace("'", "\\'")

    return f"""
    SELECT
        DATE_TRUNC(DATE(_PARTITIONTIME), WEEK(MONDAY)) AS week,
        SourceCommonName AS source_country,
        COUNTIF(
            REGEXP_CONTAINS(LOWER(Extras), r'(?i)\\b{rus.lower()}\\b')
            OR REGEXP_CONTAINS(LOWER(V2Extras), r'(?i)\\b{rus.lower()}\\b')
        ) AS russian_count,
        COUNTIF(
            REGEXP_CONTAINS(LOWER(Extras), r'(?i)\\b{ukr.lower()}\\b')
            OR REGEXP_CONTAINS(LOWER(V2Extras), r'(?i)\\b{ukr.lower()}\\b')
        ) AS ukrainian_count,
        COUNT(*) AS total_articles
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE
        _PARTITIONTIME >= TIMESTAMP('{start}')
        AND _PARTITIONTIME < TIMESTAMP('{end}')
        AND (
            REGEXP_CONTAINS(LOWER(Extras), r'(?i)\\b{rus.lower()}\\b')
            OR REGEXP_CONTAINS(LOWER(V2Extras), r'(?i)\\b{rus.lower()}\\b')
            OR REGEXP_CONTAINS(LOWER(Extras), r'(?i)\\b{ukr.lower()}\\b')
            OR REGEXP_CONTAINS(LOWER(V2Extras), r'(?i)\\b{ukr.lower()}\\b')
        )
    GROUP BY week, source_country
    ORDER BY week, source_country
    """


def build_locations_query(russian: str, ukrainian: str, start: str, end: str) -> str:
    """Alternative query using the Locations field in GKG for place name mentions."""
    rus = russian.replace("'", "\\'")
    ukr = ukrainian.replace("'", "\\'")

    return f"""
    SELECT
        DATE_TRUNC(DATE(_PARTITIONTIME), WEEK(MONDAY)) AS week,
        SourceCommonName AS source_country,
        COUNTIF(
            REGEXP_CONTAINS(LOWER(V2Locations), r'(?i){rus.lower()}')
        ) AS russian_count,
        COUNTIF(
            REGEXP_CONTAINS(LOWER(V2Locations), r'(?i){ukr.lower()}')
        ) AS ukrainian_count,
        COUNT(*) AS total_articles
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE
        _PARTITIONTIME >= TIMESTAMP('{start}')
        AND _PARTITIONTIME < TIMESTAMP('{end}')
        AND (
            REGEXP_CONTAINS(LOWER(V2Locations), r'(?i){rus.lower()}')
            OR REGEXP_CONTAINS(LOWER(V2Locations), r'(?i){ukr.lower()}')
        )
    GROUP BY week, source_country
    ORDER BY week, source_country
    """


def estimate_query_cost(client: bigquery.Client, query: str) -> float:
    """Estimate query cost in TB using dry run."""
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job = client.query(query, job_config=job_config)
    bytes_processed = job.total_bytes_processed
    tb_processed = bytes_processed / BYTES_PER_TB
    return tb_processed


def run_query(client: bigquery.Client, query: str, pair_id: int, dry_run: bool = False) -> pd.DataFrame | None:
    """Execute a BigQuery query and return results as DataFrame."""
    tb_estimate = estimate_query_cost(client, query)
    log.info(f"  Pair {pair_id}: estimated cost = {tb_estimate:.4f} TB")

    if tb_estimate > BQ_COST_LIMIT_TB:
        log.warning(f"  Pair {pair_id}: query exceeds {BQ_COST_LIMIT_TB} TB limit, skipping")
        return None

    if dry_run:
        log.info(f"  Pair {pair_id}: dry run, skipping execution")
        return None

    job = client.query(query)
    df = job.to_dataframe()
    log.info(f"  Pair {pair_id}: {len(df)} rows returned, {job.total_bytes_processed / 1e9:.2f} GB processed")
    return df


def build_the_ukraine_query(start: str, end: str) -> str:
    """Special query for 'the Ukraine' vs 'Ukraine' (pair 27).

    The naive regex \\bthe ukraine\\b matches false positives like
    'the Ukraine war', 'the Ukraine crisis', 'the Ukraine conflict'
    where 'the' modifies the noun phrase, not the country name.

    This query counts 'the Ukraine' only when used as a standalone
    country reference — i.e., followed by a verb, punctuation, comma,
    preposition, or end of string — NOT followed by a noun/adjective
    that would make it a compound modifier.
    """
    # Match "the ukraine" only when followed by:
    #   - punctuation (.,;:!?'")
    #   - common verbs/prepositions (is, was, has, will, and, or, to, in, as, etc.)
    #   - end of string
    # Do NOT match when followed by nouns like: war, crisis, conflict, situation,
    #   government, military, army, border, invasion, peace, aid, etc.
    return f"""
    SELECT
        DATE_TRUNC(DATE(_PARTITIONTIME), WEEK(MONDAY)) AS week,
        SourceCommonName AS source_country,
        COUNTIF(
            REGEXP_CONTAINS(LOWER(Extras),
                r'(?i)\\bthe ukraine\\b(?!\\s+(?:war|crisis|conflict|situation|government|military|army|navy|border|invasion|peace|aid|question|issue|problem|scandal|affair|deal|front|offensive|counteroffensive|forces|troops|grain|grain deal|support|package|aid package|defense|ministry|people|population|territory|region|side|leadership|president|government|parliament|economy|energy|refugee|reconstruction|drone|weapon|missile|attack|strike|operation)\\b)')
            OR REGEXP_CONTAINS(LOWER(V2Extras),
                r'(?i)\\bthe ukraine\\b(?!\\s+(?:war|crisis|conflict|situation|government|military|army|navy|border|invasion|peace|aid|question|issue|problem|scandal|affair|deal|front|offensive|counteroffensive|forces|troops|grain|grain deal|support|package|aid package|defense|ministry|people|population|territory|region|side|leadership|president|government|parliament|economy|energy|refugee|reconstruction|drone|weapon|missile|attack|strike|operation)\\b)')
        ) AS russian_count,
        COUNTIF(
            REGEXP_CONTAINS(LOWER(Extras), r'(?i)\\bukraine\\b')
            OR REGEXP_CONTAINS(LOWER(V2Extras), r'(?i)\\bukraine\\b')
        ) AS total_ukraine_count,
        COUNT(*) AS total_articles
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE
        _PARTITIONTIME >= TIMESTAMP('{start}')
        AND _PARTITIONTIME < TIMESTAMP('{end}')
        AND (
            REGEXP_CONTAINS(LOWER(Extras), r'(?i)\\bukraine\\b')
            OR REGEXP_CONTAINS(LOWER(V2Extras), r'(?i)\\bukraine\\b')
        )
    GROUP BY week, source_country
    ORDER BY week, source_country
    """


def collect_pair(client: bigquery.Client, pair: dict, dry_run: bool = False) -> pd.DataFrame | None:
    """Collect GDELT data for a single toponym pair."""
    pair_id = pair["id"]
    russian = pair["russian"]
    ukrainian = pair["ukrainian"]

    log.info(f"Collecting pair {pair_id}: '{russian}' vs '{ukrainian}'")

    if pair["is_control"] and russian == ukrainian:
        log.info(f"  Pair {pair_id}: control case (identical spellings), skipping")
        return None

    # Special handling for "the Ukraine" vs "Ukraine" (pair 27)
    if pair_id == 27:
        query = build_the_ukraine_query(START_DATE, END_DATE)
    # Use locations query for geographical pairs, extras query for others
    elif pair["category"] == "geographical":
        query = build_locations_query(russian, ukrainian, START_DATE, END_DATE)
    else:
        query = build_query(russian, ukrainian, START_DATE, END_DATE)

    df = run_query(client, query, pair_id, dry_run)

    if df is not None and not df.empty:
        df["pair_id"] = pair_id
        df["russian_term"] = russian
        df["ukrainian_term"] = ukrainian
        df["category"] = pair["category"]

        out_path = GDELT_RAW_DIR / f"pair_{pair_id:02d}_{russian.lower().replace(' ', '_')}.parquet"
        df.to_parquet(out_path, index=False)
        log.info(f"  Pair {pair_id}: saved to {out_path}")

    return df


def collect_all(pair_ids: list[int] | None = None, dry_run: bool = False) -> dict[int, pd.DataFrame]:
    """Collect GDELT data for all (or selected) toponym pairs."""
    ensure_dirs()
    client = bigquery.Client()
    pairs = get_all_pairs()

    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    results = {}

    for pair in pairs:
        df = collect_pair(client, pair, dry_run)
        if df is not None:
            results[pair["id"]] = df
        # Small delay between queries to be polite
        time.sleep(2)

    log.info(f"Collection complete: {len(results)} pairs collected")
    return results


def main():
    parser = argparse.ArgumentParser(description="Collect GDELT data for toponym pairs")
    parser.add_argument("--pair-ids", type=str, default=None,
                        help="Comma-separated pair IDs to collect (default: all)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Estimate costs without running queries")
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    collect_all(pair_ids=pair_ids, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
