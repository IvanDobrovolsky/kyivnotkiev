"""Fetch article bodies from GDELT URLs using trafilatura.

Samples URLs from BigQuery raw_gdelt_url, fetches article text,
tracks status for every attempt (fetched/paywall/dead/timeout/too_short).

Usage:
    python -m pipeline.cl.extract.gdelt_articles [--pair-ids 1,3,10] [--max-per-pair 2000]
"""

import argparse
import logging
import time
from datetime import datetime

import pandas as pd
from google.cloud import bigquery

from pipeline.cl.config import (
    BQ_DATASET, BQ_PROJECT, CL_RAW_DIR,
    GDELT_FETCH_DELAY, GDELT_FETCH_TIMEOUT,
    GDELT_MAX_ARTICLE_WORDS, GDELT_MAX_URLS_PER_PAIR,
    GDELT_MIN_ARTICLE_WORDS, ensure_cl_dirs,
)
from pipeline.config import get_enabled_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = CL_RAW_DIR / "gdelt_articles"

# Lazy import — trafilatura is heavy
_trafilatura = None


def _get_trafilatura():
    global _trafilatura
    if _trafilatura is None:
        import trafilatura
        _trafilatura = trafilatura
    return _trafilatura


def _get_config():
    from trafilatura.settings import use_config
    config = use_config()
    config.set("DEFAULT", "USER_AGENTS",
               "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
               "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
    return config


def sample_urls_from_bq(client, pair_ids, max_per_pair):
    """Sample GDELT URLs from BigQuery, stratified by variant and year."""
    query = f"""
    WITH numbered AS (
        SELECT
            pair_id,
            source_url AS url,
            variant,
            matched_term,
            EXTRACT(YEAR FROM date) AS year,
            date,
            ROW_NUMBER() OVER (
                PARTITION BY pair_id, variant,
                CASE
                    WHEN EXTRACT(YEAR FROM date) BETWEEN 2010 AND 2013 THEN '2010-2013'
                    WHEN EXTRACT(YEAR FROM date) BETWEEN 2014 AND 2017 THEN '2014-2017'
                    WHEN EXTRACT(YEAR FROM date) BETWEEN 2018 AND 2021 THEN '2018-2021'
                    ELSE '2022-2026'
                END
                ORDER BY RAND()
            ) AS rn
        FROM `{BQ_PROJECT}.{BQ_DATASET}.raw_gdelt_url`
        WHERE pair_id IN UNNEST(@pair_ids)
          AND source_url IS NOT NULL
          AND source_url != ''
    )
    SELECT pair_id, url, variant, matched_term, year, date
    FROM numbered
    WHERE rn <= @max_per_stratum
    ORDER BY pair_id, variant, year
    """

    # max_per_stratum: divide evenly across 4 year strata × 2 variants = 8 cells
    max_per_stratum = max(max_per_pair // 8, 50)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("pair_ids", "INT64", pair_ids),
            bigquery.ScalarQueryParameter("max_per_stratum", "INT64", max_per_stratum),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()
    log.info(f"Sampled {len(df)} GDELT URLs across {len(pair_ids)} pairs")
    return df


def fetch_article(url, timeout=GDELT_FETCH_TIMEOUT):
    """Fetch and extract article text from a URL.

    Returns (text, status) where status is one of:
    fetched, paywall, dead_link, timeout, too_short, extraction_failed, blocked
    """
    traf = _get_trafilatura()
    config = _get_config()

    try:
        downloaded = traf.fetch_url(url, config=config)
        if downloaded is None:
            return None, "dead_link"

        text = traf.extract(
            downloaded,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            config=config,
        )

        if text is None:
            return None, "extraction_failed"

        word_count = len(text.split())

        if word_count < GDELT_MIN_ARTICLE_WORDS:
            return text, "too_short"

        # Truncate very long articles
        if word_count > GDELT_MAX_ARTICLE_WORDS:
            words = text.split()[:GDELT_MAX_ARTICLE_WORDS]
            text = " ".join(words)

        # Simple paywall detection
        paywall_signals = [
            "subscribe to continue", "subscription required",
            "sign in to read", "create a free account",
            "this content is for subscribers",
            "premium content", "paywall",
        ]
        text_lower = text.lower()
        if any(sig in text_lower for sig in paywall_signals) and word_count < 200:
            return text, "paywall"

        return text, "fetched"

    except TimeoutError:
        return None, "timeout"
    except Exception as e:
        error_str = str(e).lower()
        if "403" in error_str or "forbidden" in error_str:
            return None, "blocked"
        if "404" in error_str or "not found" in error_str:
            return None, "dead_link"
        return None, f"error:{str(e)[:80]}"


def fetch_pair_articles(urls_df, pair_id):
    """Fetch articles for a single pair, return results DataFrame."""
    results = []
    total = len(urls_df)
    fetched_count = 0

    for idx, row in urls_df.iterrows():
        text, status = fetch_article(row["url"])
        word_count = len(text.split()) if text else 0

        results.append({
            "pair_id": row["pair_id"],
            "url": row["url"],
            "variant": row["variant"],
            "matched_term": row["matched_term"],
            "year": row["year"],
            "date": row["date"],
            "text": text if status == "fetched" else None,
            "word_count": word_count,
            "fetch_status": status,
            "fetched_at": datetime.utcnow().isoformat(),
            "source": "gdelt",
        })

        if status == "fetched":
            fetched_count += 1

        if (len(results) % 50) == 0:
            rate = fetched_count / len(results) * 100
            log.info(f"    Progress: {len(results)}/{total} ({rate:.0f}% yield)")

        time.sleep(GDELT_FETCH_DELAY)

    rdf = pd.DataFrame(results)
    yield_rate = fetched_count / total * 100 if total > 0 else 0
    log.info(f"  Pair {pair_id}: {fetched_count}/{total} fetched ({yield_rate:.1f}% yield)")
    return rdf


def extract_gdelt(pair_ids=None, max_per_pair=GDELT_MAX_URLS_PER_PAIR):
    ensure_cl_dirs()
    client = bigquery.Client(project=BQ_PROJECT)

    if pair_ids is None:
        pairs = get_enabled_pairs()
        pair_ids = [p["id"] for p in pairs if not p.get("is_control", False)]

    log.info(f"GDELT article extraction for {len(pair_ids)} pairs (max {max_per_pair}/pair)")

    # Sample URLs
    urls_df = sample_urls_from_bq(client, pair_ids, max_per_pair)

    if urls_df.empty:
        log.warning("No GDELT URLs found")
        return pd.DataFrame()

    # Fetch articles per pair
    all_results = []
    fetch_log = []

    for pair_id in sorted(urls_df["pair_id"].unique()):
        pair_urls = urls_df[urls_df["pair_id"] == pair_id]
        log.info(f"  Pair {pair_id}: fetching {len(pair_urls)} URLs...")

        rdf = fetch_pair_articles(pair_urls, pair_id)

        # Save per-pair results
        out_path = OUT_DIR / f"pair_{pair_id}.parquet"
        fetched_df = rdf[rdf["fetch_status"] == "fetched"].copy()
        if not fetched_df.empty:
            fetched_df.to_parquet(out_path, index=False)

        # Log all attempts
        fetch_log.append(rdf[["pair_id", "url", "variant", "year",
                              "fetch_status", "word_count", "fetched_at"]])
        all_results.append(fetched_df)

    # Save combined fetch log
    if fetch_log:
        log_df = pd.concat(fetch_log, ignore_index=True)
        log_path = OUT_DIR / "fetch_log.parquet"
        log_df.to_parquet(log_path, index=False)
        log.info(f"Fetch log: {log_path} ({len(log_df)} attempts)")

        # Summary
        status_counts = log_df["fetch_status"].value_counts()
        log.info("Fetch status summary:")
        for status, count in status_counts.items():
            log.info(f"  {status}: {count}")

    # Save combined articles
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        combined_path = OUT_DIR / "all_pairs.parquet"
        combined.to_parquet(combined_path, index=False)
        log.info(f"Total fetched articles: {len(combined)}")
        return combined

    return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-ids", type=str, default=None,
                        help="Comma-separated pair IDs (default: all)")
    parser.add_argument("--max-per-pair", type=int, default=GDELT_MAX_URLS_PER_PAIR)
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    extract_gdelt(pair_ids=pair_ids, max_per_pair=args.max_per_pair)


if __name__ == "__main__":
    main()
