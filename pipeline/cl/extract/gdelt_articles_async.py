"""Async GDELT article fetcher using aiohttp + trafilatura.

Replaces the sequential gdelt_articles.py with concurrent HTTP fetching.
Same BQ sampling, same output format (per-pair parquet + fetch_log + all_pairs).

Usage:
    python -m pipeline.cl.extract.gdelt_articles_async \
        [--pair-ids 1,3,10] [--max-per-pair 2000] [--concurrency 20]
"""

import argparse
import asyncio
import logging
from datetime import datetime, UTC

import aiohttp
import pandas as pd
import trafilatura
from trafilatura.settings import use_config
from google.cloud import bigquery

from pipeline.cl.config import (
    BQ_DATASET, BQ_PROJECT, CL_RAW_DIR,
    GDELT_FETCH_TIMEOUT,
    GDELT_MAX_ARTICLE_WORDS, GDELT_MAX_URLS_PER_PAIR,
    GDELT_MIN_ARTICLE_WORDS, ensure_cl_dirs,
)
from pipeline.config import get_enabled_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = CL_RAW_DIR / "gdelt_articles"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

DEFAULT_CONCURRENCY = 20

PAYWALL_SIGNALS = [
    "subscribe to continue", "subscription required",
    "sign in to read", "create a free account",
    "this content is for subscribers",
    "premium content", "paywall",
]


def _trafilatura_config():
    config = use_config()
    config.set("DEFAULT", "USER_AGENTS", USER_AGENT)
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
        FROM `{BQ_PROJECT}.{BQ_DATASET}.raw_gdelt`
        WHERE pair_id IN UNNEST(@pair_ids)
          AND source_url IS NOT NULL
          AND source_url != ''
    )
    SELECT pair_id, url, variant, matched_term, year, date
    FROM numbered
    WHERE rn <= @max_per_stratum
    ORDER BY pair_id, variant, year
    """

    # max_per_stratum: divide evenly across 4 year strata x 2 variants = 8 cells
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


def _classify_article(text):
    """Classify extracted text into a fetch status.

    Returns (possibly truncated text, status).
    """
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
    text_lower = text.lower()
    if any(sig in text_lower for sig in PAYWALL_SIGNALS) and word_count < 200:
        return text, "paywall"

    return text, "fetched"


async def fetch_one(session, semaphore, url, traf_config):
    """Fetch a single URL with aiohttp, extract text with trafilatura.

    Returns (text_or_none, status_string).
    """
    async with semaphore:
        try:
            async with session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=GDELT_FETCH_TIMEOUT),
                allow_redirects=True,
                ssl=False,
            ) as resp:
                if resp.status == 403:
                    return None, "blocked"
                if resp.status == 404:
                    return None, "dead_link"
                if resp.status >= 400:
                    return None, f"dead_link"

                html = await resp.text(errors="replace")

        except asyncio.TimeoutError:
            return None, "timeout"
        except aiohttp.ClientError:
            return None, "dead_link"
        except Exception as e:
            error_str = str(e).lower()
            if "403" in error_str or "forbidden" in error_str:
                return None, "blocked"
            return None, f"error:{str(e)[:80]}"

    # trafilatura extract is CPU-bound; run in thread to avoid blocking the loop
    loop = asyncio.get_running_loop()
    text = await loop.run_in_executor(
        None,
        lambda: trafilatura.extract(
            html,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            config=traf_config,
        ),
    )

    return _classify_article(text)


async def fetch_all_urls(urls_df, concurrency):
    """Fetch all URLs concurrently, return list of result dicts."""
    semaphore = asyncio.Semaphore(concurrency)
    traf_config = _trafilatura_config()

    headers = {"User-Agent": USER_AGENT}
    connector = aiohttp.TCPConnector(limit=concurrency, enable_cleanup_closed=True)

    results = []
    fetched_count = 0
    total = len(urls_df)

    async with aiohttp.ClientSession(headers=headers, connector=connector) as session:
        # Build tasks preserving row order
        tasks = []
        rows = list(urls_df.itertuples(index=False))
        for row in rows:
            tasks.append(fetch_one(session, semaphore, row.url, traf_config))

        # Gather results maintaining order
        outcomes = await asyncio.gather(*tasks, return_exceptions=True)

        for row, outcome in zip(rows, outcomes):
            if isinstance(outcome, Exception):
                text, status = None, f"error:{str(outcome)[:80]}"
            else:
                text, status = outcome

            word_count = len(text.split()) if text else 0

            results.append({
                "pair_id": row.pair_id,
                "url": row.url,
                "variant": row.variant,
                "matched_term": row.matched_term,
                "year": row.year,
                "date": row.date,
                "text": text if status == "fetched" else None,
                "word_count": word_count,
                "fetch_status": status,
                "fetched_at": datetime.now(UTC).isoformat(),
                "source": "gdelt",
            })

            if status == "fetched":
                fetched_count += 1

            if len(results) % 50 == 0:
                rate = fetched_count / len(results) * 100
                log.info(
                    f"  Progress: {len(results)}/{total} "
                    f"({rate:.0f}% yield, {concurrency} concurrent)"
                )

    log.info(
        f"  Completed: {fetched_count}/{total} fetched "
        f"({fetched_count / total * 100:.1f}% yield)"
        if total > 0
        else "  No URLs to fetch"
    )
    return results


def extract_gdelt_async(pair_ids=None, max_per_pair=GDELT_MAX_URLS_PER_PAIR,
                         concurrency=DEFAULT_CONCURRENCY):
    """Main entry point: sample from BQ, fetch async, save parquet."""
    ensure_cl_dirs()
    client = bigquery.Client(project=BQ_PROJECT)

    if pair_ids is None:
        pairs = get_enabled_pairs()
        pair_ids = [p["id"] for p in pairs if not p.get("is_control", False)]

    log.info(
        f"GDELT async article extraction for {len(pair_ids)} pairs "
        f"(max {max_per_pair}/pair, concurrency={concurrency})"
    )

    # Sample URLs from BigQuery
    urls_df = sample_urls_from_bq(client, pair_ids, max_per_pair)

    if urls_df.empty:
        log.warning("No GDELT URLs found")
        return pd.DataFrame()

    # Fetch all articles concurrently
    results = asyncio.run(fetch_all_urls(urls_df, concurrency))
    rdf = pd.DataFrame(results)

    # Save per-pair parquet files
    all_fetched = []
    for pair_id in sorted(rdf["pair_id"].unique()):
        pair_df = rdf[rdf["pair_id"] == pair_id]
        fetched_df = pair_df[pair_df["fetch_status"] == "fetched"].copy()

        pair_total = len(pair_df)
        pair_ok = len(fetched_df)
        log.info(f"  Pair {pair_id}: {pair_ok}/{pair_total} fetched")

        if not fetched_df.empty:
            out_path = OUT_DIR / f"pair_{pair_id}.parquet"
            fetched_df.to_parquet(out_path, index=False)
            all_fetched.append(fetched_df)

    # Save fetch log (all attempts, without article text)
    log_df = rdf[["pair_id", "url", "variant", "year",
                   "fetch_status", "word_count", "fetched_at"]].copy()
    log_path = OUT_DIR / "fetch_log.parquet"
    log_df.to_parquet(log_path, index=False)
    log.info(f"Fetch log: {log_path} ({len(log_df)} attempts)")

    # Status summary
    status_counts = log_df["fetch_status"].value_counts()
    log.info("Fetch status summary:")
    for status, count in status_counts.items():
        log.info(f"  {status}: {count}")

    # Save combined articles
    if all_fetched:
        combined = pd.concat(all_fetched, ignore_index=True)
        combined_path = OUT_DIR / "all_pairs.parquet"
        combined.to_parquet(combined_path, index=False)
        log.info(f"Total fetched articles: {len(combined)}")
        return combined

    return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(
        description="Async GDELT article fetcher (aiohttp + trafilatura)"
    )
    parser.add_argument("--pair-ids", type=str, default=None,
                        help="Comma-separated pair IDs (default: all)")
    parser.add_argument("--max-per-pair", type=int, default=GDELT_MAX_URLS_PER_PAIR,
                        help=f"Max URLs to sample per pair (default: {GDELT_MAX_URLS_PER_PAIR})")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                        help=f"Max concurrent HTTP connections (default: {DEFAULT_CONCURRENCY})")
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    extract_gdelt_async(
        pair_ids=pair_ids,
        max_per_pair=args.max_per_pair,
        concurrency=args.concurrency,
    )


if __name__ == "__main__":
    main()
