"""Collect Google Trends data for toponym pairs using pytrends.

Handles rate limiting with exponential backoff, collects both worldwide
and per-country data for geographic diffusion analysis.

Usage:
    python -m src.pipeline.collect_trends [--pair-ids 1,2,3] [--countries-only] [--worldwide-only]
"""

import argparse
import logging
import time

import pandas as pd
from pytrends.request import TrendReq

from src.config import (
    TARGET_COUNTRIES,
    TRENDS_BACKOFF_FACTOR,
    TRENDS_MAX_RETRIES,
    TRENDS_RAW_DIR,
    TRENDS_REQUEST_DELAY,
    TRENDS_TIMEFRAME,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def create_pytrends_client() -> TrendReq:
    """Create a pytrends client with retry-friendly settings."""
    return TrendReq(
        hl="en-US",
        tz=0,  # UTC
        retries=3,
        backoff_factor=1.0,
    )


def fetch_with_retry(
    pytrends: TrendReq,
    keywords: list[str],
    timeframe: str,
    geo: str = "",
    max_retries: int = TRENDS_MAX_RETRIES,
) -> pd.DataFrame | None:
    """Fetch interest over time with exponential backoff on rate limits."""
    for attempt in range(max_retries):
        try:
            pytrends.build_payload(keywords, timeframe=timeframe, geo=geo)
            df = pytrends.interest_over_time()
            if df.empty:
                log.warning(f"  Empty result for {keywords} (geo={geo or 'worldwide'})")
                return None
            # Drop the isPartial column
            if "isPartial" in df.columns:
                df = df.drop(columns=["isPartial"])
            return df
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "Too Many Requests" in error_msg:
                wait = TRENDS_REQUEST_DELAY * (TRENDS_BACKOFF_FACTOR ** attempt)
                log.warning(f"  Rate limited (attempt {attempt + 1}/{max_retries}), waiting {wait:.0f}s")
                time.sleep(wait)
            else:
                log.error(f"  Error fetching {keywords}: {e}")
                return None

    log.error(f"  Max retries exceeded for {keywords} (geo={geo or 'worldwide'})")
    return None


def collect_pair_worldwide(pytrends: TrendReq, pair: dict) -> pd.DataFrame | None:
    """Collect worldwide interest over time for a toponym pair."""
    pair_id = pair["id"]
    russian = pair["russian"]
    ukrainian = pair["ukrainian"]

    log.info(f"  Worldwide: '{russian}' vs '{ukrainian}'")

    if pair["is_control"] and russian == ukrainian:
        log.info(f"  Pair {pair_id}: control case, collecting single term")
        df = fetch_with_retry(pytrends, [russian], TRENDS_TIMEFRAME)
    else:
        df = fetch_with_retry(pytrends, [russian, ukrainian], TRENDS_TIMEFRAME)

    if df is not None:
        df["pair_id"] = pair_id
        df["geo"] = "worldwide"
        out_path = TRENDS_RAW_DIR / f"pair_{pair_id:02d}_worldwide.csv"
        df.to_csv(out_path)
        log.info(f"  Saved: {out_path}")

    return df


def collect_pair_by_country(pytrends: TrendReq, pair: dict) -> dict[str, pd.DataFrame]:
    """Collect per-country interest for a toponym pair."""
    pair_id = pair["id"]
    russian = pair["russian"]
    ukrainian = pair["ukrainian"]
    results = {}

    if pair["is_control"] and russian == ukrainian:
        keywords = [russian]
    else:
        keywords = [russian, ukrainian]

    for country_code in TARGET_COUNTRIES:
        log.info(f"  Country {country_code}: '{russian}' vs '{ukrainian}'")
        df = fetch_with_retry(pytrends, keywords, TRENDS_TIMEFRAME, geo=country_code)

        if df is not None:
            df["pair_id"] = pair_id
            df["geo"] = country_code
            results[country_code] = df

        time.sleep(TRENDS_REQUEST_DELAY)

    if results:
        combined = pd.concat(results.values())
        out_path = TRENDS_RAW_DIR / f"pair_{pair_id:02d}_countries.csv"
        combined.to_csv(out_path)
        log.info(f"  Saved {len(results)} countries: {out_path}")

    return results


def collect_all(
    pair_ids: list[int] | None = None,
    worldwide: bool = True,
    countries: bool = True,
) -> None:
    """Collect Google Trends data for all (or selected) toponym pairs."""
    ensure_dirs()
    pytrends = create_pytrends_client()
    pairs = get_all_pairs()

    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    log.info(f"Collecting {len(pairs)} pairs (worldwide={worldwide}, countries={countries})")

    for pair in pairs:
        pair_id = pair["id"]
        log.info(f"Pair {pair_id}: '{pair['russian']}' vs '{pair['ukrainian']}'")

        if worldwide:
            collect_pair_worldwide(pytrends, pair)
            time.sleep(TRENDS_REQUEST_DELAY)

        if countries:
            collect_pair_by_country(pytrends, pair)

        log.info(f"Pair {pair_id}: done")


def main():
    parser = argparse.ArgumentParser(description="Collect Google Trends data for toponym pairs")
    parser.add_argument("--pair-ids", type=str, default=None,
                        help="Comma-separated pair IDs to collect (default: all)")
    parser.add_argument("--worldwide-only", action="store_true",
                        help="Only collect worldwide data (skip per-country)")
    parser.add_argument("--countries-only", action="store_true",
                        help="Only collect per-country data (skip worldwide)")
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    worldwide = not args.countries_only
    countries = not args.worldwide_only

    collect_all(pair_ids=pair_ids, worldwide=worldwide, countries=countries)


if __name__ == "__main__":
    main()
