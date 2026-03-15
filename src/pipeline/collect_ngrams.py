"""Collect Google Books Ngram data for toponym pairs.

Downloads ngram frequency data for each spelling variant from the
Google Books Ngram Viewer corpus (English, 1900-2022).

Usage:
    python -m src.pipeline.collect_ngrams [--pair-ids 1,2,3]
"""

import argparse
import logging
import time
import urllib.parse

import pandas as pd
import requests

from src.config import (
    NGRAMS_RAW_DIR,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

NGRAM_URL = "https://books.google.com/ngrams/json"
NGRAM_START_YEAR = 1900
NGRAM_END_YEAR = 2022
NGRAM_CORPUS = 26  # English (2019 corpus)
NGRAM_SMOOTHING = 0


def fetch_ngram(term: str) -> pd.DataFrame | None:
    """Fetch ngram frequency data for a single term."""
    params = {
        "content": term,
        "year_start": NGRAM_START_YEAR,
        "year_end": NGRAM_END_YEAR,
        "corpus": NGRAM_CORPUS,
        "smoothing": NGRAM_SMOOTHING,
    }

    try:
        resp = requests.get(NGRAM_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"  Failed to fetch ngram for '{term}': {e}")
        return None

    if not data:
        log.warning(f"  No ngram data for '{term}'")
        return None

    entry = data[0]
    years = list(range(NGRAM_START_YEAR, NGRAM_END_YEAR + 1))
    timeseries = entry["timeseries"]

    if len(timeseries) != len(years):
        log.warning(f"  Ngram length mismatch for '{term}': {len(timeseries)} vs {len(years)}")
        years = years[:len(timeseries)]

    df = pd.DataFrame({"year": years, "frequency": timeseries})
    df["term"] = term
    return df


def collect_pair(pair: dict) -> pd.DataFrame | None:
    """Collect ngram data for both variants of a toponym pair."""
    pair_id = pair["id"]
    russian = pair["russian"]
    ukrainian = pair["ukrainian"]

    log.info(f"Pair {pair_id}: '{russian}' vs '{ukrainian}'")

    if pair["is_control"] and russian == ukrainian:
        log.info(f"  Control case, collecting single term")
        df_r = fetch_ngram(russian)
        if df_r is not None:
            df_r["variant"] = "both"
            df_r["pair_id"] = pair_id
            out_path = NGRAMS_RAW_DIR / f"pair_{pair_id:02d}.csv"
            df_r.to_csv(out_path, index=False)
            log.info(f"  Saved: {out_path}")
            return df_r
        return None

    df_r = fetch_ngram(russian)
    time.sleep(1)
    df_u = fetch_ngram(ukrainian)

    frames = []
    if df_r is not None:
        df_r["variant"] = "russian"
        frames.append(df_r)
    if df_u is not None:
        df_u["variant"] = "ukrainian"
        frames.append(df_u)

    if not frames:
        log.warning(f"  No data for pair {pair_id}")
        return None

    combined = pd.concat(frames, ignore_index=True)
    combined["pair_id"] = pair_id

    out_path = NGRAMS_RAW_DIR / f"pair_{pair_id:02d}.csv"
    combined.to_csv(out_path, index=False)
    log.info(f"  Saved: {out_path}")

    return combined


def collect_all(pair_ids: list[int] | None = None) -> None:
    """Collect ngram data for all (or selected) toponym pairs."""
    ensure_dirs()
    pairs = get_all_pairs()

    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    log.info(f"Collecting ngrams for {len(pairs)} pairs")

    for pair in pairs:
        collect_pair(pair)
        time.sleep(2)

    log.info("Ngram collection complete")


def main():
    parser = argparse.ArgumentParser(description="Collect Google Ngrams data for toponym pairs")
    parser.add_argument("--pair-ids", type=str, default=None,
                        help="Comma-separated pair IDs to collect (default: all)")
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    collect_all(pair_ids=pair_ids)


if __name__ == "__main__":
    main()
