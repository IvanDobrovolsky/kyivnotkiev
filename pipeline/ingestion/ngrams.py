"""Collect Google Books Ngram data for toponym pairs.

Downloads ngram frequency data for each spelling variant from the
Google Books Ngram Viewer.

History: this script originally used corpus 26 (English 2019) which is
hard-capped at 2019. In April 2026 we discovered Google had quietly
shipped corpus 37 (English 2022) which extends through 2022 — the
Ngrams Viewer URL accepts it and returns real per-year data. We
verified this directly: Kiev/Kyiv crosses over in 2022 in this corpus.

Corpus 37 = English All 2022. Corpus 38 = English Fiction 2022.
We use 37 for the broadest coverage.

Not part of any rebuild. Live values no longer reproduce the stored ones (Kyiv
2018-2022 live/stored 1.023, 0.983, 1.008, 1.027, 1.028 on 2026-10-15), so a
refetch would move history, and collect_pair still keys its CSVs by the numeric
pair id that config/pairs.yaml dropped in 51f9cfef: it refuses below rather
than write pair files nobody can join.

Usage:
    python -m pipeline.ingestion.ngrams [--pair-ids 1,2,3]
"""

import argparse
import logging
import time

import pandas as pd
import requests

from pipeline.config import RAW_DIR, load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# pipeline.config lost NGRAMS_RAW_DIR, ensure_dirs and get_all_pairs in 51f9cfef,
# which left this module failing at import.
NGRAMS_RAW_DIR = RAW_DIR / "ngrams"

NGRAM_URL = "https://books.google.com/ngrams/json"
NGRAM_START_YEAR = 1900
# Stays 2022 when the study window moves: corpus 37 (English 2022) is the newest
# Google Books corpus and has no later year. Asked for year_end=2026, the viewer
# returned exactly 5 values, 2018-2022 (probe 2026-10-15). SOURCE_SERIES_END
# ["ngrams"] in pipeline.config holds the same limit for the site.
NGRAM_END_YEAR = 2022
NGRAM_CORPUS = 37          # English All 2022 — replaces previous corpus 26 (English 2019)
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
        log.info("  Control case, collecting single term")
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
    pairs = load_pairs()["pairs"]
    if any("id" not in p for p in pairs):
        raise SystemExit("config/pairs.yaml pairs are slug-keyed; this collector still writes "
                         "pair_{id}.csv by numeric id. Re-key it before any refetch.")
    NGRAMS_RAW_DIR.mkdir(parents=True, exist_ok=True)

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
