"""Collect YouTube data for toponym adoption analysis.

Uses YouTube's public search (via yt-dlp or direct API) to measure how
video titles, descriptions, and channel names spell Ukrainian place names.

This provides social media video platform data — complementing Reddit
for the social media adoption analysis.

Usage:
    python -m pipeline.ingestion.collect_youtube [--pair-ids 1,2,3]
"""

import argparse
import json
import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.config import DATA_DIR, PROCESSED_DIR, ensure_dirs, get_all_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

YOUTUBE_RAW_DIR = DATA_DIR / "raw" / "youtube"
REQUEST_DELAY = 2

# YouTube Data API v3 (requires API key — set via env or config)
# If no API key, falls back to scraping YouTube search page
YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/search"

# Search terms per pair — same as Reddit
SEARCH_TERMS = {
    1: {"russian": "Kiev", "ukrainian": "Kyiv"},
    2: {"russian": "Kharkov", "ukrainian": "Kharkiv"},
    3: {"russian": "Odessa Ukraine", "ukrainian": "Odesa Ukraine"},
    10: {"russian": "Chernobyl", "ukrainian": "Chornobyl"},
    21: {"russian": "Chicken Kiev", "ukrainian": "Chicken Kyiv"},
    27: {"russian": '"the Ukraine"', "ukrainian": "Ukraine"},
    32: {"russian": "Dynamo Kiev", "ukrainian": "Dynamo Kyiv"},
    35: {"russian": "Kievan Rus", "ukrainian": "Kyivan Rus"},
}


def search_youtube_api(
    query: str,
    api_key: str,
    max_results: int = 50,
    published_after: str | None = None,
    published_before: str | None = None,
) -> list[dict]:
    """Search YouTube via Data API v3."""
    params = {
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": min(max_results, 50),
        "relevanceLanguage": "en",
        "key": api_key,
    }
    if published_after:
        params["publishedAfter"] = published_after
    if published_before:
        params["publishedBefore"] = published_before

    try:
        resp = requests.get(YOUTUBE_API_URL, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items", [])
    except requests.RequestException as e:
        log.warning(f"  YouTube API request failed: {e}")
        return []


def search_youtube_noapi(query: str, max_results: int = 20) -> list[dict]:
    """Search YouTube without API key using Invidious public instances."""
    # Use Invidious API (public YouTube frontend with API)
    instances = [
        "https://vid.puffyan.us",
        "https://invidious.fdn.fr",
        "https://inv.tux.pizza",
    ]

    for instance in instances:
        url = f"{instance}/api/v1/search"
        params = {
            "q": query,
            "type": "video",
            "sort_by": "relevance",
        }
        try:
            resp = requests.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                results = resp.json()
                return results[:max_results]
        except (requests.RequestException, json.JSONDecodeError):
            continue

    log.warning(f"  All Invidious instances failed for: {query}")
    return []


def count_term_in_titles(results: list[dict], term: str) -> int:
    """Count how many video titles contain the exact term."""
    count = 0
    term_lower = term.lower()
    for item in results:
        title = ""
        if "snippet" in item:  # YouTube API format
            title = item["snippet"].get("title", "")
        elif "title" in item:  # Invidious format
            title = item.get("title", "")
        if term_lower in title.lower():
            count += 1
    return count


def collect_pair_youtube(pair_id: int, api_key: str | None = None) -> pd.DataFrame | None:
    """Collect YouTube search data for a toponym pair."""
    if pair_id not in SEARCH_TERMS:
        return None

    config = SEARCH_TERMS[pair_id]
    russian = config["russian"]
    ukrainian = config["ukrainian"]

    log.info(f"  Collecting YouTube data: '{russian}' vs '{ukrainian}'")

    rows = []

    # Search for each variant by year
    for year in range(2018, 2027):
        published_after = f"{year}-01-01T00:00:00Z"
        published_before = f"{year}-12-31T23:59:59Z" if year < 2026 else "2026-03-15T23:59:59Z"

        for variant, term in [("russian", russian), ("ukrainian", ukrainian)]:
            if api_key:
                results = search_youtube_api(
                    term, api_key,
                    published_after=published_after,
                    published_before=published_before,
                )
            else:
                # Without API key, search without date filter (less precise)
                results = search_youtube_noapi(f"{term} {year}")

            title_matches = count_term_in_titles(results, term.strip('"'))

            rows.append({
                "pair_id": pair_id,
                "variant": variant,
                "term": term,
                "year": year,
                "search_results": len(results),
                "title_matches": title_matches,
                "source": "youtube_api" if api_key else "youtube_invidious",
            })
            log.info(f"    {year} '{term}': {len(results)} results, {title_matches} title matches")
            time.sleep(REQUEST_DELAY)

    if not rows:
        return None

    return pd.DataFrame(rows)


def collect_all(pair_ids: list[int] | None = None, api_key: str | None = None):
    """Collect YouTube data for all mapped pairs."""
    ensure_dirs()
    YOUTUBE_RAW_DIR.mkdir(parents=True, exist_ok=True)

    target_pairs = pair_ids if pair_ids else list(SEARCH_TERMS.keys())

    results = {}
    for pair_id in target_pairs:
        log.info(f"Pair {pair_id}:")
        df = collect_pair_youtube(pair_id, api_key=api_key)
        if df is not None:
            out_path = YOUTUBE_RAW_DIR / f"pair_{pair_id:02d}.csv"
            df.to_csv(out_path, index=False)
            log.info(f"  Saved: {out_path} ({len(df)} rows)")
            results[pair_id] = df

    log.info(f"Collection complete: {len(results)} pairs collected")
    return results


def preprocess_youtube() -> pd.DataFrame | None:
    """Preprocess YouTube data into adoption ratios."""
    csv_files = sorted(YOUTUBE_RAW_DIR.glob("pair_*.csv"))
    if not csv_files:
        log.warning("No YouTube CSV files found")
        return None

    pairs_lookup = {p["id"]: p for p in get_all_pairs()}
    frames = []

    for f in csv_files:
        df = pd.read_csv(f)
        if df.empty:
            continue

        pair_id = df["pair_id"].iloc[0]
        pair = pairs_lookup.get(pair_id)
        if pair is None:
            continue

        # Aggregate by year
        for year in df["year"].unique():
            sub = df[df["year"] == year]
            russian_count = sub[sub["variant"] == "russian"]["search_results"].sum()
            ukrainian_count = sub[sub["variant"] == "ukrainian"]["search_results"].sum()
            total = russian_count + ukrainian_count
            ratio = ukrainian_count / total if total > 0 else float("nan")

            frames.append({
                "pair_id": pair_id,
                "category": pair["category"],
                "russian_term": pair["russian"],
                "ukrainian_term": pair["ukrainian"],
                "year": int(year),
                "russian_count": russian_count,
                "ukrainian_count": ukrainian_count,
                "adoption_ratio": ratio,
                "source": "youtube",
            })

    if not frames:
        return None

    result = pd.DataFrame(frames)
    out_path = PROCESSED_DIR / "youtube_summary.csv"
    result.to_csv(out_path, index=False)
    log.info(f"YouTube processed: {len(result)} rows -> {out_path}")
    return result


def main():
    import os
    parser = argparse.ArgumentParser(description="Collect YouTube data for toponym pairs")
    parser.add_argument("--pair-ids", type=str, default=None)
    parser.add_argument("--api-key", type=str, default=os.environ.get("YOUTUBE_API_KEY"))
    parser.add_argument("--preprocess-only", action="store_true")
    args = parser.parse_args()

    if args.preprocess_only:
        preprocess_youtube()
        return

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    collect_all(pair_ids=pair_ids, api_key=args.api_key)
    preprocess_youtube()


if __name__ == "__main__":
    main()
