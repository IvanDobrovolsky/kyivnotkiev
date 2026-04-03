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
# All enabled non-control pairs
SEARCH_TERMS = {
    1: {"russian": "Kiev", "ukrainian": "Kyiv"},
    2: {"russian": "Kharkov", "ukrainian": "Kharkiv"},
    3: {"russian": "Odessa Ukraine", "ukrainian": "Odesa Ukraine"},
    4: {"russian": "Lvov", "ukrainian": "Lviv"},
    5: {"russian": "Zaporozhye", "ukrainian": "Zaporizhzhia"},
    6: {"russian": "Nikolaev", "ukrainian": "Mykolaiv"},
    7: {"russian": "Dnepropetrovsk", "ukrainian": "Dnipro"},
    8: {"russian": "Vinnitsa", "ukrainian": "Vinnytsia"},
    9: {"russian": "Rovno", "ukrainian": "Rivne"},
    10: {"russian": "Chernobyl", "ukrainian": "Chornobyl"},
    11: {"russian": "Lugansk", "ukrainian": "Luhansk"},
    15: {"russian": "Dnieper river", "ukrainian": "Dnipro river"},
    16: {"russian": "Dniester river", "ukrainian": "Dnister river"},
    17: {"russian": "Donbass", "ukrainian": "Donbas"},
    19: {"russian": "Zakarpattia", "ukrainian": "Transcarpathia Ukraine"},
    20: {"russian": "Podolye Ukraine", "ukrainian": "Podillia Ukraine"},
    21: {"russian": "Chicken Kiev", "ukrainian": "Chicken Kyiv"},
    22: {"russian": "Kiev cake", "ukrainian": "Kyiv cake"},
    23: {"russian": "Borscht", "ukrainian": "Borshch"},
    24: {"russian": "Pechersk Lavra Kiev", "ukrainian": "Pechersk Lavra Kyiv"},
    25: {"russian": "Saint Sophia Kiev", "ukrainian": "Saint Sophia Kyiv"},
    26: {"russian": "Chernobyl Exclusion Zone", "ukrainian": "Chornobyl Exclusion Zone"},
    27: {"russian": '"the Ukraine"', "ukrainian": "Ukraine"},
    28: {"russian": "Kiev National University", "ukrainian": "Kyiv National University"},
    29: {"russian": "Kharkov University", "ukrainian": "Kharkiv University"},
    30: {"russian": "Kiev Polytechnic", "ukrainian": "Kyiv Polytechnic"},
    31: {"russian": "Kiev Patriarchate", "ukrainian": "Kyiv Patriarchate"},
    32: {"russian": "Dynamo Kiev", "ukrainian": "Dynamo Kyiv"},
    34: {"russian": "Kiev ballet", "ukrainian": "Kyiv ballet"},
    35: {"russian": "Kievan Rus", "ukrainian": "Kyivan Rus"},
    36: {"russian": "Kazak Ukraine", "ukrainian": "Kozak Ukraine"},
    38: {"russian": "Chernigov", "ukrainian": "Chernihiv"},
    39: {"russian": "Chernovtsy", "ukrainian": "Chernivtsi"},
    40: {"russian": "Zhitomir", "ukrainian": "Zhytomyr"},
    41: {"russian": "Cherkassy", "ukrainian": "Cherkasy"},
    42: {"russian": "Uzhgorod", "ukrainian": "Uzhhorod"},
    43: {"russian": "Kremenchug", "ukrainian": "Kremenchuk"},
    44: {"russian": "Kirovograd", "ukrainian": "Kropyvnytskyi"},
    45: {"russian": "Ternopol", "ukrainian": "Ternopil"},
    46: {"russian": "Vareniki", "ukrainian": "Varenyky"},
    48: {"russian": "Gopak dance", "ukrainian": "Hopak dance"},
    51: {"russian": "Zorya Lugansk", "ukrainian": "Zorya Luhansk"},
    52: {"russian": "Metalist Kharkov", "ukrainian": "Metalist Kharkiv"},
    53: {"russian": "Karpaty Lvov", "ukrainian": "Karpaty Lviv"},
    54: {"russian": "Babi Yar", "ukrainian": "Babyn Yar"},
    55: {"russian": "Potemkin Stairs Odessa", "ukrainian": "Potemkin Stairs Odesa"},
    56: {"russian": "Motherland Monument Kiev", "ukrainian": "Motherland Monument Kyiv"},
    57: {"russian": "Lvov Polytechnic", "ukrainian": "Lviv Polytechnic"},
    58: {"russian": "Odessa National University", "ukrainian": "Odesa National University"},
    60: {"russian": "Alexander Usyk", "ukrainian": "Oleksandr Usyk"},
    61: {"russian": "Vladimir Zelensky", "ukrainian": "Volodymyr Zelenskyy"},
    62: {"russian": "Andrei Shevchenko", "ukrainian": "Andriy Shevchenko"},
    64: {"russian": "Borsch", "ukrainian": "Borshch"},
    69: {"russian": "Sergei Rebrov", "ukrainian": "Serhiy Rebrov"},
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
    """Search YouTube without API key using yt-dlp or Invidious."""
    # Method 1: yt-dlp (most reliable, no API key needed)
    results = _search_ytdlp(query, max_results)
    if results:
        return results

    # Method 2: Invidious public instances (updated list)
    instances = [
        "https://invidious.privacyredirect.com",
        "https://inv.nadeko.net",
        "https://invidious.nerdvpn.de",
        "https://invidious.jing.rocks",
        "https://invidious.protokolla.fi",
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
                if results:
                    return results[:max_results]
        except (requests.RequestException, json.JSONDecodeError):
            continue

    log.warning(f"  All search methods failed for: {query}")
    return []


def _search_ytdlp(query: str, max_results: int = 20) -> list[dict]:
    """Search YouTube via yt-dlp (no API key needed)."""
    import shutil
    import subprocess

    if not shutil.which("yt-dlp"):
        return []

    try:
        cmd = [
            "yt-dlp",
            f"ytsearch{max_results}:{query}",
            "--dump-json",
            "--flat-playlist",
            "--no-download",
            "--quiet",
            "--no-warnings",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return []

        items = []
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            try:
                data = json.loads(line)
                items.append({"title": data.get("title", ""), "id": data.get("id", "")})
            except json.JSONDecodeError:
                continue
        return items
    except (subprocess.TimeoutExpired, OSError) as e:
        log.warning(f"  yt-dlp search failed: {e}")
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

    # Search for each variant by year (2010-2026 for full coverage)
    for year in range(2010, 2027):
        published_after = f"{year}-01-01T00:00:00Z"
        published_before = f"{year}-12-31T23:59:59Z" if year < 2026 else "2026-04-01T23:59:59Z"

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


def run(pair_ids: list[int] | None = None):
    """Entry point for orchestrator."""
    import os
    api_key = os.environ.get("YOUTUBE_API_KEY")
    target = [p for p in (pair_ids or list(SEARCH_TERMS.keys())) if p in SEARCH_TERMS]
    collect_all(pair_ids=target, api_key=api_key)
    preprocess_youtube()


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
