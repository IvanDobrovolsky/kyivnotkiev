"""Collect Reddit data for toponym adoption analysis.

Uses Reddit's public JSON API (no auth required) and Pushshift/Arctic Shift
archives for historical data. Measures how Reddit users spell Ukrainian
place names in titles, comments, and subreddit discussions.

This provides social media adoption data — a dimension missing from all
prior research on the #KyivNotKiev campaign.

Usage:
    python -m src.pipeline.collect_reddit [--pair-ids 1,2,3]
"""

import argparse
import json
import logging
import time
from pathlib import Path

import pandas as pd
import requests

from src.config import DATA_DIR, PROCESSED_DIR, ensure_dirs, get_all_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

REDDIT_RAW_DIR = DATA_DIR / "raw" / "reddit"
USER_AGENT = "KyivNotKiev-Research/1.0 (academic research)"
REQUEST_DELAY = 2  # Reddit rate limit: ~60 req/min for unauthenticated

# Arctic Shift API for historical Reddit data (free, research-friendly)
ARCTIC_SHIFT_API = "https://arctic-shift.photon-reddit.com/api"

# Target subreddits for analysis
SUBREDDITS = [
    "ukraine", "worldnews", "europe", "UkrainianConflict",
    "UkraineWarVideoReport", "geopolitics", "news",
    "food", "Cooking", "recipes",  # for food terms
    "soccer", "football",  # for sports terms
]

# Search terms per pair
SEARCH_TERMS = {
    1: {"russian": "Kiev", "ukrainian": "Kyiv",
        "exclude": ["Chicken Kiev", "chicken kiev", "Kiev cake", "kiev cake",
                     "Dynamo Kiev", "dynamo kiev", "Kievan Rus", "kievan rus"]},
    2: {"russian": "Kharkov", "ukrainian": "Kharkiv"},
    3: {"russian": "Odessa", "ukrainian": "Odesa",
        "exclude": ["Odessa, Texas", "Odessa TX", "Odessa, TX"]},
    4: {"russian": "Lvov", "ukrainian": "Lviv"},
    10: {"russian": "Chernobyl", "ukrainian": "Chornobyl"},
    21: {"russian": "Chicken Kiev", "ukrainian": "Chicken Kyiv"},
    27: {"russian": "the Ukraine", "ukrainian": "Ukraine"},
    32: {"russian": "Dynamo Kiev", "ukrainian": "Dynamo Kyiv"},
    35: {"russian": "Kievan Rus", "ukrainian": "Kyivan Rus"},
}


def search_arctic_shift(
    term: str,
    subreddit: str | None = None,
    after: str = "2015-01-01",
    before: str = "2026-03-15",
    search_type: str = "submissions",  # "submissions" or "comments"
    limit: int = 100,
) -> list[dict]:
    """Search Arctic Shift API for Reddit posts/comments containing a term."""
    params = {
        "q": term,
        "after": after,
        "before": before,
        "limit": limit,
        "sort": "created_utc",
        "order": "asc",
    }
    if subreddit:
        params["subreddit"] = subreddit

    url = f"{ARCTIC_SHIFT_API}/{search_type}/search"
    headers = {"User-Agent": USER_AGENT}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code == 404:
            return []
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])
    except requests.RequestException as e:
        log.warning(f"  Arctic Shift request failed: {e}")
        return []


def search_reddit_json(
    term: str,
    subreddit: str | None = None,
    sort: str = "relevance",
    time_filter: str = "all",
    limit: int = 100,
) -> list[dict]:
    """Search Reddit's public JSON API for posts containing a term."""
    if subreddit:
        url = f"https://www.reddit.com/r/{subreddit}/search.json"
    else:
        url = "https://www.reddit.com/search.json"

    params = {
        "q": f'"{term}"',  # exact phrase
        "sort": sort,
        "t": time_filter,
        "limit": min(limit, 100),
        "restrict_sr": "true" if subreddit else "false",
    }
    headers = {"User-Agent": USER_AGENT}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code == 429:
            log.warning("  Rate limited, waiting 60s...")
            time.sleep(60)
            resp = requests.get(url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        children = data.get("data", {}).get("children", [])
        return [c["data"] for c in children]
    except requests.RequestException as e:
        log.warning(f"  Reddit API request failed: {e}")
        return []


def count_term_in_subreddit(
    term: str,
    subreddit: str,
    time_periods: list[str] | None = None,
) -> dict:
    """Count approximate mentions of a term in a subreddit across time periods."""
    if time_periods is None:
        time_periods = ["year", "month"]

    results = {}
    for period in time_periods:
        posts = search_reddit_json(term, subreddit=subreddit, time_filter=period)
        results[period] = len(posts)
        time.sleep(REQUEST_DELAY)

    return results


def collect_pair_reddit(pair_id: int) -> pd.DataFrame | None:
    """Collect Reddit mention data for a toponym pair."""
    if pair_id not in SEARCH_TERMS:
        return None

    config = SEARCH_TERMS[pair_id]
    russian = config["russian"]
    ukrainian = config["ukrainian"]

    log.info(f"  Collecting Reddit data: '{russian}' vs '{ukrainian}'")

    rows = []

    # Method 1: Reddit search API (current data, last year/month/week)
    for time_filter in ["all", "year", "month"]:
        for variant, term in [("russian", russian), ("ukrainian", ukrainian)]:
            # Search across key subreddits
            for sub in ["ukraine", "worldnews", "europe"]:
                posts = search_reddit_json(term, subreddit=sub, time_filter=time_filter)
                count = len(posts)
                rows.append({
                    "pair_id": pair_id,
                    "variant": variant,
                    "term": term,
                    "subreddit": sub,
                    "time_filter": time_filter,
                    "count": count,
                    "source": "reddit_search",
                })
                log.info(f"    r/{sub} [{time_filter}] '{term}': {count} posts")
                time.sleep(REQUEST_DELAY)

    # Method 2: Arctic Shift for historical data (if available)
    for variant, term in [("russian", russian), ("ukrainian", ukrainian)]:
        # Try yearly buckets
        for year in range(2015, 2027):
            after = f"{year}-01-01"
            before = f"{year}-12-31" if year < 2026 else "2026-03-15"
            results = search_arctic_shift(
                term, after=after, before=before,
                search_type="submissions", limit=100,
            )
            rows.append({
                "pair_id": pair_id,
                "variant": variant,
                "term": term,
                "subreddit": "all",
                "time_filter": f"year_{year}",
                "count": len(results),
                "source": "arctic_shift",
            })
            if results:
                log.info(f"    Arctic Shift {year} '{term}': {len(results)} submissions")
            time.sleep(REQUEST_DELAY)

    if not rows:
        return None

    return pd.DataFrame(rows)


def collect_all(pair_ids: list[int] | None = None):
    """Collect Reddit data for all mapped pairs."""
    ensure_dirs()
    REDDIT_RAW_DIR.mkdir(parents=True, exist_ok=True)

    target_pairs = pair_ids if pair_ids else list(SEARCH_TERMS.keys())

    results = {}
    for pair_id in target_pairs:
        log.info(f"Pair {pair_id}:")
        df = collect_pair_reddit(pair_id)
        if df is not None:
            out_path = REDDIT_RAW_DIR / f"pair_{pair_id:02d}.csv"
            df.to_csv(out_path, index=False)
            log.info(f"  Saved: {out_path} ({len(df)} rows)")
            results[pair_id] = df

    log.info(f"Collection complete: {len(results)} pairs collected")
    return results


def preprocess_reddit() -> pd.DataFrame | None:
    """Preprocess Reddit data into adoption ratios."""
    csv_files = sorted(REDDIT_RAW_DIR.glob("pair_*.csv"))
    if not csv_files:
        log.warning("No Reddit CSV files found")
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

        # Aggregate by time period
        for time_filter in df["time_filter"].unique():
            sub = df[df["time_filter"] == time_filter]
            russian_count = sub[sub["variant"] == "russian"]["count"].sum()
            ukrainian_count = sub[sub["variant"] == "ukrainian"]["count"].sum()
            total = russian_count + ukrainian_count
            ratio = ukrainian_count / total if total > 0 else float("nan")

            frames.append({
                "pair_id": pair_id,
                "category": pair["category"],
                "russian_term": pair["russian"],
                "ukrainian_term": pair["ukrainian"],
                "time_filter": time_filter,
                "russian_count": russian_count,
                "ukrainian_count": ukrainian_count,
                "adoption_ratio": ratio,
                "source": "reddit",
            })

    if not frames:
        return None

    result = pd.DataFrame(frames)
    out_path = PROCESSED_DIR / "reddit_summary.csv"
    result.to_csv(out_path, index=False)
    log.info(f"Reddit processed: {len(result)} rows -> {out_path}")
    return result


def main():
    parser = argparse.ArgumentParser(description="Collect Reddit data for toponym pairs")
    parser.add_argument("--pair-ids", type=str, default=None)
    parser.add_argument("--preprocess-only", action="store_true")
    args = parser.parse_args()

    if args.preprocess_only:
        preprocess_reddit()
        return

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    collect_all(pair_ids=pair_ids)
    preprocess_reddit()


if __name__ == "__main__":
    main()
