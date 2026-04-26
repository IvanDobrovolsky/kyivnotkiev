"""Collect Reddit data for toponym adoption analysis.

Uses Reddit's public JSON API (no auth required) and Pushshift/Arctic Shift
archives for historical data. Measures how Reddit users spell Ukrainian
place names in titles, comments, and subreddit discussions.

This provides social media adoption data — a dimension missing from all
prior research on the #KyivNotKiev campaign.

Usage:
    python -m pipeline.ingestion.collect_reddit [--pair-ids 1,2,3]
"""

import argparse
import json
import logging
import time
from pathlib import Path

import pandas as pd
import requests

from pipeline.config import DATA_DIR, PROCESSED_DIR, ensure_dirs, get_all_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

REDDIT_RAW_DIR = DATA_DIR / "raw" / "reddit"
USER_AGENT = "KyivNotKiev-Research/1.0 (academic research)"
REQUEST_DELAY = 2  # Reddit rate limit: ~60 req/min for unauthenticated

# Arctic Shift API for historical Reddit data (free, research-friendly)
# Correct endpoint: /api/posts/search (requires subreddit param)
ARCTIC_SHIFT_BASE = "https://arctic-shift.photon-reddit.com/api"

# Target subreddits for analysis
SUBREDDITS = [
    "ukraine", "worldnews", "europe", "UkrainianConflict",
    "UkraineWarVideoReport", "geopolitics", "news",
    "food", "Cooking", "recipes",  # for food terms
    "soccer", "football",  # for sports terms
]

# Search terms per pair — all enabled non-control pairs
SEARCH_TERMS = {
    1: {"russian": "Kiev", "ukrainian": "Kyiv"},
    2: {"russian": "Kharkov", "ukrainian": "Kharkiv"},
    3: {"russian": "Odessa", "ukrainian": "Odesa",
        "exclude": ["Odessa, Texas", "Odessa TX", "Odessa, TX"]},
    4: {"russian": "Lvov", "ukrainian": "Lviv"},
    5: {"russian": "Zaporozhye", "ukrainian": "Zaporizhzhia"},
    6: {"russian": "Nikolaev", "ukrainian": "Mykolaiv"},
    7: {"russian": "Dnepropetrovsk", "ukrainian": "Dnipro"},
    8: {"russian": "Vinnitsa", "ukrainian": "Vinnytsia"},
    9: {"russian": "Rovno", "ukrainian": "Rivne"},
    10: {"russian": "Chernobyl", "ukrainian": "Chornobyl"},
    11: {"russian": "Lugansk", "ukrainian": "Luhansk"},
    15: {"russian": "Dnieper River", "ukrainian": "Dnipro River"},
    16: {"russian": "Dniester", "ukrainian": "Dnister"},
    17: {"russian": "Donbass", "ukrainian": "Donbas"},
    19: {"russian": "Zakarpatye", "ukrainian": "Zakarpattia"},
    20: {"russian": "Podolye", "ukrainian": "Podillia"},
    21: {"russian": "Chicken Kiev", "ukrainian": "Chicken Kyiv"},
    22: {"russian": "Kiev cake", "ukrainian": "Kyiv cake"},
    23: {"russian": "Borsch", "ukrainian": "Borscht"},
    24: {"russian": "Kiev Pechersk Lavra", "ukrainian": "Kyiv Pechersk Lavra"},
    25: {"russian": "Saint Sophia Cathedral Kiev", "ukrainian": "Saint Sophia Cathedral Kyiv"},
    26: {"russian": "Chernobyl Exclusion Zone", "ukrainian": "Chornobyl Exclusion Zone"},
    27: {"russian": "the Ukraine", "ukrainian": "Ukraine"},
    28: {"russian": "Kiev National University", "ukrainian": "Kyiv National University"},
    29: {"russian": "Kharkov University", "ukrainian": "Kharkiv University"},
    30: {"russian": "Kiev Polytechnic", "ukrainian": "Kyiv Polytechnic"},
    31: {"russian": "Kiev Patriarchate", "ukrainian": "Kyiv Patriarchate"},
    32: {"russian": "Dynamo Kiev", "ukrainian": "Dynamo Kyiv"},
    34: {"russian": "Kiev ballet", "ukrainian": "Kyiv ballet"},
    35: {"russian": "Kievan Rus", "ukrainian": "Kyivan Rus"},
    36: {"russian": "Kazak", "ukrainian": "Cossack"},
    38: {"russian": "Chernigov", "ukrainian": "Chernihiv"},
    39: {"russian": "Chernovtsy", "ukrainian": "Chernivtsi"},
    40: {"russian": "Zhitomir", "ukrainian": "Zhytomyr"},
    41: {"russian": "Cherkassy", "ukrainian": "Cherkasy"},
    42: {"russian": "Uzhgorod", "ukrainian": "Uzhhorod"},
    43: {"russian": "Kremenchug", "ukrainian": "Kremenchuk"},
    44: {"russian": "Kirovograd", "ukrainian": "Kropyvnytskyi"},
    45: {"russian": "Ternopol", "ukrainian": "Ternopil"},
    46: {"russian": "Vareniki", "ukrainian": "Varenyky"},
    48: {"russian": "Gopak", "ukrainian": "Hopak"},
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
    62: {"russian": "Andrey Shevchenko", "ukrainian": "Andrii Shevchenko"},
    69: {"russian": "Sergei Rebrov", "ukrainian": "Serhiy Rebrov"},
    70: {"russian": "Vladimir the Great", "ukrainian": "Volodymyr the Great"},
    71: {"russian": "Prince of Kiev", "ukrainian": "Prince of Kyiv"},
    72: {"russian": "Artemovsk", "ukrainian": "Bakhmut"},
    80: {"russian": "Nikolai Gogol", "ukrainian": "Mykola Hohol"},
    82: {"russian": "Feodosiya", "ukrainian": "Feodosiia"},
    83: {"russian": "Olga of Kiev", "ukrainian": "Olha of Kyiv"},
    84: {"russian": "Igor Sikorsky", "ukrainian": "Ihor Sikorsky"},
    85: {"russian": "Sergei Korolev", "ukrainian": "Serhii Korolyov"},
    86: {"russian": "Bogdan Khmelnitsky", "ukrainian": "Bohdan Khmelnytskyi"},
    87: {"russian": "Kazimir Malevich", "ukrainian": "Kazymyr Malevych"},
    89: {"russian": "Sviatogorsk Lavra", "ukrainian": "Sviatohirsk Lavra"},
}


def search_arctic_shift(
    term: str,
    subreddit: str = "worldnews",
    after: str = "2010-01-01",
    before: str = "2026-04-01",
    search_type: str = "posts",  # "posts" or "comments"
    limit: int = 100,
) -> list[dict]:
    """Search Arctic Shift API for Reddit posts/comments containing a term.

    Note: Arctic Shift requires a subreddit parameter for text searches.
    """
    params = {
        "title": term,
        "subreddit": subreddit,
        "after": after,
        "before": before,
        "limit": min(limit, 100),
        "sort": "asc",
    }

    url = f"{ARCTIC_SHIFT_BASE}/{search_type}/search"
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

    # Method 2: Arctic Shift for historical yearly data (2010-2026)
    arctic_subreddits = ["worldnews", "ukraine", "europe", "news", "UkrainianConflict",
                         "geopolitics", "RussiaUkraineWar2022", "UkraineWarVideoReport",
                         "history", "AskHistorians", "soccer", "football"]
    for variant, term in [("russian", russian), ("ukrainian", ukrainian)]:
        for year in range(2010, 2027):
            after = f"{year}-01-01"
            before = f"{year}-12-31" if year < 2026 else "2026-04-01"
            total_count = 0
            for sub in arctic_subreddits:
                results = search_arctic_shift(
                    term, subreddit=sub, after=after, before=before,
                    search_type="posts", limit=500,
                )
                total_count += len(results)
                time.sleep(REQUEST_DELAY)
            rows.append({
                "pair_id": pair_id,
                "variant": variant,
                "term": term,
                "subreddit": "multi",
                "time_filter": f"year_{year}",
                "count": total_count,
                "source": "arctic_shift",
            })
            if total_count:
                log.info(f"    Arctic Shift {year} '{term}': {total_count} posts")

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


def run(pair_ids: list[int] | None = None):
    """Entry point for orchestrator."""
    target = [p for p in (pair_ids or list(SEARCH_TERMS.keys())) if p in SEARCH_TERMS]
    collect_all(pair_ids=target)
    preprocess_reddit()


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
