"""Collect Wikipedia pageview data for toponym pairs.

Uses the Wikimedia REST API (free, no auth required) to get daily/monthly
pageview counts for Wikipedia articles corresponding to each spelling variant.

This provides a fourth independent data source measuring public interest
in each spelling form.

API docs: https://wikimedia.org/api/rest_v1/

Usage:
    python -m pipeline.ingestion.collect_wikipedia [--pair-ids 1,2,3]
"""

import argparse
import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import requests

from pipeline.config import (
    DATA_DIR,
    PROCESSED_DIR,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

WIKI_RAW_DIR = DATA_DIR / "raw" / "wikipedia"
WIKI_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
USER_AGENT = "KyivNotKiev-Research/1.0 (academic research; contact: kyivnotkiev@example.com)"
REQUEST_DELAY = 0.5  # seconds between requests (API is generous but be polite)

# Start/end for pageview data (available from July 2015)
START = "20150701"
END = "20260315"

# Wikipedia article mappings: russian_article, ukrainian_article
# These map toponym pairs to their actual Wikipedia article titles
# Wikipedia redirects handle most cases, but we track the canonical title
WIKI_ARTICLES = {
    # Geographical
    1: {"russian": "Kiev", "ukrainian": "Kyiv"},
    2: {"russian": "Kharkiv", "ukrainian": "Kharkiv",
        "note": "Article always at Kharkiv; track redirect from Kharkov"},
    3: {"russian": "Odessa", "ukrainian": "Odesa",
        "redirects": ["Odessa", "Odesa"]},
    4: {"russian": "Lvov", "ukrainian": "Lviv"},
    5: {"russian": "Zaporizhzhia", "ukrainian": "Zaporizhzhia"},
    7: {"russian": "Dnipro", "ukrainian": "Dnipro"},
    10: {"russian": "Chernobyl", "ukrainian": "Chornobyl",
         "note": "Article at 'Chernobyl disaster' is the main draw"},
    11: {"russian": "Luhansk", "ukrainian": "Luhansk"},
    # Food
    21: {"russian": "Chicken_Kiev", "ukrainian": "Chicken_Kyiv",
         "note": "Check both article titles"},
    23: {"russian": "Borscht", "ukrainian": "Borscht",
         "note": "Single article, no Ukrainian variant article"},
    # Landmarks
    24: {"russian": "Kyiv_Pechersk_Lavra", "ukrainian": "Kyiv_Pechersk_Lavra"},
    # Country
    27: {"russian": "Ukraine", "ukrainian": "Ukraine",
         "note": "Single article; track 'The_Ukraine' redirect pageviews"},
    # Institutional
    28: {"russian": "Taras_Shevchenko_National_University_of_Kyiv",
         "ukrainian": "Taras_Shevchenko_National_University_of_Kyiv"},
    # Sports
    32: {"russian": "FC_Dynamo_Kyiv", "ukrainian": "FC_Dynamo_Kyiv"},
    # Historical
    35: {"russian": "Kievan_Rus%27", "ukrainian": "Kievan_Rus%27",
         "note": "Article at Kievan Rus'; Kyivan Rus redirects"},
    36: {"russian": "Cossacks", "ukrainian": "Cossacks"},
}

# Additional redirect/search tracking: measure how people FIND the article
# By tracking redirect pageviews, we can see if people type "Kiev" or "Kyiv"
REDIRECT_PAIRS = {
    1: [
        ("Kiev", "Russian-derived search"),
        ("Kyiv", "Ukrainian-derived search"),
    ],
    3: [
        ("Odessa", "Russian-derived"),
        ("Odesa", "Ukrainian-derived"),
    ],
    10: [
        ("Chernobyl_disaster", "Chernobyl (disaster)"),
        ("Chornobyl_disaster", "Chornobyl (disaster)"),
        ("Chernobyl", "Chernobyl (city)"),
        ("Chornobyl", "Chornobyl (city)"),
    ],
    21: [
        ("Chicken_Kiev", "Chicken Kiev"),
        ("Chicken_Kyiv", "Chicken Kyiv"),
    ],
    27: [
        ("Ukraine", "Ukraine (no article)"),
        ("The_Ukraine", "The Ukraine (with article)"),
    ],
    32: [
        ("FC_Dynamo_Kyiv", "Dynamo Kyiv"),
        ("FC_Dynamo_Kiev", "Dynamo Kiev"),
    ],
    35: [
        ("Kievan_Rus%27", "Kievan Rus'"),
        ("Kyivan_Rus%27", "Kyivan Rus'"),
    ],
    # People — new pairs
    62: [
        ("Andriy_Shevchenko", "Andriy Shevchenko"),
        ("Andrei_Shevchenko", "Andrei Shevchenko"),
    ],
    80: [
        ("Nikolai_Gogol", "Nikolai Gogol"),
        ("Mykola_Hohol", "Mykola Hohol"),
    ],
    83: [
        ("Olga_of_Kiev", "Olga of Kiev"),
        ("Olha_of_Kyiv", "Olha of Kyiv"),
    ],
    84: [
        ("Igor_Sikorsky", "Igor Sikorsky"),
        ("Ihor_Sikorsky", "Ihor Sikorsky"),
    ],
    85: [
        ("Sergei_Korolev", "Sergei Korolev"),
        ("Serhii_Korolyov", "Serhii Korolyov"),
    ],
    86: [
        ("Bohdan_Khmelnytsky", "Bohdan Khmelnytsky"),
        ("Bogdan_Khmelnitsky", "Bogdan Khmelnitsky"),
    ],
    87: [
        ("Kazimir_Malevich", "Kazimir Malevich"),
        ("Kazymyr_Malevych", "Kazymyr Malevych"),
    ],
    # Geographical — new
    82: [
        ("Feodosia", "Feodosia"),
        ("Feodosiia", "Feodosiia"),
    ],
    # Landmarks — new
    89: [
        ("Sviatohirsk_Lavra", "Sviatohirsk Lavra"),
        ("Sviatogorsk_Lavra", "Sviatogorsk Lavra"),
    ],
}


@dataclass
class WikiPageviews:
    article: str
    granularity: str  # "daily" or "monthly"
    views: list[dict]  # [{timestamp, views}, ...]


def fetch_pageviews(
    article: str,
    project: str = "en.wikipedia",
    access: str = "all-access",
    agent: str = "all-agents",
    granularity: str = "monthly",
    start: str = START,
    end: str = END,
) -> pd.DataFrame | None:
    """Fetch pageview data for a single Wikipedia article."""
    url = f"{WIKI_API}/{project}/{access}/{agent}/{article}/{granularity}/{start}/{end}"
    headers = {"User-Agent": USER_AGENT}

    try:
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code == 404:
            log.warning(f"  Article not found: {article}")
            return None
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        log.error(f"  Request failed for {article}: {e}")
        return None

    items = data.get("items", [])
    if not items:
        return None

    rows = []
    for item in items:
        ts = item["timestamp"]
        # Parse timestamp: "2015070100" -> "2015-07-01"
        date = f"{ts[:4]}-{ts[4:6]}-{ts[6:8]}"
        rows.append({
            "date": date,
            "article": article,
            "views": item["views"],
        })

    return pd.DataFrame(rows)


def collect_pair_pageviews(pair_id: int) -> pd.DataFrame | None:
    """Collect pageviews for all article variants of a toponym pair."""
    if pair_id not in REDIRECT_PAIRS:
        log.info(f"  Pair {pair_id}: no Wikipedia articles mapped, skipping")
        return None

    frames = []
    for article, label in REDIRECT_PAIRS[pair_id]:
        log.info(f"  Fetching: {article} ({label})")
        df = fetch_pageviews(article, granularity="monthly")
        if df is not None:
            df["label"] = label
            df["variant"] = "russian" if REDIRECT_PAIRS[pair_id].index((article, label)) == 0 else "ukrainian"
            frames.append(df)
        time.sleep(REQUEST_DELAY)

        # Also try daily for the last 2 years (more granular)
        df_daily = fetch_pageviews(article, granularity="daily", start="20240101", end=END)
        if df_daily is not None:
            df_daily["label"] = label
            df_daily["variant"] = "russian" if REDIRECT_PAIRS[pair_id].index((article, label)) == 0 else "ukrainian"
            daily_path = WIKI_RAW_DIR / f"pair_{pair_id:02d}_{article.lower().replace('%27','').replace(' ','_')}_daily.csv"
            df_daily.to_csv(daily_path, index=False)
        time.sleep(REQUEST_DELAY)

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    combined["pair_id"] = pair_id
    return combined


def collect_all(pair_ids: list[int] | None = None):
    """Collect Wikipedia pageview data for all mapped pairs."""
    ensure_dirs()
    WIKI_RAW_DIR.mkdir(parents=True, exist_ok=True)

    target_pairs = pair_ids if pair_ids else list(REDIRECT_PAIRS.keys())

    results = {}
    for pair_id in target_pairs:
        log.info(f"Pair {pair_id}:")
        df = collect_pair_pageviews(pair_id)
        if df is not None:
            out_path = WIKI_RAW_DIR / f"pair_{pair_id:02d}_monthly.csv"
            df.to_csv(out_path, index=False)
            log.info(f"  Saved: {out_path} ({len(df)} rows)")
            results[pair_id] = df
        else:
            log.warning(f"  No data for pair {pair_id}")

    log.info(f"Collection complete: {len(results)} pairs collected")
    return results


def preprocess_wikipedia() -> pd.DataFrame | None:
    """Preprocess Wikipedia pageview data into adoption ratios."""
    csv_files = sorted(WIKI_RAW_DIR.glob("pair_*_monthly.csv"))
    if not csv_files:
        log.warning("No Wikipedia CSV files found")
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

        # Pivot: sum views by date and variant
        pivot = df.groupby(["date", "variant"])["views"].sum().reset_index()
        pivot_wide = pivot.pivot(index="date", columns="variant", values="views").fillna(0)

        if "russian" not in pivot_wide.columns:
            pivot_wide["russian"] = 0
        if "ukrainian" not in pivot_wide.columns:
            pivot_wide["ukrainian"] = 0

        result = pd.DataFrame({
            "date": pivot_wide.index,
            "russian_views": pivot_wide["russian"].values,
            "ukrainian_views": pivot_wide["ukrainian"].values,
        })

        total = result["russian_views"] + result["ukrainian_views"]
        result["adoption_ratio"] = result["ukrainian_views"] / total.replace(0, float("nan"))
        result["pair_id"] = pair_id
        result["category"] = pair["category"]
        result["russian_term"] = pair["russian"]
        result["ukrainian_term"] = pair["ukrainian"]
        result["source"] = "wikipedia"

        frames.append(result)

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    out_path = PROCESSED_DIR / "wikipedia_merged.parquet"
    combined.to_parquet(out_path, index=False)
    log.info(f"Wikipedia processed: {len(combined)} rows, {combined['pair_id'].nunique()} pairs -> {out_path}")
    return combined


def main():
    parser = argparse.ArgumentParser(description="Collect Wikipedia pageview data")
    parser.add_argument("--pair-ids", type=str, default=None,
                        help="Comma-separated pair IDs (default: all mapped)")
    parser.add_argument("--preprocess-only", action="store_true",
                        help="Only preprocess existing data, don't collect")
    args = parser.parse_args()

    if args.preprocess_only:
        preprocess_wikipedia()
        return

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    collect_all(pair_ids=pair_ids)
    preprocess_wikipedia()


if __name__ == "__main__":
    main()
