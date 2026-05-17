"""YouTube data collection via SerpApi.

SerpApi provides structured YouTube search results with pagination,
no quota limits beyond monthly credit allocation. Returns video metadata
including title, views, channel, and relative publish dates.

Relative dates ("3 years ago") are converted to approximate year-month.
For exact dates, follow up with YouTube API videos.list (1 unit per 50 IDs).

Usage:
    python -m pipeline.ingestion.youtube_serpapi [--pair chicken-kyiv] [--dry-run]
"""

import argparse
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
OUT_DIR = ROOT / "data" / "cl" / "raw" / "youtube_serpapi"
KEY_PATH = Path("/etc/secrets/serpapi")
REQUEST_DELAY = 1.5


def load_key():
    return KEY_PATH.read_text().strip()


def load_pairs():
    with open(CONFIG_PATH) as f:
        data = yaml.safe_load(f)
    return [p for p in data["pairs"] if p.get("enabled", True)]


def parse_relative_date(rel_str: str) -> str | None:
    """Convert '3 years ago' to YYYY-MM format."""
    if not rel_str:
        return None
    now = datetime(2026, 5, 15)
    clean = rel_str.replace("Streamed ", "").strip()
    m = re.match(r"(\d+)\s+(year|month|week|day|hour|minute)s?\s+ago", clean)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "year":
            dt = now - timedelta(days=n * 365)
        elif unit == "month":
            dt = now - timedelta(days=n * 30)
        elif unit == "week":
            dt = now - timedelta(weeks=n)
        elif unit == "day":
            dt = now - timedelta(days=n)
        else:
            dt = now
        return dt.strftime("%Y-%m")
    return None


def search_youtube(query: str, api_key: str, max_pages: int = 10) -> list[dict]:
    """Search YouTube via SerpApi, paginate through all results."""
    all_results = []
    params = {
        "engine": "youtube",
        "search_query": query,
        "api_key": api_key,
    }

    for page in range(max_pages):
        try:
            resp = requests.get("https://serpapi.com/search", params=params, timeout=20)
            if resp.status_code != 200:
                log.warning(f"  HTTP {resp.status_code}")
                break

            data = resp.json()
            results = data.get("video_results", [])
            if not results:
                break

            all_results.extend(results)

            # Check for next page
            next_token = data.get("serpapi_pagination", {}).get("next_page_token")
            if not next_token:
                break

            params["sp"] = next_token
            time.sleep(REQUEST_DELAY)

        except requests.RequestException as e:
            log.warning(f"  Request error: {e}")
            break

    return all_results


def process_results(results: list[dict], pair_slug: str, variant: str, term: str) -> list[dict]:
    """Convert SerpApi results to our standard format."""
    rows = []
    for r in results:
        video_id = r.get("video_id", "")
        if not video_id:
            continue

        title = r.get("title", "")
        views = r.get("views", 0)
        if isinstance(views, str):
            views = int(re.sub(r"[^\d]", "", views) or 0)

        channel = r.get("channel", {})
        pub_date = parse_relative_date(r.get("published_date", ""))

        rows.append({
            "video_id": video_id,
            "title": title,
            "channel": channel.get("name", ""),
            "channel_id": channel.get("link", "").split("/")[-1] if channel.get("link") else "",
            "views": views,
            "published_date_approx": pub_date or "",
            "published_date_raw": r.get("published_date", ""),
            "description": (r.get("description", "") or "")[:500],
            "pair_slug": pair_slug,
            "variant": variant,
            "matched_term": term,
        })

    return rows


def collect_pair(pair: dict, api_key: str, max_pages: int = 5) -> pd.DataFrame:
    """Collect YouTube data for a pair."""
    slug = pair["slug"]
    ru_term = pair["russian"]
    ua_term = pair["ukrainian"]

    all_rows = []
    seen_ids = set()

    for variant, term in [("russian", ru_term), ("ukrainian", ua_term)]:
        log.info(f"  [{slug}] '{term}' ({variant})")
        results = search_youtube(term, api_key, max_pages=max_pages)
        rows = process_results(results, slug, variant, term)

        # Dedup by video_id
        new = [r for r in rows if r["video_id"] not in seen_ids]
        seen_ids.update(r["video_id"] for r in new)
        all_rows.extend(new)

        log.info(f"    {len(results)} raw → {len(new)} new unique")
        time.sleep(REQUEST_DELAY)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)

    # Re-classify variant based on title content (more accurate than search query)
    def title_variant(row):
        title_lower = row["title"].lower()
        has_ru = ru_term.lower() in title_lower
        has_ua = ua_term.lower() in title_lower
        if has_ru and has_ua:
            return "both"
        if has_ua:
            return "ukrainian"
        if has_ru:
            return "russian"
        return row["variant"]

    df["variant"] = df.apply(title_variant, axis=1)
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", type=str, default=None)
    parser.add_argument("--max-pages", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    api_key = load_key()
    pairs = load_pairs()

    if args.pair:
        pairs = [p for p in pairs if p["slug"] == args.pair]

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        for p in pairs:
            log.info(f"Would fetch: {p['slug']}")
        return

    # Check remaining credits
    acct = requests.get("https://serpapi.com/account", params={"api_key": api_key}, timeout=10).json()
    remaining = acct.get("total_searches_left", 0)
    log.info(f"SerpApi credits remaining: {remaining}")

    total = 0
    for i, pair in enumerate(pairs):
        log.info(f"\n[{i+1}/{len(pairs)}] {pair['slug']}")

        out_path = OUT_DIR / f"{pair['slug']}.parquet"
        if out_path.exists():
            existing = pd.read_parquet(out_path)
            log.info(f"  Already exists: {len(existing)} videos — skipping")
            total += len(existing)
            continue

        df = collect_pair(pair, api_key, max_pages=args.max_pages)
        if df.empty:
            log.info(f"  No results")
            continue

        df.to_parquet(out_path, index=False)
        total += len(df)

        log.info(f"  Saved: {out_path} ({len(df)} videos)")

        # Per-variant summary
        for v, sub in df.groupby("variant"):
            dates = sub["published_date_approx"].value_counts().sort_index()
            log.info(f"    {v}: {len(sub)} videos, years: {dates.index.min() if len(dates) else '?'} to {dates.index.max() if len(dates) else '?'}")

    log.info(f"\nDONE: {total} videos across {len(pairs)} pairs")


if __name__ == "__main__":
    main()
