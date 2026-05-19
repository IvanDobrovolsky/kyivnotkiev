"""YouTube adaptive fetch — drills down from month to week to day when results hit cap.

For each pair and variant:
1. Search per month (2015-2025)
2. If a month returns 500+ results → split into weeks
3. If a week returns 500+ results → split into days
4. Dedup by video_id across all windows

Usage:
    python -m pipeline.ingestion.youtube_adaptive_fetch --pair chornobyl --api-keys key1,key2,...
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
OUT_DIR = ROOT / "data" / "cl" / "raw" / "youtube_adaptive"

API_URL = "https://www.googleapis.com/youtube/v3"
PAGE_CAP = 500  # if we get this many, drill down

# Key rotation
_keys = []
_key_idx = 0
_units = 0


def next_key():
    global _key_idx
    key = _keys[_key_idx % len(_keys)]
    _key_idx += 1
    return key


def track(n):
    global _units
    _units += n


def search_window(query, after, before, max_pages=10):
    """Search one time window. Returns list of video dicts."""
    results = []
    page_token = None

    for page in range(max_pages):
        key = next_key()
        params = {
            "part": "snippet", "q": query, "type": "video",
            "maxResults": 50, "relevanceLanguage": "en",
            "publishedAfter": after, "publishedBefore": before,
            "key": key,
        }
        if page_token:
            params["pageToken"] = page_token

        resp = requests.get(f"{API_URL}/search", params=params, timeout=15)
        track(100)

        if resp.status_code == 403:
            log.warning(f"  Key exhausted, rotating...")
            continue
        if resp.status_code != 200:
            break

        data = resp.json()
        for item in data.get("items", []):
            if "videoId" in item.get("id", {}):
                results.append({
                    "video_id": item["id"]["videoId"],
                    "title": item["snippet"]["title"],
                    "channel": item["snippet"]["channelTitle"],
                    "published_at": item["snippet"]["publishedAt"],
                })

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.1)

    return results


def adaptive_search(query, year_start, year_end):
    """Search adaptively — month → week → day when cap is hit."""
    all_videos = {}

    current = datetime(year_start, 1, 1)
    end = datetime(year_end, 12, 31)

    while current < end:
        # Monthly window
        month_end = min(current + timedelta(days=31), end)
        month_end = month_end.replace(day=1) if month_end.month != current.month else month_end
        if month_end <= current:
            month_end = current + timedelta(days=28)

        after = current.strftime("%Y-%m-%dT00:00:00Z")
        before = min(month_end, end).strftime("%Y-%m-%dT23:59:59Z")

        results = search_window(query, after, before)
        new_count = sum(1 for r in results if r["video_id"] not in all_videos)

        if len(results) >= PAGE_CAP:
            # Drill down to weeks
            log.info(f"    {current.strftime('%Y-%m')}: {len(results)} results (cap hit) → drilling to weeks")
            week_start = current
            while week_start < month_end:
                week_end = min(week_start + timedelta(days=7), month_end)
                w_after = week_start.strftime("%Y-%m-%dT00:00:00Z")
                w_before = week_end.strftime("%Y-%m-%dT23:59:59Z")

                w_results = search_window(query, w_after, w_before)

                if len(w_results) >= PAGE_CAP:
                    # Drill down to days
                    log.info(f"      Week {week_start.strftime('%m-%d')}: {len(w_results)} (cap) → days")
                    day = week_start
                    while day < week_end:
                        d_after = day.strftime("%Y-%m-%dT00:00:00Z")
                        d_before = (day + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
                        d_results = search_window(query, d_after, d_before, max_pages=5)
                        for r in d_results:
                            all_videos[r["video_id"]] = r
                        day += timedelta(days=1)
                else:
                    for r in w_results:
                        all_videos[r["video_id"]] = r

                week_start = week_end
        else:
            for r in results:
                all_videos[r["video_id"]] = r
            log.info(f"    {current.strftime('%Y-%m')}: {new_count} new ({len(all_videos)} total, {_units} units)")

        current = month_end

    return all_videos


def get_video_details(video_ids):
    """Batch metadata. 1 unit per 50."""
    details = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        key = next_key()
        resp = requests.get(f"{API_URL}/videos", params={
            "part": "snippet,statistics", "id": ",".join(batch), "key": key,
        }, timeout=15)
        track(1)
        if resp.status_code == 200:
            for item in resp.json().get("items", []):
                snippet = item.get("snippet", {})
                stats = item.get("statistics", {})
                details[item["id"]] = {
                    "description": snippet.get("description", ""),
                    "view_count": int(stats.get("viewCount", 0)),
                    "channel_id": snippet.get("channelId", ""),
                }
        time.sleep(0.1)
    return details


def collect_pair(pair):
    slug = pair["slug"]
    ru_term = pair["russian"]
    ua_term = pair["ukrainian"]

    out_path = OUT_DIR / f"{slug}.parquet"
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        log.info(f"  Already exists: {len(existing)} videos — delete to re-fetch")
        return existing

    all_videos = {}

    for variant, term in [("russian", ru_term), ("ukrainian", ua_term)]:
        log.info(f"  [{slug}] Adaptive search: '{term}' ({variant})")
        videos = adaptive_search(term, 2015, 2025)
        for vid, info in videos.items():
            if vid not in all_videos:
                info["search_variant"] = variant
                info["matched_term"] = term
                all_videos[vid] = info
        log.info(f"  [{slug}] '{term}': {len(videos)} videos ({len(all_videos)} total unique, {_units} units)")

    if not all_videos:
        return pd.DataFrame()

    video_ids = list(all_videos.keys())
    log.info(f"  [{slug}] Fetching metadata for {len(video_ids)} videos...")
    details = get_video_details(video_ids)

    rows = []
    for vid, info in all_videos.items():
        detail = details.get(vid, {})
        title = info["title"]
        has_ru = bool(re.search(re.escape(ru_term), title, re.IGNORECASE))
        has_ua = bool(re.search(re.escape(ua_term), title, re.IGNORECASE))
        variant = "both" if (has_ru and has_ua) else ("ukrainian" if has_ua else "russian") if (has_ru or has_ua) else info["search_variant"]

        desc = detail.get("description", "")
        rows.append({
            "video_id": vid, "title": title, "channel": info["channel"],
            "channel_id": detail.get("channel_id", ""),
            "published_at": info["published_at"],
            "date": info["published_at"][:10],
            "view_count": detail.get("view_count", 0),
            "variant": variant, "matched_term": info["matched_term"],
            "pair_slug": slug, "has_transcript": False,
            "text": (title + "\n\n" + desc)[:2000],
            "text_len": min(len(title + "\n\n" + desc), 2000),
        })

    df = pd.DataFrame(rows)
    df.to_parquet(out_path, index=False)

    log.info(f"  [{slug}] SAVED: {len(df)} videos, {_units} units used")
    log.info(f"  Variants: {df['variant'].value_counts().to_dict()}")
    df['year'] = df['date'].str[:4]
    for yr, sub in df.groupby('year'):
        ua = (sub['variant'] == 'ukrainian').sum()
        ru = (sub['variant'] == 'russian').sum()
        log.info(f"    {yr}: {len(sub)} (UA={ua}, RU={ru})")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", type=str, required=True)
    parser.add_argument("--api-keys", type=str, required=True, help="Comma-separated API keys")
    args = parser.parse_args()

    global _keys
    _keys = args.api_keys.split(",")

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    pairs = [p for p in cfg["pairs"] if p["slug"] == args.pair and p.get("enabled", True)]
    if not pairs:
        log.error(f"Pair '{args.pair}' not found")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    collect_pair(pairs[0])
    log.info(f"\nTotal units: {_units}")


if __name__ == "__main__":
    main()
