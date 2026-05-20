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
_dead_keys = set()


def next_key():
    global _key_idx
    # Skip dead keys
    attempts = 0
    while attempts < len(_keys):
        key = _keys[_key_idx % len(_keys)]
        _key_idx += 1
        if key not in _dead_keys:
            return key
        attempts += 1
    return None  # all keys dead


def all_keys_dead():
    return len(_dead_keys) >= len(_keys)


def track(n):
    global _units
    _units += n


def search_window(query, after, before, max_pages=10):
    """Search one time window. Returns list of video dicts."""
    results = []
    page_token = None

    if all_keys_dead():
        return results

    for page in range(max_pages):
        key = next_key()
        if key is None:
            log.warning("  All keys exhausted — stopping")
            break

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
            _dead_keys.add(key)
            log.warning(f"  Key {key[:15]}... exhausted ({len(_dead_keys)}/{len(_keys)} dead)")
            if all_keys_dead():
                break
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


def adaptive_search(query, year_start, year_end, checkpoint_dir=None, slug=None):
    """Search adaptively — month → week → day when cap is hit. Checkpoints per month."""
    import json as _json

    all_videos = {}
    completed_months = set()

    # Load checkpoint if exists
    cp_path = None
    if checkpoint_dir and slug:
        cp_path = Path(checkpoint_dir) / f"{slug}_{query.replace(' ', '_')}_checkpoint.json"
        if cp_path.exists():
            with open(cp_path) as f:
                cp = _json.load(f)
            all_videos = {v['video_id']: v for v in cp.get('videos', [])}
            completed_months = set(cp.get('completed_months', []))
            log.info(f"    Resumed checkpoint: {len(all_videos):,} videos, {len(completed_months)} months done")

    current = datetime(year_start, 1, 1)
    end = datetime(year_end, 12, 31)

    while current < end:
        month_key = current.strftime("%Y-%m")

        # Skip completed months
        if month_key in completed_months:
            current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)
            continue

        # Monthly window
        month_end = min(current + timedelta(days=31), end)
        month_end = month_end.replace(day=1) if month_end.month != current.month else month_end
        if month_end <= current:
            month_end = current + timedelta(days=28)

        after = current.strftime("%Y-%m-%dT00:00:00Z")
        before = min(month_end, end).strftime("%Y-%m-%dT23:59:59Z")

        results = search_window(query, after, before)
        new_count = sum(1 for r in results if r["video_id"] not in all_videos)

        if all_keys_dead():
            log.warning(f"    All keys dead at {current.strftime('%Y-%m')} — saving progress")
            break

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

        # Mark month complete and checkpoint
        completed_months.add(month_key)
        if cp_path:
            with open(cp_path, 'w') as f:
                _json.dump({
                    'videos': list(all_videos.values()),
                    'completed_months': sorted(completed_months),
                }, f)

        current = (current.replace(day=1) + timedelta(days=32)).replace(day=1)

    # Clean up checkpoint on completion
    if cp_path and cp_path.exists() and not all_keys_dead():
        cp_path.unlink()
        log.info(f"    Checkpoint cleaned up — all months complete")

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

    # Load existing data to merge with (resume support)
    all_videos = {}
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        for _, r in existing.iterrows():
            all_videos[r['video_id']] = {
                'video_id': r['video_id'], 'title': r['title'],
                'channel': r['channel'], 'published_at': r['published_at'],
                'search_variant': r['variant'], 'matched_term': r['matched_term'],
            }
        log.info(f"  Loaded {len(all_videos):,} existing videos — will merge new")

    variant_pairs = [("russian", ru_term), ("ukrainian", ua_term)]
    if hasattr(collect_pair, '_only_variant') and collect_pair._only_variant:
        variant_pairs = [(v, t) for v, t in variant_pairs if v == collect_pair._only_variant]

    cp_dir = OUT_DIR / ".checkpoints"
    cp_dir.mkdir(parents=True, exist_ok=True)

    for variant, term in variant_pairs:
        log.info(f"  [{slug}] Adaptive search: '{term}' ({variant})")
        videos = adaptive_search(term, 2015, 2025, checkpoint_dir=cp_dir, slug=f"{slug}_{variant}")
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
    parser.add_argument("--variant", type=str, default=None, choices=["russian", "ukrainian"], help="Only fetch one variant")
    args = parser.parse_args()

    global _keys
    _keys = args.api_keys.split(",")

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    pairs = [p for p in cfg["pairs"] if p["slug"] == args.pair and p.get("enabled", True)]
    if not pairs:
        log.error(f"Pair '{args.pair}' not found")
        return

    collect_pair._only_variant = args.variant

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    collect_pair(pairs[0])
    log.info(f"\nTotal units: {_units}")


if __name__ == "__main__":
    main()
