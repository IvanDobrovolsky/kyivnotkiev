"""Resume donbas: finish Ukrainian variant (2019-05+), fetch metadata for valid videos.

Usage:
    python -m pipeline.ingestion.youtube_donbas_resume --api-keys key1,key2,...
"""

import argparse
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
OUT_PATH = ROOT / "data" / "cl" / "raw" / "youtube_adaptive" / "donbas.parquet"
API_URL = "https://www.googleapis.com/youtube/v3"

_keys = []
_dead_keys = set()
_key_idx = 0
_units = 0


def next_key():
    global _key_idx
    attempts = 0
    while attempts < len(_keys):
        k = _keys[_key_idx % len(_keys)]
        _key_idx += 1
        if k not in _dead_keys:
            return k
        attempts += 1
    return None


def track(n):
    global _units
    _units += n


def all_keys_dead():
    return len(_dead_keys) >= len(_keys)


def search_window(query, after, before, max_pages=10):
    results = []
    page_token = None
    q = f'"{query}"' if ' ' in query else query
    for _ in range(max_pages):
        key = next_key()
        if key is None:
            break
        params = {
            "part": "snippet", "q": q, "type": "video",
            "maxResults": 50, "relevanceLanguage": "en",
            "publishedAfter": after, "publishedBefore": before, "key": key,
        }
        if page_token:
            params["pageToken"] = page_token
        try:
            resp = requests.get(f"{API_URL}/search", params=params, timeout=30)
        except Exception as e:
            log.warning(f"  Request error: {e}")
            break
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


def get_video_details(video_ids):
    details = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        key = next_key()
        if key is None:
            log.warning(f"  Keys exhausted — got {len(details)}/{len(video_ids)}")
            break
        resp = requests.get(f"{API_URL}/videos", params={
            "part": "snippet", "id": ",".join(batch), "key": key,
        }, timeout=15)
        track(1)
        if resp.status_code == 403:
            _dead_keys.add(key)
            if all_keys_dead():
                break
            continue
        if resp.status_code == 200:
            for item in resp.json().get("items", []):
                snippet = item.get("snippet", {})
                details[item["id"]] = {
                    "description": snippet.get("description", ""),
                    "channel_id": snippet.get("channelId", ""),
                }
        time.sleep(0.1)
        if (i // 50) % 20 == 0 and i > 0:
            log.info(f"  Metadata: {len(details)}/{len(video_ids)} ({_units} units)")
    return details


def classify_variant(title):
    has_donbass = bool(re.search(r'\bDonbass\b', title, re.IGNORECASE))
    has_donbas = bool(re.search(r'\bDonbas\b', title, re.IGNORECASE)) and not has_donbass
    if has_donbass and has_donbas:
        return "both"
    elif has_donbass:
        return "russian"
    elif has_donbas:
        return "ukrainian"
    return "unknown"


def is_exact_match(title):
    return bool(re.search(r'\bDonbass?\b', title, re.IGNORECASE))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-keys", type=str, required=True)
    args = parser.parse_args()

    global _keys
    _keys = args.api_keys.split(",")

    existing = pd.read_parquet(OUT_PATH)
    existing_ids = set(existing['video_id'])
    log.info(f"Existing: {len(existing):,} filtered videos")

    # Step 1: Finish Ukrainian "Donbas" from 2019-05 onward
    log.info("=== STEP 1: Finish Ukrainian variant (2019-05 -> 2025-12) ===")
    new_videos = {}
    current = datetime(2019, 5, 1)
    end = datetime(2026, 1, 1)

    while current < end and not all_keys_dead():
        month_end = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        after = current.strftime("%Y-%m-%dT00:00:00Z")
        before = min(month_end, end).strftime("%Y-%m-%dT00:00:00Z")

        results = search_window("Donbas", after, before)
        new_count = 0
        for r in results:
            vid = r["video_id"]
            if vid not in existing_ids and vid not in new_videos:
                if is_exact_match(r["title"]):
                    r["search_variant"] = "ukrainian"
                    r["matched_term"] = "Donbas"
                    new_videos[vid] = r
                    new_count += 1

        log.info(f"  {current.strftime('%Y-%m')}: {new_count} new exact matches ({len(new_videos)} total, {_units} units)")
        current = month_end

    log.info(f"\nNew videos (exact match): {len(new_videos):,}")

    # Step 2: Merge
    if new_videos:
        new_rows = []
        for vid, info in new_videos.items():
            title = info["title"]
            new_rows.append({
                "video_id": vid, "title": title, "channel": info["channel"],
                "channel_id": "", "published_at": info["published_at"],
                "date": info["published_at"][:10],
                "variant": classify_variant(title),
                "matched_term": info["matched_term"],
                "pair_slug": "donbas", "has_transcript": False,
                "text": title, "text_len": len(title),
            })
        merged = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
        merged = merged.drop_duplicates(subset="video_id", keep="first").reset_index(drop=True)
    else:
        merged = existing

    log.info(f"Merged: {len(merged):,} videos")

    # Step 3: Metadata for all valid videos
    log.info(f"\n=== STEP 2: Fetch metadata for {len(merged):,} videos ===")
    details = get_video_details(merged['video_id'].tolist())
    log.info(f"Metadata: {len(details):,}/{len(merged):,}")

    enriched = 0
    for idx, row in merged.iterrows():
        d = details.get(row['video_id'])
        if d and d['description']:
            full_text = f"{row['title']}\n\n{d['description']}"
            merged.at[idx, 'text'] = full_text[:2000]
            merged.at[idx, 'text_len'] = min(len(full_text), 2000)
            if d['channel_id']:
                merged.at[idx, 'channel_id'] = d['channel_id']
            enriched += 1

    log.info(f"Enriched: {enriched:,}/{len(merged):,}")

    # Re-validate
    valid = merged[merged['title'].str.contains(r'\bDonbass?\b', case=False, na=False)]
    valid.to_parquet(OUT_PATH, index=False)
    log.info(f"\nSaved: {len(valid):,} videos")
    log.info(f"Variants: {valid['variant'].value_counts().to_dict()}")
    log.info(f"Units: {_units}")

    valid['year'] = valid['date'].str[:4]
    for yr, sub in valid.groupby('year'):
        ua = (sub['variant'] == 'ukrainian').sum()
        ru = (sub['variant'] == 'russian').sum()
        print(f"  {yr}: {len(sub)} (UA={ua}, RU={ru})")


if __name__ == "__main__":
    main()
