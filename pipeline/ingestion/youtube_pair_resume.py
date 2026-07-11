"""General resume script: finish a variant from a given month, filter exact matches, fetch metadata.

Usage:
    python -m pipeline.ingestion.youtube_pair_resume \
        --pair bakhmut --term Bakhmut --resume-from 2018-03 \
        --match-pattern '\bBakhmut\b|\bArtemovsk\b|\bArtyomovsk\b' \
        --api-keys key1,key2,...
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
        try:
            resp = requests.get(f"{API_URL}/videos", params={
                "part": "snippet", "id": ",".join(batch), "key": key,
            }, timeout=30)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            log.warning(f"  Timeout on batch {i // 50}, retrying...")
            time.sleep(5)
            try:
                resp = requests.get(f"{API_URL}/videos", params={
                    "part": "snippet", "id": ",".join(batch), "key": key,
                }, timeout=30)
            except Exception:
                log.warning(f"  Retry failed, skipping batch")
                continue
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", required=True)
    parser.add_argument("--term", required=True, help="Search term to resume")
    parser.add_argument("--resume-from", required=True, help="YYYY-MM to resume from")
    parser.add_argument("--match-pattern", required=True, help="Regex for exact match filter")
    parser.add_argument("--ru-term", default="", help="Russian term for variant classification")
    parser.add_argument("--ua-term", default="", help="Ukrainian term for variant classification")
    parser.add_argument("--api-keys", required=True)
    args = parser.parse_args()

    global _keys
    _keys = args.api_keys.split(",")

    out_path = ROOT / "data" / "cl" / "raw" / "youtube_adaptive" / f"{args.pair}.parquet"
    existing = pd.read_parquet(out_path)
    existing_ids = set(existing['video_id'])
    match_re = re.compile(args.match_pattern, re.IGNORECASE)
    log.info(f"Existing: {len(existing):,} videos")

    # Step 1: Resume search
    year, month = map(int, args.resume_from.split("-"))
    current = datetime(year, month, 1)
    end = datetime(2026, 1, 1)
    new_videos = {}

    log.info(f"=== Resuming '{args.term}' from {args.resume_from} ===")
    while current < end and not all_keys_dead():
        month_end = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        after = current.strftime("%Y-%m-%dT00:00:00Z")
        before = min(month_end, end).strftime("%Y-%m-%dT00:00:00Z")
        results = search_window(args.term, after, before)
        new_count = 0
        for r in results:
            vid = r["video_id"]
            if vid not in existing_ids and vid not in new_videos:
                if match_re.search(r["title"]):
                    r["search_variant"] = "ukrainian"
                    r["matched_term"] = args.term
                    new_videos[vid] = r
                    new_count += 1
        log.info(f"  {current.strftime('%Y-%m')}: {new_count} new ({len(new_videos)} total, {_units} units)")
        current = month_end

    log.info(f"New exact matches: {len(new_videos):,}")

    # Step 2: Merge
    if new_videos:
        new_rows = []
        for vid, info in new_videos.items():
            title = info["title"]
            has_ru = bool(re.search(re.escape(args.ru_term), title, re.IGNORECASE)) if args.ru_term else False
            has_ua = bool(re.search(re.escape(args.ua_term), title, re.IGNORECASE)) if args.ua_term else False
            variant = "both" if (has_ru and has_ua) else "ukrainian" if has_ua else "russian" if has_ru else info["search_variant"]
            new_rows.append({
                "video_id": vid, "title": title, "channel": info["channel"],
                "channel_id": "", "published_at": info["published_at"],
                "date": info["published_at"][:10], "variant": variant,
                "matched_term": info["matched_term"], "pair_slug": args.pair,
                "has_transcript": False, "text": title, "text_len": len(title),
            })
        merged = pd.concat([existing, pd.DataFrame(new_rows)], ignore_index=True)
        merged = merged.drop_duplicates(subset="video_id", keep="first").reset_index(drop=True)
    else:
        merged = existing

    log.info(f"Merged: {len(merged):,}")

    # Checkpoint: save before metadata so we don't lose search results
    merged.to_parquet(out_path, index=False)
    log.info(f"Checkpoint saved: {len(merged):,} videos (pre-metadata)")

    # Step 3: Metadata
    log.info(f"\n=== Metadata for {len(merged):,} videos ===")
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

    valid = merged[merged['title'].str.contains(args.match_pattern, case=False, na=False, regex=True)]
    valid.to_parquet(out_path, index=False)
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
