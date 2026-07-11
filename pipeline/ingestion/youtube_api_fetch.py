"""YouTube full data collection via YouTube Data API v3.

Pipeline: search.list (per year) → videos.list (metadata) → transcripts (free).
Produces holdouts + corpus segments.

Usage:
    python -m pipeline.ingestion.youtube_api_fetch --pair chornobyl
    YOUTUBE_API_KEY=xxx python -m pipeline.ingestion.youtube_api_fetch --pair kyiv
"""

import argparse
import json
import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
OUT_DIR = ROOT / "data" / "cl" / "raw" / "youtube_api"

API_URL = "https://www.googleapis.com/youtube/v3"
UNITS_USED = 0


def load_pairs():
    with open(CONFIG_PATH) as f:
        data = yaml.safe_load(f)
    return [p for p in data["pairs"] if p.get("enabled", True)]


def track_units(n):
    global UNITS_USED
    UNITS_USED += n
    return UNITS_USED


def search_videos(query: str, api_key: str, published_after: str, published_before: str, max_pages: int = 3) -> list[dict]:
    """Search YouTube, paginate up to max_pages. Returns list of video snippets."""
    results = []
    page_token = None

    for page in range(max_pages):
        # Wrap multi-word queries in quotes for exact phrase matching
        q = f'"{query}"' if ' ' in query else query
        params = {
            "part": "snippet",
            "q": q,
            "type": "video",
            "maxResults": 50,
            "relevanceLanguage": "en",
            "publishedAfter": published_after,
            "publishedBefore": published_before,
            "key": api_key,
        }
        if page_token:
            params["pageToken"] = page_token

        resp = requests.get(f"{API_URL}/search", params=params, timeout=15)
        track_units(100)

        if resp.status_code == 403:
            log.warning(f"  Quota exceeded at {UNITS_USED} units")
            return results
        if resp.status_code != 200:
            log.warning(f"  HTTP {resp.status_code}: {resp.text[:100]}")
            break

        data = resp.json()
        for item in data.get("items", []):
            results.append({
                "video_id": item["id"]["videoId"],
                "title": item["snippet"]["title"],
                "channel": item["snippet"]["channelTitle"],
                "published_at": item["snippet"]["publishedAt"],
                "description_snippet": item["snippet"]["description"],
            })

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.2)

    return results


def get_video_details(video_ids: list[str], api_key: str) -> dict[str, dict]:
    """Batch fetch video metadata. 1 unit per 50 IDs."""
    details = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        resp = requests.get(f"{API_URL}/videos", params={
            "part": "snippet",
            "id": ",".join(batch),
            "key": api_key,
        }, timeout=15)
        track_units(1)

        if resp.status_code == 200:
            for item in resp.json().get("items", []):
                vid = item["id"]
                snippet = item.get("snippet", {})
                details[vid] = {
                    "description": snippet.get("description", ""),
                    "channel_id": snippet.get("channelId", ""),
                }
        time.sleep(0.2)
    return details


def fetch_transcripts(video_ids: list[str], max_videos: int = 500) -> dict[str, str]:
    """Fetch transcripts via youtube_transcript_api v1.2+ (free, no quota)."""
    from youtube_transcript_api import YouTubeTranscriptApi
    api = YouTubeTranscriptApi()
    transcripts = {}
    attempted = 0

    for vid in video_ids[:max_videos]:
        attempted += 1
        try:
            # v1.2+: instance methods, .fetch() returns FetchedTranscript
            result = api.fetch(vid, languages=['en', 'uk', 'ru'])
            text = " ".join(s.text for s in result.snippets)
            if len(text) > 50:
                transcripts[vid] = text
        except Exception:
            pass
        time.sleep(0.3)

        if attempted % 50 == 0:
            log.info(f"    Transcripts: {len(transcripts)}/{attempted} attempted ({len(video_ids)} total)")

    log.info(f"    Transcripts done: {len(transcripts)}/{attempted} attempted")
    return transcripts


def _save_videos(all_videos, details, transcripts, slug, ru_term, ua_term, out_path):
    """Build and save video dataframe."""
    rows = []
    for vid, info in all_videos.items():
        detail = details.get(vid, {})
        transcript = transcripts.get(vid, "")

        title = info["title"]
        has_ru = bool(re.search(re.escape(ru_term), title, re.IGNORECASE))
        has_ua = bool(re.search(re.escape(ua_term), title, re.IGNORECASE))
        if has_ru and has_ua:
            variant = "both"
        elif has_ua:
            variant = "ukrainian"
        elif has_ru:
            variant = "russian"
        else:
            variant = info["search_variant"]

        desc = detail.get("description", info.get("description_snippet", ""))
        corpus_text = f"{title}\n\n{desc}"
        if transcript:
            corpus_text += f"\n\n{transcript[:1500]}"

        rows.append({
            "video_id": vid, "title": title, "channel": info["channel"],
            "channel_id": detail.get("channel_id", ""),
            "published_at": info["published_at"], "date": info["published_at"][:10],
            "variant": variant,
            "matched_term": info["matched_term"], "pair_slug": slug,
            "has_transcript": bool(transcript),
            "text": corpus_text[:2000], "text_len": min(len(corpus_text), 2000),
        })

    df = pd.DataFrame(rows)
    df.to_parquet(out_path, index=False)
    return df


def collect_pair(pair: dict, api_key: str) -> pd.DataFrame:
    """Full pipeline for one pair."""
    slug = pair["slug"]
    ru_term = pair["russian"]
    ua_term = pair["ukrainian"]

    out_path = OUT_DIR / f"{slug}.parquet"
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        log.info(f"  Already exists: {len(existing)} videos — skipping")
        return existing

    all_videos = {}

    for variant, term in [("russian", ru_term), ("ukrainian", ua_term)]:
        for year in range(2015, 2026):
            after = f"{year}-01-01T00:00:00Z"
            before = f"{year}-12-31T23:59:59Z" if year < 2025 else "2025-12-31T23:59:59Z"

            results = search_videos(term, api_key, after, before)
            for r in results:
                vid = r["video_id"]
                if vid not in all_videos:
                    r["search_variant"] = variant
                    r["matched_term"] = term
                    all_videos[vid] = r

            log.info(f"  [{slug}] {year} '{term}': {len(results)} results (total: {len(all_videos)}, units: {UNITS_USED})")

            if UNITS_USED >= 9800:
                log.warning(f"  Near quota limit ({UNITS_USED}), stopping search")
                break
        if UNITS_USED >= 9800:
            break

    if not all_videos:
        return pd.DataFrame()

    video_ids = list(all_videos.keys())
    log.info(f"  [{slug}] {len(video_ids)} unique videos. Fetching metadata...")

    # Step 2: Video details
    details = get_video_details(video_ids, api_key)
    log.info(f"  [{slug}] Metadata for {len(details)} videos (units: {UNITS_USED})")

    # Save checkpoint after metadata (before slow transcripts)
    _save_videos(all_videos, details, {}, slug, ru_term, ua_term, out_path)
    log.info(f"  [{slug}] Checkpoint saved (pre-transcripts)")

    # Step 3: Transcripts (free, capped at 500 per pair)
    log.info(f"  [{slug}] Fetching transcripts (max 500)...")
    transcripts = fetch_transcripts(video_ids, max_videos=500)
    log.info(f"  [{slug}] Transcripts: {len(transcripts)}/{min(500, len(video_ids))}")

    # Final save with transcripts
    df = _save_videos(all_videos, details, transcripts, slug, ru_term, ua_term, out_path)
    log.info(f"  [{slug}] Final: {len(df)} videos, {df['has_transcript'].sum()} with transcripts")
    log.info(f"  Variants: {df['variant'].value_counts().to_dict()}")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", type=str, action="append", help="Pair slug(s)")
    parser.add_argument("--api-key", type=str, default=None)
    args = parser.parse_args()

    api_key = args.api_key or "AIzaSyAUKUYVX-HcbzeR8rqijffviRaYGBF7rEQ"
    pairs = load_pairs()

    if args.pair:
        pairs = [p for p in pairs if p["slug"] in args.pair]

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    total = 0
    for i, pair in enumerate(pairs):
        log.info(f"\n[{i + 1}/{len(pairs)}] {pair['slug']} (units used: {UNITS_USED})")
        if UNITS_USED >= 9800:
            log.warning("Quota nearly exhausted, stopping")
            break
        df = collect_pair(pair, api_key)
        total += len(df)

    log.info(f"\nDONE: {total} videos, {UNITS_USED} API units used")


if __name__ == "__main__":
    main()
