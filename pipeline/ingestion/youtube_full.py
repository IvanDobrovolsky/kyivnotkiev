"""Full YouTube data collection via yt-dlp (no API key needed).

Uses yt-dlp to search YouTube for each toponym variant, splitting by year
to get historical coverage. Then does a second pass to fetch full metadata
(upload_date, view_count) for each video ID found.

If YOUTUBE_API_KEY is set, uses YouTube Data API for metadata lookups
(much faster: 50 videos per request, 1 quota unit each).

Usage:
    python -m pipeline.ingestion.youtube_full [--pair kyiv] [--dry-run]
    YOUTUBE_API_KEY=xxx python -m pipeline.ingestion.youtube_full
"""

import argparse
import json
import logging
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
OUT_DIR = ROOT / "data" / "cl" / "raw" / "youtube_full"

SEARCH_RESULTS_PER_QUERY = 100  # yt-dlp max useful results per search


def load_pairs():
    with open(CONFIG_PATH) as f:
        data = yaml.safe_load(f)
    return [p for p in data["pairs"] if p.get("enabled", True)]


def ytdlp_search(query: str, max_results: int = SEARCH_RESULTS_PER_QUERY) -> list[dict]:
    """Search YouTube via yt-dlp, return list of {id, title}."""
    try:
        cmd = [
            "yt-dlp",
            f"ytsearch{max_results}:{query}",
            "--dump-json", "--flat-playlist",
            "--no-download", "--quiet", "--no-warnings",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode != 0:
            return []

        items = []
        for line in result.stdout.strip().split("\n"):
            if not line:
                continue
            try:
                data = json.loads(line)
                items.append({
                    "video_id": data.get("id", ""),
                    "title": data.get("title", ""),
                    "channel": data.get("channel", "") or data.get("uploader", ""),
                    "channel_id": data.get("channel_id", "") or data.get("uploader_id", ""),
                })
            except json.JSONDecodeError:
                continue
        return items
    except (subprocess.TimeoutExpired, OSError) as e:
        log.warning(f"  yt-dlp search failed: {e}")
        return []


def ytdlp_metadata(video_ids: list[str]) -> dict[str, dict]:
    """Fetch full metadata for a list of video IDs via yt-dlp."""
    results = {}
    for vid in video_ids:
        try:
            cmd = [
                "yt-dlp",
                f"https://www.youtube.com/watch?v={vid}",
                "--dump-json", "--no-download", "--quiet", "--no-warnings",
                "--skip-download",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            if result.returncode == 0 and result.stdout.strip():
                data = json.loads(result.stdout.strip())
                results[vid] = {
                    "upload_date": data.get("upload_date", ""),
                    "view_count": data.get("view_count", 0),
                    "like_count": data.get("like_count", 0),
                    "duration": data.get("duration", 0),
                    "description": (data.get("description", "") or "")[:500],
                    "channel": data.get("channel", "") or data.get("uploader", ""),
                    "channel_id": data.get("channel_id", ""),
                }
        except (subprocess.TimeoutExpired, json.JSONDecodeError, OSError):
            continue
    return results


def youtube_api_metadata(video_ids: list[str], api_key: str) -> dict[str, dict]:
    """Fetch metadata for video IDs via YouTube Data API (50 at a time, 1 unit each)."""
    results = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i+50]
        try:
            resp = requests.get(
                "https://www.googleapis.com/youtube/v3/videos",
                params={
                    "part": "snippet,statistics",
                    "id": ",".join(batch),
                    "key": api_key,
                },
                timeout=15,
            )
            if resp.status_code == 200:
                for item in resp.json().get("items", []):
                    vid = item["id"]
                    snippet = item.get("snippet", {})
                    stats = item.get("statistics", {})
                    results[vid] = {
                        "upload_date": snippet.get("publishedAt", "")[:10].replace("-", ""),
                        "view_count": int(stats.get("viewCount", 0)),
                        "like_count": int(stats.get("likeCount", 0)),
                        "duration": 0,
                        "description": snippet.get("description", "")[:500],
                        "channel": snippet.get("channelTitle", ""),
                        "channel_id": snippet.get("channelId", ""),
                    }
            elif resp.status_code == 403:
                log.warning("YouTube API quota exceeded, falling back to yt-dlp")
                remaining = video_ids[i:]
                results.update(ytdlp_metadata(remaining))
                break
            time.sleep(0.2)
        except requests.RequestException as e:
            log.warning(f"YouTube API error: {e}")
    return results


def collect_pair(pair: dict, api_key: str | None = None) -> pd.DataFrame:
    """Collect YouTube data for a pair by searching each variant per year."""
    slug = pair["slug"]
    ru_term = pair["russian"]
    ua_term = pair["ukrainian"]

    all_videos = {}  # video_id -> row dict (dedup by id)

    for variant, term in [("russian", ru_term), ("ukrainian", ua_term)]:
        # Search per year to get historical spread
        for year in range(2010, 2026):
            query = f"{term} {year}"
            log.info(f"  [{slug}] yt-dlp: '{query}' ({variant})")
            items = ytdlp_search(query, max_results=50)

            for item in items:
                vid = item["video_id"]
                if vid and vid not in all_videos:
                    all_videos[vid] = {
                        "video_id": vid,
                        "title": item["title"],
                        "channel": item["channel"],
                        "channel_id": item["channel_id"],
                        "pair_slug": slug,
                        "variant": variant,
                        "matched_term": term,
                    }

            time.sleep(0.5)

        # Also do a general search without year (catches things year-search misses)
        log.info(f"  [{slug}] yt-dlp: '{term}' general ({variant})")
        items = ytdlp_search(term, max_results=100)
        for item in items:
            vid = item["video_id"]
            if vid and vid not in all_videos:
                all_videos[vid] = {
                    "video_id": vid,
                    "title": item["title"],
                    "channel": item["channel"],
                    "channel_id": item["channel_id"],
                    "pair_slug": slug,
                    "variant": variant,
                    "matched_term": term,
                }
        time.sleep(1)

    if not all_videos:
        return pd.DataFrame()

    log.info(f"  [{slug}] Found {len(all_videos)} unique videos, fetching metadata...")

    # Fetch metadata for all videos
    video_ids = list(all_videos.keys())
    if api_key:
        metadata = youtube_api_metadata(video_ids, api_key)
    else:
        # yt-dlp metadata is slow (~2s per video), batch in parallel
        metadata = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Split into chunks
            chunk_size = 10
            chunks = [video_ids[i:i+chunk_size] for i in range(0, len(video_ids), chunk_size)]
            futures = {executor.submit(ytdlp_metadata, chunk): chunk for chunk in chunks}
            done = 0
            for future in as_completed(futures):
                result = future.result()
                metadata.update(result)
                done += len(futures[future])
                if done % 50 == 0:
                    log.info(f"  [{slug}] Metadata: {done}/{len(video_ids)}")

    # Merge metadata into video records
    rows = []
    for vid, row in all_videos.items():
        meta = metadata.get(vid, {})
        row["upload_date"] = meta.get("upload_date", "")
        row["view_count"] = meta.get("view_count", 0)
        row["like_count"] = meta.get("like_count", 0)
        row["description"] = meta.get("description", "")
        if row["upload_date"]:
            try:
                dt = datetime.strptime(row["upload_date"][:8], "%Y%m%d")
                row["date"] = dt.strftime("%Y-%m-%d")
                row["year"] = dt.year
            except ValueError:
                row["date"] = ""
                row["year"] = 0
        else:
            row["date"] = ""
            row["year"] = 0
        rows.append(row)

    df = pd.DataFrame(rows)

    # Determine variant from title (more accurate than search query)
    def title_variant(row):
        title = row["title"].lower() if isinstance(row["title"], str) else ""
        ru = row["matched_term"].lower()
        ua_term_for_pair = ua_term.lower() if row["variant"] == "ukrainian" else ru_term.lower()
        # Check what's actually in the title
        has_ru = ru_term.lower() in title
        has_ua = ua_term.lower() in title
        if has_ru and has_ua:
            return "both"
        elif has_ua:
            return "ukrainian"
        elif has_ru:
            return "russian"
        return row["variant"]  # fallback to search variant

    df["variant"] = df.apply(title_variant, axis=1)

    return df


def main():
    import os
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--api-key", type=str, default=os.environ.get("YOUTUBE_API_KEY"))
    args = parser.parse_args()

    pairs = load_pairs()
    if args.pair:
        pairs = [p for p in pairs if p["slug"] == args.pair]
        if not pairs:
            log.error(f"Pair '{args.pair}' not found")
            return

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        for p in pairs:
            log.info(f"Would fetch: {p['slug']} — '{p['russian']}' vs '{p['ukrainian']}'")
        return

    if args.api_key:
        log.info("Using YouTube Data API for metadata lookups")
    else:
        log.info("No API key — using yt-dlp for metadata (slower)")

    total = 0
    for i, pair in enumerate(pairs):
        log.info(f"\n[{i+1}/{len(pairs)}] {pair['slug']}")

        out_path = OUT_DIR / f"{pair['slug']}.parquet"
        if out_path.exists():
            existing = pd.read_parquet(out_path)
            log.info(f"  Already exists: {len(existing)} videos — skipping")
            total += len(existing)
            continue

        df = collect_pair(pair, api_key=args.api_key)
        if df.empty:
            log.info(f"  No results")
            continue

        df.to_parquet(out_path, index=False)
        log.info(f"  Saved: {out_path} ({len(df)} videos)")
        total += len(df)

        if df["year"].any():
            yearly = df.groupby(["year", "variant"]).size().unstack(fill_value=0)
            log.info(f"  Per year:\n{yearly.to_string()}")

    log.info(f"\nDONE: {total} total videos across {len(pairs)} pairs")


if __name__ == "__main__":
    main()
