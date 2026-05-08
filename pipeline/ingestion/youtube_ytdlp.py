"""YouTube data collection using yt-dlp flat-playlist search.

Two-phase approach:
1. Fast search (flat-playlist): get 50 titles per query, match terms. ~3s/query.
2. Date extraction: batch-extract upload dates for matched videos only. ~1s/video.

No API key needed. No IP ban (flat-playlist doesn't trigger rate limits).

Usage:
    python -m pipeline.ingestion.youtube_ytdlp [--pair-ids 1,2,3]
"""

import argparse
import json
import logging
import os
import re
import subprocess
import time
from pathlib import Path

import pandas as pd
import yaml

from pipeline.config import ROOT_DIR, DATA_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = DATA_DIR / "raw" / "youtube"

with open(ROOT_DIR / "config" / "pairs.yaml") as f:
    _cfg = yaml.safe_load(f)

PAIRS = []
for p in _cfg["pairs"]:
    if not p.get("enabled") or p.get("is_control"):
        continue
    PAIRS.append({"id": p["id"], "russian": p["russian"], "ukrainian": p["ukrainian"]})


def flat_search(query: str, max_results: int = 50) -> list[dict]:
    """Fast flat-playlist search. Returns {title, id} without dates."""
    cmd = [
        "yt-dlp", f"ytsearch{max_results}:{query}",
        "--flat-playlist",
        "--print", "%(title)s\t%(id)s",
        "--no-warnings",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        videos = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split("\t", 1)
            if len(parts) == 2:
                videos.append({"title": parts[0], "id": parts[1]})
        return videos
    except Exception as e:
        log.warning(f"  Search failed: {query}: {e}")
        return []


def get_upload_dates(video_ids: list[str]) -> dict[str, str]:
    """Batch-extract upload dates for specific video IDs."""
    if not video_ids:
        return {}

    dates = {}
    # Process in batches of 10 to avoid timeouts
    for i in range(0, len(video_ids), 10):
        batch = video_ids[i:i+10]
        for vid in batch:
            cmd = [
                "yt-dlp", f"https://youtube.com/watch?v={vid}",
                "--no-download", "--print", "%(upload_date)s",
                "--no-warnings", "--socket-timeout", "10",
            ]
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
                date_str = result.stdout.strip()
                if date_str and date_str != "NA":
                    dates[vid] = date_str
            except:
                pass
            time.sleep(0.5)

    return dates


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-ids", type=str, default="")
    parser.add_argument("--results-per-query", type=int, default=50)
    parser.add_argument("--skip-dates", action="store_true",
                        help="Skip date extraction (faster, uses year from search context)")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    pairs = PAIRS
    if args.pair_ids:
        ids = {int(x) for x in args.pair_ids.split(",")}
        pairs = [p for p in pairs if p["id"] in ids]

    log.info(f"Processing {len(pairs)} pairs")

    for p in pairs:
        pid = p["id"]
        ru = p["russian"]
        ua = p["ukrainian"]
        log.info(f"\nPair {pid}: '{ru}' vs '{ua}'")

        all_matches = []

        for variant, term in [("russian", ru), ("ukrainian", ua)]:
            term_re = re.compile(re.escape(term), re.IGNORECASE)

            # Search current results
            log.info(f"  Searching: '{term}'")
            videos = flat_search(term, max_results=args.results_per_query)

            matched = [v for v in videos if term_re.search(v["title"])]
            log.info(f"    {len(matched)}/{len(videos)} title matches")

            for v in matched:
                all_matches.append({
                    "pair_id": pid,
                    "variant": variant,
                    "term": term,
                    "title": v["title"],
                    "video_id": v["id"],
                })

            time.sleep(1)

        if not all_matches:
            log.info(f"  No matches")
            continue

        df = pd.DataFrame(all_matches).drop_duplicates(subset=["video_id"])

        # Extract dates for matched videos
        if not args.skip_dates:
            log.info(f"  Extracting dates for {len(df)} videos...")
            dates = get_upload_dates(df["video_id"].tolist())
            df["upload_date"] = df["video_id"].map(dates).fillna("")
            df["year"] = df["upload_date"].str[:4]
            log.info(f"    Got dates for {len(dates)}/{len(df)} videos")
        else:
            df["upload_date"] = ""
            df["year"] = ""

        out_path = OUT_DIR / f"pair_{pid:02d}.csv"
        df.to_csv(out_path, index=False)

        ru_n = len(df[df["variant"] == "russian"])
        ua_n = len(df[df["variant"] == "ukrainian"])
        total = ru_n + ua_n
        adoption = ua_n / total * 100 if total > 0 else 0
        log.info(f"  Saved: {out_path} — RU:{ru_n} UA:{ua_n} ({adoption:.0f}%)")

    log.info("\nComplete")


if __name__ == "__main__":
    main()
