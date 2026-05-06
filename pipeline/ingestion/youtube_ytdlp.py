"""YouTube data collection using yt-dlp.

No API key needed. Uses yt-dlp to search YouTube for each pair term
and extract video metadata (title, date, channel). Handles rate
limiting properly by mimicking browser behavior.

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


def search_youtube(query: str, max_results: int = 50) -> list[dict]:
    """Search YouTube via yt-dlp. Returns list of {title, date, id}."""
    cmd = [
        "yt-dlp",
        f"ytsearch{max_results}:{query}",
        "--no-download",
        "--print", "%(title)s\t%(upload_date)s\t%(id)s\t%(channel)s",
        "--no-warnings",
        "--socket-timeout", "10",
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        videos = []
        for line in result.stdout.strip().split("\n"):
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) >= 3:
                videos.append({
                    "title": parts[0],
                    "upload_date": parts[1] if parts[1] != "NA" else "",
                    "video_id": parts[2],
                    "channel": parts[3] if len(parts) > 3 else "",
                })
        return videos
    except subprocess.TimeoutExpired:
        log.warning(f"  Timeout searching: {query}")
        return []
    except Exception as e:
        log.warning(f"  Error searching: {query}: {e}")
        return []


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair-ids", type=str, default="",
                        help="Comma-separated pair IDs to process (default: all)")
    parser.add_argument("--results-per-query", type=int, default=50)
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    pairs = PAIRS
    if args.pair_ids:
        ids = {int(x) for x in args.pair_ids.split(",")}
        pairs = [p for p in pairs if p["id"] in ids]

    log.info(f"Processing {len(pairs)} pairs, {args.results_per_query} results per query")

    for p in pairs:
        pid = p["id"]
        ru = p["russian"]
        ua = p["ukrainian"]
        log.info(f"\nPair {pid}: '{ru}' vs '{ua}'")

        rows = []

        for variant, term in [("russian", ru), ("ukrainian", ua)]:
            # Search with and without "Ukraine" context for disambiguation
            queries = [term]
            # For single-word terms, add "Ukraine" to disambiguate
            if " " not in term and len(term) < 15:
                queries.append(f"{term} Ukraine")

            for query in queries:
                log.info(f"  Searching: '{query}'")
                videos = search_youtube(query, max_results=args.results_per_query)
                log.info(f"    Got {len(videos)} videos")

                # Check title for exact term match
                term_re = re.compile(re.escape(term), re.IGNORECASE)
                for v in videos:
                    if term_re.search(v["title"]):
                        year = v["upload_date"][:4] if v["upload_date"] else ""
                        rows.append({
                            "pair_id": pid,
                            "variant": variant,
                            "term": term,
                            "title": v["title"],
                            "video_id": v["video_id"],
                            "channel": v["channel"],
                            "year": year,
                            "upload_date": v["upload_date"],
                        })

                time.sleep(3)  # be respectful

        if rows:
            df = pd.DataFrame(rows).drop_duplicates(subset=["video_id"])
            out_path = OUT_DIR / f"pair_{pid:02d}.csv"
            df.to_csv(out_path, index=False)

            # Count by variant
            ru_n = len(df[df["variant"] == "russian"])
            ua_n = len(df[df["variant"] == "ukrainian"])
            total = ru_n + ua_n
            adoption = ua_n / total * 100 if total > 0 else 0
            log.info(f"  Saved: {out_path} — RU:{ru_n} UA:{ua_n} ({adoption:.0f}% adoption)")
        else:
            log.info(f"  No matches for pair {pid}")

        log.info(f"Pair {pid}: done")

    log.info("\nYouTube collection complete")


if __name__ == "__main__":
    main()
