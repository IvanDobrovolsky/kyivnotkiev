"""Stream GDELT GKG files from data.gdeltproject.org, extract matching URLs.

Downloads each 15-min GKG file, scans for pair terms in DocumentIdentifier (URL),
applies disambiguation filters, saves matching URLs to local parquet.

No BigQuery, no API keys, no rate limits. Just HTTP + grep.

Usage:
    python -m pipeline.ingestion.gdelt_stream [--start 201502 --end 202604 --workers 30]
"""

import argparse
import concurrent.futures
import io
import logging
import re
import zipfile
from collections import defaultdict
from pathlib import Path

import pandas as pd
import requests
import yaml

from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = ROOT_DIR / "data" / "raw" / "gdelt"
MASTERLIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
GKG_COL_URL = 4  # DocumentIdentifier (URL) is column index 4
GKG_COL_DATE = 1  # DATE column
GKG_COL_DOMAIN = 3  # SOURCECOMMONNAME

# Build regex patterns for all enabled pairs with disambiguation
with open(ROOT_DIR / "config" / "pairs.yaml") as f:
    _cfg = yaml.safe_load(f)

PAIR_PATTERNS = []
for pair in _cfg["pairs"]:
    if not pair.get("enabled") or pair.get("is_control"):
        continue
    slug = pair["slug"]
    ru = pair["russian"].lower()
    ua = pair["ukrainian"].lower()

    # Disambiguation from pairs.yaml homonym_filters (no hardcoded IDs)
    negatives = [re.compile(f, re.IGNORECASE)
                 for f in pair.get("homonym_filters", [])]

    PAIR_PATTERNS.append({
        "slug": slug,
        "russian": ru,
        "ukrainian": ua,
        "ru_re": re.compile(re.escape(ru).replace(r"\ ", r"[\s\-_+%20]+"), re.IGNORECASE),
        "ua_re": re.compile(re.escape(ua).replace(r"\ ", r"[\s\-_+%20]+"), re.IGNORECASE),
        "negatives": negatives,
    })


def match_url(url_lower):
    """Match a URL against all pair patterns. Returns list of (pair_id, variant) tuples."""
    matches = []
    for p in PAIR_PATTERNS:
        # Check negative filters first
        if any(neg.search(url_lower) for neg in p["negatives"]):
            continue

        if p["ru_re"].search(url_lower):
            matches.append((p["slug"], "russian", p["russian"]))
        elif p["ua_re"].search(url_lower):
            matches.append((p["slug"], "ukrainian", p["ukrainian"]))
    return matches


def process_gkg_file(file_url):
    """Download, decompress, scan one GKG file. Returns list of match dicts."""
    results = []
    try:
        resp = requests.get(file_url, timeout=30)
        if resp.status_code != 200:
            return results

        z = zipfile.ZipFile(io.BytesIO(resp.content))
        fname = z.namelist()[0]
        with z.open(fname) as f:
            for line in f:
                try:
                    cols = line.decode("utf-8", errors="replace").split("\t")
                    if len(cols) <= GKG_COL_URL:
                        continue
                    url = cols[GKG_COL_URL].strip()
                    if not url:
                        continue

                    url_lower = url.lower()
                    for pair_id, variant, term in match_url(url_lower):
                        results.append({
                            "pair_slug": pair_id,
                            "url": url,
                            "variant": variant,
                            "matched_term": term,
                            "date": cols[GKG_COL_DATE][:8] if len(cols) > GKG_COL_DATE else "",
                            "domain": cols[GKG_COL_DOMAIN] if len(cols) > GKG_COL_DOMAIN else "",
                        })
                except Exception:
                    continue
    except Exception as e:
        log.debug(f"Error processing {file_url}: {e}")
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="201502", help="Start month YYYYMM")
    parser.add_argument("--end", default="202612", help="End month YYYYMM")
    parser.add_argument("--workers", type=int, default=100, help="Parallel downloads")
    parser.add_argument("--checkpoint-every", type=int, default=2000, help="Save every N files")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_path = OUT_DIR / "gdelt_stream_checkpoint.json"
    out_path = OUT_DIR / "gdelt_urls_complete.parquet"

    # Load checkpoint (set of processed URLs)
    done_urls = set()
    if checkpoint_path.exists():
        import json
        with open(checkpoint_path) as f:
            done_urls = set(json.load(f))
        log.info(f"Resuming from checkpoint: {len(done_urls):,} files already done")

    # Load existing results
    all_matches = []
    if out_path.exists() and done_urls:
        existing = pd.read_parquet(out_path)
        all_matches = existing.to_dict("records")
        log.info(f"Loaded {len(all_matches):,} existing matches")

    # Get master file list
    log.info("Fetching GDELT master file list...")
    resp = requests.get(MASTERLIST_URL, timeout=30)
    all_lines = resp.text.strip().split("\n")
    gkg_files = []
    for line in all_lines:
        parts = line.split()
        if len(parts) < 3 or ".gkg." not in parts[-1]:
            continue
        fname = parts[-1].split("/")[-1]
        month = fname[:6]
        if args.start <= month <= args.end:
            if parts[-1] not in done_urls:
                gkg_files.append(parts[-1])

    log.info(f"GKG files remaining: {len(gkg_files):,} (skipped {len(done_urls):,} done)")

    # Process in parallel with checkpointing
    processed = 0
    batch_matches = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_gkg_file, url): url for url in gkg_files}
        for future in concurrent.futures.as_completed(futures):
            url = futures[future]
            matches = future.result()
            batch_matches.extend(matches)
            done_urls.add(url)
            processed += 1

            if processed % 100 == 0:
                log.info(f"  {processed:,}/{len(gkg_files):,} files, {len(batch_matches):,} new matches")

            if processed % args.checkpoint_every == 0:
                all_matches.extend(batch_matches)
                batch_matches = []
                # Save checkpoint
                df = pd.DataFrame(all_matches)
                if len(df):
                    df = df.drop_duplicates(subset=["pair_slug", "url"])
                    df.to_parquet(out_path, index=False)
                import json
                with open(checkpoint_path, "w") as f:
                    json.dump(list(done_urls), f)
                log.info(f"  CHECKPOINT: {len(done_urls):,} files done, {len(df) if len(df) else 0:,} matches saved")

    # Final save
    all_matches.extend(batch_matches)
    if all_matches:
        df = pd.DataFrame(all_matches)
        df = df.drop_duplicates(subset=["pair_slug", "url"])
        df.to_parquet(out_path, index=False)
        import json
        with open(checkpoint_path, "w") as f:
            json.dump(list(done_urls), f)
        log.info(f"\nDone: {processed:,} files, {len(df):,} unique matches")
        log.info(f"Pairs: {df['pair_id'].nunique()}")
        log.info(f"Variants: {df['variant'].value_counts().to_dict()}")

        for pid, grp in df.groupby("pair_slug"):
            pair = next((p for p in _cfg["pairs"] if p["slug"] == pid), {})
            ru_n = (grp["variant"] == "russian").sum()
            ua_n = (grp["variant"] == "ukrainian").sum()
            log.info(f"  Pair {pid} ({pair.get('russian','')}/{pair.get('ukrainian','')}): {len(grp):,} URLs (RU:{ru_n:,} UA:{ua_n:,})")


if __name__ == "__main__":
    main()
