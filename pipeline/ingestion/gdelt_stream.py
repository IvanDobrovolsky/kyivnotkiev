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
    pid = pair["id"]
    ru = pair["russian"].lower()
    ua = pair["ukrainian"].lower()

    # Build negative patterns for disambiguation
    # Each pattern excludes known homonym collisions from Wikipedia disambiguation
    negatives = []
    if pid == 1:  # Kiev city — exclude food (pair 21/22), Kievan (pair 35)
        negatives = ["kievan", "chicken.{0,3}ki[ey]v", "ki[ey]v.{0,3}cake",
                     "ki[ey]v.{0,3}cutlet", "ki[ey]v.{0,3}recipe"]
    elif pid == 3:  # Odessa city — exclude US cities, film, actress
        negatives = ["texas", "permian", "midland", "odessa.{0,5}fl\\b",
                     "odessa.{0,5}missouri", "odesza",  # the band
                     "odessa.{0,5}a.?zion"]  # actress
    elif pid == 6:  # Nikolaev — surname collision (can't fully filter)
        negatives = ["nikolaev.{0,10}(born|author|professor|medal|coach|player)"]
    elif pid == 7:  # Dnipro city — exclude river (pair 15)
        negatives = ["dnipro.{0,5}river", "river.{0,5}dnipro",
                     "reservoir", "hydroelectric", "dam.{0,3}dnipro"]
    elif pid == 15:  # Dnieper river — exclude city (pair 7)
        negatives = ["dnieper.{0,5}city", "fc.{0,5}dnieper",
                     "airport", "metro.{0,5}dnieper"]
    elif pid == 72:  # Artemovsk — exclude composer, champagne
        negatives = ["hulak", "gulak", "artemovsk[ioay]",
                     "champagne", "sparkling", "winery"]
    elif pid == 38:  # Chernigov — exclude restaurant names
        negatives = ["restaurant", "bar.{0,5}chernigov", "barcelona"]
    elif pid == 9:  # Rovno — exclude Slavic adverb usage
        negatives = ["rovno.{0,5}(v|na|po)\\b"]  # Slavic prepositions after rovno

    PAIR_PATTERNS.append({
        "id": pid,
        "russian": ru,
        "ukrainian": ua,
        "ru_re": re.compile(re.escape(ru), re.IGNORECASE),
        "ua_re": re.compile(re.escape(ua), re.IGNORECASE),
        "negatives": [re.compile(n, re.IGNORECASE) for n in negatives],
    })


def match_url(url_lower):
    """Match a URL against all pair patterns. Returns list of (pair_id, variant) tuples."""
    matches = []
    for p in PAIR_PATTERNS:
        # Check negative filters first
        if any(neg.search(url_lower) for neg in p["negatives"]):
            continue

        if p["ru_re"].search(url_lower):
            matches.append((p["id"], "russian", p["russian"]))
        elif p["ua_re"].search(url_lower):
            matches.append((p["id"], "ukrainian", p["ukrainian"]))
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
                            "pair_id": pair_id,
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
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)

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
            gkg_files.append(parts[-1])

    log.info(f"GKG files in range {args.start}-{args.end}: {len(gkg_files):,}")

    # Process in parallel with progress
    all_matches = []
    processed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(process_gkg_file, url): url for url in gkg_files}
        for future in concurrent.futures.as_completed(futures):
            matches = future.result()
            all_matches.extend(matches)
            processed += 1

            if processed % 500 == 0:
                log.info(f"  {processed:,}/{len(gkg_files):,} files, {len(all_matches):,} matches")

    log.info(f"\nDone: {processed:,} files, {len(all_matches):,} total matches")

    if all_matches:
        df = pd.DataFrame(all_matches)
        df = df.drop_duplicates(subset=["pair_id", "url"])
        out_path = OUT_DIR / "gdelt_urls_complete.parquet"
        df.to_parquet(out_path, index=False)
        log.info(f"Saved: {out_path} ({len(df):,} unique URL-pair matches)")
        log.info(f"Pairs: {df['pair_id'].nunique()}")
        log.info(f"Variants: {df['variant'].value_counts().to_dict()}")

        # Per-pair summary
        for pid, grp in df.groupby("pair_id"):
            pair = next((p for p in _cfg["pairs"] if p["id"] == pid), {})
            ru_n = (grp["variant"] == "russian").sum()
            ua_n = (grp["variant"] == "ukrainian").sum()
            log.info(f"  Pair {pid} ({pair.get('russian','')}/{pair.get('ukrainian','')}): {len(grp):,} URLs (RU:{ru_n:,} UA:{ua_n:,})")


if __name__ == "__main__":
    main()
