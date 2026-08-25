"""Size every pair before committing to a census fetch.

One search call per pair-variant over the whole range, reading YouTube's
totalResults estimate. 47 pairs x 2 variants = 94 calls = 9,400 units, under
10% of a day's quota, and it replaces every guess about campaign cost with a
measurement.

Usage:
    python -m pipeline.ingestion.youtube_size_probe --api-keys key1,key2,...
"""

import argparse
import json
import logging
import time
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
OUT_PATH = ROOT / "data" / "cl" / "raw" / "youtube_size_probe.parquet"
API_URL = "https://www.googleapis.com/youtube/v3/search"

AFTER = "2010-01-01T00:00:00Z"
BEFORE = "2025-12-31T23:59:59Z"


def probe(term, key):
    """One search call. Returns (total_results, http_status)."""
    q = f'"{term}"' if " " in term else term
    resp = requests.get(API_URL, params={
        "part": "snippet", "q": q, "type": "video",
        "maxResults": 1, "relevanceLanguage": "en",
        "publishedAfter": AFTER, "publishedBefore": BEFORE,
        "key": key,
    }, timeout=30)
    if resp.status_code != 200:
        return None, resp.status_code
    return resp.json().get("pageInfo", {}).get("totalResults"), 200


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-keys", required=True, help="Comma-separated API keys")
    args = ap.parse_args()

    keys = [k.strip() for k in args.api_keys.split(",") if k.strip()]
    ki = 0
    dead = set()

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    pairs = [p for p in cfg["pairs"] if p.get("enabled", True)]

    rows, units = [], 0
    for p in pairs:
        for variant in ("russian", "ukrainian"):
            term = p[variant]
            while ki < len(keys) * 3 and keys[ki % len(keys)] in dead:
                ki += 1
            if len(dead) >= len(keys):
                log.error("All keys exhausted — partial results saved")
                break
            key = keys[ki % len(keys)]
            ki += 1

            total, status = probe(term, key)
            units += 100
            if status in (403, 429):
                dead.add(key)
                log.warning(f"Key ...{key[-6:]} exhausted ({len(dead)}/{len(keys)})")
                continue
            rows.append({"slug": p["slug"], "variant": variant, "term": term,
                         "total_results": total, "status": status})
            log.info(f"{p['slug']:<28} {variant:<10} '{term}': {total} ({units} units)")
            time.sleep(0.1)
        if len(dead) >= len(keys):
            break

    df = pd.DataFrame(rows)
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)

    log.info(f"\nSaved {len(df)} rows to {OUT_PATH} ({units} units used)")
    if not df.empty and df.total_results.notna().any():
        tot = int(df.total_results.fillna(0).sum())
        log.info(f"Estimated videos across all pairs: {tot:,}")
        log.info(f"Implied census cost: ~{tot // 50:,} search calls, ~{tot * 2:,} units")
        log.info("\nLargest 10 pair-variants:")
        for _, r in df.nlargest(10, "total_results").iterrows():
            log.info(f"  {r.slug:<28} {r.variant:<10} {int(r.total_results):>10,}")
        log.info("\nSmallest 10 (best probe candidates):")
        for _, r in df[df.total_results > 0].nsmallest(10, "total_results").iterrows():
            log.info(f"  {r.slug:<28} {r.variant:<10} {int(r.total_results):>10,}")


if __name__ == "__main__":
    main()
