"""Fetch article bodies from GDELT URLs using trafilatura.

Reads URLs from gdelt_urls_complete.parquet, fetches article text,
verifies exact pair term presence, saves incrementally to parquet.

Usage:
    python -m pipeline.ingestion.gdelt_fetch_articles [--workers 4 --batch-size 500]
"""

import argparse
import concurrent.futures
import logging
import re
import time
from pathlib import Path

import pandas as pd
import yaml
import trafilatura

from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

IN_DIR = ROOT_DIR / "data" / "raw" / "gdelt"
OUT_DIR = ROOT_DIR / "data" / "cl" / "raw" / "gdelt"

# Load pair config for term verification
with open(ROOT_DIR / "config" / "pairs.yaml") as f:
    _cfg = yaml.safe_load(f)

PAIRS = {}
for p in _cfg["pairs"]:
    if not p.get("enabled") or p.get("is_control"):
        continue
    PAIRS[p["id"]] = {
        "russian": p["russian"],
        "ukrainian": p["ukrainian"],
        "ru_re": re.compile(r"\b" + re.escape(p["russian"]) + r"\b", re.IGNORECASE),
        "ua_re": re.compile(r"\b" + re.escape(p["ukrainian"]) + r"\b", re.IGNORECASE),
    }

# Latin script check — reject pages that are predominantly Cyrillic/CJK
LATIN_RE = re.compile(r"[a-zA-Z]")
NON_LATIN_RE = re.compile(r"[\u0400-\u04FF\u4E00-\u9FFF\u3040-\u309F\u30A0-\u30FF\uAC00-\uD7AF]")


def is_latin_dominant(text: str) -> bool:
    """Check if text is predominantly Latin script (>60%)."""
    if not text:
        return False
    sample = text[:2000]
    latin = len(LATIN_RE.findall(sample))
    non_latin = len(NON_LATIN_RE.findall(sample))
    total = latin + non_latin
    if total < 20:
        return False
    return latin / total > 0.6


def fetch_one(row: dict) -> dict | None:
    """Fetch and extract article text from a single URL."""
    url = row["url"]
    pair_id = row["pair_id"]
    variant = row["variant"]

    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return None

        text = trafilatura.extract(downloaded, include_comments=False,
                                   include_tables=False, no_fallback=True)
        if not text or len(text) < 50:
            return None

        # Latin script filter
        if not is_latin_dominant(text):
            return None

        # Verify exact pair term presence in extracted body
        pair = PAIRS.get(pair_id)
        if not pair:
            return None

        has_ru = pair["ru_re"].search(text)
        has_ua = pair["ua_re"].search(text)

        if not has_ru and not has_ua:
            return None

        # Determine actual variant from body text (not URL)
        if has_ua and has_ru:
            body_variant = "both"
        elif has_ua:
            body_variant = "ukrainian"
        else:
            body_variant = "russian"

        return {
            "pair_id": pair_id,
            "text": text[:5000],  # cap at 5K chars for corpus
            "source": "gdelt",
            "variant": body_variant,
            "url_variant": variant,  # keep original URL-based variant
            "matched_term_ru": pair["russian"] if has_ru else "",
            "matched_term_ua": pair["ukrainian"] if has_ua else "",
            "domain": row.get("domain", ""),
            "date": row.get("date", ""),
            "url": url,
            "text_len": len(text),
        }
    except Exception as e:
        log.debug(f"Error fetching {url}: {e}")
        return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=4,
                        help="Parallel fetch workers (be gentle with domains)")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Save checkpoint every N URLs")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit total URLs to process (0=all)")
    parser.add_argument("--resume", action="store_true",
                        help="Skip URLs already fetched in previous runs")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint_path = OUT_DIR / "gdelt_articles_checkpoint.parquet"
    final_path = OUT_DIR / "gdelt_articles.parquet"

    # Load URLs
    url_df = pd.read_parquet(IN_DIR / "gdelt_urls_complete.parquet")
    log.info(f"Loaded {len(url_df):,} URLs from {url_df['pair_id'].nunique()} pairs")

    # Filter out non-HTTP URLs (some GDELT entries are just text descriptions)
    url_df = url_df[url_df["url"].str.startswith("http", na=False)].copy()
    log.info(f"HTTP URLs: {len(url_df):,}")

    # Resume from checkpoint
    done_urls = set()
    existing_results = []
    if args.resume and checkpoint_path.exists():
        prev = pd.read_parquet(checkpoint_path)
        done_urls = set(prev["url"].tolist())
        existing_results = prev.to_dict("records")
        log.info(f"Resuming: {len(done_urls):,} URLs already fetched")
        url_df = url_df[~url_df["url"].isin(done_urls)]

    if args.limit > 0:
        url_df = url_df.head(args.limit)

    log.info(f"URLs to fetch: {len(url_df):,}")

    # Shuffle to spread across domains (avoid hammering one domain)
    url_df = url_df.sample(frac=1, random_state=42).reset_index(drop=True)

    rows = url_df.to_dict("records")
    results = list(existing_results)
    fetched = 0
    success = 0
    t0 = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_one, r): r for r in rows}
        for future in concurrent.futures.as_completed(futures):
            fetched += 1
            result = future.result()
            if result:
                results.append(result)
                success += 1

            if fetched % args.batch_size == 0:
                elapsed = time.time() - t0
                rate = fetched / elapsed if elapsed > 0 else 0
                log.info(
                    f"  {fetched:,}/{len(rows):,} fetched, "
                    f"{success:,} extracted ({success/fetched*100:.1f}%), "
                    f"{rate:.1f} URLs/sec"
                )
                # Save checkpoint
                if results:
                    pd.DataFrame(results).to_parquet(checkpoint_path, index=False)

    elapsed = time.time() - t0
    log.info(f"\nDone: {fetched:,} fetched in {elapsed:.0f}s, {success:,} articles extracted")

    if results:
        df = pd.DataFrame(results)
        df = df.drop_duplicates(subset=["pair_id", "url"])
        df.to_parquet(final_path, index=False)
        log.info(f"Saved: {final_path} ({len(df):,} articles)")
        log.info(f"Pairs: {df['pair_id'].nunique()}")
        log.info(f"Variants: {df['variant'].value_counts().to_dict()}")
        log.info(f"Median text length: {df['text_len'].median():.0f} chars")

        # Per-pair summary
        for pid, grp in df.groupby("pair_id"):
            pair = PAIRS.get(pid, {})
            log.info(
                f"  Pair {pid} ({pair.get('russian','')}/{pair.get('ukrainian','')}): "
                f"{len(grp):,} articles"
            )

        # Remove checkpoint after final save
        if checkpoint_path.exists():
            checkpoint_path.unlink()
    else:
        log.info("No articles extracted")


if __name__ == "__main__":
    main()
