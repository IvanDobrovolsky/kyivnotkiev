"""Full Reddit data collection via PullPush.io API.

PullPush is a Pushshift successor that indexes ALL of Reddit historically.
Unlike Arctic Shift, it does NOT require a subreddit parameter — searches
across all subreddits. Returns 100 results per page, supports timestamp
pagination for exhaustive collection.

Usage:
    python -m pipeline.ingestion.reddit_pullpush [--pair kyiv] [--dry-run]
"""

import argparse
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
OUT_DIR = ROOT / "data" / "cl" / "raw" / "reddit_full"

PULLPUSH_URL = "https://api.pullpush.io/reddit/search/submission/"
USER_AGENT = "KyivNotKiev-Research/1.0 (academic research)"
PAGE_SIZE = 100
REQUEST_DELAY = 1.0  # be polite


def load_pairs():
    with open(CONFIG_PATH) as f:
        data = yaml.safe_load(f)
    return [p for p in data["pairs"] if p.get("enabled", True)]


_pairs_cache = None
def _pairs_yaml_cache():
    global _pairs_cache
    if _pairs_cache is None:
        _pairs_cache = load_pairs()
    return _pairs_cache


CHECKPOINT_EVERY = 100  # save to disk every N pages (~10K results)


def fetch_all_submissions(term: str, after_ts: int, before_ts: int, checkpoint_path: Path | None = None) -> list[dict]:
    """Paginate through ALL PullPush results for a term, with heavy checkpointing."""
    import json as _json

    all_results = []
    cursor = after_ts
    page = 0

    # Resume from checkpoint if exists
    if checkpoint_path and checkpoint_path.exists():
        with open(checkpoint_path) as f:
            cp = _json.load(f)
        all_results = cp["results"]
        cursor = cp["cursor"]
        page = cp["page"]
        log.info(f"    Resumed from checkpoint: {len(all_results):,} results, page {page}, cursor {cursor}")

    while cursor < before_ts:
        params = {
            "q": f'"{term}"',
            "after": cursor,
            "before": before_ts,
            "size": PAGE_SIZE,
            "sort": "asc",
            "sort_type": "created_utc",
        }
        headers = {"User-Agent": USER_AGENT}

        try:
            resp = requests.get(PULLPUSH_URL, params=params, headers=headers, timeout=30)
            if resp.status_code == 429:
                log.warning("Rate limited, waiting 30s...")
                time.sleep(30)
                continue
            if resp.status_code != 200:
                log.warning(f"HTTP {resp.status_code}, waiting 10s...")
                time.sleep(10)
                continue

            data = resp.json().get("data", [])
            if not data:
                break

            all_results.extend(data)
            page += 1

            # Advance cursor past last result
            last_ts = int(data[-1].get("created_utc", 0))
            if last_ts <= cursor:
                cursor = last_ts + 1
            else:
                cursor = last_ts

            if page % 50 == 0:
                log.info(f"    ... {len(all_results):,} results so far (page {page})")

            # Heavy checkpoint every N pages
            if checkpoint_path and page % CHECKPOINT_EVERY == 0:
                with open(checkpoint_path, "w") as f:
                    _json.dump({"results": all_results, "cursor": cursor, "page": page}, f)
                log.info(f"    CHECKPOINT: {len(all_results):,} results saved to {checkpoint_path.name}")

            if len(data) < PAGE_SIZE:
                break

            time.sleep(REQUEST_DELAY)

        except requests.exceptions.Timeout:
            log.warning("Timeout, retrying in 10s...")
            time.sleep(10)
        except requests.RequestException as e:
            log.warning(f"Request error: {e}, waiting 10s...")
            time.sleep(10)

    # Remove checkpoint after successful completion
    if checkpoint_path and checkpoint_path.exists():
        checkpoint_path.unlink()

    return all_results


def process_submission(sub: dict) -> dict:
    """Extract relevant fields from a PullPush submission."""
    created = int(sub.get("created_utc", 0) or 0)
    dt = datetime.utcfromtimestamp(created) if created else None
    title = sub.get("title", "")
    selftext = sub.get("selftext", "")
    # Clean selftext
    if selftext in ("[removed]", "[deleted]", ""):
        selftext = ""

    return {
        "post_id": sub.get("id", ""),
        "subreddit": sub.get("subreddit", ""),
        "author": sub.get("author", ""),
        "title": title,
        "selftext": selftext,
        "score": sub.get("score", 0),
        "num_comments": sub.get("num_comments", 0),
        "created_utc": created,
        "date": dt.strftime("%Y-%m-%d") if dt else "",
        "year": dt.year if dt else 0,
        "url": sub.get("url", ""),
        "permalink": sub.get("permalink", ""),
    }


def collect_pair(pair: dict, after: str = "2010-01-01", before: str = "2026-01-01") -> pd.DataFrame:
    """Collect all Reddit submissions for both variants of a pair."""
    slug = pair["slug"]
    ru_term = pair["russian"]
    ua_term = pair["ukrainian"]

    after_ts = int(datetime.strptime(after, "%Y-%m-%d").timestamp())
    before_ts = int(datetime.strptime(before, "%Y-%m-%d").timestamp())

    rows = []
    checkpoint_dir = OUT_DIR / ".checkpoints"
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    for variant, term in [("russian", ru_term), ("ukrainian", ua_term)]:
        # Check if this variant's processed data was already checkpointed
        cp_done_path = checkpoint_dir / f"{slug}_{variant}.parquet"
        if cp_done_path.exists():
            cp_df = pd.read_parquet(cp_done_path)
            log.info(f"  [{slug}] '{term}' ({variant}): loaded checkpoint ({len(cp_df):,} rows)")
            rows.extend(cp_df.to_dict("records"))
            continue

        # Fetch with in-flight checkpointing (saves raw JSON every 100 pages)
        fetch_cp_path = checkpoint_dir / f"{slug}_{variant}_fetch.json"
        log.info(f"  [{slug}] Fetching '{term}' ({variant})...")
        subs = fetch_all_submissions(term, after_ts, before_ts, checkpoint_path=fetch_cp_path)
        log.info(f"  [{slug}] '{term}': {len(subs):,} submissions")

        variant_rows = []
        for s in subs:
            row = process_submission(s)
            row["pair_slug"] = slug
            row["variant"] = variant
            row["matched_term"] = term
            variant_rows.append(row)

        # Save processed checkpoint after variant completes
        if variant_rows:
            cp_df = pd.DataFrame(variant_rows)
            cp_df.to_parquet(cp_done_path, index=False)
            log.info(f"  [{slug}] Variant checkpoint: {cp_done_path.name} ({len(cp_df):,} rows)")
            rows.extend(variant_rows)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Deduplicate by post_id (same post can appear in multiple pages)
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["post_id"], keep="first")
    if before_dedup > len(df):
        log.info(f"  [{slug}] Deduped: {before_dedup} -> {len(df)}")

    # Apply homonym filters from pairs.yaml (remove false positives)
    import re as _re
    for p in _pairs_yaml_cache():
        if p["slug"] == slug:
            for filt in p.get("homonym_filters", []):
                pattern = _re.compile(filt, _re.IGNORECASE)
                text_col = df["title"].fillna("") + " " + df["selftext"].fillna("")
                mask = text_col.apply(lambda t: bool(pattern.search(t)))
                if mask.any():
                    df = df[~mask]
                    log.info(f"  [{slug}] Homonym filter '{filt}': removed {mask.sum()} posts")
            break

    # Clean up checkpoints after successful save
    for variant in ["russian", "ukrainian"]:
        cp_path = checkpoint_dir / f"{slug}_{variant}.parquet"
        if cp_path.exists():
            cp_path.unlink()

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", type=str, default=None, help="Single pair slug to fetch")
    parser.add_argument("--batch", type=str, default=None, help="Batch range: '0-15' or '16-31' etc for parallel runs")
    parser.add_argument("--dry-run", action="store_true", help="Just show what would be fetched")
    parser.add_argument("--after", default="2010-01-01", help="Start date")
    parser.add_argument("--before", default="2026-01-01", help="End date")
    parser.add_argument("--gap-fill", action="store_true",
                        help="Append only what is newer than each pair's existing data. "
                             "Reads the existing parquet, resumes from its last date, "
                             "merges and dedups by post_id. Never overwrites blind.")
    args = parser.parse_args()

    pairs = load_pairs()
    if args.pair:
        pairs = [p for p in pairs if p["slug"] == args.pair]
        if not pairs:
            log.error(f"Pair '{args.pair}' not found")
            return
    elif args.batch:
        start, end = map(int, args.batch.split("-"))
        pairs = pairs[start:end+1]
        log.info(f"Batch {args.batch}: {len(pairs)} pairs")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.dry_run:
        for p in pairs:
            log.info(f"Would fetch: {p['slug']} — '{p['russian']}' vs '{p['ukrainian']}'")
        return

    total_all = 0
    for i, pair in enumerate(pairs):
        log.info(f"\n[{i+1}/{len(pairs)}] {pair['slug']}")

        out_path = OUT_DIR / f"{pair['slug']}.parquet"
        existing = None

        if out_path.exists():
            if not args.gap_fill:
                existing = pd.read_parquet(out_path)
                log.info(f"  Already exists: {len(existing):,} rows — skipping (use --gap-fill to extend)")
                total_all += len(existing)
                continue
            # Gap fill: resume from the day after this pair's last record.
            existing = pd.read_parquet(out_path)
            last = str(existing["date"].max())[:10]
            resume = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            if resume >= args.before:
                log.info(f"  Up to date (last {last}) — nothing to fetch")
                total_all += len(existing)
                continue
            log.info(f"  Gap fill: {len(existing):,} existing rows, last {last} -> fetching {resume}..{args.before}")
            after_arg = resume
        else:
            after_arg = args.after

        df = collect_pair(pair, after=after_arg, before=args.before)

        if existing is not None:
            if df.empty:
                log.info(f"  No new results — {len(existing):,} rows unchanged")
                total_all += len(existing)
                continue
            before_n = len(existing)
            merged = pd.concat([existing, df], ignore_index=True)
            merged = merged.drop_duplicates(subset=["post_id"], keep="first")
            added = len(merged) - before_n
            # Never write a file smaller than what was there.
            if len(merged) < before_n:
                log.error(f"  REFUSING to write: merge would shrink {before_n:,} -> {len(merged):,}")
                continue
            merged.to_parquet(out_path, index=False)
            log.info(f"  Saved: {before_n:,} + {added:,} new = {len(merged):,} rows "
                     f"(last now {str(merged['date'].max())[:10]})")
            total_all += len(merged)
            continue

        if df.empty:
            log.info(f"  No results")
            continue

        df.to_parquet(out_path, index=False)
        log.info(f"  Saved: {out_path} ({len(df):,} rows)")
        total_all += len(df)

        # Summary per year
        yearly = df.groupby(["year", "variant"]).size().unstack(fill_value=0)
        log.info(f"  Per year:\n{yearly.to_string()}")

    log.info(f"\nDONE: {total_all:,} total submissions across {len(pairs)} pairs")


if __name__ == "__main__":
    main()
