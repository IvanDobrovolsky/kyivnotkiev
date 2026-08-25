"""YouTube adaptive fetch — drills down from month to week to day when results hit cap.

For each pair and variant:
1. Search per month (2015-2025)
2. If a month returns 500+ results → split into weeks
3. If a week returns 500+ results → split into days
4. Dedup by video_id across all windows

Usage:
    python -m pipeline.ingestion.youtube_adaptive_fetch --pair chornobyl --api-keys key1,key2,...
"""

import argparse
import logging
import re
import time
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
OUT_DIR = ROOT / "data" / "cl" / "raw" / "youtube_adaptive"

API_URL = "https://www.googleapis.com/youtube/v3"
RESULT_CEILING = 500   # YouTube serves at most ~500 results per query, ever
MIN_WINDOW_SECONDS = 60       # floor for bisection
MAX_WINDOW_SECONDS = 31*86400  # MEASURED: YouTube under-serves windows wider than a
                               # month. A 2022 yearly query returned 23 videos where the
                               # same year sliced monthly returned 175 (87% only visible
                               # when split), and identical queries return different sets
                               # run to run. So never *search* a window wider than this —
                               # subdivide first. "No ceiling hit" does NOT mean complete.
MAX_PAGES = 20           # safety bound only — real pagination ends at nextPageToken

# Key rotation
_keys = []
_key_idx = 0
_units = 0
_dead_keys = set()
_searches = 0          # search.list calls — THE binding quota dimension (10,000/day)
_max_searches = None   # daily search-call budget; None = unlimited
_passes = 3            # repeated traversals; API non-determinism means one pass misses a lot

# Search Queries per minute is capped at 100. A window that paginates 10 pages
# fires 10 calls in a couple of seconds, so the average rate is not a safe
# guide — enforce a sliding-window ceiling on every individual call.
SEARCHES_PER_MINUTE = 80  # headroom under the 100/min ceiling; Google's window may not align with ours
_search_times = deque()
_max_units = None      # unit budget; searches cost 0 units, so this is a backstop only
_year_start = 2010
_year_end = 2025


def next_key():
    global _key_idx
    # Skip dead keys
    attempts = 0
    while attempts < len(_keys):
        key = _keys[_key_idx % len(_keys)]
        _key_idx += 1
        if key not in _dead_keys:
            return key
        attempts += 1
    return None  # all keys dead


def all_keys_dead():
    return len(_dead_keys) >= len(_keys)


def budget_exhausted():
    """Search calls are the real limit (10,000/day, measured — see memory).
    Units are a backstop for videos.list, which searches do not consume."""
    if _max_searches is not None and _searches >= _max_searches:
        return True
    return _max_units is not None and _units >= _max_units


def should_stop():
    """Stop signal: keys exhausted OR unit budget spent."""
    return all_keys_dead() or budget_exhausted()


class Exhausted(Exception):
    """Raised when keys/budget run out mid-window, so no interval is falsely
    marked complete. Caught at the top of adaptive_search, which checkpoints."""


def track(n):
    global _units
    _units += n


def track_search():
    global _searches
    _searches += 1


def rate_limit():
    """Block until another search call fits under the 100/min ceiling."""
    now = time.time()
    while _search_times and now - _search_times[0] > 60:
        _search_times.popleft()
    if len(_search_times) >= SEARCHES_PER_MINUTE:
        wait = 60 - (now - _search_times[0]) + 0.05
        if wait > 0:
            log.info(f"  Rate limit: pausing {wait:.1f}s (100 searches/min ceiling)")
            time.sleep(wait)
            while _search_times and time.time() - _search_times[0] > 60:
                _search_times.popleft()
    _search_times.append(time.time())


def search_window(query, after, before, max_pages=MAX_PAGES):
    """Search one time window, paginating until YouTube stops serving pages.

    Returns (results, truncated). truncated=True means the window hit the ~500
    ceiling and provably holds more than the API will show — the caller must
    split it into narrower windows. Raises Exhausted if keys/budget run out.
    """
    results = []
    page_token = None

    if should_stop():
        raise Exhausted()

    page = 0
    backoff = 0
    while page < max_pages:
        if should_stop():
            raise Exhausted()
        key = next_key()
        if key is None:
            raise Exhausted()

        # Wrap multi-word queries in quotes for exact phrase matching
        # Without quotes, YouTube treats "Vladimir the Great" as Vladimir OR Great
        q = f'"{query}"' if ' ' in query else query
        params = {
            "part": "snippet", "q": q, "type": "video",
            "maxResults": 50, "relevanceLanguage": "en",
            "publishedAfter": after, "publishedBefore": before,
            "key": key,
        }
        if page_token:
            params["pageToken"] = page_token

        for _attempt in range(3):
            try:
                rate_limit()
                resp = requests.get(f"{API_URL}/search", params=params, timeout=30)
                break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if _attempt < 2:
                    log.warning(f"  Network error (attempt {_attempt+1}/3): {e.__class__.__name__}")
                    time.sleep(5)
                else:
                    log.warning(f"  Network error after 3 attempts — skipping window")
                    resp = None
        if resp is None:
            continue
        track(100)
        track_search()

        if resp.status_code in (403, 429):
            reasons = set()
            try:
                reasons = {e.get("reason") for e in resp.json().get("error", {}).get("errors", [])}
            except ValueError:
                pass

            # A per-minute rate limit is TRANSIENT. Treating it as a dead key
            # aborts the whole run on a single key — that is what killed the
            # first census attempt. Back off and retry the same page instead.
            if resp.status_code == 429 or "rateLimitExceeded" in reasons or "userRateLimitExceeded" in reasons:
                backoff += 1
                if backoff > 8:
                    log.error("  Rate limited 8x in a row — giving up on this window")
                    raise Exhausted()
                wait = min(120, 15 * backoff)
                log.warning(f"  Rate limited (per-minute) — backing off {wait}s, retry {backoff}/8")
                time.sleep(wait)
                _search_times.clear()
                continue

            # Genuine daily exhaustion — this key really is done for the day.
            _dead_keys.add(key)
            log.warning(f"  Key exhausted for the day ({sorted(reasons)}) "
                        f"({len(_dead_keys)}/{len(_keys)} dead)")
            if should_stop():
                raise Exhausted()
            continue
        if resp.status_code != 200:
            log.warning(f"  HTTP {resp.status_code} — abandoning window")
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

        page += 1
        backoff = 0
        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.1)

    truncated = len(results) >= RESULT_CEILING or bool(page_token)
    return results, truncated


def adaptive_search(query, year_start, year_end, checkpoint_dir=None, slug=None, passes=1):
    """Census a time range by recursive bisection.

    Starts with the whole range as one query. Any window that comes back at
    YouTube's ~500-result ceiling provably hides more results, so it is split in
    half and each half searched again — recursing to MIN_WINDOW_SECONDS. Sparse
    pairs therefore cost a handful of calls; only dense stretches pay for depth.

    Results from a parent window are always kept, not discarded when it splits.
    """
    import json as _json

    all_videos = {}
    completed = set()
    pass_start = 0

    cp_path = None
    if checkpoint_dir and slug:
        cp_path = Path(checkpoint_dir) / f"{slug}.json"
        if cp_path.exists():
            with open(cp_path) as f:
                cp = _json.load(f)
            for v in cp.get("videos", []):
                all_videos[v["video_id"]] = v
            completed = set(cp.get("completed_intervals", []))
            pass_start = cp.get("pass", 0)
            log.info(f"    Resumed: {len(all_videos):,} videos, pass {pass_start}, "
                     f"{len(completed)} intervals done")

    ledger = []

    def _iso(t):
        return t.strftime("%Y-%m-%dT%H:%M:%SZ")

    def _key(a, b):
        return f"{_iso(a)}|{_iso(b)}"

    current_pass = [pass_start]

    def _save():
        if not cp_path:
            return
        with open(cp_path, "w") as f:
            _json.dump({
                "videos": list(all_videos.values()),
                "completed_intervals": sorted(completed),
                "pass": current_pass[0],
            }, f)

    def visit(a, b, depth):
        k = _key(a, b)
        if k in completed:
            return
        span = (b - a).total_seconds()

        # Windows wider than a month are split without being searched — querying
        # them wastes a call and returns a small, arbitrary subset either way.
        if span > MAX_WINDOW_SECONDS:
            mid = a + timedelta(seconds=span / 2)
            visit(a, mid, depth + 1)
            visit(mid, b, depth + 1)
            completed.add(k)
            if depth <= 3:
                _save()
            return

        results, truncated = search_window(query, _iso(a), _iso(b))

        # Keep everything this window returned, whether or not it splits.
        new = 0
        for r in results:
            if r["video_id"] not in all_videos:
                new += 1
            all_videos[r["video_id"]] = r

        ledger.append({
            "slug": slug, "start": _iso(a), "end": _iso(b),
            "span_seconds": span, "depth": depth,
            "results": len(results), "new": new,
            "truncated": truncated, "units_after": _units,
        })

        if truncated:
            if span <= MIN_WINDOW_SECONDS:
                log.error(
                    f"    IRREDUCIBLE: {_iso(a)}..{_iso(b)} ({span:.0f}s) still at "
                    f"ceiling with {len(results)} results — census incomplete here"
                )
            else:
                mid = a + timedelta(seconds=span / 2)
                if mid <= a or mid >= b:
                    log.error(f"    Cannot split {_iso(a)}..{_iso(b)} further")
                else:
                    log.info(
                        f"    {_iso(a)[:10]}..{_iso(b)[:10]} ({span/86400:.1f}d): "
                        f"{len(results)} at ceiling → splitting"
                    )
                    visit(a, mid, depth + 1)
                    visit(mid, b, depth + 1)
        else:
            log.info(
                f"    {_iso(a)[:10]}..{_iso(b)[:10]} ({span/86400:.1f}d): "
                f"{len(results)} results, {new} new ({len(all_videos):,} total, {_units} units)"
            )

        completed.add(k)
        if depth <= 3:
            _save()

    start_dt = datetime(year_start, 1, 1)
    end_dt = datetime(year_end, 12, 31, 23, 59, 59)

    # Repeated passes: identical queries return different subsets run to run, so
    # each pass recovers videos the last one missed. Stop when a pass adds
    # nothing new — that is saturation, the only honest completeness signal here.
    try:
        for pnum in range(pass_start, passes):
            current_pass[0] = pnum
            before = len(all_videos)
            if pnum > pass_start or not completed:
                pass  # fresh traversal below
            visit(start_dt, end_dt, 0)
            gained = len(all_videos) - before
            log.info(f"    Pass {pnum + 1}/{passes}: +{gained} new "
                     f"({len(all_videos):,} total, {_searches} searches)")
            completed.clear()
            current_pass[0] = pnum + 1
            _save()
            if gained == 0 and pnum > pass_start:
                log.info(f"    Saturated after pass {pnum + 1} — no new videos")
                break
    except Exhausted:
        why = f"search budget spent ({_searches}/{_max_searches})" if budget_exhausted() else "all keys dead"
        log.warning(f"    Stopped early ({why}, {_searches} searches) — progress checkpointed")

    _save()

    if ledger and slug:
        led_dir = OUT_DIR / ".ledger"
        led_dir.mkdir(parents=True, exist_ok=True)
        with open(led_dir / f"{slug}.jsonl", "a") as f:
            for row in ledger:
                f.write(_json.dumps(row) + "\n")
        calls = sum(1 for r in ledger)
        split = sum(1 for r in ledger if r["truncated"])
        log.info(f"    Ledger: {calls} windows searched, {split} hit ceiling and split")

    return all_videos



def get_video_details(video_ids):
    """Batch metadata. 1 unit per 50."""
    details = {}
    if should_stop():
        log.warning("  Stopping — skipping metadata fetch")
        return details
    i = 0
    while i < len(video_ids):
        batch = video_ids[i:i + 50]
        key = next_key()
        if key is None:
            log.warning(f"  Keys exhausted at batch {i // 50} — got metadata for {len(details)}/{len(video_ids)}")
            break
        resp = requests.get(f"{API_URL}/videos", params={
            "part": "snippet", "id": ",".join(batch), "key": key,
        }, timeout=15)
        track(1)
        if resp.status_code in (403, 429):
            reasons = set()
            try:
                reasons = {e.get("reason") for e in resp.json().get("error", {}).get("errors", [])}
            except ValueError:
                pass
            if resp.status_code == 429 or "rateLimitExceeded" in reasons or "userRateLimitExceeded" in reasons:
                log.warning("  Metadata rate limited — backing off 20s and retrying batch")
                time.sleep(20)
                continue
            _dead_keys.add(key)
            log.warning(f"  Key exhausted during metadata fetch ({sorted(reasons)})")
            if should_stop():
                break
            continue
        if resp.status_code == 200:
            for item in resp.json().get("items", []):
                snippet = item.get("snippet", {})
                details[item["id"]] = {
                    "description": snippet.get("description", ""),
                    "channel_id": snippet.get("channelId", ""),
                }
        i += 50
        time.sleep(0.1)
    return details


def collect_pair(pair):
    slug = pair["slug"]
    ru_term = pair["russian"]
    ua_term = pair["ukrainian"]

    out_path = OUT_DIR / f"{slug}.parquet"

    # Load existing data to merge with (resume support)
    all_videos = {}
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        for _, r in existing.iterrows():
            all_videos[r['video_id']] = {
                'video_id': r['video_id'], 'title': r['title'],
                'channel': r['channel'], 'published_at': r['published_at'],
                'search_variant': r['variant'], 'matched_term': r['matched_term'],
            }
        log.info(f"  Loaded {len(all_videos):,} existing videos — will merge new")

    variant_pairs = [("russian", ru_term), ("ukrainian", ua_term)]
    if hasattr(collect_pair, '_only_variant') and collect_pair._only_variant:
        variant_pairs = [(v, t) for v, t in variant_pairs if v == collect_pair._only_variant]

    cp_dir = OUT_DIR / ".checkpoints"
    cp_dir.mkdir(parents=True, exist_ok=True)

    for variant, term in variant_pairs:
        log.info(f"  [{slug}] Adaptive search: '{term}' ({variant})")
        videos = adaptive_search(term, _year_start, _year_end, checkpoint_dir=cp_dir,
                                 slug=f"{slug}_{variant}", passes=_passes)
        for vid, info in videos.items():
            if vid not in all_videos:
                info["search_variant"] = variant
                info["matched_term"] = term
                all_videos[vid] = info
        log.info(f"  [{slug}] '{term}': {len(videos)} videos ({len(all_videos)} total unique, {_units} units)")

    if not all_videos:
        return pd.DataFrame()

    def _build_df(all_videos, details=None):
        if details is None:
            details = {}
        # Word-boundary match so "Kievan" does not count as a "Kiev" mention.
        ru_re = re.compile(rf"\b{re.escape(ru_term)}\b", re.IGNORECASE)
        ua_re = re.compile(rf"\b{re.escape(ua_term)}\b", re.IGNORECASE)

        rows = []
        for vid, info in all_videos.items():
            detail = details.get(vid, {})
            title = info["title"]
            desc = detail.get("description", "")

            # A mention is the variant appearing in title OR description.
            ru_t, ua_t = bool(ru_re.search(title)), bool(ua_re.search(title))
            ru_d, ua_d = bool(ru_re.search(desc)), bool(ua_re.search(desc))
            has_ru, has_ua = ru_t or ru_d, ua_t or ua_d

            if has_ru and has_ua:
                variant = "both"
            elif has_ua:
                variant = "ukrainian"
            elif has_ru:
                variant = "russian"
            else:
                # Search matched on tags/other signals; the term is not in the
                # text we hold. Keep it, but flag it — filtering is a later,
                # explicit decision, not a silent drop here.
                variant = info["search_variant"]

            rows.append({
                "video_id": vid, "title": title, "channel": info["channel"],
                "channel_id": detail.get("channel_id", ""),
                "published_at": info["published_at"],
                "date": info["published_at"][:10],
                "variant": variant, "matched_term": info["matched_term"],
                "search_variant": info["search_variant"],
                "in_title": ru_t or ua_t,
                "in_description": ru_d or ua_d,
                "verified": has_ru or has_ua,
                "has_description": bool(desc),
                "pair_slug": slug, "has_transcript": False,
                "text": (title + "\n\n" + desc)[:2000],
                "text_len": min(len(title + "\n\n" + desc), 2000),
            })
        return pd.DataFrame(rows)

    # SAVE SEARCH RESULTS FIRST — never lose data waiting for metadata
    df = _build_df(all_videos)
    df.to_parquet(out_path, index=False)
    log.info(f"  [{slug}] SAVED search results: {len(df)} videos")

    # Now try metadata (enrichment, not critical)
    video_ids = list(all_videos.keys())
    log.info(f"  [{slug}] Fetching metadata for {len(video_ids)} videos...")
    details = get_video_details(video_ids)

    if details:
        df = _build_df(all_videos, details)
        df.to_parquet(out_path, index=False)
        log.info(f"  [{slug}] UPDATED with metadata: {len(details)}/{len(video_ids)} enriched")

    log.info(f"  [{slug}] FINAL: {len(df)} videos, {_units} units used")
    log.info(f"  Variants: {df['variant'].value_counts().to_dict()}")
    if "verified" in df.columns:
        v = int(df["verified"].sum())
        log.info(f"  Verified (term in title or description): {v}/{len(df)} "
                 f"({100*v/max(len(df),1):.1f}%) | in_title={int(df['in_title'].sum())} "
                 f"in_desc={int(df['in_description'].sum())} "
                 f"no_desc_fetched={int((~df['has_description']).sum())}")
    df['year'] = df['date'].str[:4]
    for yr, sub in df.groupby('year'):
        ua = (sub['variant'] == 'ukrainian').sum()
        ru = (sub['variant'] == 'russian').sum()
        log.info(f"    {yr}: {len(sub)} (UA={ua}, RU={ru})")

    # NOW safe to clean up checkpoints — parquet is saved
    import glob as _glob
    for cp_file in _glob.glob(str(cp_dir / f"{slug}_*_checkpoint.json")):
        Path(cp_file).unlink()
        log.info(f"  Checkpoint cleaned: {Path(cp_file).name}")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", type=str, required=True, help="Slug, or comma-separated slugs, fetched in order")
    parser.add_argument("--api-keys", type=str, required=True, help="Comma-separated API keys")
    parser.add_argument("--variant", type=str, default=None, choices=["russian", "ukrainian"], help="Only fetch one variant")
    parser.add_argument("--year-start", type=int, default=2010)
    parser.add_argument("--year-end", type=int, default=2025)
    parser.add_argument("--passes", type=int, default=3,
                        help="Repeat traversals until saturation (API returns different subsets each run)")
    parser.add_argument("--max-searches", type=int, default=10000,
                        help="Stop after this many search.list calls (daily quota is 10,000)")
    parser.add_argument("--max-units", type=int, default=None,
                        help="Backstop unit budget for videos.list; searches cost 0 units")
    args = parser.parse_args()

    global _keys, _max_units, _max_searches, _passes, _year_start, _year_end
    _keys = args.api_keys.split(",")
    _max_units = args.max_units
    _max_searches = args.max_searches
    _passes = args.passes
    _year_start, _year_end = args.year_start, args.year_end

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    wanted = [s.strip() for s in args.pair.split(",") if s.strip()]
    by_slug = {p["slug"]: p for p in cfg["pairs"] if p.get("enabled", True)}
    missing = [w for w in wanted if w not in by_slug]
    if missing:
        log.error(f"Pair(s) not found: {', '.join(missing)}")
        return
    pairs = [by_slug[w] for w in wanted]

    collect_pair._only_variant = args.variant

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"Range {_year_start}-{_year_end} | budget {_max_searches or 'unlimited'} searches | {len(pairs)} pair(s)")
    for pair in pairs:
        if should_stop():
            log.warning(f"Budget/keys exhausted — skipping remaining: {', '.join(p['slug'] for p in pairs[pairs.index(pair):])}")
            break
        collect_pair(pair)
    log.info(f"\nTotal: {_searches} search calls, {_units} units")


if __name__ == "__main__":
    main()
