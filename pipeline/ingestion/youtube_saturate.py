"""Drain hour-capped census windows with a date-ordered cursor.

The census escapes YouTube's ~500-results-per-query ceiling by splitting
windows month -> week -> day -> hour. An hour that STILL ends on a full page
holds more matching videos than one query will serve, and the ladder has
nowhere lower to go — those months were flagged unresolved and excluded.

One extraction mode remains that has no per-query ceiling: `order=date`
with a moving `publishedBefore` cursor. Each pass returns the newest <=500
videos in the window; the cursor then steps back to the oldest timestamp
seen and pulls the next slice. When a slice ends on a PARTIAL page, the
window is provably drained under date ordering, and the month can resolve.

Bulk uploads can share one timestamp to the second. If the cursor stops
moving, the window is recorded as still-capped rather than looping.

Every call is ledgered to the target's census ledger, one row per cursor pass
with tool="saturate", so the orchestrator's quota-day count sees it. Saturation
used to write no ledger at all, and the calls it spent never reached any budget.

Usage:
    python -m pipeline.ingestion.youtube_saturate --pair mykola-hohol \
        --api-key "$KEY" [--year 2026] [--max-searches 1500] [--dry-run]
"""

import argparse
import json
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
import yaml

from pipeline.ingestion import youtube_window as yw
from pipeline.ingestion.youtube_census import (
    API, OUT_DIR, CONFIG_PATH, PAGE_SIZE, Budget, Exhausted, Ledger, iso, save_atomic,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

MAX_CURSOR_STEPS = 30          # per window; 30 x ~10 pages ≈ 300 calls worst case


def parse_iso(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def drain_window(term, start, end, key, budget, ledger=None):
    """Date-ordered cursor scan of one window. Returns (videos, drained)."""
    q = f'"{term}"' if " " in term else term
    videos = {}
    cursor = end

    def record(pass_start, pass_end, n, pages, capped, incomplete=False):
        if ledger is not None and pages:
            row = {
                "start": iso(pass_start), "end": iso(pass_end),
                "span_days": round((pass_end - pass_start).total_seconds() / 86400, 4),
                "results": n, "pages": pages, "capped": capped,
                "searches_after": budget.searches, "order": "date",
            }
            if incomplete:
                row["incomplete"] = True
            ledger.append(row)

    for _ in range(MAX_CURSOR_STEPS):
        token, pages, last_full, oldest = None, 0, False, None
        pass_ids: set = set()        # ids this pass returned: the ledger's `results`
        # Billed calls in this pass, ledgered in `finally` however the pass ends.
        # The record used to be written only on a clean end or on Exhausted, so a
        # crash or KeyboardInterrupt mid-pass lost every call it had charged.
        charged, finished = 0, False
        try:
            while pages < 12:
                if budget.exhausted:
                    raise Exhausted()
                params = {
                    "part": "snippet", "q": q, "type": "video",
                    "maxResults": PAGE_SIZE, "relevanceLanguage": "en",
                    "order": "date",
                    "publishedAfter": iso(start), "publishedBefore": iso(cursor),
                    "key": key,
                }
                if token:
                    params["pageToken"] = token
                budget.wait_for_slot()
                try:
                    resp = requests.get(f"{API}/search", params=params, timeout=30)
                except (requests.Timeout, requests.ConnectionError):
                    time.sleep(10)
                    continue
                if resp.status_code == 200:
                    budget.searches += 1
                    charged += 1
                else:
                    budget.rejected += 1
                if resp.status_code in (403, 429):
                    reasons = set()
                    try:
                        reasons = {e.get("reason") for e in
                                   resp.json().get("error", {}).get("errors", [])}
                    except ValueError:
                        pass
                    if resp.status_code == 429 or reasons & {"rateLimitExceeded",
                                                             "userRateLimitExceeded"}:
                        time.sleep(30)
                        continue
                    raise Exhausted()
                if resp.status_code != 200:
                    break
                data = resp.json()
                items = data.get("items", [])
                for item in items:
                    vid = item.get("id", {}).get("videoId")
                    if not vid:
                        continue
                    sn = item["snippet"]
                    pass_ids.add(vid)
                    videos[vid] = {
                        "video_id": vid, "title": sn.get("title", ""),
                        "channel": sn.get("channelTitle", ""),
                        "channel_id": sn.get("channelId", ""),
                        "published_at": sn.get("publishedAt", ""),
                    }
                    pa = sn.get("publishedAt", "")
                    if pa:
                        try:
                            t = datetime.strptime(pa[:19] + "Z", "%Y-%m-%dT%H:%M:%SZ")
                            if oldest is None or t < oldest:
                                oldest = t
                        except ValueError:
                            pass
                if items:
                    last_full = len(items) >= PAGE_SIZE
                pages += 1
                token = data.get("nextPageToken")
                if not token or not items:
                    break
                time.sleep(0.05)
            finished = True
        finally:
            if finished:
                record(start, cursor, len(pass_ids), charged, last_full)
            else:
                record(start, cursor, len(pass_ids), charged, True, incomplete=True)
        if not last_full:
            return videos, True            # partial final page: drained
        if oldest is None or oldest >= cursor:
            log.warning(f"    cursor stalled at {iso(cursor)} — window still capped")
            return videos, False           # same-second bulk wall: cannot advance
        cursor = oldest
        if cursor <= start:
            return videos, True
    log.warning(f"    {MAX_CURSOR_STEPS} cursor steps exhausted — window still capped")
    return videos, False


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    ap.add_argument("--api-key", required=True)
    ap.add_argument("--max-searches", type=int, default=1500)
    ap.add_argument("--year", type=int, default=None,
                    help="only this year's checkpoints (default: every year of the pair)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args(argv)

    cfg = yaml.safe_load(open(CONFIG_PATH))
    pair = next((p for p in cfg["pairs"] if p["slug"] == args.pair), None)
    if pair is None:
        log.error(f"pair '{args.pair}' not found")
        return 1

    budget = Budget(args.max_searches)
    total_new, resolved_months, still_capped = 0, 0, 0

    # Exact stems only: the old `<pair>_*.json` glob parsed the variant out of
    # whatever matched, and read every year with no window check.
    stem_re = re.compile(rf"^{re.escape(args.pair)}_(russian|ukrainian)_(\d{{4}})$")
    for cp_path in sorted((OUT_DIR / ".checkpoints").glob(f"{args.pair}_*.json")):
        stem = cp_path.stem
        m = stem_re.match(stem)
        if not m:
            continue
        variant, year = m.group(1), int(m.group(2))
        if args.year is not None and year != args.year:
            continue
        term = pair.get(variant)
        if not term:
            continue
        state = json.loads(cp_path.read_text())
        ledger = Ledger(OUT_DIR / ".ledger" / f"{stem}.jsonl", tool="saturate")
        unresolved = {k: m for k, m in state.get("months", {}).items()
                      if not m.get("resolved")}
        if not unresolved:
            continue

        # hour-level done windows inside each unresolved month are the only
        # candidates: terminal caps live there by construction
        done = state.get("done_windows", [])
        changed = False
        for mkey in sorted(unresolved):
            wins = []
            for w in done:
                a, b = w.split("|")
                if a[:7] == mkey and (parse_iso(b) - parse_iso(a)) <= timedelta(hours=1, seconds=1):
                    wins.append((parse_iso(a), parse_iso(b)))
            if args.dry_run:
                log.info(f"{stem} {mkey}: {len(wins)} hour window(s) to drain")
                continue
            if not wins:
                log.warning(f"{stem} {mkey}: unresolved but no hour windows — skipping")
                continue
            log.info(f"{stem} {mkey}: draining {len(wins)} hour window(s)")
            all_ok = True
            for a, b in wins:
                try:
                    vids, drained = drain_window(term, a, b, args.api_key, budget, ledger)
                except Exhausted:
                    log.error("budget/key exhausted — stopping cleanly")
                    save_atomic(cp_path, state)
                    return 0
                new = {k: v for k, v in vids.items() if k not in state["videos"]}
                state["videos"].update(new)
                total_new += len(new)
                if new:
                    log.info(f"    {iso(a)[:13]}: +{len(new)} new video(s), "
                             f"drained={drained}")
                all_ok = all_ok and drained
            month_count = sum(1 for v in state["videos"].values()
                              if str(v.get("published_at", ""))[:7] == mkey)
            # Keep the census stamps (collected_at, min_depth, descend_above):
            # rewriting the record whole erased them.
            state["months"][mkey] = {**state["months"][mkey], "count": month_count,
                                     "resolved": all_ok, "saturated": True,
                                     "saturated_at": iso(yw.utcnow())}
            resolved_months += int(all_ok)
            still_capped += int(not all_ok)
            changed = True

        if changed and not args.dry_run:
            save_atomic(cp_path, state)
            df = pd.DataFrame(list(state["videos"].values()))
            df["pair_slug"] = args.pair
            df["variant"] = variant
            df["matched_term"] = term
            df.to_parquet(OUT_DIR / f"{stem}.parquet", index=False)
            log.info(f"{stem}: checkpoint + parquet rewritten "
                     f"({len(df)} videos)")

    log.info(f"\nSaturation done: +{total_new} videos, {resolved_months} month(s) "
             f"resolved, {still_capped} still capped, "
             f"{budget.searches} calls ({budget.rejected} rejected)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
