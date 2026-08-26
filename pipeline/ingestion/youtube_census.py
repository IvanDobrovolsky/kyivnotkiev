"""Month-by-month YouTube collection with recursive halving to escape the result cap.

DESIGN
------
The unit of measurement is the CALENDAR MONTH, because that is the unit the
adoption chart displays. Everything below a month is an implementation detail
that disappears on aggregation.

For each (pair, variant, month):

  1. Query the whole month.
  2. If the query came back at YouTube's ~500-result ceiling, it is truncated —
     the month holds more than the API will show. Split the window in half and
     query each half.
  3. Recurse only into halves that are themselves truncated. A half that comes
     back clean is kept as-is and never touched again.
  4. Accumulate every video found at any depth, dedup by video_id, and report
     one count for the month.

A month is marked `resolved` when no window inside it ended truncated. Only
resolved months carry a trustworthy count; unresolved ones are recorded but
flagged, never silently mixed in.

MEASURED CONSTRAINTS (not assumptions)
--------------------------------------
- `maxResults` is capped at 50 by the API, so one call returns at most 50 videos.
- A single query stops yielding `nextPageToken` at roughly 500 results.
- `search.list` costs 1 against "Search Queries per day" (10,000) and 0 against
  the 110,000 unit pool. Verified via Cloud Monitoring.
- "Search Queries per minute" is 100. Exceeding it returns HTTP 429
  `rateLimitExceeded`, which is TRANSIENT — it must never be treated as a dead
  key. Doing so aborted an earlier run mid-pass.

CHECKPOINTING
-------------
State is flushed after every single window, via atomic replace. Killing the
process at any moment loses at most the window in flight. Resuming skips every
window already recorded.

Usage:
    python -m pipeline.ingestion.youtube_census --pair volodymyr-the-great \
        --year 2010 --api-key "$KEY" --max-searches 2000
"""

import argparse
import json
import logging
import os
import time
from calendar import monthrange
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
OUT_DIR = ROOT / "data" / "cl" / "raw" / "youtube_census"
API = "https://www.googleapis.com/youtube/v3"

PAGE_SIZE = 50                # API maximum per request
MAX_PAGES = 15                # safety bound on pagination

# TRUNCATION SIGNAL — measured, and neither the token nor a fixed threshold works:
#   "Vladimir the Great" 2010-02 returned 50 (one FULL page) then an empty page 2
#      with no token. Looked exhausted. Splitting into weeks recovered 170 — 3.4x.
#   "Kiev" 2022-03 returned 338; the last page held 38, not a full 50. Splitting
#      into days returned FEWER unique videos (280) for 4x the calls.
# So the signal is whether the last non-empty page came back FULL. A full final
# page means the API stopped early and more exists; a partial one means it ran out.
LADDER = ["month", "week", "day", "hour"]   # 'hour' is terminal
SEARCHES_PER_MINUTE = 80      # headroom under the 100/min ceiling


class Budget:
    """Tracks search calls and enforces the per-minute ceiling."""

    def __init__(self, max_searches: int | None):
        self.max_searches = max_searches
        self.searches = 0
        self.units = 0
        self._times: deque[float] = deque()

    @property
    def exhausted(self) -> bool:
        return self.max_searches is not None and self.searches >= self.max_searches

    def wait_for_slot(self) -> None:
        now = time.time()
        while self._times and now - self._times[0] > 60:
            self._times.popleft()
        if len(self._times) >= SEARCHES_PER_MINUTE:
            wait = 60 - (now - self._times[0]) + 0.05
            if wait > 0:
                log.info(f"    rate limit: pausing {wait:.1f}s")
                time.sleep(wait)
                while self._times and time.time() - self._times[0] > 60:
                    self._times.popleft()
        self._times.append(time.time())


class Exhausted(Exception):
    """Budget spent or key dead for the day — stop cleanly, keep the checkpoint."""


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def search_window(term, start, end, key, budget, ledger):
    """One time window, paginated to exhaustion.

    Returns (videos, truncated). truncated=True means the window hit the result
    ceiling and provably holds more than the API will serve.
    """
    q = f'"{term}"' if " " in term else term
    videos, token, pages, backoff = {}, None, 0, 0
    last_page_full = False

    while pages < MAX_PAGES:
        if budget.exhausted:
            raise Exhausted()
        params = {
            "part": "snippet", "q": q, "type": "video",
            "maxResults": PAGE_SIZE, "relevanceLanguage": "en",
            "publishedAfter": iso(start), "publishedBefore": iso(end),
            "key": key,
        }
        if token:
            params["pageToken"] = token

        budget.wait_for_slot()
        try:
            resp = requests.get(f"{API}/search", params=params, timeout=30)
        except (requests.Timeout, requests.ConnectionError) as e:
            backoff += 1
            if backoff > 4:
                log.error(f"    network failed 4x on {iso(start)} — abandoning window")
                break
            log.warning(f"    network {e.__class__.__name__}, retry {backoff}/4")
            time.sleep(5 * backoff)
            continue

        budget.searches += 1

        if resp.status_code in (403, 429):
            reasons = set()
            try:
                reasons = {e.get("reason") for e in resp.json().get("error", {}).get("errors", [])}
            except ValueError:
                pass
            transient = resp.status_code == 429 or reasons & {"rateLimitExceeded", "userRateLimitExceeded"}
            if transient:
                backoff += 1
                if backoff > 8:
                    raise Exhausted()
                wait = min(120, 15 * backoff)
                log.warning(f"    429 rate limited — backing off {wait}s ({backoff}/8)")
                time.sleep(wait)
                budget._times.clear()
                continue
            log.error(f"    quota exhausted for the day: {sorted(reasons)}")
            raise Exhausted()

        if resp.status_code != 200:
            log.warning(f"    HTTP {resp.status_code} — abandoning window")
            break

        data = resp.json()
        for item in data.get("items", []):
            vid = item.get("id", {}).get("videoId")
            if not vid:
                continue
            sn = item["snippet"]
            videos[vid] = {
                "video_id": vid, "title": sn.get("title", ""),
                "channel": sn.get("channelTitle", ""),
                "channel_id": sn.get("channelId", ""),
                "published_at": sn.get("publishedAt", ""),
            }
        n_items = len(data.get("items", []))
        if n_items:
            last_page_full = n_items >= PAGE_SIZE
        pages += 1
        backoff = 0
        token = data.get("nextPageToken")
        if not token or not n_items:
            break
        time.sleep(0.05)

    # Capped if the API stopped on a full page, or if our own page bound cut it off.
    capped = last_page_full or (pages >= MAX_PAGES and bool(token))
    ledger.append({
        "start": iso(start), "end": iso(end),
        "span_days": round((end - start).total_seconds() / 86400, 4),
        "results": len(videos), "pages": pages, "capped": capped,
        "searches_after": budget.searches,
    })
    return videos, capped


def subwindows(a, b, level):
    """Split [a,b] into the next finer calendar unit. Calendar-aligned rather
    than binary-halved: boundaries stay meaningful and roll up cleanly."""
    step = {"month": timedelta(days=7), "week": timedelta(days=1),
            "day": timedelta(hours=1)}[level]
    cur = a
    while cur <= b:
        nxt = min(cur + step - timedelta(seconds=1), b)
        yield cur, nxt
        cur = nxt + timedelta(seconds=1)


def collect_month(term, year, month, key, budget, ledger,
                  done_windows, split_windows, accum):
    """Collect one calendar month, descending the ladder only where capped.

    month -> week -> day -> hour. A window is descended ONLY if its last
    non-empty page came back full, which is the measured signal that the API
    withheld results. Uncapped windows are kept as-is: descending them costs
    calls and returns fewer unique videos (measured on "Kiev" 2022-03).

    Results from every level are accumulated, including from a window that was
    later split — a parent routinely holds a few videos its children never
    return (2 and 5 in the two months measured).

    Two window sets support resume:
      done_windows  — terminal windows, nothing left to do
      split_windows — capped windows whose children carry the work; re-entered
                      on resume WITHOUT being re-queried

    Returns True if no window ended capped at the bottom of the ladder.
    """
    last_day = monthrange(year, month)[1]
    start = datetime(year, month, 1)
    end = datetime(year, month, last_day, 23, 59, 59)
    resolved = [True]

    def visit(a, b, level):
        wkey = f"{iso(a)}|{iso(b)}"
        if wkey in done_windows:
            return
        if wkey in split_windows:
            for ca, cb in subwindows(a, b, level):
                visit(ca, cb, LADDER[LADDER.index(level) + 1])
            return

        videos, capped = search_window(term, a, b, key, budget, ledger)
        new = sum(1 for v in videos if v not in accum)
        accum.update(videos)          # keep parent results regardless

        if capped and level != LADDER[-1]:
            nxt = LADDER[LADDER.index(level) + 1]
            log.info(f"    {iso(a)[:13]}..{iso(b)[:13]} [{level}]: "
                     f"{len(videos)} CAPPED -> descending to {nxt}")
            split_windows.add(wkey)
            collect_month.checkpoint_cb()
            for ca, cb in subwindows(a, b, level):
                visit(ca, cb, nxt)
            return

        if capped:
            resolved[0] = False
            log.error(f"    {iso(a)}..{iso(b)} still capped at "
                      f"{LADDER[-1]} level — cannot descend further")
        else:
            log.info(f"    {iso(a)[:13]}..{iso(b)[:13]} [{level}]: "
                     f"{len(videos)} results, {new} new "
                     f"(month {len(accum)}, {budget.searches} searches)")
        done_windows.add(wkey)
        collect_month.checkpoint_cb()

    visit(start, end, "month")
    return resolved[0]


def save_atomic(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w") as f:
        json.dump(payload, f)
    os.replace(tmp, path)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--variant", choices=["russian", "ukrainian"], default=None,
                    help="default: both, queried over identical windows")
    ap.add_argument("--api-key", required=True)
    ap.add_argument("--max-searches", type=int, default=2000)
    args = ap.parse_args()

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    pair = next((p for p in cfg["pairs"] if p["slug"] == args.pair), None)
    if pair is None:
        log.error(f"pair '{args.pair}' not found")
        return

    variants = [args.variant] if args.variant else ["russian", "ukrainian"]
    budget = Budget(args.max_searches)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / ".checkpoints").mkdir(exist_ok=True)
    (OUT_DIR / ".ledger").mkdir(exist_ok=True)

    log.info(f"pair={args.pair} year={args.year} variants={variants} "
             f"budget={args.max_searches} searches")

    summary = []
    for variant in variants:
        term = pair[variant]
        stem = f"{args.pair}_{variant}_{args.year}"
        cp_path = OUT_DIR / ".checkpoints" / f"{stem}.json"
        led_path = OUT_DIR / ".ledger" / f"{stem}.jsonl"

        state = {"months": {}, "done_windows": [], "split_windows": [],
                 "videos": {}, "current_month": None, "current_videos": {}}
        if cp_path.exists():
            state = json.loads(cp_path.read_text())
            log.info(f"  [{variant}] resumed: {len(state['videos'])} videos, "
                     f"{len(state['months'])} months done"
                     + (f", mid-month {state['current_month']} with "
                        f"{len(state['current_videos'])} videos"
                        if state.get("current_month") else ""))

        done_windows = set(state["done_windows"])
        split_windows = set(state["split_windows"])
        ledger = []

        def flush(month_key, accum):
            save_atomic(cp_path, {
                "months": state["months"],
                "done_windows": sorted(done_windows),
                "split_windows": sorted(split_windows),
                "videos": state["videos"],
                "current_month": month_key,
                "current_videos": accum,
            })

        log.info(f"  [{variant}] '{term}'")
        stopped = False
        for month in range(1, 13):
            mkey = f"{args.year}-{month:02d}"
            if mkey in state["months"]:
                continue

            # restore partial progress if this month was interrupted
            accum = dict(state["current_videos"]) if state.get("current_month") == mkey else {}
            if accum:
                log.info(f"  [{variant}] {mkey}: restoring {len(accum)} videos from checkpoint")

            collect_month.checkpoint_cb = lambda: flush(mkey, accum)
            try:
                resolved = collect_month(term, args.year, month, args.api_key,
                                         budget, ledger, done_windows,
                                         split_windows, accum)
            except Exhausted:
                log.warning(f"  [{variant}] stopped in {mkey} "
                            f"({budget.searches} searches) — checkpointed, "
                            f"{len(accum)} videos held for this month")
                flush(mkey, accum)
                stopped = True
                break

            state["videos"].update(accum)
            state["months"][mkey] = {"count": len(accum), "resolved": resolved}
            state["current_month"] = None
            state["current_videos"] = {}
            flush(None, {})
            flag = "" if resolved else "  [NOT RESOLVED]"
            log.info(f"  [{variant}] {mkey}: {len(accum)} videos{flag}")

        with open(led_path, "a") as f:
            for row in ledger:
                f.write(json.dumps(row) + "\n")

        all_videos = {**state["videos"], **state.get("current_videos", {})}
        if all_videos:
            df = pd.DataFrame(list(all_videos.values()))
            df["pair_slug"] = args.pair
            df["variant"] = variant
            df["matched_term"] = term
            df.to_parquet(OUT_DIR / f"{stem}.parquet", index=False)
            log.info(f"  [{variant}] saved {len(df)} videos -> {stem}.parquet")

        for mkey, m in sorted(state["months"].items()):
            summary.append({"month": mkey, "variant": variant, **m})
        if stopped:
            break

    if summary:
        s = pd.DataFrame(summary)
        piv = s.pivot_table(index="month", columns="variant", values="count", fill_value=0)
        res = s.pivot_table(index="month", columns="variant", values="resolved",
                            aggfunc="min", fill_value=True)
        log.info("\n" + "=" * 62)
        log.info(f"{'month':<10}{'RU':>8}{'UA':>8}{'UA share':>11}   resolved")
        log.info("-" * 62)
        for mkey in piv.index:
            ru = int(piv.loc[mkey].get("russian", 0))
            ua = int(piv.loc[mkey].get("ukrainian", 0))
            tot = ru + ua
            share = f"{100*ua/tot:.1f}%" if tot else "—"
            ok = all(bool(v) for v in res.loc[mkey].values) if mkey in res.index else True
            log.info(f"{mkey:<10}{ru:>8}{ua:>8}{share:>11}   {'yes' if ok else 'NO'}")
        log.info("=" * 62)
    log.info(f"\nTotal: {budget.searches} search calls used")


if __name__ == "__main__":
    main()
