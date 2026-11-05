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

A recorded month is never revisited, so a month must be CLOSED before it is
recorded. A run queries only months <= min(--through, the last closed month);
--through defaults to pipeline.config.SERIES_END, and a --through that has not
closed is refused (exit 2). Each recorded month carries collected_at,
min_depth and descend_above. The checkpoint carries a top-level min_depth
(read by the exporter's depth check) once every recorded month agrees on one.

DEPTH comes from the pair's youtube_depth in config/pairs.yaml. A --min-depth
that contradicts it is refused for the real census directory: counts track
window width, so a year collected at another floor is not comparable with the
pair's own history. A --descend-above other than DESCEND_ABOVE is refused there
for the same reason, and so is resuming a checkpoint whose months record another
floor or threshold.

LEDGER: one row per window, written as the window ends, including a window the
run stops inside (flagged "incomplete"). The file exists from the start of each
variant, so a variant that spent nothing leaves an empty ledger, not none.

SEPARATE DIRECTORY (--checkpoint-dir, with --months): re-collects chosen months
into a directory of its own, with its own checkpoints, ledger and parquets, so
the real census, its budget ledger and every glob over it stay untouched. Ledger
rows written there are flagged "experiment": true.

Usage:
    python -m pipeline.ingestion.youtube_census --pair volodymyr-the-great \
        --year 2010 --api-key "$KEY" --max-searches 2000
    python -m pipeline.ingestion.youtube_census --pair chornobyl --variant russian \
        --year 2026 --months 01..07 \
        --checkpoint-dir data/cl/raw/youtube_census_experiments/settle_2026-10-19 \
        --api-key "$KEY" --max-searches 1500
"""

import argparse
import json
import logging
import os
import sys
import time
from calendar import monthrange
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml

from pipeline.config import SERIES_END
from pipeline.ingestion import youtube_window as yw

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

# DESCENT POLICY — the cap signal alone is NOT sufficient.
#
# Descending only when the last non-empty page came back full leaves months with
# wildly different sampling depth, because YouTube stops serving well before it
# signals truncation. Measured on chornobyl: 2013-04 returned 579 videos in 2
# windows with no cap flag, while 2022-02 — the month Russian forces seized the
# plant — returned 333 in 2 windows, also unflagged. April 2013 out-collected
# February 2022. Across 180 months the coefficient of variation was 0.33, i.e.
# nearly flat, which is impossible for real content volume and proves the counts
# were tracking sampling depth rather than content.
#
# Two additional triggers, either of which forces a descent:
#   DESCEND_ABOVE  a window returning at least this many results is presumed
#                  under-served even if it did not signal a cap
#   --min-depth    a uniform floor: always descend to this level regardless of
#                  what any window returns, so every month gets equal treatment
DESCEND_ABOVE = 150
SEARCHES_PER_MINUTE = 80      # headroom under the 100/min ceiling

# set from CLI in main(); see the DESCENT POLICY note above
_min_depth = "month"
_descend_above = DESCEND_ABOVE


class Budget:
    """Tracks search calls and enforces the per-minute ceiling."""

    def __init__(self, max_searches: int | None):
        self.max_searches = max_searches
        self.searches = 0
        self.rejected = 0
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


class Ledger:
    """Append-only call log, one row per window, written as the window finishes.

    The orchestrator bills its budget from this file (sum of `pages`). Rows used
    to be held in memory and written only after the whole variant finished, so
    any exception other than Exhausted (KeyboardInterrupt, a JSON decode error)
    dropped the record of calls the API had already charged. A window the run
    stops inside is ledgered too, with the pages charged so far and
    "incomplete": true.

    `at` (UTC) makes a quota day reconstructible from ledgers alone. `tool`
    separates census rows from youtube_saturate rows. `experiment` marks rows
    written to a separate --checkpoint-dir, which must never be billed to a
    real target.

    create=True makes the file exist from the start of a variant, so a variant
    that spends nothing leaves an empty ledger: the orchestrator reads "absent or
    empty" as 0 calls, which is what its dead-key stop keys on. Saturate leaves
    it False so a pass over 802 checkpoints does not litter .ledger/ with empty
    files.
    """

    def __init__(self, path: Path, tool: str = "census", experiment: bool = False,
                 create: bool = False):
        self.path, self.tool, self.experiment = path, tool, experiment
        self.rows: list[dict] = []
        if create:
            path.touch(exist_ok=True)

    def append(self, row: dict) -> None:
        row = {**row, "at": iso(yw.utcnow()), "tool": self.tool}
        if self.experiment:
            row["experiment"] = True
        with open(self.path, "a") as f:
            f.write(json.dumps(row) + "\n")
        self.rows.append(row)


def iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_utc(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def search_window(term, start, end, key, budget, ledger):
    """One time window, paginated to exhaustion.

    Returns (videos, truncated). truncated=True means the window hit the result
    ceiling and provably holds more than the API will serve.
    """
    q = f'"{term}"' if " " in term else term
    videos, token, pages, backoff = {}, None, 0, 0
    last_page_full = False
    # `charged` counts accepted (billed) calls in this window; `pages` counts
    # pages parsed. They differ only when the run stops between the two, and the
    # ledger must carry what was billed. It used to be written only after
    # pagination finished, so a window the run stopped inside (budget spent,
    # quota 403, 429 x8, KeyboardInterrupt) lost up to MAX_PAGES - 1 = 14
    # billed calls per stop, invisible to the quota-day count.
    charged, finished = 0, False

    try:
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

            # Only accepted calls consume the daily quota; 429/403 rejects are free.
            # Counting every response made the budget self-stop early on 429 storms.
            if resp.status_code == 200:
                budget.searches += 1
                charged += 1
            else:
                budget.rejected += 1

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
                    # Keep the sliding window: clearing it here let the client
                    # re-burst at full speed and provoke the next 429 immediately.
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
        finished = True
    finally:
        # Capped if the API stopped on a full page, or if our own page bound cut it off.
        capped = last_page_full or (pages >= MAX_PAGES and bool(token))
        if finished or charged:
            row = {
                "start": iso(start), "end": iso(end),
                "span_days": round((end - start).total_seconds() / 86400, 4),
                "results": len(videos), "pages": charged, "capped": capped,
                "searches_after": budget.searches,
            }
            if not finished:
                # Not in done_windows: a resume re-queries it, and these calls
                # are billed again then. Both spends are real.
                row["incomplete"] = True
            ledger.append(row)
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

        li = LADDER.index(level)
        below_floor = li < LADDER.index(_min_depth)
        dense = len(videos) >= _descend_above
        should_descend = capped or below_floor or dense

        if should_descend and level != LADDER[-1]:
            nxt = LADDER[li + 1]
            why = "CAPPED" if capped else ("floor" if below_floor else f">={_descend_above}")
            log.info(f"    {iso(a)[:13]}..{iso(b)[:13]} [{level}]: "
                     f"{len(videos)} {why} -> descending to {nxt}")
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


def months_collected_while_open(state: dict, written_at: datetime) -> list[str]:
    """Months in a checkpoint that provably hold data queried before they closed.

    A stamped month carries collected_at. A legacy month carries nothing, so the
    file's mtime stands in: every window in the file was queried at or before
    its last write, so an mtime before a month's end proves the month was open.
    The test cannot return a false positive. chornobyl_russian_2026 holds such a
    month: August was in flight when the file was last written at
    2026-08-28T21:39Z, and day/hour windows exist only for Aug 1-9.
    """
    bad = []
    for mkey, m in state.get("months", {}).items():
        at = m.get("collected_at")
        if (parse_utc(at) if at else written_at) <= yw.month_end(mkey):
            bad.append(mkey)
    cm = state.get("current_month")
    if cm and written_at <= yw.month_end(cm):
        windows = list(state.get("done_windows", [])) + list(state.get("split_windows", []))
        if state.get("current_videos") or any(w.startswith(cm) for w in windows):
            bad.append(cm)
    return sorted(set(bad))


def recorded_depths(state: dict) -> set:
    """Floors this checkpoint already records. Legacy months record none."""
    out = {m.get("min_depth") for m in state.get("months", {}).values()} - {None}
    if state.get("min_depth"):
        out.add(state["min_depth"])
    return out


def recorded_descend_above(state: dict) -> set:
    """Density thresholds this checkpoint already records. Legacy months record none."""
    return {m.get("descend_above") for m in state.get("months", {}).values()} - {None}


def resume_problems(state: dict, written_at: datetime, min_depth: str,
                    descend_above: int) -> list[str]:
    """Why this checkpoint must not be resumed by a run at these settings.

    One function for the census (which refuses) and the orchestrator (which skips
    the target before rotating its ledger, and names it in a dry run). The
    orchestrator used to learn of a refusal only from the census's exit 2, after
    it had already moved the target's ledger to the archive as stale.
    """
    out = []
    opened = months_collected_while_open(state, written_at)
    if opened:
        out.append(f"holds {', '.join(opened)} collected before the month closed "
                   f"(last written {iso(written_at)})")
    depths = recorded_depths(state)
    if depths and depths != {min_depth}:
        out.append(f"records min_depth {sorted(depths)}; this run is {min_depth}. "
                   f"One year must not mix floors")
    thresholds = recorded_descend_above(state)
    if thresholds and thresholds != {descend_above}:
        out.append(f"records descend_above {sorted(thresholds)}; this run is "
                   f"{descend_above}. One year must not mix descent thresholds")
    return out


def resolve_descend_above(requested: int | None, separate_dir: bool) -> int:
    """DESCEND_ABOVE, unless a separate directory measures another value.

    The threshold changes window structure just as the floor does: a window at or
    above it is split whatever it signalled. Every census run in data/audit/*.log
    (64 headers) used 150, and the orchestrator never passed another value, so
    the real census holds no year collected at any other threshold.
    """
    if requested is None or requested == DESCEND_ABOVE:
        return DESCEND_ABOVE
    if not separate_dir:
        raise yw.WindowError(
            f"--descend-above {requested} differs from {DESCEND_ABOVE}, which every "
            f"year in the real census was collected at. Use a separate "
            f"--checkpoint-dir to measure another threshold")
    return requested


def resolve_depth(pair: dict, requested: str | None, separate_dir: bool, year: int) -> str:
    """The floor for this run: the pair's declared youtube_depth, raised for
    `year` by pipeline.config.YOUTUBE_FLOOR_FROM_YEAR (yw.declared_depth).

    Counts track window width: "Kyiv" on 2022-02-24 returned 8 ids in 6 hours,
    30 in a day, 115 in a week and 188 in a month, against 19 for an ordinary day
    in 2013 (measured 2026-08-27). A year collected at a different floor from the
    pair's own history is therefore not comparable with it. zaporizhzhia 2010,
    re-run at week floor, read 1,607 hits where the month floor had read 1,097 in
    the 10 months the month floor left undivided (1.010x in the other 14).

    The orchestrator used to apply one --min-depth, default 'day', to every pair
    in a run. 22 of the 24 enabled pairs were collected at week floor and
    zaporizhzhia at month floor, and no checkpoint recorded its depth, so nothing
    would have noticed. A separate --checkpoint-dir may override the declaration:
    measuring what another floor does is the point of such a run.
    """
    declared = yw.declared_depth(pair, year)
    if declared is not None and declared not in LADDER:
        raise yw.WindowError(f"{pair['slug']}: youtube_depth {declared!r} is not one of "
                             f"{LADDER}; the census cannot collect at it")
    if requested is None:
        if declared is None:
            raise yw.WindowError(
                f"{pair['slug']} declares no youtube_depth in config/pairs.yaml. Declare "
                f"the floor its history used before collecting, or pass --min-depth "
                f"with a separate --checkpoint-dir")
        return declared
    if declared is not None and requested != declared and not separate_dir:
        raise yw.WindowError(
            f"--min-depth {requested} contradicts {pair['slug']}'s declared youtube_depth "
            f"{declared}; a year at another floor is not comparable with its history. "
            f"Use a separate --checkpoint-dir to measure the difference")
    if declared is None and not separate_dir:
        raise yw.WindowError(
            f"{pair['slug']} declares no youtube_depth in config/pairs.yaml; the real "
            f"census will not guess it from --min-depth. Declare it first")
    return requested


def resolve_checkpoint_root(arg: str | None) -> tuple[Path, bool]:
    """(root, is_separate). A separate root may not sit inside the real census
    directory: the exporter, enrich, migrate and green_check all glob it."""
    if arg is None:
        return OUT_DIR, False
    root = Path(arg).expanduser()
    if not root.is_absolute():
        root = ROOT / root
    root = root.resolve()
    real = OUT_DIR.resolve()
    if root == real:
        return OUT_DIR, False
    if real in root.parents:
        raise yw.WindowError(f"--checkpoint-dir {root} is inside the real census "
                             f"directory {real}; choose a directory outside it")
    return root, True


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--variant", choices=["russian", "ukrainian"], default=None,
                    help="default: both, queried over identical windows")
    ap.add_argument("--api-key", required=True)
    ap.add_argument("--max-searches", type=int, default=2000)
    ap.add_argument("--min-depth", choices=LADDER, default=None,
                    help="Uniform descent floor. Default: the pair's youtube_depth in "
                         "config/pairs.yaml. 'month' is the old adaptive behaviour; "
                         "'week'/'day' force every month to equal depth so months are "
                         "comparable (costs roughly 5x / 30x).")
    ap.add_argument("--descend-above", type=int, default=None,
                    help=f"Descend any window returning at least this many results even "
                         f"if it did not signal a cap. The API under-serves before "
                         f"signalling, so the cap alone is insufficient. Default "
                         f"{DESCEND_ABOVE}; another value needs a separate --checkpoint-dir.")
    ap.add_argument("--through", default=SERIES_END,
                    help=f"last month to collect, YYYY-MM (default SERIES_END {SERIES_END}). "
                         "Refused unless it has closed.")
    ap.add_argument("--months", default=None,
                    help="re-collect only these months of --year: '01..07', '03,09' or "
                         "'2026-07'. Requires a separate --checkpoint-dir.")
    ap.add_argument("--checkpoint-dir", default=None,
                    help="write checkpoints, ledger and parquets under this directory "
                         "instead of the real census directory")
    args = ap.parse_args(argv)

    global _min_depth, _descend_above

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    pair = next((p for p in cfg["pairs"] if p["slug"] == args.pair), None)
    if pair is None:
        log.error(f"pair '{args.pair}' not found")
        return 2

    now = yw.utcnow()
    try:
        root, separate = resolve_checkpoint_root(args.checkpoint_dir)
        if args.months and not separate:
            raise yw.WindowError("--months re-collects into a separate --checkpoint-dir "
                                 "only; it never edits the real checkpoints")
        schedule = yw.collectable_months(args.year, args.through, now, args.months)
        _min_depth = resolve_depth(pair, args.min_depth, separate, args.year)
        _descend_above = resolve_descend_above(args.descend_above, separate)
    except yw.WindowError as e:
        log.error(f"REFUSED: {e}")
        return 2

    variants = [args.variant] if args.variant else ["russian", "ukrainian"]
    ck_dir, led_dir = root / ".checkpoints", root / ".ledger"

    # Every refusal is decided before the first call: a refusal on the second
    # variant after the first had spent its calls would leave the pair's two
    # halves from different runs.
    states, problems = {}, []
    for variant in variants:
        stem = f"{args.pair}_{variant}_{args.year}"
        cp_path = ck_dir / f"{stem}.json"
        state = {"months": {}, "done_windows": [], "split_windows": [],
                 "videos": {}, "current_month": None, "current_videos": {}}
        if cp_path.exists():
            state = json.loads(cp_path.read_text())
            written = datetime.fromtimestamp(cp_path.stat().st_mtime, timezone.utc)
            for p in resume_problems(state, written, _min_depth, _descend_above):
                problems.append(
                    f"{cp_path.name} {p}. Archive it and recollect: "
                    f"python -m pipeline.build_youtube_census --pair {args.pair} "
                    f"--year-start {args.year} --year-end {args.year} --force")
        states[variant] = state
    if problems:
        for p in problems:
            log.error(f"REFUSED: {p}")
        return 2

    budget = Budget(args.max_searches)
    root.mkdir(parents=True, exist_ok=True)
    ck_dir.mkdir(exist_ok=True)
    led_dir.mkdir(exist_ok=True)

    log.info(f"pair={args.pair} year={args.year} variants={variants} "
             f"budget={args.max_searches} searches | min-depth={_min_depth} "
             f"descend-above={_descend_above} | months {schedule[0]}..{schedule[-1]} "
             f"({len(schedule)}) | last closed {yw.last_closed_month(now)}"
             + (f" | SEPARATE DIR {root}" if separate else ""))

    summary = []
    for variant in variants:
        term = pair[variant]
        stem = f"{args.pair}_{variant}_{args.year}"
        cp_path = ck_dir / f"{stem}.json"
        ledger = Ledger(led_dir / f"{stem}.jsonl", experiment=separate, create=True)
        state = states[variant]
        if cp_path.exists():
            log.info(f"  [{variant}] resumed: {len(state['videos'])} videos, "
                     f"{len(state['months'])} months done"
                     + (f", mid-month {state['current_month']} with "
                        f"{len(state['current_videos'])} videos"
                        if state.get("current_month") else ""))

        done_windows = set(state["done_windows"])
        split_windows = set(state["split_windows"])

        def flush(month_key, accum):
            payload = {
                "months": state["months"],
                "done_windows": sorted(done_windows),
                "split_windows": sorted(split_windows),
                "videos": state["videos"],
                "current_month": month_key,
                "current_videos": accum,
            }
            # Top-level min_depth is what export_site_data._stale_youtube_years
            # reads; 0 of 802 checkpoints carried it, so its depth check never
            # fired. Claim it only when every recorded month says so: a legacy
            # month's floor is unknown and must not be asserted.
            depths = {m.get("min_depth") for m in state["months"].values()}
            if state["months"] and len(depths) == 1 and None not in depths:
                payload["min_depth"] = depths.pop()
            save_atomic(cp_path, payload)

        log.info(f"  [{variant}] '{term}'")
        stopped = False
        for mkey in schedule:
            if mkey in state["months"]:
                continue
            month = int(mkey[5:7])

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
            state["months"][mkey] = {"count": len(accum), "resolved": resolved,
                                     "collected_at": iso(yw.utcnow()),
                                     "min_depth": _min_depth,
                                     "descend_above": _descend_above}
            state["current_month"] = None
            state["current_videos"] = {}
            flush(None, {})
            flag = "" if resolved else "  [NOT RESOLVED]"
            log.info(f"  [{variant}] {mkey}: {len(accum)} videos{flag}")

        all_videos = {**state["videos"], **state.get("current_videos", {})}
        if all_videos:
            df = pd.DataFrame(list(all_videos.values()))
            df["pair_slug"] = args.pair
            df["variant"] = variant
            df["matched_term"] = term
            df.to_parquet(root / f"{stem}.parquet", index=False)
            log.info(f"  [{variant}] saved {len(df)} videos -> {root / stem}.parquet")

        for mkey, m in sorted(state["months"].items()):
            summary.append({"month": mkey, "variant": variant,
                            "count": m["count"], "resolved": m["resolved"]})
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
    log.info(f"\nTotal: {budget.searches} search calls used, {budget.rejected} rejected (not charged)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
