"""Orchestrate the YouTube census across pairs, years and variants.

    python -m pipeline.build_youtube_census --all --dry-run
    python -m pipeline.build_youtube_census --all
    python -m pipeline.build_youtube_census --pair chornobyl --year-start 2026 --force

The census itself collects ONE (pair, year, variant) per invocation. Driving that by
hand was 24 pairs x 16 years x 2 variants = 768 commands for 2010-2025, which is how
a whole day's collection was produced with a descent policy that had not been
validated, and how a re-run silently did nothing: every month was already marked
resolved in the checkpoint, so the command exited reporting success with 0 search
calls used.

This adds the parts that were missing:

  * a daily search-quota budget, tracked across the whole run and stopped cleanly.
    Calls already ledgered in the current quota day (midnight Pacific) by any
    census, saturate or experiment run come off it first
  * --force, which archives the checkpoint, parquet and ledger for a target before
    recollecting, so a re-run actually re-runs instead of skipping. Archived files
    are moved, never deleted
  * resume by default: a target is skipped once every month the window expects is
    recorded (12 for a full year, January..SERIES_END's month for the final one)
  * depth per pair from config/pairs.yaml youtube_depth; a pair that declares none
    is refused rather than given a default
  * a target the census would refuse to resume (a month collected while open, or
    months at another floor or threshold) is skipped before its ledger is touched
  * jobs run pair by pair in pairs.yaml order, years ascending, and the two variants
    of a year back to back. Pair-major order means a budget that runs out leaves
    whole pairs finished; adjacent variants keep a year's RU and UA queries minutes
    apart, which matters because a month's count depends on how long after it
    closed it was queried

The window comes from pipeline.config (STUDY_START, SERIES_END). Advancing it by a
month is one edit there: the final year then reads incomplete, the census resumes
it, skips every recorded month and queries only the new one.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess
import sys
import time
from datetime import datetime, time as dtime, timezone
from zoneinfo import ZoneInfo

from pipeline.config import SERIES_END, STUDY_START, get_enabled_pairs
from pipeline.ingestion import youtube_window as yw

ROOT = pathlib.Path(__file__).resolve().parent.parent
CENSUS_DIR = ROOT / "data" / "cl" / "raw" / "youtube_census"
CKPT_DIR = CENSUS_DIR / ".checkpoints"
ARCHIVE = ROOT / "data" / "archive" / "youtube_census_superseded"
# Separate-directory census runs (settle-lag, floor comparisons) belong here, so
# the quota-day count below can see them. Their ledgers never bill a real target.
EXPERIMENTS = ROOT / "data" / "cl" / "raw" / "youtube_census_experiments"
KEY_ID = "029b0141-1889-4665-a569-36d75c0f6191"
KEY_PROJECT = "kyivnotkiev-yt"
DAILY_SEARCH_QUOTA = 10_000
# Google resets the per-day quotas at midnight Pacific time.
QUOTA_TZ = ZoneInfo("America/Los_Angeles")
STUDY_START_YEAR = int(STUDY_START[:4])
STUDY_END_YEAR = int(SERIES_END[:4])     # the final year; partial until SERIES_END is a December
VARIANTS = ("russian", "ukrainian")
LADDER = ("month", "week", "day", "hour")


def api_key() -> str:
    r = subprocess.run(["gcloud", "services", "api-keys", "get-key-string", KEY_ID,
                        f"--project={KEY_PROJECT}", "--format=value(keyString)"],
                       capture_output=True, text=True)
    key = r.stdout.strip()
    if not key:
        print("could not retrieve the API key via gcloud", file=sys.stderr)
        raise SystemExit(1)
    return key


def enabled_pairs() -> list[str]:
    return [p["slug"] for p in get_enabled_pairs()]


def declared_depths() -> dict[str, str | None]:
    return {p["slug"]: p.get("youtube_depth") for p in get_enabled_pairs()}


def ckpt_path(pair: str, variant: str, year: int) -> pathlib.Path:
    return CKPT_DIR / f"{pair}_{variant}_{year}.json"


def recorded_months(pair: str, variant: str, year: int) -> dict:
    p = ckpt_path(pair, variant, year)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text()).get("months", {}) or {}
    except Exception:                                  # noqa: BLE001
        return {}


def is_complete(pair: str, variant: str, year: int, through: str = SERIES_END) -> bool:
    """A checkpoint existing is not the same as a year being finished.

    chornobyl 2018 russian stopped at 8 of 12 months when the daily quota ran out and
    exited cleanly. Treating the checkpoint's existence as "collected" would have
    skipped those 4 months on every future run, permanently. A year counts as complete
    only when every month the window expects for it is recorded.
    """
    # A month is only recorded once every window in it was visited, so the expected
    # months recorded means the whole year was explored. Do NOT additionally require
    # every month `resolved`: a window capped at the bottom of the ladder (hour) is a
    # TERMINAL state — the count is a flagged lower bound, not missing work.
    # Requiring resolution re-queued fully-explored targets forever (mykola-hohol
    # 2021/2022 ukrainian re-ran daily at 0 collectable calls, 2026-09-03).
    #
    # The test names the expected months rather than counting to 12. A final year
    # holds only January..SERIES_END's month, and a count cannot tell a recorded
    # September from a recorded December.
    return yw.is_complete_months(recorded_months(pair, variant, year), year, through)


def searches_used(pair: str, variant: str, year: int) -> int:
    """Count actual API calls from the per-call ledger.

    The checkpoint's month entries carry no `searches` field. Reading one returned 0
    for every target, which both raised a false "0 search calls used" warning and left
    the budget permanently at zero, so it could never have stopped the run. The ledger
    writes one line per window.
    """
    # A ledger line is a WINDOW, not an API call: windows paginate, and each page is a
    # separate search.list call. Counting lines undercounted by ~25% (8,005 tracked vs
    # 10,078 actually spent), so the budget never fired and the run continued until the
    # API cut it off mid-year. Sum the `pages` field instead.
    #
    # The ledger is the only source. The run loop rotates it away before every
    # census call and the census creates it as each variant starts, so an absent
    # or empty ledger means no calls. The old fallback, the checkpoint's window
    # count, billed a RESUMED target its whole history whenever the ledger was
    # missing: a dead key on the first call read as 25 calls spent on a
    # 25-window checkpoint, so the dead-key stop below (which needs 0) never
    # fired, and a target the census refused (exit 2, 0 calls) was billed 671.
    led = CENSUS_DIR / ".ledger" / f"{pair}_{variant}_{year}.jsonl"
    if not led.exists():
        return 0
    total = 0
    try:
        for line in led.open():
            try:
                total += json.loads(line).get("pages", 1)
            except Exception:                          # noqa: BLE001
                total += 1
    except OSError:
        pass
    return total


def spent_today(now: datetime | None = None) -> int:
    """Search calls already ledgered in the current quota day, by every tool.

    The run budget used to start at zero on every invocation. A second run on the
    same day, a youtube_saturate pass (which wrote no ledger at all) or a
    separate-directory experiment was invisible to it. Only rows stamped with `at`
    count, so ledgers written before the stamp existed contribute nothing.
    Experiment ledgers are read only under EXPERIMENTS; a --checkpoint-dir elsewhere
    still spends quota that this cannot see.
    """
    now = now or yw.utcnow()
    day = now.astimezone(QUOTA_TZ).date()
    day_start = datetime.combine(day, dtime(0), tzinfo=QUOTA_TZ).timestamp()
    files = [*(CENSUS_DIR / ".ledger").glob("*.jsonl"), *ARCHIVE.glob("*.jsonl"),
             *EXPERIMENTS.glob("**/.ledger/*.jsonl")]
    total = 0
    for f in files:
        try:
            if f.stat().st_mtime < day_start:    # shutil.move keeps mtime, so this holds
                continue                         # for ledgers archived today as well
            for line in f.open():
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                at = r.get("at")
                if at and datetime.strptime(at, "%Y-%m-%dT%H:%M:%SZ").replace(
                        tzinfo=timezone.utc).astimezone(QUOTA_TZ).date() == day:
                    total += int(r.get("pages", 0))
        except OSError:
            continue
    return total


def resume_refusals(pair: str, variant: str, year: int, depth: str) -> list[str]:
    """Why the census will refuse to resume this target (see youtube_census:
    resume_problems): a month collected while open, or months recorded at another
    floor or descent threshold. Checked before the run loop touches the target,
    because the loop rotates the target's ledger first: run blind, chornobyl
    russian 2026 lost its 909-page ledger to the archive as "stale", which is the
    settle-lag experiment's baseline, and then failed with exit 2."""
    from pipeline.ingestion.youtube_census import DESCEND_ABOVE, resume_problems
    p = ckpt_path(pair, variant, year)
    if not p.exists():
        return []
    try:
        state = json.loads(p.read_text())
    except Exception:                                  # noqa: BLE001
        return []
    return resume_problems(state, datetime.fromtimestamp(p.stat().st_mtime, timezone.utc),
                           depth, DESCEND_ABOVE)


def archive_target(pair: str, variant: str, year: int) -> None:
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    # One stamp for the target's files, so youtube_settle_compare finds an archived
    # checkpoint's ledger beside it (<ts>_<stem>.json / <ts>_<stem>.jsonl). A stamp
    # per file split them whenever the second ticked between two moves.
    ts = int(time.time())
    # The ledger must be archived too, or searches_used() counts calls from the run
    # being replaced and the budget is wrong from the first target onward.
    for src in (ckpt_path(pair, variant, year),
                CENSUS_DIR / f"{pair}_{variant}_{year}.parquet",
                CENSUS_DIR / ".ledger" / f"{pair}_{variant}_{year}.jsonl"):
        if src.exists():
            shutil.move(str(src), str(ARCHIVE / f"{ts}_{src.name}"))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pair")
    g.add_argument("--pairs", help="comma-separated")
    g.add_argument("--all", action="store_true")
    ap.add_argument("--year-start", type=int, default=STUDY_START_YEAR)
    # The final year is SERIES_END's. Its months beyond SERIES_END are never queried:
    # the census refuses any month that has not closed, and a recorded month is
    # frozen. chornobyl 2026 shows the failure this replaces: seven months of russian
    # and an open August, against a ukrainian half that was never queried.
    ap.add_argument("--year-end", type=int, default=STUDY_END_YEAR)
    ap.add_argument("--through", default=SERIES_END,
                    help=f"last month to collect (default SERIES_END {SERIES_END}); "
                         "must have closed. Lower it to hold back a month that has "
                         "closed but not settled.")
    ap.add_argument("--budget", type=int, default=DAILY_SEARCH_QUOTA,
                    help="search calls for this run; stops cleanly when reached")
    ap.add_argument("--force", action="store_true",
                    help="archive existing checkpoint+parquet+ledger so targets actually recollect")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)

    enabled = enabled_pairs()
    if a.pair:
        targets = [a.pair]
    elif a.pairs:
        targets = [x.strip() for x in a.pairs.split(",") if x.strip()]
    else:
        targets = enabled
    bad = [t for t in targets if t not in enabled]
    if bad:
        print(f"not enabled: {bad}", file=sys.stderr)
        return 1

    now = yw.utcnow()
    try:
        yw.validate_through(a.through, now)
        if a.year_end > int(a.through[:4]):
            raise yw.WindowError(f"--year-end {a.year_end} is past --through {a.through}")
        if a.year_start < STUDY_START_YEAR or a.year_start > a.year_end:
            raise yw.WindowError(f"--year-start {a.year_start} is outside "
                                 f"{STUDY_START_YEAR}..{a.year_end}")
    except yw.WindowError as e:
        print(f"REFUSED: {e}", file=sys.stderr)
        return 1

    depths = declared_depths()
    undeclared = [t for t in targets if depths.get(t) not in LADDER]

    years = list(range(a.year_start, a.year_end + 1))
    jobs = [(p, v, y) for p in targets for y in years for v in VARIANTS]
    pending = [j for j in jobs if a.force or not is_complete(j[0], j[1], j[2], a.through)]
    already = len(jobs) - len(pending)
    today = spent_today(now)
    run_cap = max(0, min(a.budget, DAILY_SEARCH_QUOTA - today))

    print(f"{len(targets)} pair(s) x {len(years)} years x 2 variants = {len(jobs)} targets"
          f"   (through {a.through}; last closed month {yw.last_closed_month(now)})")
    print(f"  already collected: {already}   pending: {len(pending)}")
    print(f"  budget {a.budget:,}; already ledgered this quota day {today:,}; "
          f"run cap {run_cap:,} search calls"
          + ("   [FORCE: existing data will be archived]" if a.force else ""))
    # Targets the census would refuse are skipped, never run: running one cost
    # nothing but rotated its ledger into the archive first.
    refusals = {}
    if not a.force:
        for p, v, y in pending:
            if depths.get(p) not in LADDER:
                continue
            why = resume_refusals(p, v, y, depths[p])
            if why:
                refusals[(p, v, y)] = why
                print(f"  WILL BE REFUSED: {p} {y} {v} {'; '.join(why)}. Rerun it with "
                      f"--force (archives, never deletes)", file=sys.stderr)
    if undeclared:
        print(f"REFUSED: no youtube_depth in config/pairs.yaml for {', '.join(undeclared)}. "
              f"Declare the floor each pair's history used; one default applied to every "
              f"pair is how a run would collect at a floor its history never used.",
              file=sys.stderr)
    if a.dry_run:
        for p, v, y in pending[:60]:
            todo = [m for m in yw.expected_months(y, a.through)
                    if a.force or m not in recorded_months(p, v, y)]
            span = f"{todo[0]}..{todo[-1]} ({len(todo)})" if todo else "none"
            print(f"    {p} {y} {v}  depth {depths.get(p) or 'UNDECLARED'}  months {span}")
        if len(pending) > 60:
            print(f"    ... and {len(pending)-60} more")
        return 1 if (undeclared or refusals) else 0
    if undeclared:
        return 1

    key = api_key()
    spent, done, failed, refused, ran = 0, 0, [], [], set()
    t0 = time.time()
    for i, (pair, variant, year) in enumerate(pending, 1):
        if spent >= run_cap:
            print(f"\nbudget reached ({spent:,} searches) — stopping cleanly with "
                  f"{len(pending)-i+1} target(s) left")
            break
        if (pair, variant, year) in refusals:
            refused.append((pair, year, variant))
            print(f"[{i}/{len(pending)}] {pair} {year} {variant}  REFUSED, not run, ledger "
                  f"left in place: {'; '.join(refusals[(pair, variant, year)])}")
            continue
        ran.add((pair, year))
        if a.force:
            archive_target(pair, variant, year)
        # Rotate any ledger a prior run left behind: searches_used() sums the whole
        # file, so stale rows get billed to TODAY's budget. Measured 2026-09-03:
        # 1,610 stale pages made the chain stop with 22% of real quota unspent.
        led = CENSUS_DIR / ".ledger" / f"{pair}_{variant}_{year}.jsonl"
        if led.exists():
            ARCHIVE.mkdir(parents=True, exist_ok=True)
            shutil.move(str(led), str(ARCHIVE / f"{int(time.time())}_stale_{led.name}"))
        print(f"[{i}/{len(pending)}] {pair} {year} {variant}  depth {depths[pair]}  "
              f"(spent {spent:,}/{run_cap:,})", flush=True)
        r = subprocess.run([sys.executable, "-u", "-m", "pipeline.ingestion.youtube_census",
                            "--pair", pair, "--year", str(year), "--variant", variant,
                            "--min-depth", depths[pair], "--through", a.through,
                            "--api-key", key, "--max-searches", str(run_cap - spent)],
                           capture_output=True, text=True, cwd=ROOT)
        used = searches_used(pair, variant, year)
        spent += used
        if r.returncode != 0:
            failed.append((pair, year, variant))
            print(f"    FAILED: {(r.stderr or r.stdout).strip().splitlines()[-1][:300]}")
        elif used == 0:
            if is_complete(pair, variant, year, a.through):
                # raced to completion by a parallel/prior run — genuinely done
                done += 1
                print("    0 search calls used — target already complete")
            else:
                # 0 calls AND an incomplete checkpoint: the key is dead for the
                # day (census exits cleanly on quotaExceeded). Marching on would
                # zero through every remaining target and let the chain enrich
                # and DEPLOY a half-collected pair (seen 2026-09-03, kyivan-rus).
                print(f"    0 calls, checkpoint incomplete — API key dead for the day")
                print(f"\nstopping cleanly with {len(pending)-i+1} target(s) left")
                break
        else:
            done += 1
            print(f"    {used:,} searches")

    print(f"\n{done} target(s) collected, {spent:,} searches, {(time.time()-t0)/60:.0f} min")
    if failed:
        print(f"failed: {failed}")
    if refused:
        print(f"refused (not run): {refused}")
    # The enrich step names the collected years. Without --years it rebuilds every
    # year of the pair, which re-fetches stored rows lacking metadata and meets the
    # 3,550 historical orphans (2,298 verified on chornobyl, 2010-2024), both of
    # which move the shipped 2010-2025 series.
    for pair in dict.fromkeys(p for p, _ in sorted(ran, key=lambda t: targets.index(t[0]))):
        ys = ",".join(str(y) for y in sorted({y for p, y in ran if p == pair}))
        print(f"next: python -m pipeline.ingestion.youtube_enrich --pair {pair} --incremental "
              f"--years {ys} --dry-run   (then without --dry-run, then pipeline.rebuild)")
    return 0 if not (failed or refused) else 1


if __name__ == "__main__":
    raise SystemExit(main())
