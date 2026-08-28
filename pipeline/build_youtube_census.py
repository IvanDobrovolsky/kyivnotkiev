"""Orchestrate the YouTube census across pairs, years and variants.

    python -m pipeline.build_youtube_census --all --min-depth day
    python -m pipeline.build_youtube_census --pair chornobyl --force
    python -m pipeline.build_youtube_census --all --dry-run

The census itself collects ONE (pair, year, variant) per invocation. Driving that by
hand is 24 pairs x 16 years x 2 variants = 768 commands, which is how a whole day's
collection was produced with a descent policy that had not been validated, and how a
re-run silently did nothing: every month was already marked resolved in the
checkpoint, so the command exited reporting success with 0 search calls used.

This adds the parts that were missing:

  * a daily search-quota budget, tracked across the whole run and stopped cleanly
  * --force, which archives the checkpoint and parquet for a target before recollecting,
    so a re-run actually re-runs instead of skipping
  * resume by default: targets that already have a checkpoint are skipped
  * ordering by pair size, smallest first, so a budget that runs out leaves whole pairs
    finished rather than every pair half-done
"""
from __future__ import annotations

import argparse
import json
import pathlib
import shutil
import subprocess
import sys
import time

import yaml

CENSUS_DIR = pathlib.Path("data/cl/raw/youtube_census")
CKPT_DIR = CENSUS_DIR / ".checkpoints"
ARCHIVE = pathlib.Path("data/archive/youtube_census_superseded")
KEY_ID = "029b0141-1889-4665-a569-36d75c0f6191"
KEY_PROJECT = "kyivnotkiev-yt"
DAILY_SEARCH_QUOTA = 10_000
VARIANTS = ("russian", "ukrainian")


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
    doc = yaml.safe_load(pathlib.Path("config/pairs.yaml").read_text())
    pairs = doc["pairs"] if isinstance(doc, dict) and "pairs" in doc else doc
    return [p["slug"] for p in pairs if p.get("enabled")]


def ckpt_path(pair: str, variant: str, year: int) -> pathlib.Path:
    return CKPT_DIR / f"{pair}_{variant}_{year}.json"


def searches_used(pair: str, variant: str, year: int) -> int:
    """Count actual API calls from the per-call ledger.

    The checkpoint's month entries carry only `count` and `resolved` -- there is no
    `searches` field. Reading one returned 0 for every target, which both raised a
    false "0 search calls used" warning and left the budget permanently at zero, so it
    could never have stopped the run. The ledger writes one line per call.
    """
    # A ledger line is a WINDOW, not an API call: windows paginate, and each page is a
    # separate search.list call. Counting lines undercounted by ~25% (8,005 tracked vs
    # 10,078 actually spent), so the budget never fired and the run continued until the
    # API cut it off mid-year. Sum the `pages` field instead.
    led = CENSUS_DIR / ".ledger" / f"{pair}_{variant}_{year}.jsonl"
    if led.exists():
        try:
            total = 0
            for line in led.open():
                try:
                    total += json.loads(line).get("pages", 1)
                except Exception:                      # noqa: BLE001
                    total += 1
            return total
        except Exception:                              # noqa: BLE001
            return 0
    # fall back to window count, which is a lower bound on calls made
    p = ckpt_path(pair, variant, year)
    if not p.exists():
        return 0
    try:
        d = json.loads(p.read_text())
        return len(d.get("done_windows", [])) + len(d.get("split_windows", []))
    except Exception:                                  # noqa: BLE001
        return 0


def archive_target(pair: str, variant: str, year: int) -> None:
    ARCHIVE.mkdir(parents=True, exist_ok=True)
    # The ledger must be archived too, or searches_used() counts calls from the run
    # being replaced and the budget is wrong from the first target onward.
    for src in (ckpt_path(pair, variant, year),
                CENSUS_DIR / f"{pair}_{variant}_{year}.parquet",
                CENSUS_DIR / ".ledger" / f"{pair}_{variant}_{year}.jsonl"):
        if src.exists():
            shutil.move(str(src), str(ARCHIVE / f"{int(time.time())}_{src.name}"))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pair")
    g.add_argument("--pairs", help="comma-separated")
    g.add_argument("--all", action="store_true")
    ap.add_argument("--year-start", type=int, default=2010)
    ap.add_argument("--year-end", type=int, default=2026)
    ap.add_argument("--min-depth", default="day", choices=["month", "week", "day", "hour"])
    ap.add_argument("--budget", type=int, default=DAILY_SEARCH_QUOTA,
                    help="search calls for this run; stops cleanly when reached")
    ap.add_argument("--force", action="store_true",
                    help="archive existing checkpoint+parquet so targets actually recollect")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

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

    years = list(range(a.year_start, a.year_end + 1))
    jobs = [(p, v, y) for p in targets for y in years for v in VARIANTS]
    pending = [j for j in jobs if a.force or not ckpt_path(*[j[0], j[1], j[2]]).exists()]
    already = len(jobs) - len(pending)

    print(f"{len(targets)} pair(s) x {len(years)} years x 2 variants = {len(jobs)} targets")
    print(f"  already collected: {already}   pending: {len(pending)}")
    print(f"  min-depth {a.min_depth}, budget {a.budget:,} search calls"
          + ("   [FORCE: existing data will be archived]" if a.force else ""))
    if a.dry_run:
        for p, v, y in pending[:15]:
            print(f"    {p} {y} {v}")
        if len(pending) > 15:
            print(f"    ... and {len(pending)-15} more")
        return 0

    key = api_key()
    spent, done, failed = 0, 0, []
    t0 = time.time()
    for i, (pair, variant, year) in enumerate(pending, 1):
        if spent >= a.budget:
            print(f"\nbudget reached ({spent:,} searches) — stopping cleanly with "
                  f"{len(pending)-i+1} target(s) left")
            break
        if a.force:
            archive_target(pair, variant, year)
        print(f"[{i}/{len(pending)}] {pair} {year} {variant}  (spent {spent:,}/{a.budget:,})",
              flush=True)
        r = subprocess.run([sys.executable, "-u", "-m", "pipeline.ingestion.youtube_census",
                            "--pair", pair, "--year", str(year), "--variant", variant,
                            "--min-depth", a.min_depth, "--api-key", key,
                            "--max-searches", str(a.budget - spent)],
                           capture_output=True, text=True)
        used = searches_used(pair, variant, year)
        spent += used
        if r.returncode != 0:
            failed.append((pair, year, variant))
            print(f"    FAILED: {(r.stderr or r.stdout).strip().splitlines()[-1][:140]}")
        elif used == 0:
            # the exact failure mode that wasted a run: exits successfully having done nothing
            print(f"    WARNING: 0 search calls used — target already resolved; "
                  f"use --force to recollect")
        else:
            done += 1
            print(f"    {used:,} searches")

    print(f"\n{done} target(s) collected, {spent:,} searches, {(time.time()-t0)/60:.0f} min")
    if failed:
        print(f"failed: {failed}")
    print("next: python -m pipeline.ingestion.youtube_enrich  (then pipeline.rebuild)")
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
