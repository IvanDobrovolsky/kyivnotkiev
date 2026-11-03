"""Compare one census target measured on two dates: does a closed month settle?

chornobyl russian, collected 2026-08-28 with the same code and day floor as its
2025: July 2026 (queried 28 days after it closed) read 3,802 videos against 443
for July 2025, and cost 428 search pages against 42. GDELT and Wikipedia moved
1.1x over the same months. Either a freshly closed month is over-served by search
for some weeks, or July 2026 really was different. Re-collecting the same months
later, into a separate --checkpoint-dir, and comparing month by month separates
the two. Ages are measured from each month's end to its collection.

    python -m pipeline.ingestion.youtube_settle_compare \\
        --baseline data/archive/youtube_census_superseded/<ts>_chornobyl_russian_2026.json \\
        --new data/cl/raw/youtube_census_experiments/settle_2026-10-19/.checkpoints/chornobyl_russian_2026.json \\
        --reference data/cl/raw/youtube_census/.checkpoints/chornobyl_russian_2025.json \\
        --enriched data/archive/youtube_census_superseded/<ts>_chornobyl_enriched.parquet \\
        --enriched data/cl/raw/youtube_census_experiments/settle_2026-10-19/chornobyl_enriched.parquet

A checkpoint's ledger is found beside it (<root>/.ledger/<stem>.jsonl, or the
same archive name with .jsonl). Collection time comes from the month's
collected_at stamp, else the latest ledger `at` in the month, else the file's
mtime, which is only an upper bound and is printed with "<=".
Months align by calendar month, so --reference can be another year.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.ingestion import youtube_window as yw


def ledger_for(ckpt: Path) -> Path | None:
    side = ckpt.with_suffix(".jsonl")                 # archive: <ts>_<stem>.jsonl
    if side.exists():
        return side
    if ckpt.parent.name == ".checkpoints":
        led = ckpt.parent.parent / ".ledger" / f"{ckpt.stem}.jsonl"
        if led.exists():
            return led
    return None


def load(ckpt: Path, ledger: Path | None, verified: dict | None) -> pd.DataFrame:
    state = json.loads(ckpt.read_text())
    mtime = datetime.fromtimestamp(ckpt.stat().st_mtime, timezone.utc)
    pages, last_at = {}, {}
    ledger = ledger or ledger_for(ckpt)
    if ledger is not None:
        for line in ledger.open():
            try:
                r = json.loads(line)
            except ValueError:
                continue
            m = r.get("start", "")[:7]
            pages[m] = pages.get(m, 0) + int(r.get("pages", 0))
            if r.get("at"):
                last_at[m] = max(last_at.get(m, ""), r["at"])
    by_month: dict[str, list] = {}
    for vid, v in state.get("videos", {}).items():
        by_month.setdefault(str(v.get("published_at", ""))[:7], []).append(vid)
    rows = []
    for mkey, m in sorted(state.get("months", {}).items()):
        if m.get("collected_at"):
            at, bound = m["collected_at"], ""
        elif mkey in last_at:
            at, bound = last_at[mkey], ""
        else:
            at, bound = mtime.strftime("%Y-%m-%dT%H:%M:%SZ"), "<="
        t = datetime.strptime(at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        age = (t - yw.month_end(mkey)).total_seconds() / 86400
        row = {"mm": mkey[5:7], "month": mkey, "count": m["count"],
               "pages": pages.get(mkey) if ledger is not None else None,
               "age_days": f"{bound}{age:.0f}", "depth": m.get("min_depth") or state.get("min_depth")}
        if verified is not None:
            forms = [verified.get(v) for v in by_month.get(mkey, [])]
            row["ver_ru"] = sum(f in ("russian", "both") for f in forms)
            row["ver_ua"] = sum(f in ("ukrainian", "both") for f in forms)
            row["unenriched"] = sum(f is None for f in forms)
        rows.append(row)
    return pd.DataFrame(rows)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--baseline", required=True, type=Path, help="first measurement")
    ap.add_argument("--new", type=Path, help="second measurement of the same months")
    ap.add_argument("--reference", type=Path, help="a settled year, aligned by month")
    ap.add_argument("--baseline-ledger", type=Path)
    ap.add_argument("--new-ledger", type=Path)
    ap.add_argument("--reference-ledger", type=Path)
    ap.add_argument("--enriched", type=Path, action="append", default=[],
                    help="enriched parquet(s) whose `form` labels the videos; repeatable")
    ap.add_argument("--csv", type=Path, help="also write the table here")
    a = ap.parse_args(argv)

    verified = None
    if a.enriched:
        e = pd.concat([pd.read_parquet(f, columns=["video_id", "verified", "span_artifact", "form"])
                       for f in a.enriched], ignore_index=True).drop_duplicates("video_id")
        e = e.assign(lab=e.form.where(e.verified & ~e.span_artifact.fillna(False), "unverified"))
        verified = dict(zip(e.video_id, e.lab))

    sets = {"base": (a.baseline, a.baseline_ledger)}
    if a.new:
        sets["new"] = (a.new, a.new_ledger)
    if a.reference:
        sets["ref"] = (a.reference, a.reference_ledger)
    tables = {k: load(p, l, verified).set_index("mm") for k, (p, l) in sets.items()}
    t = pd.concat({k: v.drop(columns=["month"]) for k, v in tables.items()}, axis=1)
    t.columns = [f"{k}_{c}" for k, c in t.columns]
    if "new" in tables:
        t["new/base"] = (t.new_count / t.base_count).round(2)
        t["pages new/base"] = (t.new_pages / t.base_pages).round(2)
    if "ref" in tables:
        t["base/ref"] = (t.base_count / t.ref_count).round(2)
        if "new" in tables:
            t["new/ref"] = (t.new_count / t.ref_count).round(2)
    t = t.dropna(subset=["base_count"]).sort_index()
    with pd.option_context("display.width", 250, "display.max_columns", 50):
        print(f"base = {a.baseline}" + (f"\nnew  = {a.new}" if a.new else "")
              + (f"\nref  = {a.reference}" if a.reference else ""))
        print(t.to_string())
    if a.csv:
        t.to_csv(a.csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
