"""Merge dump-sweep output into the backfill parquet that pipeline/store/migrate.py reads.

    python -m pipeline.ingestion.reddit_backfill_merge --dry-run [--by-month]
    python -m pipeline.ingestion.reddit_backfill_merge [--through YYYY-MM]

Reads data/raw/reddit/backfill/RS_YYYY-MM_matches.jsonl, written by
pipeline/ingestion/reddit_dump_sweep.py, for DUMP_ERA_START..--through, and
emits one row per (post, pair) with the dataset's exact conventions (post_id,
subreddit, author, title, selftext, score, variant, date, pair_slug,
matched_term, created_utc), for every pair defined in pairs.yaml (migrate drops
the disabled ones from the processed tier, as it does for PullPush's raw rows).
A text matching both variants of a pair takes the variant of the FIRST
occurrence, which is deterministic and keeps the dataset's one-variant-per-row
shape. Rows whose (post_id, pair_slug) already exist in
dataset/raw_reddit.parquet are dropped: the near-post-time PullPush capture
wins over the dump snapshot.

--through defaults to series_end('reddit') from pipeline/config.py, and the
merge will not WRITE past it. migrate reads every row of this file and runs a
corpus-wide bot rule (>=100 posts and <=2 authors, or >=10% chain titles)
before any date clamp, so a month past the series end can still remove
2010-2025 rows: DefenceProfessionals holds 98 posts from one author, all
2025-07..12, and two more posts would drop all 98 (measured 2026-10-19). Later
months stay on disk, are listed as held back, and can be inspected with
--dry-run --through.

Refuses, and writes nothing, when:
  - any RS jsonl in range has no sidecar, or was filtered with a different pair
    set, matcher or record kind, or changed after the sweep wrote it.
    Re-attributing the 2025-09-05 sweep with today's pairs.yaml would give
    zaporizhzhia 1,055 rows in 2025-05..12 that are there only because they
    co-mention another pair's term: 33-193/month, against 206-325/month in
    PullPush's 2025-01..04;
  - a month was re-swept (an earlier output sits in archive/) and the latest
    `reddit_dump_sweep gate` entry for it is missing, failed, or describes a
    different re-sweep, earlier file or pair set;
  - the months are not contiguous from DUMP_ERA_START to --through, since a
    missing month draws as a collapse in volume;
  - a record is not a submission, has no id, or has an unparseable created_utc
    (the 2025 code turned that into 1970-01-01);
  - the result has fewer rows than the file it replaces, or is missing any
    (post_id, pair_slug) the old file held for a pair and term still searched
    for. Rows whose pair or term is gone from pairs.yaml are dropped only with
    --drop-retired, and named. Disabling a pair does not retire it: borscht's
    1,244 rows in the 2025 backfill (Borscht 915, Borsch 329) are still searched
    for and must come back. The one retired term today is serhii-korolyov's
    'Serhii Korolyov' (now 'Serhii Korolov', 2026-09-11), and none of the 33
    serhii-korolyov rows in the 2025 backfill use it: all are 'Sergei Korolev'.

Submissions only. backfill_comments.parquet, written by the 2025 version of this
script, is left untouched and is read by nothing.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import fcntl
import json
import os
import pathlib
import re
import shutil
import sys

import pandas as pd

from pipeline.config import series_end
from pipeline.ingestion.reddit_dump_filter import compile_matcher, pair_set, swept_pairs, text_of
from pipeline.ingestion.reddit_dump_sweep import (
    GATE_REPORT, load_gate_report, output_problems, read_meta, sha256_file)

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
BF = ROOT / "data" / "raw" / "reddit" / "backfill"
PULLPUSH = ROOT / "dataset" / "raw_reddit.parquet"
DEST_NAME = "backfill_submissions.parquet"

# PullPush is the source through 2025-05-08 (it collapses after 2025-05-09 from
# indexing lag); the dumps take over from 2025-05. A dump month before this would
# add dump-only rows inside the PullPush era, splicing a second sampling frame
# into it: in 2025-05-01..07 the dump-only rows were +8.5% of volume and 25.8%
# Ukrainian-form against 64.9% for PullPush rows.
DUMP_ERA_START = "2025-05"

COLUMNS = ["post_id", "subreddit", "author", "title", "selftext", "score", "variant",
           "date", "pair_slug", "matched_term", "created_utc"]


class Refused(Exception):
    def __init__(self, msg: str):
        super().__init__(msg)
        self.msg = msg


def _rows_where(df: pd.DataFrame, keys, pred) -> pd.DataFrame:
    """Row filter. A plain Python list of booleans is not safe here: when it is
    empty, df[[]] selects zero COLUMNS and the next column access raises."""
    return df.loc[pd.Series([pred(k) for k in keys], index=df.index, dtype=bool)]


def _next_month(ym: str) -> str:
    y, m = int(ym[:4]), int(ym[5:7])
    return f"{y + 1:04d}-01" if m == 12 else f"{y:04d}-{m + 1:02d}"


def _int(v, what: str, where: str) -> int:
    try:
        if isinstance(v, bool):
            raise ValueError
        return int(v) if isinstance(v, int) else int(float(v))
    except (TypeError, ValueError):
        raise Refused(f"{where}: {what} {v!r} is not a number")


def gate_problems(bf: pathlib.Path, ym: str, meta: dict, ps: dict, gate: dict, gate_path) -> list[str]:
    """A month with an earlier output in archive/ was re-swept, and the re-sweep
    is only trusted once `gate` has compared it with an earlier output: the gate
    fails on content diffs and on posts one run lost that the other's terms
    match, which the row-count checks below cannot see."""
    archived = sorted((bf / "archive").glob(f"*/RS_{ym}_matches.jsonl"))
    if not archived:
        return []
    e = gate.get("months", {}).get(ym)
    where = f"{gate_path}"
    if not e:
        return [f"re-swept ({len(archived)} earlier output(s) in archive/) and {where} has no gate "
                "result for it; run `reddit_dump_sweep gate`"]
    probs = []
    if not e.get("pass"):
        probs.append(f"gate FAILED ({'; '.join(e.get('error') or []) or 'see ' + where})")
    if e.get("pairset") != ps["sha1"]:
        probs.append(f"gated under pair set {str(e.get('pairset'))[:12]}, not {ps['sha1'][:12]}")
    if e.get("new_sha256") != (meta.get("output") or {}).get("sha256"):
        probs.append("gate compared a different re-sweep of this month; re-run the gate")
    if e.get("pass") and e.get("legacy_sha256") not in {sha256_file(f)[0] for f in archived}:
        probs.append("gate compared an earlier file that is not in archive/")
    return probs


def inputs(bf: pathlib.Path, ps: dict, through: str, gate_path: pathlib.Path) -> tuple[list, list]:
    files = sorted(bf.glob("RS_*_matches.jsonl"))
    held = [f.name[3:10] for f in files if f.name[3:10] > through]
    files = [f for f in files if f.name[3:10] <= through]
    if not files:
        raise Refused(f"no RS_*_matches.jsonl in {bf} through {through}")
    gate = load_gate_report(gate_path) if (bf / "archive").exists() else {"months": {}}
    bad = []
    months = []
    for f in files:
        ym = f.name[3:10]
        meta = read_meta(f)
        probs = output_problems(f, meta, ps)
        if ym < DUMP_ERA_START:
            probs.append(f"month before DUMP_ERA_START {DUMP_ERA_START} (PullPush era)")
        if not probs:
            probs = gate_problems(bf, ym, meta, ps, gate, gate_path)
        if probs:
            bad.append(f"  {f.name}: {'; '.join(probs)}")
        months.append((ym, f))
    if bad:
        raise Refused("inputs not usable with the current pair set "
                      f"{ps['sha1'][:12]}:\n" + "\n".join(bad))
    yms = [m for m, _ in months]
    if yms[0] != DUMP_ERA_START:
        raise Refused(f"months start at {yms[0]}, not {DUMP_ERA_START}")
    for a, b in zip(yms, yms[1:]):
        if b != _next_month(a):
            raise Refused(f"month gap: {a} is followed by {b}")
    if yms[-1] != through:
        raise Refused(f"months end at {yms[-1]}, --through is {through}: "
                      f"{_next_month(yms[-1])}..{through} not swept")
    return months, held


def attribute(months, pairs: list[dict]) -> tuple[list[dict], dict]:
    rx = {p["slug"]: (compile_matcher([p["ukrainian"]]), compile_matcher([p["russian"]]),
                      p["ukrainian"], p["russian"]) for p in pairs}
    rows, info = [], {"records": 0, "out_of_month": 0}
    for ym, f in months:
        with open(f, "rb") as fh:
            for ln, line in enumerate(fh, 1):
                where = f"{f.name}:{ln}"
                try:
                    d = json.loads(line)
                except ValueError:
                    raise Refused(f"{where}: not JSON")
                if d.get("kind") != "submission" or "title" not in d:
                    raise Refused(f"{where}: not a submission (kind={d.get('kind')!r})")
                pid = str(d.get("id") or "")
                if not pid:
                    raise Refused(f"{where}: no id")
                ts = _int(d.get("created_utc"), "created_utc", where)
                if ts <= 0:
                    raise Refused(f"{where}: created_utc {ts}")
                day = pd.Timestamp(ts, unit="s")
                info["records"] += 1
                # 0 of 60,711 records in the 2025 RS files fall outside their file's
                # month; count rather than refuse, the (post, pair) dedup covers it.
                if day.strftime("%Y-%m") != ym:
                    info["out_of_month"] += 1
                text = text_of(d)
                for slug, (rua, rru, tua, tru) in rx.items():
                    mu, mr = rua.search(text), rru.search(text)
                    if not mu and not mr:
                        continue
                    if mu and mr:
                        variant, term = (("ukrainian", tua) if mu.start() <= mr.start()
                                         else ("russian", tru))
                    elif mu:
                        variant, term = "ukrainian", tua
                    else:
                        variant, term = "russian", tru
                    rows.append({
                        "post_id": pid, "subreddit": d.get("subreddit"), "author": d.get("author"),
                        "title": d.get("title"), "selftext": d.get("selftext"),
                        "score": _int(d.get("score") or 0, "score", where),
                        "variant": variant, "date": day.strftime("%Y-%m-%d"),
                        "pair_slug": slug, "matched_term": term, "created_utc": ts,
                    })
    return rows, info


def build(bf: pathlib.Path, pullpush: pathlib.Path, config: pathlib.Path | None, through: str,
          gate_path: pathlib.Path = GATE_REPORT) -> tuple[pd.DataFrame, dict]:
    pairs = swept_pairs(config)
    ps = pair_set(pairs)
    months, held = inputs(bf, ps, through, gate_path)
    rows, info = attribute(months, pairs)
    df = pd.DataFrame(rows, columns=COLUMNS)
    n_attr = len(df)
    have = pd.read_parquet(pullpush, columns=["post_id", "pair_slug"])
    # Only the dump's own post ids can collide; a set of all 840K PullPush
    # (post, pair) tuples would cost more memory than the merge itself.
    have = have[have.post_id.astype(str).isin(set(df.post_id))]
    have = set(zip(have.post_id.astype(str), have.pair_slug))
    df = _rows_where(df, zip(df.post_id, df.pair_slug), lambda k: k not in have)
    n_pp = n_attr - len(df)
    # The grain is one row per (post, pair). Deduplicating on post_id alone would
    # delete a post's second pair: 3,274 of the 2025 backfill posts name more than
    # one pair (3,990 rows).
    before = len(df)
    df = df.drop_duplicates(["post_id", "pair_slug"], keep="first").reset_index(drop=True)
    df["score"] = df["score"].astype("int64")
    df["created_utc"] = df["created_utc"].astype("int64")
    info.update({"pairset": ps, "through": through, "held_back": held, "months": [m for m, _ in months],
                 "inputs": [{"file": f.name, "month": m, **(read_meta(f) or {}).get("output", {})}
                            for m, f in months],
                 "rows_attributed": n_attr, "dropped_in_pullpush": n_pp,
                 "dropped_duplicates": before - len(df), "rows": len(df)})
    return df, info


def compare(df: pd.DataFrame, dest: pathlib.Path, pairs: list[dict], drop_retired: bool) -> dict:
    if not dest.exists():
        return {"old_rows": 0, "missing_in_scope": 0, "missing_retired": {}}
    old = pd.read_parquet(dest, columns=["post_id", "pair_slug", "matched_term"])
    live = {(p["slug"], t) for p in pairs for t in (p["ukrainian"], p["russian"])}
    new_keys = set(zip(df.post_id, df.pair_slug))
    gone = _rows_where(old, zip(old.post_id.astype(str), old.pair_slug), lambda k: k not in new_keys)
    retired = _rows_where(gone, zip(gone.pair_slug, gone.matched_term), lambda k: k not in live)
    in_scope = gone.drop(retired.index)
    by = collections.Counter(zip(retired.pair_slug, retired.matched_term))
    res = {"old_rows": len(old), "missing_in_scope": len(in_scope),
           "missing_retired": {f"{s}/{t}": n for (s, t), n in sorted(by.items())}}
    if len(in_scope):
        ex = in_scope.groupby("pair_slug").size().sort_values(ascending=False).head(10).to_dict()
        raise Refused(f"{len(in_scope):,} (post_id, pair_slug) in {dest.name} for pairs and terms "
                      f"still searched for are missing from the merge: {ex}. A sweep lost "
                      "rows or a month file was removed; nothing written.")
    if len(retired) and not drop_retired:
        raise Refused(f"{len(retired):,} rows in {dest.name} belong to retired pairs or terms "
                      f"{res['missing_retired']}. Re-run with --drop-retired to drop them.")
    floor = len(old) - (len(retired) if drop_retired else 0)
    if len(df) < floor:
        raise Refused(f"merge has {len(df):,} rows, fewer than the {floor:,} it must keep "
                      f"from {dest.name} ({len(old):,} read)")
    return res


def write(df: pd.DataFrame, dest: pathlib.Path, info: dict) -> pathlib.Path | None:
    stamp = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    backup = None
    if dest.exists():
        backup = dest.with_name(f"{dest.stem}.bak-{stamp}.parquet")
        n = 1
        while backup.exists():                 # two merges in one second keep both backups
            backup = dest.with_name(f"{dest.stem}.bak-{stamp}-{n}.parquet")
            n += 1
        shutil.copy2(dest, backup)
    tmp = dest.with_name(f".{dest.name}.tmp")
    df.to_parquet(tmp, index=False)
    back = pd.read_parquet(tmp, columns=["post_id", "pair_slug"])
    if len(back) != len(df):
        raise Refused(f"{tmp} read back {len(back)} rows, wrote {len(df)}")
    os.replace(tmp, dest)
    meta = dest.with_name(dest.stem + ".meta.json")
    mt = meta.with_name(meta.name + ".tmp")
    mt.write_text(json.dumps({**info, "written_at": dt.datetime.now().astimezone().isoformat(
        timespec="seconds"), "backup": backup.name if backup else None}, indent=1, ensure_ascii=False))
    os.replace(mt, meta)
    return backup


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bf-dir", type=pathlib.Path, default=BF)
    ap.add_argument("--pullpush", type=pathlib.Path, default=PULLPUSH)
    ap.add_argument("--config", type=pathlib.Path, default=None)
    ap.add_argument("--through", default=None,
                    help="last month to merge (default series_end('reddit') from pipeline/config.py; "
                         "a write past it is refused, a dry run is not)")
    ap.add_argument("--gate-report", type=pathlib.Path, default=GATE_REPORT)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--drop-retired", action="store_true",
                    help="allow dropping rows whose pair or term is no longer in pairs.yaml")
    ap.add_argument("--by-month", action="store_true", help="print a pair x month row table")
    a = ap.parse_args(argv)
    limit = series_end("reddit")
    through = a.through or limit
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", through):
        print(f"REFUSED: --through {through!r} is not one month (YYYY-MM)", file=sys.stderr)
        return 2
    if through > limit and not a.dry_run:
        print(f"REFUSED: --through {through} is past series_end('reddit') {limit}. migrate would "
              "ingest months the charts do not plot, and its corpus-wide bot rule would let them "
              "remove 2010-2025 rows. Bump SOURCE_SERIES_END['reddit'] in pipeline/config.py once "
              "migrate bounds that rule; until then use --dry-run.", file=sys.stderr)
        return 2
    dest = a.bf_dir / DEST_NAME
    # A running sweep renames month files into place; merging mid-sweep would
    # read a prefix of the months and look complete. No lock file means no sweep
    # has ever run here, and opening it read-only keeps a dry run write-free.
    lock_path = a.bf_dir / ".sweep.lock"
    lock = open(lock_path) if lock_path.exists() else None
    if lock is not None:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(f"REFUSED: a sweep holds {lock_path}", file=sys.stderr)
            return 2
    try:
        df, info = build(a.bf_dir, a.pullpush, a.config, through, a.gate_report)
        pairs = swept_pairs(a.config)
        info["compare"] = compare(df, dest, pairs, a.drop_retired)
    except Refused as e:
        print(f"REFUSED: {e.msg}", file=sys.stderr)
        return 2
    vc = df.variant.value_counts().to_dict() if len(df) else {}
    c = info["compare"]
    print(f"months {info['months'][0]}..{info['months'][-1]} ({len(info['months'])}), "
          f"{info['records']:,} records, pair set {info['pairset']['sha1'][:12]} "
          f"({len(info['pairset']['slugs'])} pairs)"
          + (f"; held back past --through {through}: {', '.join(info['held_back'])}"
             if info["held_back"] else ""))
    print(f"  {info['rows_attributed']:,} pair-rows, -{info['dropped_in_pullpush']:,} already in "
          f"PullPush, -{info['dropped_duplicates']:,} duplicate (post, pair) -> {len(df):,} rows "
          f"{vc}; out-of-month records {info['out_of_month']}")
    print(f"  replaces {c['old_rows']:,} rows; retired rows dropped {c['missing_retired'] or 0}")
    per = df.groupby("pair_slug").size().to_dict()
    print("  " + ", ".join(f"{s} {n:,}" for s, n in sorted(per.items(), key=lambda x: -x[1])))
    if a.by_month and len(df):
        # For a new month's pilot: read each pair's count against the month
        # before it, and zaporizhzhia's 2025-05..12 against its PullPush months
        # (206-325/month in 2025-01..04).
        tab = df.assign(m=df.date.str[:7]).pivot_table(
            index="pair_slug", columns="m", values="post_id", aggfunc="size", fill_value=0)
        print(tab.to_string())
    if a.dry_run:
        print(f"dry run: {dest} not written")
        return 0
    backup = write(df, dest, info)
    print(f"wrote {dest} ({len(df):,} rows){'; backup ' + backup.name if backup else ''}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
