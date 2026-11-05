"""Diff a staged OpenAlex snapshot against what ships, then promote it.

    stage    check the staged file's schema, diff it per pair-year against the
             live data/raw/openalex/openalex_all_pairs.json, write a report.
    promote  re-check the report, back up the live file, replace it.

Closed years revise between snapshots, so a refreshed file always moves history
a little. Measured with the same query: Kyiv/Kiev 2025 went 712/37 -> 632/45
between 2026-04-03 and 2026-10-15 (share 95.06% -> 93.35%), and every year
moved between the 2026-04-25 git snapshot and the 2026-05-11 file. The gate
separates that drift from a broken query. Promotion refuses, unless the pair is
named in --accept, if for any enabled pair a closed year with at least 20
papers on both snapshots moves more than 5 pp (Kyiv 2025's revision was 1.7 pp),
the closed-year paper total changes by more than 25% (the per_page=50
truncation zeroed whole years, which is the signature of a query fault rather
than indexing), or the terms changed.

It refuses outright when the schema differs from what the exporter reads, when
a pair is missing, or when a carried-over entry (not refreshed) is not
byte-for-byte the live one. A gated pair needs its slug in --accept; one
expected trip (serhii-korolyov's term change) does not wave through the rest.

HELD BACK. The collector counts through SERIES_END, so a refreshed entry
carries the partial year (2026, January to September). The exporter reads this
JSON with no year clamp in all three of its readers (the timeseries loop, the
manifest paper totals and total_map), so a 2026 key in the live file would ship
unmarked while SOURCE_SERIES_END['openalex'] is 2025-12. stage therefore writes
the promote candidate, openalex_all_pairs.staged.json, clamped to
series_end('openalex'): later years dropped, totals re-summed, `through` set to
the last day counted. The full snapshot stays in the run directory and the
report shows its partial year. After SOURCE_SERIES_END['openalex'] moves, stage
the same run again (no requests) and promote; promote refuses if it moved since
staging. A year cannot be cut by month, so when series_end('openalex') is not a
December, every refreshed entry must have been counted through exactly that
month (collect with --end at it); otherwise stage refuses.

Usage:
    python -m pipeline.ingestion.openalex_merge stage   --run data/raw/openalex/staging/2010_2026-09
    python -m pipeline.ingestion.openalex_merge promote --run data/raw/openalex/staging/2010_2026-09 [--accept SLUG,...]
"""

import argparse
import copy
import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import RAW_DIR, SERIES_END, last_day, load_pairs, series_end
from pipeline.ingestion.openalex import LIVE_PATH, OA_RAW_DIR
# One copy of the promote machinery (copy, verify, swap, roll back; per-slug accept).
from pipeline.ingestion.trends_merge import check_accept, promote_files

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SOURCE = "openalex"
BACKUP_DIR = RAW_DIR / "openalex" / "backups"
RUN_FILES = ("openalex_all_pairs.json", "run.json")
STAGED_NAME = "openalex_all_pairs.staged.json"
LIVE_KEYS = {"russian_term", "ukrainian_term", "category", "yearly",
             "total_russian", "total_ukrainian", "pair_slug"}     # what ships today
YEAR_KEYS = {"year", "russian_count", "ukrainian_count", "total"}
MIN_TOTAL = 20       # the exporter's smooth_ratio_series pools until a denominator of 20
MAX_PP = 5.0
MAX_TOTAL_CHANGE = 0.25


def _sha1(p: Path) -> str | None:
    return hashlib.sha1(p.read_bytes()).hexdigest() if p.exists() else None


def _years(e: dict) -> dict[int, tuple[int, int]]:
    return {int(y["year"]): (int(y["russian_count"]), int(y["ukrainian_count"])) for y in e["yearly"]}


def _share(ru: int, ua: int) -> float | None:
    return round(ua / (ru + ua) * 100, 2) if ru + ua else None


def schema_problems(staged: list[dict], live: list[dict], slugs: list[str]) -> list[str]:
    out = []
    if len(staged) != len(slugs):
        out.append(f"{len(staged)} entries, pairs.yaml defines {len(slugs)}")
    if [e.get("pair_slug") for e in staged] != slugs:
        out.append("entries are not the pairs.yaml slugs in pairs.yaml order")
    for e in staged:
        miss = LIVE_KEYS - set(e)
        if miss:
            out.append(f"{e.get('pair_slug')}: missing keys {sorted(miss)}")
        for y in e.get("yearly", []):
            if set(y) != YEAR_KEYS or y["total"] != y["russian_count"] + y["ukrainian_count"]:
                out.append(f"{e.get('pair_slug')}: malformed year {y}")
                break
        # Fetched entries only: carried ones inherit the live file's totals, which
        # disagree with their years in 34 of 47 entries and are read by nothing.
        if e.get("as_of") and (e["total_russian"] != sum(y["russian_count"] for y in e["yearly"])
                               or e["total_ukrainian"] != sum(y["ukrainian_count"] for y in e["yearly"])):
            out.append(f"{e.get('pair_slug')}: totals do not sum the years")
    by = {e["pair_slug"]: e for e in live}
    for e in staged:
        if e.get("as_of") is None:                 # carried over: must equal the live entry
            old = by.get(e["pair_slug"])
            if old is None or {k: e.get(k) for k in LIVE_KEYS} != {k: old.get(k) for k in LIVE_KEYS}:
                out.append(f"{e['pair_slug']}: carried-over entry differs from the live file")
    return out


def clamp(entries: list[dict], through_ym: str) -> tuple[list[dict], dict, list[str]]:
    """(entries without years after through_ym, {slug: held years}, problems)."""
    last_year, whole, cut = int(through_ym[:4]), through_ym.endswith("-12"), last_day(through_ym)
    out, held, problems = [], {}, []
    for e in entries:
        e = copy.deepcopy(e)
        slug = e.get("pair_slug")
        drop = [y for y in e["yearly"] if int(y["year"]) > last_year]
        if e.get("as_of"):
            t = e.get("through") or ""
            if t < cut or (not whole and t[:7] != through_ym):
                problems.append(f"{slug}: counted through {t or '?'}, series_end('{SOURCE}') is "
                                f"{through_ym}; collect with --end {through_ym}")
        elif drop:
            problems.append(f"{slug}: carried-over entry has years after {through_ym}")
        if drop:
            held[slug] = [{k: y[k] for k in ("year", "russian_count", "ukrainian_count")} for y in drop]
            e["yearly"] = [y for y in e["yearly"] if int(y["year"]) <= last_year]
            e["total_russian"] = sum(y["russian_count"] for y in e["yearly"])
            e["total_ukrainian"] = sum(y["ukrainian_count"] for y in e["yearly"])
            e["through"] = cut
        out.append(e)
    return out, held, problems


def diff(staged: list[dict], live: list[dict], enabled: set[str], partial_year: int | None,
         max_pp: float = MAX_PP, max_total_change: float = MAX_TOTAL_CHANGE) -> tuple[dict, list]:
    by = {e["pair_slug"]: e for e in live}
    pairs, gate = {}, []
    for e in staged:
        slug = e["pair_slug"]
        if e.get("as_of") is None:
            continue
        old = by.get(slug, {"yearly": [], "russian_term": None, "ukrainian_term": None})
        yo, yn = _years(old), _years(e)
        years = []
        for y in sorted(set(yo) | set(yn)):
            (ro, uo), (rn, un) = yo.get(y, (0, 0)), yn.get(y, (0, 0))
            so, sn = _share(ro, uo), _share(rn, un)
            years.append({"year": y, "old": [ro, uo], "new": [rn, un],
                          "share_old": so, "share_new": sn,
                          "d_pp": round(sn - so, 2) if so is not None and sn is not None else None})
        closed = [r for r in years if partial_year is None or r["year"] < partial_year]
        gated = [r for r in closed if r["d_pp"] is not None
                 and min(sum(r["old"]), sum(r["new"])) >= MIN_TOTAL]
        worst = max(gated, key=lambda r: abs(r["d_pp"]), default=None)
        t_old = sum(sum(r["old"]) for r in closed)
        t_new = sum(sum(r["new"]) for r in closed)
        row = {"terms_old": [old.get("russian_term"), old.get("ukrainian_term")],
               "terms_new": [e["russian_term"], e["ukrainian_term"]],
               "closed_total_old": t_old, "closed_total_new": t_new,
               "closed_total_change": round((t_new - t_old) / t_old, 4) if t_old else None,
               "years_gated": len(gated),
               "max_abs_pp": abs(worst["d_pp"]) if worst else None,
               "max_year": worst["year"] if worst else None,
               "years_only_old": [r["year"] for r in years if sum(r["new"]) == 0],
               "years_only_new": [r["year"] for r in years if sum(r["old"]) == 0],
               "partial": (next((r for r in years if r["year"] == partial_year), None)
                           if partial_year is not None else None),
               "years": years}
        why = []
        if slug in enabled:
            if row["max_abs_pp"] is not None and row["max_abs_pp"] > max_pp:
                why.append(f"{row['max_year']} share moved {row['max_abs_pp']} pp > {max_pp}")
            if row["closed_total_change"] is not None and abs(row["closed_total_change"]) > max_total_change:
                why.append(f"closed-year papers {t_old} -> {t_new} "
                           f"({row['closed_total_change']:+.1%}) beyond {max_total_change:.0%}")
            if row["terms_old"] != row["terms_new"] and old.get("russian_term") is not None:
                why.append(f"terms changed {row['terms_old']} -> {row['terms_new']}")
        if why:
            gate.append({"slug": slug, "why": why})
        pairs[slug] = row
    return pairs, gate


def stage(run: Path, live_path: Path = LIVE_PATH, max_pp: float = MAX_PP,
          max_total_change: float = MAX_TOTAL_CHANGE) -> dict:
    snap_path, staged_path = run / "openalex_all_pairs.json", run / STAGED_NAME
    if run.resolve() in (OA_RAW_DIR.resolve(), live_path.parent.resolve()):
        raise SystemExit(f"--run {run} is the live directory; stage writes there without a backup")
    live_sha = _sha1(live_path)                     # before anything is written
    run_files = {n: _sha1(run / n) for n in RUN_FILES}
    snapshot = json.loads(snap_path.read_text())
    live = json.loads(live_path.read_text())
    cfg = load_pairs()["pairs"]
    slugs = [p["slug"] for p in cfg]
    enabled = {p["slug"] for p in cfg if p.get("enabled", True)}
    hard = schema_problems(snapshot, live, slugs)
    meta = json.loads((run / "run.json").read_text()) if (run / "run.json").exists() else {}
    through_ym = series_end(SOURCE)
    staged, held, problems = clamp(snapshot, through_ym)
    hard += problems + [h for h in schema_problems(staged, live, slugs) if h not in hard]
    # The partial year of the SNAPSHOT, reported (not gated: it has no earlier value).
    t = meta.get("through") or last_day(SERIES_END)
    partial_year = None if t[5:7] == "12" else int(t[:4])
    stale = sorted(enabled - {e["pair_slug"] for e in snapshot if e.get("as_of")})
    pairs, gate = diff(snapshot, live, enabled, partial_year, max_pp, max_total_change)
    staged_path.write_text(json.dumps(staged, indent=2))
    r = {"staged_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
         "run": meta, "hard": hard, "gate": gate, "partial_year": partial_year,
         "series_end": through_ym, "held_back": held,
         "enabled_not_refreshed": stale, "pairs": pairs,
         "dropped_from_live": sorted({e["pair_slug"] for e in live} - set(slugs)),
         "thresholds": {"min_total": MIN_TOTAL, "max_pp": max_pp,
                        "max_total_change": max_total_change},
         "run_files": run_files,
         "live": {"path": str(live_path), "sha1": live_sha, "entries": len(live)},
         "snapshot": {"path": str(snap_path), "sha1": run_files["openalex_all_pairs.json"],
                      "entries": len(snapshot)},
         "staged": {"path": str(staged_path), "sha1": _sha1(staged_path), "entries": len(staged)}}
    (run / "stage_report.json").write_text(json.dumps(r, indent=1))
    return r


def print_report(r: dict) -> None:
    print(f"{'pair':<22} {'closed old':>10} {'new':>7} {'chg':>7} {'max|d|pp':>9} {'yr':>5}  "
          f"partial {r['partial_year'] or '-'} (ru, ua, share)")
    for slug, p in r["pairs"].items():
        part = p["partial"]
        chg = "-" if p["closed_total_change"] is None else f"{p['closed_total_change']:+.1%}"
        print(f"{slug:<22} {p['closed_total_old']:>10} {p['closed_total_new']:>7} {chg:>7} "
              f"{p['max_abs_pp'] if p['max_abs_pp'] is not None else '-':>9} "
              f"{p['max_year'] or '-':>5}  "
              + (f"{part['new'][0]}, {part['new'][1]}, {part['share_new']}" if part else "-"))
    for s in r["enabled_not_refreshed"]:
        print(f"NOTE: enabled pair {s} was carried over, not refreshed")
    for s in r["dropped_from_live"]:
        print(f"NOTE: live entry {s} is not in pairs.yaml and is dropped")
    if r["held_back"]:
        yrs = sorted({y["year"] for v in r["held_back"].values() for y in v})
        print(f"HELD BACK: {yrs} for {len(r['held_back'])} pairs, after series_end('{SOURCE}') "
              f"{r['series_end']}; stage this run again once SOURCE_SERIES_END['{SOURCE}'] moves")
    for h in r["hard"]:
        print(f"HARD: {h}")
    for g in r["gate"]:
        print(f"GATE: {g['slug']}: {'; '.join(g['why'])}")
    if r["gate"]:
        print(f"promote names each reviewed pair: --accept {','.join(g['slug'] for g in r['gate'])}")


def promote(run: Path, accept=(), live_path: Path = LIVE_PATH,
            backups: Path = BACKUP_DIR) -> None:
    r = json.loads((run / "stage_report.json").read_text())
    staged = Path(r["staged"]["path"])
    if r["hard"]:
        raise SystemExit("refusing: " + "; ".join(r["hard"]))
    if series_end(SOURCE) != r.get("series_end"):
        raise SystemExit(f"series_end('{SOURCE}') is {series_end(SOURCE)}, staged at "
                         f"{r.get('series_end')}; stage again")
    changed = [str(p) for p, sha in ((live_path, r["live"]["sha1"]), (staged, r["staged"]["sha1"]))
               if _sha1(p) != sha]
    changed += [str(run / n) for n in RUN_FILES if _sha1(run / n) != (r.get("run_files") or {}).get(n)]
    if changed:
        raise SystemExit(f"changed since staging: {changed}; stage again")
    check_accept(r["gate"], accept)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    promote_files([(staged, live_path, r["staged"]["sha1"])], stamp, backups)
    years = sorted({y["year"] for v in r["held_back"].values() for y in v})
    log.info(f"promoted {staged} -> {live_path}; backup in {backups} ({stamp}). Years run through "
             f"series_end('{SOURCE}') {r['series_end']}"
             + (f"; {years} held back until SOURCE_SERIES_END['{SOURCE}'] moves (then stage "
                f"this run again)" if years else ""))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("stage")
    s.add_argument("--run", type=Path, required=True)
    s.add_argument("--live", type=Path, default=LIVE_PATH)
    s.add_argument("--max-pp", type=float, default=MAX_PP)
    s.add_argument("--max-total-change", type=float, default=MAX_TOTAL_CHANGE)
    p = sub.add_parser("promote")
    p.add_argument("--run", type=Path, required=True)
    p.add_argument("--accept", default="",
                   help="comma-separated slugs past the gate that you reviewed; each gated pair "
                        "must be named")
    args = ap.parse_args()
    if args.cmd == "stage":
        r = stage(args.run, args.live, args.max_pp, args.max_total_change)
        print_report(r)
        print(f"report: {args.run / 'stage_report.json'}")
    else:
        promote(args.run, args.accept.split(","))


if __name__ == "__main__":
    main()
