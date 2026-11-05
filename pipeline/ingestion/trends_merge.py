"""Stage a Trends run against dataset/raw_trends.parquet, diff it, then promote it.

Nothing in the tree wrote dataset/raw_trends.parquet, the file the exporter
reads: trends.py wrote data/raw/trends/trends_world_monthly.parquet, and its 9,216
rows were copied in by a step that is not in the repo. This is that step, with a
gate in front of it.

    stage    merge the run's world rows into a copy of the live dataset, in the
             live schema, and write a diff report. Touches nothing live.
    promote  re-check the report's gate, back up the live files, replace them.

HELD BACK. Rows after series_end('trends') are not staged: the dataset never
holds a month the config has not admitted. migrate's processed tier clamps at
SERIES_END, not per source, and rebuild.push_hf publishes dataset/ with no date
clamp at all, so a 2026 row here would reach the store and HuggingFace before
anyone bumped SOURCE_SERIES_END['trends']. The 2026 rows wait in the run
directory; after the bump, stage the same run again (0 searches) and promote.
promote refuses if series_end('trends') moved since staging. The run's own
trends_world_monthly.parquet is promoted whole, as provenance: nothing reads it
(grep, 2026-10-19), and it is not under dataset/.

WHAT PROMOTE TRUSTS. Only what stage hashed: the staged dataset, the live
dataset and calibration report, and every run file it copies (run.json,
trends_world_monthly.parquet, calibration_report.csv). The exporter builds its
k = 0 set from calibration_report.csv; a one-pair report beside the 24-pair
dataset empties that set and draws feodosiia, ihor-sikorsky, kazymyr-malevych and
serhii-korolyov's 635 defined months as 0.0% (2026-10-19 review, on copies).
All files are copied and verified before the first one is swapped in, and a
failed swap restores the ones already replaced.

What the merge replaces: the world rows (geo == "") of every pair in the run.
What it keeps: the world rows of pairs not in the run (22 disabled pairs, 8,256
rows today) and all per-country rows (96,330, ending 2025-12, producer deleted in
5f113ea0). They are carried byte-for-byte.

THE GATE. A refetch over a longer span renormalises every month, so history
moves. The calibrated share is invariant to that rescaling in exact arithmetic,
so what is left is quantisation, sampling noise and calibration-window moves.
The 2026-09-05 audit measured k moving at most 7% across alternative windows,
which is at most about 1.8 pp on a share at 50%. Defaults follow from that: a
pair whose MEDIAN month moves more than 2 pp has shifted systematically, and
any month moving more than 15 pp is a gross change. Either refuses promotion
unless that pair is named in --accept, as does a pair moving into or out of
k = 0, a pair whose query term changed, or stored months the run lacks. --accept
takes slugs, not a switch: one expected trip (zaporizhzhia's out-of-tree series)
must not wave through every other pair's. The thresholds are a starting point,
not a calibration; the first real run's report is.

The comparison is the share the exporter computes, ua_cal / (ua_cal + ru) per
month, before smoothing. timeseries.json is not the reference because
smooth_series rewrites up to 6 pairs (audit D1), which would mix a smoothing
difference into a data difference.

Usage:
    python -m pipeline.ingestion.trends_merge stage   --run data/raw/trends/staging/2010-01_2026-09
    python -m pipeline.ingestion.trends_merge promote --run data/raw/trends/staging/2010-01_2026-09
    python -m pipeline.ingestion.trends_merge promote --run ... --accept zaporizhzhia,serhii-korolyov
"""

import argparse
import hashlib
import json
import logging
import os
import shutil
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.config import ROOT_DIR, get_enabled_pairs, series_end
from pipeline.ingestion import trends as tr

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SOURCE = "trends"
LIVE_DATASET = ROOT_DIR / "dataset" / "raw_trends.parquet"
LIVE_DIR = tr.LIVE_DIR
KEY = ["pair_slug", "date", "variant", "geo"]     # rebuild.validate_datasets' key
RUN_FILES = ("run.json", "trends_world_monthly.parquet", "calibration_report.csv")

MAX_MEDIAN_PP = 2.0
MAX_ABS_PP = 15.0


def _sha1(p: Path) -> str | None:
    return hashlib.sha1(p.read_bytes()).hexdigest() if p.exists() else None


def _world(df: pd.DataFrame) -> pd.DataFrame:
    return df[(df["geo"] == "") | df["geo"].isna()]


def _k0(cal: pd.DataFrame) -> set[str]:
    if cal is None or "k" not in cal.columns:
        return set()
    return set(cal[pd.to_numeric(cal["k"], errors="coerce") <= 0]["slug"].astype(str))


def shares(world: pd.DataFrame, k0: set[str]) -> pd.DataFrame:
    """Per (pair_slug, month): the share export_site_data computes, before smoothing.

    None where both calibrated values are 0, and for every month of a k = 0 pair,
    which the exporter also draws as a gap.
    """
    w = world.assign(month=world["date"].astype(str).str[:7])
    g = w.pivot_table(index=["pair_slug", "month"], columns="variant",
                      values="interest_calibrated", aggfunc="sum", fill_value=0)
    ua = g.get("ukrainian", 0.0)
    ru = g.get("russian", 0.0)
    tot = ua + ru
    share = (ua / tot.where(tot > 0) * 100).rename("share")
    out = share.reset_index()
    out.loc[out["pair_slug"].isin(k0), "share"] = None
    return out


def _solo_shape(old_w: pd.DataFrame, new_w: pd.DataFrame, slug: str) -> dict:
    """new/old ratio of the per-variant solo `interest` where both are non-zero.

    A uniform rescale gives a tight ratio; a wide one means Google's sampling moved
    individual months, which the share then inherits.
    """
    out = {}
    for v in ("russian", "ukrainian"):
        a = old_w[(old_w["pair_slug"] == slug) & (old_w["variant"] == v)].set_index("date")["interest"]
        b = new_w[(new_w["pair_slug"] == slug) & (new_w["variant"] == v)].set_index("date")["interest"]
        j = pd.concat([a.rename("old"), b.rename("new")], axis=1, join="inner")
        j = j[(j["old"] > 0) & (j["new"] > 0)]
        if len(j):
            r = j["new"] / j["old"]
            out[v] = {"n": int(len(j)), "median": round(float(r.median()), 4),
                      "q25": round(float(r.quantile(.25)), 4), "q75": round(float(r.quantile(.75)), 4)}
        else:
            out[v] = {"n": 0}
    return out


def diff(live: pd.DataFrame, new: pd.DataFrame, live_cal: pd.DataFrame | None,
         new_cal: pd.DataFrame, max_median_pp: float = MAX_MEDIAN_PP,
         max_abs_pp: float = MAX_ABS_PP) -> dict:
    slugs = sorted(new["pair_slug"].unique())
    old_w = _world(live)
    old_w = old_w[old_w["pair_slug"].isin(slugs)]
    k0_old, k0_new = _k0(live_cal), _k0(new_cal)
    s_old = shares(old_w, k0_old)
    s_new = shares(new, k0_new)
    m = s_old.merge(s_new, on=["pair_slug", "month"], how="outer", suffixes=("_old", "_new"),
                    indicator=True)
    old_max = old_w.groupby("pair_slug")["date"].max().astype(str).str[:7].to_dict()
    calo = {} if live_cal is None else live_cal.set_index("slug").to_dict("index")
    caln = new_cal.set_index("slug").to_dict("index")
    terms_old = (old_w.groupby(["pair_slug", "variant"])["term"].first().unstack()
                 if len(old_w) else pd.DataFrame())

    pairs, all_abs, gate = {}, [], []
    for slug in slugs:
        g = m[m["pair_slug"] == slug]
        overlap = g[g["_merge"] == "both"]
        both = overlap.dropna(subset=["share_old", "share_new"])
        d = (both["share_new"] - both["share_old"])
        a = d.abs()
        all_abs.extend(a.tolist())
        defchg = int((overlap["share_old"].isna() != overlap["share_new"].isna()).sum())
        added = sorted(g[g["_merge"] == "right_only"]["month"])
        lost = sorted(g[g["_merge"] == "left_only"]["month"])
        co, cn = calo.get(slug, {}), caln.get(slug, {})
        row = {
            "months_compared": int(len(both)),
            "median_abs_pp": round(float(a.median()), 3) if len(a) else None,
            "p90_abs_pp": round(float(a.quantile(.9)), 3) if len(a) else None,
            "max_abs_pp": round(float(a.max()), 3) if len(a) else None,
            "max_month": str(both.loc[a.idxmax(), "month"]) if len(a) else None,
            "mean_signed_pp": round(float(d.mean()), 3) if len(d) else None,
            "definedness_changed": defchg,
            "old_last_month": old_max.get(slug),
            "months_added": len(added),
            "added_range": [added[0], added[-1]] if added else None,
            "months_lost": lost,
            "k_old": co.get("k"), "k_new": cn.get("k"),
            "window_old": co.get("cal_window"), "window_new": cn.get("cal_window"),
            "k0_old": slug in k0_old, "k0_new": slug in k0_new,
            "solo_shape": _solo_shape(old_w, new, slug),
        }
        if slug in terms_old.index:
            for v, col in (("ua_term", "ukrainian"), ("ru_term", "russian")):
                if col in terms_old.columns and cn.get(v) and terms_old.loc[slug, col] != cn.get(v):
                    row[f"{v}_changed"] = [terms_old.loc[slug, col], cn.get(v)]
        why = []
        # A new term is a new measurement even where no share can be compared:
        # serhii-korolyov is k = 0 before and after, so 'Serhii Korolyov' ->
        # 'Serhii Korolov' (81e4d270) would otherwise pass with nothing to show.
        for v in ("ru_term", "ua_term"):
            if f"{v}_changed" in row:
                why.append(f"{v} changed {row[f'{v}_changed'][0]!r} -> {row[f'{v}_changed'][1]!r}")
        if row["median_abs_pp"] is not None and row["median_abs_pp"] > max_median_pp:
            why.append(f"median |d| {row['median_abs_pp']} pp > {max_median_pp}")
        if row["max_abs_pp"] is not None and row["max_abs_pp"] > max_abs_pp:
            why.append(f"max |d| {row['max_abs_pp']} pp at {row['max_month']} > {max_abs_pp}")
        if row["k0_old"] != row["k0_new"]:
            why.append(f"k = 0 status {row['k0_old']} -> {row['k0_new']}")
        if lost:
            why.append(f"{len(lost)} stored months absent from the run")
        if why:
            gate.append({"slug": slug, "why": why})
        pairs[slug] = row
    s = pd.Series(all_abs, dtype=float)
    return {"pairs": pairs, "gate": gate,
            "thresholds": {"max_median_pp": max_median_pp, "max_abs_pp": max_abs_pp},
            "overall": {"pair_months": int(len(s)),
                        "median_abs_pp": round(float(s.median()), 3) if len(s) else None,
                        "max_abs_pp": round(float(s.max()), 3) if len(s) else None}}


def merge(live: pd.DataFrame, new: pd.DataFrame) -> pd.DataFrame:
    slugs = set(new["pair_slug"])
    keep = live[~(((live["geo"] == "") | live["geo"].isna()) & live["pair_slug"].isin(slugs))]
    add = new[list(live.columns)] if set(live.columns) <= set(new.columns) else None
    if add is None:
        raise SystemExit(f"run lacks columns {sorted(set(live.columns) - set(new.columns))}")
    out = pd.concat([keep, add], ignore_index=True).sort_values(
        ["pair_slug", "geo", "date", "variant"], kind="stable").reset_index(drop=True)
    dups = int(out.duplicated(KEY).sum())
    if dups:
        raise SystemExit(f"merged file has {dups} duplicate rows on {KEY}")
    return out


def _write_like(df: pd.DataFrame, schema: pa.Schema, path: Path) -> None:
    """Write `df` with the live file's column order and types.

    The pandas metadata is the new table's own: the live file's would describe
    the live row count and index, not these rows.
    """
    t = pa.Table.from_pandas(df[schema.names], preserve_index=False)
    pq.write_table(t.cast(pa.schema([schema.field(n) for n in schema.names],
                                    metadata=t.schema.metadata)), path)


def _clean(o):
    """NaN -> None, so the report is strict JSON."""
    if isinstance(o, float) and o != o:
        return None
    if isinstance(o, dict):
        return {k: _clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_clean(v) for v in o]
    return o


def _guard_run_dir(run: Path, live_path: Path, live_dir: Path) -> None:
    """A run directory is never a live one: stage writes into it, unbacked and ungated."""
    r = run.resolve()
    ds = live_path.parent.resolve()
    if r == live_dir.resolve() or r == ds or ds in r.parents:
        raise SystemExit(f"--run {run} is a live directory; stage writes there without a backup")


def stage(run: Path, live_path: Path = LIVE_DATASET, live_cal_path: Path | None = None,
          enabled: list[str] | None = None, max_median_pp: float = MAX_MEDIAN_PP,
          max_abs_pp: float = MAX_ABS_PP) -> dict:
    live_cal_path = live_cal_path or LIVE_DIR / "calibration_report.csv"
    _guard_run_dir(run, live_path, live_cal_path.parent)
    # Hash before anything is written, and hash every run file promote will copy.
    live_sha, live_cal_sha = _sha1(live_path), _sha1(live_cal_path)
    run_files = {n: _sha1(run / n) for n in RUN_FILES}
    meta = json.loads((run / "run.json").read_text())
    new_all = pd.read_parquet(run / "trends_world_monthly.parquet")
    new_cal = pd.read_csv(run / "calibration_report.csv")
    live = pd.read_parquet(live_path)
    live_cal = pd.read_csv(live_cal_path) if live_cal_path.exists() else None
    enabled = enabled if enabled is not None else [p["slug"] for p in get_enabled_pairs()]
    through = series_end(SOURCE)

    hard = []
    if not meta.get("complete"):
        hard.append(f"run is incomplete: {meta.get('stopped') or 'pairs failed'}")
    missing = sorted(set(enabled) - set(new_all["pair_slug"]))
    if missing:
        hard.append(f"enabled pairs absent from the run: {missing}")
    # The report and the rows must describe the same pairs: the exporter takes its
    # k = 0 set from the report, so a report missing a pinned pair draws it as 0%.
    cal_slugs, world_slugs = set(new_cal["slug"].astype(str)), set(new_all["pair_slug"])
    if cal_slugs != world_slugs or cal_slugs != set(meta.get("pairs", [])):
        hard.append(f"calibration_report.csv pairs {sorted(cal_slugs)} != world-row pairs "
                    f"{sorted(world_slugs)} != run.json pairs {sorted(meta.get('pairs', []))}")
    if "ok" in new_cal.columns and not new_cal["ok"].astype(str).str.lower().eq("true").all():
        hard.append(f"calibration_report.csv has failed pairs: "
                    f"{sorted(new_cal[~new_cal['ok'].astype(str).str.lower().eq('true')]['slug'])}")
    ym = new_all["date"].astype(str).str[:7]
    open_month = date.today().strftime("%Y-%m")
    if (ym >= open_month).any():
        hard.append(f"{int((ym >= open_month).sum())} rows in or after the open month {open_month}")
    if (ym > meta["end"]).any():
        hard.append(f"{int((ym > meta['end']).sum())} rows after the run's end {meta['end']}")

    held = new_all[ym > through]
    new = new_all[ym <= through]
    report = diff(live, new, live_cal, new_cal, max_median_pp, max_abs_pp)
    merged = merge(live, new)
    out = run / "raw_trends.parquet"
    _write_like(merged, pq.read_schema(live_path), out)
    hm = held["date"].astype(str).str[:7]
    report.update({
        "staged_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "run": meta, "hard": hard, "series_end": through,
        "held_back": {"after": through, "rows": int(len(held)),
                      "range": [hm.min(), hm.max()] if len(held) else None},
        "run_files": run_files,
        "live": {"path": str(live_path), "sha1": live_sha, "rows": len(live)},
        "live_cal": {"path": str(live_cal_path), "sha1": live_cal_sha},
        "staged": {"path": str(out), "sha1": _sha1(out), "rows": len(merged),
                   "world_rows_replaced": int(len(_world(live)[_world(live)["pair_slug"]
                                                               .isin(set(new["pair_slug"]))])),
                   "world_rows_added": int(len(new)),
                   "rows_carried": int(len(merged) - len(new))},
    })
    report = _clean(report)
    (run / "stage_report.json").write_text(json.dumps(report, indent=1, default=str))
    return report


def print_report(r: dict) -> None:
    print(f"{'pair':<22} {'n':>4} {'med|d|':>7} {'p90':>7} {'max|d|':>7} {'at':>8} "
          f"{'bias':>7} {'k old':>9} {'k new':>9} {'+mo':>4}  window")
    for slug, p in r["pairs"].items():
        f = lambda v: "-" if v is None else f"{v:.2f}"           # noqa: E731
        k = lambda v: "-" if v is None or v != v else f"{float(v):.4g}"  # noqa: E731
        win = "" if p["window_old"] == p["window_new"] else f"{p['window_old']} -> {p['window_new']}"
        print(f"{slug:<22} {p['months_compared']:>4} {f(p['median_abs_pp']):>7} "
              f"{f(p['p90_abs_pp']):>7} {f(p['max_abs_pp']):>7} {p['max_month'] or '-':>8} "
              f"{f(p['mean_signed_pp']):>7} {k(p['k_old']):>9} {k(p['k_new']):>9} "
              f"{p['months_added']:>4}  {win}")
    o = r["overall"]
    print(f"overall: {o['pair_months']} pair-months, median |d| {o['median_abs_pp']} pp, "
          f"max |d| {o['max_abs_pp']} pp")
    hb = r.get("held_back") or {}
    if hb.get("rows"):
        print(f"HELD BACK: {hb['rows']} rows {hb['range'][0]}..{hb['range'][1]}, after "
              f"series_end('{SOURCE}') {hb['after']}; stage again after SOURCE_SERIES_END moves")
    for h in r.get("hard", []):
        print(f"HARD: {h}")
    for g in r["gate"]:
        print(f"GATE: {g['slug']}: {'; '.join(g['why'])}")
    if r["gate"]:
        print(f"promote names each reviewed pair: --accept {','.join(g['slug'] for g in r['gate'])}")


def promote_files(items: list[tuple[Path, Path, str]], stamp: str, backups: Path) -> None:
    """Swap each (src, dst, sha1) in, all or nothing as far as the filesystem allows.

    Every source is copied beside its target and re-hashed before the first
    rename, so a changed source or a full disk stops the promote with nothing
    live touched. Each rename is atomic; if one fails, the targets already
    replaced are restored from their backups. Backups go under the source's
    data/raw/<source>/backups/, never beside the dataset: a copy in dataset/ is
    one glob away from being published.
    """
    tmps: list[Path] = []
    saved: dict[Path, Path | None] = {}
    try:
        for src, dst, sha in items:
            tmp = dst.with_name(dst.name + ".promote-tmp")
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, tmp)
            tmps.append(tmp)
            if _sha1(tmp) != sha:
                raise SystemExit(f"{src} changed during promote; nothing was replaced")
        backups.mkdir(parents=True, exist_ok=True)
        for _, dst, _ in items:
            saved[dst] = None
            if dst.exists():
                saved[dst] = backups / f"{dst.stem}.{stamp}{dst.suffix}"
                shutil.copy2(dst, saved[dst])
    except BaseException:
        for t in tmps:
            t.unlink(missing_ok=True)
        raise
    done: list[Path] = []
    try:
        for (_, dst, _), tmp in zip(items, tmps):
            os.replace(tmp, dst)
            done.append(dst)
    except BaseException:
        for dst in done:
            b = saved[dst]
            if b is None:
                dst.unlink(missing_ok=True)
            else:
                rb = dst.with_name(dst.name + ".rollback-tmp")
                shutil.copy2(b, rb)
                os.replace(rb, dst)
        for t in tmps:
            t.unlink(missing_ok=True)
        log.error(f"promote failed; restored {[str(d) for d in done]} from {backups}")
        raise


def check_accept(gate: list[dict], accept) -> None:
    """Every gated slug must be named; every name must be gated (a typo is not a review)."""
    names = {a.strip() for a in (accept or ()) if a and a.strip()}
    gated = {g["slug"]: g["why"] for g in gate}
    stray = sorted(names - set(gated))
    if stray:
        raise SystemExit(f"--accept names pairs that are not past the gate: {stray}")
    pending = [s for s in gated if s not in names]
    if pending:
        raise SystemExit("refusing: past the gate and not accepted: "
                         + "; ".join(f"{s} ({', '.join(gated[s])})" for s in pending)
                         + f". After review: --accept {','.join(pending)}")


def promote(run: Path, accept=(), live_path: Path = LIVE_DATASET,
            live_dir: Path = LIVE_DIR) -> None:
    r = json.loads((run / "stage_report.json").read_text())
    staged = Path(r["staged"]["path"])
    cal = live_dir / "calibration_report.csv"
    if r["hard"]:
        raise SystemExit("refusing: " + "; ".join(r["hard"]))
    if series_end(SOURCE) != r.get("series_end"):
        raise SystemExit(f"series_end('{SOURCE}') is {series_end(SOURCE)}, staged at "
                         f"{r.get('series_end')}; stage again")
    changed = [str(p) for p, sha in ((live_path, r["live"]["sha1"]), (cal, r["live_cal"]["sha1"]),
                                     (staged, r["staged"]["sha1"]))
               if _sha1(p) != sha]
    changed += [str(run / n) for n in RUN_FILES if _sha1(run / n) != (r.get("run_files") or {}).get(n)]
    if changed:
        raise SystemExit(f"changed since staging: {changed}; stage again")
    check_accept(r["gate"], accept)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backups = live_dir / "backups"
    rf = r["run_files"]
    promote_files([(staged, live_path, r["staged"]["sha1"]),
                   (run / "calibration_report.csv", cal, rf["calibration_report.csv"]),
                   (run / "trends_world_monthly.parquet", live_dir / "trends_world_monthly.parquet",
                    rf["trends_world_monthly.parquet"])], stamp, backups)
    hb = r.get("held_back") or {}
    log.info(f"promoted {run} -> {live_path}, {cal}; backups in {backups} ({stamp}). "
             f"Collected {r['run'].get('start')}..{r['run'].get('end')}; the dataset now runs "
             f"through {r['series_end']}"
             + (f", {hb['rows']} later rows held back until SOURCE_SERIES_END['{SOURCE}'] moves "
                f"(then stage this run again)" if hb.get("rows") else "")
             + ". Earlier months moved as the report shows; they reach the site on the next export.")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("stage")
    s.add_argument("--run", type=Path, required=True)
    s.add_argument("--live", type=Path, default=LIVE_DATASET)
    s.add_argument("--live-cal", type=Path, default=LIVE_DIR / "calibration_report.csv")
    s.add_argument("--max-median-pp", type=float, default=MAX_MEDIAN_PP)
    s.add_argument("--max-abs-pp", type=float, default=MAX_ABS_PP)
    p = sub.add_parser("promote")
    p.add_argument("--run", type=Path, required=True)
    p.add_argument("--accept", default="",
                   help="comma-separated slugs past the gate that you reviewed; each gated pair "
                        "must be named")
    args = ap.parse_args()
    if args.cmd == "stage":
        r = stage(args.run, args.live, args.live_cal, max_median_pp=args.max_median_pp,
                  max_abs_pp=args.max_abs_pp)
        print_report(r)
        print(f"report: {args.run / 'stage_report.json'}")
    else:
        promote(args.run, args.accept.split(","))


if __name__ == "__main__":
    main()
