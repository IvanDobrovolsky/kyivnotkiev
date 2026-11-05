"""Stage Wikipedia pageviews into dataset/raw_wikipedia.parquet, diff, then promote.

    stage    copy the live dataset, relabel variants from wikipedia_titles.py,
             append the run's months that are not stored yet, write a diff
             report. Touches nothing live.
    promote  re-check the report, back up the live files, replace them.

RELABEL. Every stored row of an enabled pair gets its variant from the declared
title map, not from the row. That is the dynamo-kyiv fix at the data layer:
252 stored rows (FC_Dynamo_Kyiv and FC_Dynamo_Kiev, 126 months each) swap
variants. `stage --relabel-only` does that with no run at all, so the fix can
ship on its own, ahead of and separately from any 2026 month.

APPEND, DON'T OVERWRITE. Months already stored keep their stored value; the
run supplies only months the dataset lacks. Closed months barely move (Kyiv
2026-04: 68,345 collected on 2026-05-04 against 68,342 now), but "barely" is
measured here, not assumed: the report compares the run with every stored month
and with the never-merged 2026-05-04 CSVs in data/raw/wikipedia/, and promotion
refuses unless the pair is named in --accept if any of its titles' overlap
total drifts more than 0.5%.

BACKFILL IS GATED. A month the run supplies at or before the pair's last
stored month rewrites shipped history rather than extending it. serhii-korolyov's
redirect 'Serhii_Korolov' adds 39 such months (2022-10..2025-12) and is meant
to; the mechanism would take any other back-fill as silently, so each one needs
its slug in --accept.

HELD BACK. Months after series_end('wikipedia') are not staged, so the dataset
never holds a month the config has not admitted: migrate's processed tier clamps
at SERIES_END rather than per source, and rebuild.push_hf publishes dataset/
with no date clamp. After SOURCE_SERIES_END['wikipedia'] moves, stage the same
run again (no requests) and promote; promote refuses if it moved since staging.

promote copies only files stage hashed (the staged dataset, and the run's
titles.json, fetched.parquet and run.json), copies all of them before swapping
any in, and restores what it replaced if a swap fails.

A title that has stored rows but came back empty is a failed fetch, never a
zero, and refuses promotion outright. A title recorded 'missing' (ihor-sikorsky's
'Ihor Sikorsky') gets no rows; titles.json says so, so the exporter can drop the
pair instead of drawing fill_value=0 as 0.0% adoption.

Usage:
    python -m pipeline.ingestion.wikipedia_merge stage --run DIR --relabel-only
    python -m pipeline.ingestion.wikipedia_merge stage --run data/raw/wikipedia/staging/2015-07_2026-09
    python -m pipeline.ingestion.wikipedia_merge promote --run DIR [--accept SLUG,...]
"""

import argparse
import hashlib
import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.config import ROOT_DIR, get_enabled_pairs, series_end
from pipeline.ingestion import wikipedia_titles as wt
# One copy of the promote machinery: the copy-verify-swap-rollback and the
# per-slug --accept check are the safety-critical part, and two copies drift.
from pipeline.ingestion.trends_merge import check_accept, promote_files

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SOURCE = "wikipedia"
LIVE_DATASET = ROOT_DIR / "dataset" / "raw_wikipedia.parquet"
RUN_FILES = ("fetched.parquet", "titles.json", "run.json")
WIKI_RAW_DIR = ROOT_DIR / "data" / "raw" / "wikipedia"
LIVE_TITLES = WIKI_RAW_DIR / "titles.json"       # the exporter's record of measurability
KEY = ["pair_slug", "date", "variant"]           # rebuild.validate_datasets' key
MAX_REL_DRIFT = 0.005


def _sha1(p: Path) -> str | None:
    return hashlib.sha1(p.read_bytes()).hexdigest() if p.exists() else None


def _canon(title: str) -> str:
    return wt.display_title(str(title))


def relabel(live: pd.DataFrame, enabled: set[str]) -> tuple[pd.DataFrame, dict]:
    """Variant from the declared map for every stored row of an enabled, distinct-title pair."""
    df = live.copy()
    changes, unknown = {}, []
    for slug in sorted(enabled & set(wt.TITLES)):
        if wt.is_shared(slug):
            continue
        by_title = {_canon(t): v for v, t in wt.TITLES[slug].items()}
        rows = df["pair_slug"] == slug
        want = df.loc[rows, "page_title"].map(lambda t: by_title.get(_canon(t)))
        bad = df.loc[rows & want.reindex(df.index).isna(), "page_title"].unique().tolist()
        if bad:
            unknown.append(f"{slug}: stored titles {bad} are not in wikipedia_titles.py")
            continue
        moved = rows & (want.reindex(df.index) != df["variant"])
        if moved.any():
            g = df[moved].groupby(["page_title", "variant"]).size()
            changes[slug] = [{"page_title": t, "from": v, "to": by_title[_canon(t)], "rows": int(n)}
                             for (t, v), n in g.items()]
            df.loc[moved, "variant"] = want[moved[rows]].values
    if unknown:
        raise SystemExit("; ".join(unknown))
    return df, changes


def _share(df: pd.DataFrame, slug: str, months: list[str]) -> dict:
    g = df[(df["pair_slug"] == slug) & df["date"].str[:7].isin(months)]
    p = g.groupby([g["date"].str[:7], "variant"])["pageviews"].sum().unstack(fill_value=0)
    out = {}
    for m in months:
        if m in p.index:
            u, r = float(p.loc[m].get("ukrainian", 0)), float(p.loc[m].get("russian", 0))
            out[m] = round(u / (u + r) * 100, 1) if u + r else None
    return out


def legacy_csv(csv_dir: Path) -> pd.DataFrame:
    """The never-merged collections in data/raw/wikipedia/, keyed by ARTICLE.

    Their variant column is the list-position rule that inverted dynamo-kyiv, so
    it is never read. Three headers exist: 16 files (2026-05-04) carry
    article/views, pair_20 and pair_34 carry article/pageviews, pair_70 carries
    page_title/pageviews.
    """
    frames = []
    for f in sorted(csv_dir.glob("pair_*_monthly.csv")):
        d = pd.read_csv(f)
        title = "article" if "article" in d.columns else "page_title"
        views = "views" if "views" in d.columns else "pageviews"
        if title not in d.columns or views not in d.columns:
            log.warning(f"  {f.name}: no title/views columns, skipped")
            continue
        frames.append(d[["date", title, views]].set_axis(["date", "article", "views"], axis=1))
    if not frames:
        return pd.DataFrame(columns=["canon", "date", "views"])
    d = pd.concat(frames, ignore_index=True)
    d["canon"] = d["article"].map(_canon)
    return d.drop_duplicates(["canon", "date"])[["canon", "date", "views"]]


def drift(stored: pd.DataFrame, fetched: pd.DataFrame, legacy: pd.DataFrame,
          max_rel: float = MAX_REL_DRIFT) -> tuple[dict, list]:
    """Per (slug, variant): how the run compares with what is stored.

    Returns (rows, {slug: [reasons]}).
    """
    out, gate = {}, {}
    for (slug, variant), f in fetched.groupby(["pair_slug", "variant"]):
        s = stored[(stored["pair_slug"] == slug) & (stored["variant"] == variant)]
        j = s.set_index("date")["pageviews"].rename("stored").to_frame().join(
            f.set_index("date")["pageviews"].rename("now"), how="inner")
        row = {"title": f["page_title"].iloc[0], "overlap_months": int(len(j))}
        if len(j):
            d = (j["now"] - j["stored"])
            rel = d.abs() / j["stored"].where(j["stored"] > 0)
            tot_s, tot_n = float(j["stored"].sum()), float(j["now"].sum())
            row.update({"months_differing": int((d != 0).sum()),
                        "max_abs": float(d.abs().max()),
                        "max_rel": round(float(rel.max()), 6) if rel.notna().any() else None,
                        "max_rel_month": str(rel.idxmax())[:7] if rel.notna().any() else None,
                        "total_stored": tot_s, "total_now": tot_n,
                        "total_rel": round(abs(tot_n - tot_s) / tot_s, 6) if tot_s else None})
            if row["total_rel"] is not None and row["total_rel"] > max_rel:
                gate.setdefault(slug, []).append(
                    f"{variant} overlap total drifts {row['total_rel']:.4%} > {max_rel:.2%}")
        lj = legacy[legacy["canon"] == _canon(row["title"])].set_index("date")["views"].rename(
            "csv").to_frame().join(f.set_index("date")["pageviews"].rename("now"), how="inner")
        if len(lj):
            ld = lj["now"] - lj["csv"]
            # Months the CSVs hold and the dataset does not are the only check
            # those months get before they are appended (Kyiv 2026-04: 68,345 then,
            # 68,342 now). Chosen by what is stored, not by a calendar year.
            have = set(s["date"])
            row["vs_2026_05_04_csv"] = {
                "months": int(len(lj)), "months_differing": int((ld != 0).sum()),
                "max_abs": float(ld.abs().max()),
                "unstored": {m[:7]: [float(lj.loc[m, "csv"]), float(lj.loc[m, "now"])]
                             for m in lj.index if m not in have}}
        out[f"{slug}/{variant}"] = row
    return out, gate


def _guard_run_dir(run: Path, live_path: Path) -> None:
    """A run directory is never a live one: stage writes into it, unbacked and ungated.

    `stage --relabel-only --run dataset` used to write raw_wikipedia.parquet over
    the live file in place, with no backup, and hash it afterwards so the report
    claimed live == staged (2026-10-19 review, on a copy).
    """
    r = run.resolve()
    ds = live_path.parent.resolve()
    if r == ds or ds in r.parents or r == WIKI_RAW_DIR.resolve():
        raise SystemExit(f"--run {run} is a live directory; stage writes there without a backup")


def _last3(ym: str) -> list[str]:
    return [str(p) for p in pd.period_range(end=ym, periods=3, freq="M")]


def stage(run: Path, live_path: Path = LIVE_DATASET, relabel_only: bool = False,
          csv_dir: Path = WIKI_RAW_DIR, enabled: list[str] | None = None,
          max_rel: float = MAX_REL_DRIFT) -> dict:
    _guard_run_dir(run, live_path)
    live_sha = _sha1(live_path)                      # before anything is written
    run_files = {} if relabel_only else {n: _sha1(run / n) for n in RUN_FILES}
    through = series_end(SOURCE)
    enabled_set = set(enabled if enabled is not None else [p["slug"] for p in get_enabled_pairs()])
    live = pd.read_parquet(live_path)
    hard: list[str] = []
    gate: dict[str, list[str]] = {}
    relabelled, changes = relabel(live, enabled_set)
    report: dict = {"staged_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                    "relabel_only": relabel_only, "relabelled": changes, "series_end": through}
    probe = _last3(min(through, live["date"].astype(str).str[:7].max()))
    report["relabel_probe"] = probe
    report["relabel_effect"] = {s: {"before": _share(live, s, probe),
                                    "after": _share(relabelled, s, probe)} for s in changes}
    merged = relabelled
    if not relabel_only:
        fetched = pd.read_parquet(run / "fetched.parquet")
        titles = json.loads((run / "titles.json").read_text())
        meta = json.loads((run / "run.json").read_text())
        report["run"] = meta
        open_month = date.today().strftime("%Y-%m")
        if (fetched["date"].str[:7] >= open_month).any():
            hard.append(f"run has rows in or after the open month {open_month}")
        if (fetched["date"].str[:7] > meta["end"]).any():
            hard.append(f"run has rows after its end {meta['end']}")
        for slug in sorted(enabled_set - set(titles)):
            hard.append(f"{slug}: enabled but absent from the run")
        if set(fetched["pair_slug"]) - set(titles):
            hard.append(f"rows for pairs titles.json lacks: {sorted(set(fetched['pair_slug']) - set(titles))}")
        # A stored series that the run returned nothing for is a failed fetch.
        for slug, e in titles.items():
            for v in wt.VARIANTS:
                had = ((relabelled["pair_slug"] == slug) & (relabelled["variant"] == v)).any()
                if had and e[v]["months"] == 0:
                    hard.append(f"{slug}/{v}: {e[v]['title']!r} has stored rows but the run "
                                f"returned none (http {e[v]['http']}, status {e[v]['status']})")
        # Same (slug, variant) must keep one title, or two pages would share a series.
        st = relabelled.groupby(["pair_slug", "variant"])["page_title"].agg(
            lambda s: sorted({_canon(t) for t in s}))
        for (slug, v), f in fetched.groupby(["pair_slug", "variant"]):
            if (slug, v) in st.index and st[(slug, v)] != [_canon(f["page_title"].iloc[0])]:
                hard.append(f"{slug}/{v}: stored title {st[(slug, v)]} != run title "
                            f"{f['page_title'].iloc[0]!r}")
        report["drift"], gate = drift(relabelled, fetched, legacy_csv(csv_dir), max_rel)
        have = set(map(tuple, relabelled[KEY].astype(str).values))
        new = fetched[[tuple(r) not in have for r in fetched[KEY].astype(str).values]]
        ym = new["date"].str[:7]
        held, new = new[ym > through], new[ym <= through]
        # The pair's last stored month, either variant; a pair with no rows at all
        # extends from the dataset's last month.
        last = relabelled.groupby("pair_slug")["date"].max().str[:7].to_dict()
        top = relabelled["date"].max()[:7]
        back = new[new["date"].str[:7] <= new["pair_slug"].map(lambda s: last.get(s, top))]

        def spans(df):
            return {f"{s}/{v}": {"rows": int(len(g)), "first": g["date"].min()[:7],
                                 "last": g["date"].max()[:7]}
                    for (s, v), g in df.groupby(["pair_slug", "variant"])}
        report["appended"] = spans(new)
        report["backfilled"] = spans(back)
        report["held_back"] = {"after": through, "by_title": spans(held), "rows": int(len(held))}
        for k, b in report["backfilled"].items():
            gate.setdefault(k.split("/")[0], []).append(
                f"{k.split('/')[1]} back-fills {b['rows']} months {b['first']}..{b['last']} "
                f"inside stored history")
        merged = pd.concat([relabelled, new[list(live.columns)]], ignore_index=True)
        report["titles"] = titles
        report["not_measurable"] = {s: ("shared article" if e["shared_article"] else
                                        "missing title: " + ", ".join(
                                            e[v]["title"] for v in wt.VARIANTS
                                            if e[v]["status"] == "missing"))
                                    for s, e in titles.items() if not e["measurable"]}
    report["gate"] = [{"slug": k, "why": v} for k, v in sorted(gate.items())]
    dups = int(merged.duplicated(KEY).sum())
    if dups:
        hard.append(f"{dups} duplicate rows on {KEY}")
    merged = merged.sort_values(["pair_slug", "variant", "date"], kind="stable").reset_index(drop=True)
    run.mkdir(parents=True, exist_ok=True)
    out = run / "raw_wikipedia.parquet"
    schema = pq.read_schema(live_path)
    t = pa.Table.from_pandas(merged[schema.names], preserve_index=False)
    pq.write_table(t.cast(pa.schema([schema.field(n) for n in schema.names],
                                    metadata=t.schema.metadata)), out)
    report.update({"hard": hard, "run_files": run_files,
                   "live": {"path": str(live_path), "sha1": live_sha, "rows": len(live)},
                   "staged": {"path": str(out), "sha1": _sha1(out), "rows": len(merged),
                              "rows_added": int(len(merged) - len(live))}})
    (run / "stage_report.json").write_text(json.dumps(report, indent=1, default=str))
    return report


def print_report(r: dict) -> None:
    for slug, ch in r["relabelled"].items():
        for c in ch:
            print(f"RELABEL {slug}: {c['page_title']!r} {c['from']} -> {c['to']} ({c['rows']} rows)")
        eff = r["relabel_effect"][slug]
        pr = r.get("relabel_probe") or ["", ""]
        print(f"        share {pr[0]}..{pr[-1]} before {eff['before']} after {eff['after']}")
    for k, d in (r.get("drift") or {}).items():
        line = (f"{k:<34} overlap {d['overlap_months']:>3}"
                + (f" differing {d['months_differing']:>3} max|d| {d['max_abs']:>7.0f}"
                   f" total drift {d['total_rel']}" if d["overlap_months"] else ""))
        c = d.get("vs_2026_05_04_csv")
        if c:
            line += f" | vs csv {c['months']} mo, {c['months_differing']} differ, unstored {c['unstored']}"
        print(line)
    for k, a in (r.get("appended") or {}).items():
        print(f"APPEND {k}: {a['rows']} rows {a['first']}..{a['last']}")
    hb = r.get("held_back") or {}
    for k, a in (hb.get("by_title") or {}).items():
        print(f"HELD BACK {k}: {a['rows']} rows {a['first']}..{a['last']} (after series_end "
              f"{hb['after']}; stage again once SOURCE_SERIES_END['{SOURCE}'] moves)")
    for s, why in (r.get("not_measurable") or {}).items():
        print(f"NOT MEASURABLE {s}: {why}")
    for h in r["hard"]:
        print(f"HARD: {h}")
    for g in r["gate"]:
        print(f"GATE: {g['slug']}: {'; '.join(g['why'])}")
    if r["gate"]:
        print(f"promote names each reviewed pair: --accept {','.join(g['slug'] for g in r['gate'])}")
    print(f"live {r['live']['rows']:,} rows -> staged {r['staged']['rows']:,} "
          f"(+{r['staged']['rows_added']:,})")


def promote(run: Path, accept=(), live_path: Path = LIVE_DATASET,
            titles_path: Path = LIVE_TITLES, backups: Path = WIKI_RAW_DIR / "backups") -> None:
    r = json.loads((run / "stage_report.json").read_text())
    staged = Path(r["staged"]["path"])
    if r["hard"]:
        raise SystemExit("refusing: " + "; ".join(r["hard"]))
    if series_end(SOURCE) != r.get("series_end"):
        raise SystemExit(f"series_end('{SOURCE}') is {series_end(SOURCE)}, staged at "
                         f"{r.get('series_end')}; stage again")
    changed = [str(p) for p, sha in ((live_path, r["live"]["sha1"]), (staged, r["staged"]["sha1"]))
               if _sha1(p) != sha]
    if not r["relabel_only"]:
        changed += [str(run / n) for n in RUN_FILES
                    if _sha1(run / n) != (r.get("run_files") or {}).get(n)]
    if changed:
        raise SystemExit(f"changed since staging: {changed}; stage again")
    check_accept(r["gate"], accept)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    items = [(staged, live_path, r["staged"]["sha1"])]
    if not r["relabel_only"]:
        items.append((run / "titles.json", titles_path, r["run_files"]["titles.json"]))
    promote_files(items, stamp, backups)
    log.info(f"promoted {staged} -> {live_path}"
             + ("" if r["relabel_only"] else f", {titles_path}")
             + f"; backups in {backups} ({stamp}). The dataset runs through {r['series_end']}"
             + (f"; {r['held_back']['rows']} later rows wait for SOURCE_SERIES_END['{SOURCE}']"
                if (r.get("held_back") or {}).get("rows") else ""))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("stage")
    s.add_argument("--run", type=Path, required=True,
                   help="the collector's directory (with --relabel-only, a new staging directory; "
                        "never dataset/ or data/raw/wikipedia/ itself)")
    s.add_argument("--live", type=Path, default=LIVE_DATASET)
    s.add_argument("--relabel-only", action="store_true",
                   help="apply the declared variants to stored rows; no run needed")
    s.add_argument("--csv-dir", type=Path, default=WIKI_RAW_DIR)
    s.add_argument("--max-rel-drift", type=float, default=MAX_REL_DRIFT)
    p = sub.add_parser("promote")
    p.add_argument("--run", type=Path, required=True)
    p.add_argument("--accept", default="",
                   help="comma-separated slugs past the gate that you reviewed; each gated pair "
                        "must be named")
    args = ap.parse_args()
    if args.cmd == "stage":
        problems = wt.check(get_enabled_pairs())
        if problems:
            raise SystemExit("wikipedia_titles.py: " + "; ".join(problems))
        r = stage(args.run, args.live, args.relabel_only, args.csv_dir,
                  max_rel=args.max_rel_drift)
        print_report(r)
        print(f"report: {args.run / 'stage_report.json'}")
    else:
        promote(args.run, args.accept.split(","))


if __name__ == "__main__":
    main()
