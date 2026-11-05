"""Build the verified GDELT record, in steps whose effects can be told apart.

ATTRIBUTABLE REBUILD (the 2026 path)
------------------------------------
    python -m pipeline.build_gdelt_verified --step filters   --since 2026-01-01           # report
    python -m pipeline.build_gdelt_verified --step filters   --since 2026-01-01 --write
    python -m pipeline.build_gdelt_verified --step additions --since 2026-01-01           # report
    python -m pipeline.build_gdelt_verified --step additions --since 2026-01-01 --write

Two causes move the verified tier when 2026 text arrives, and they must not land
in one change:

  (a) filters    today's pairs.yaml / filters.py over the text already on disk,
                 EXCLUDING every URL the date-bounded fetch targeted
                 (runs/since_<day>.parquet). 22 of 24 verified files were built on
                 2026-09-10, before 18 filter and config commits; re-applying the
                 filters was measured to remove 433 rows dated 2025 or earlier
                 (kyiv -352, odesa -77, chicken-kyiv -4). The chart reads the
                 *_series files without re-filtering, so this alone moves 2010-2025.
  (b) additions  the same build over all text, so the only difference from (a) is
                 the date-bounded fetch. It refuses to write unless the files on
                 disk are exactly (a), and unless no row dated before --since
                 differs between (a) and (b).

Either step can run before or after the fetch: (a) excludes the fetch by URL, not
by time. Each prints a per-pair delta, split at TEXT_END (pipeline.config) into
history and post-window rows. Neither clamps: the verified tier is a producer and
keeps every month it has; the exporter, migrate and analyze_pair apply the window.

Only enabled pairs are built. `gdelt_verified --all-pairs` also rebuilds retired
borscht and any disabled pair that has text.

article_texts.parquet is read ONCE and sliced per pair into a temporary
directory; `gdelt_verified --all-pairs` re-reads the 1.6 GB file for every pair.

PER-PAIR FETCH + BUILD (new-pair onboarding)
--------------------------------------------
    python -m pipeline.build_gdelt_verified --pair babyn-yar --fetch-all-dates
    python -m pipeline.build_gdelt_verified --all --skip-fetch
    python -m pipeline.build_gdelt_verified --all --dry-run     # show the work, fetch nothing

For each pair: fetch the ATTESTED urls, fetch the UNATTESTED urls, build the
verified record. That fetch is unbounded in time -- it retries every URL of the
pair outside the ledger, any year -- so it needs --fetch-all-dates. Run for every
pair it re-attempts 55,722 URLs dated before 2026-08-27 and moves 2015-2025; for
2026 use `gdelt_fetch_texts --since` and the steps above.

Both pools are fetched because the series and the corpus must come from the same
records -- a pair built from one pool and charted from the other would disagree with
itself. Fetching is resumable via the url ledger.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pandas as pd

import pipeline.config as cfg

PY = sys.executable
FINAL = Path("data/raw/gdelt/mentions_v2/gdelt_mentions_final.parquet")
MATCHED = Path("data/raw/gdelt/mentions_v2/gdelt_mentions_matched.parquet")
TEXTS = Path("data/raw/gdelt/texts/article_texts.parquet")
RUNS = Path("data/raw/gdelt/texts/runs")
VERIFIED = Path("data/cl/corpus/gdelt_verified")
STAMP = "_rebuild_stamp.json"

# zaporizhzhia, 2026-09-12: 58,194 urls in 65.1 min (data/audit/verified_build.log).
URL_PER_S = 14.9
KEY = ["url", "month", "variant"]


def enabled_pairs() -> list[str]:
    return [p["slug"] for p in cfg.get_enabled_pairs()]


# ── attributable rebuild ──────────────────────────────────────────────────────

def excluded_urls(since: str | None) -> set:
    """URLs a date-bounded fetch targeted; empty when there was none."""
    if not since:
        return set()
    p = RUNS / f"since_{since}.parquet"
    return set(pd.read_parquet(p, columns=["url"]).url) if p.exists() else set()


def texts_digest(df: pd.DataFrame) -> dict:
    """month -> "rows:sha1[:16]" over the text-bearing rows (url, text_hash) a build
    read. gdelt_paths_reconcile recomputes it from article_texts to tell whether a
    corpus month was built from the texts now on disk; a count alone misses an
    in-place fill such as merge_browser_refetch."""
    t = df[df.text_hash.notna()]
    if not len(t):
        return {}
    month = pd.to_datetime(t.date).dt.to_period("M").astype(str)
    line = t.url.astype(str) + "\x1f" + t.text_hash.astype(str)
    out = {}
    for m, g in line.groupby(month):
        h = hashlib.sha1("\n".join(sorted(g)).encode()).hexdigest()[:16]
        out[m] = f"{len(g)}:{h}"
    return out


def file_sha1(path: Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def slice_texts(targets: list[str], exclude: set, tmp: Path) -> dict:
    """One streaming pass over article_texts: per pair, the full slice and, where
    the exclusion removes anything, the slice without it. File order is kept,
    which matters: gdelt_verified's dedups keep the FIRST copy, and new fetches
    are appended at the end, so an addition can never displace an older row."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(TEXTS)
    excl = pa.array(sorted(exclude), type=pa.string()) if exclude else None
    writers, out = {}, {}
    touched = set()
    if excl is not None:
        # Two narrow columns first, so only pairs that lose rows get a second slice.
        k = pq.read_table(TEXTS, columns=["url", "pair_slug"])
        touched = set(pc.unique(k.filter(pc.is_in(k.column("url"), value_set=excl))
                                .column("pair_slug")).to_pylist())
        del k

    def put(slug, kind, table):
        k = (slug, kind)
        if k not in writers:
            path = tmp / f"{slug}.{kind}.parquet"
            writers[k] = pq.ParquetWriter(path, pf.schema_arrow, compression="zstd")
            out.setdefault(slug, {})[kind] = path
        writers[k].write_table(table)

    excluded_rows = {s: 0 for s in targets}
    for batch in pf.iter_batches(batch_size=20_000):
        t = pa.Table.from_batches([batch])
        slugs = t.column("pair_slug")
        for slug in targets:
            sub = t.filter(pc.equal(slugs, slug))
            if not sub.num_rows:
                continue
            put(slug, "full", sub)
            if slug in touched:
                drop = pc.is_in(sub.column("url"), value_set=excl)
                n = pc.sum(drop).as_py() or 0
                excluded_rows[slug] += n
                if n < sub.num_rows:
                    put(slug, "a", sub.filter(pc.invert(drop)))
    for w in writers.values():
        w.close()
    for slug, kinds in out.items():
        if excluded_rows[slug] == 0:
            kinds["a"] = kinds["full"]          # nothing excluded: (a) is the full slice
        elif "a" not in kinds:
            kinds["a"] = None                   # every row of this pair came from the fetch
    return {"paths": out, "excluded_rows": excluded_rows}


@contextlib.contextmanager
def _texts(path: Path):
    """Point gdelt_verified at one pair's slice. build() reads its module-level
    TEXTS; the rules themselves stay in that single implementation."""
    from pipeline.cl.corpus import gdelt_verified as gv
    old = gv.TEXTS
    gv.TEXTS = path
    try:
        yield gv
    finally:
        gv.TEXTS = old


def build_from(path: Path | None, slug: str):
    if path is None:
        return pd.DataFrame(columns=["url", "date", "month", "variant"]), pd.DataFrame(
            columns=["month", "variant", "articles"]), {}
    with _texts(path) as gv:
        return gv.build(slug)


def _counts(df: pd.DataFrame | None) -> pd.Series:
    if df is None or not len(df):
        return pd.Series(dtype="int64")
    return df.groupby(KEY, dropna=False).size()


def delta(old: pd.DataFrame | None, new: pd.DataFrame) -> dict:
    """Row-level difference on (url, month, variant), as a multiset, split at
    TEXT_END into history (<=) and post-window (>)."""
    out = {}
    for part, keep in (("hist", lambda m: m <= cfg.TEXT_END), ("post", lambda m: m > cfg.TEXT_END)):
        o = old[keep(old.month.astype(str))] if old is not None and len(old) else None
        n = new[keep(new.month.astype(str))] if len(new) else new
        co, cn = _counts(o), _counts(n)
        j = pd.concat([co.rename("o"), cn.rename("n")], axis=1).fillna(0)
        out[f"old_{part}"] = int(co.sum())
        out[f"new_{part}"] = int(cn.sum())
        out[f"added_{part}"] = int((j.n - j.o).clip(lower=0).sum())
        out[f"removed_{part}"] = int((j.o - j.n).clip(lower=0).sum())
    return out


def same_rows(x: pd.DataFrame | None, y: pd.DataFrame | None) -> bool:
    cx, cy = _counts(x), _counts(y)
    j = pd.concat([cx.rename("x"), cy.rename("y")], axis=1).fillna(0)
    return bool((j.x == j.y).all())


def series_months_changed(old_s: pd.DataFrame | None, new_s: pd.DataFrame) -> int:
    def piv(s):
        if s is None or not len(s):
            return pd.Series(dtype="int64")
        return s.set_index(["month", "variant"]).articles
    j = pd.concat([piv(old_s).rename("o"), piv(new_s).rename("n")], axis=1).fillna(0)
    return int(j[j.o != j.n].index.get_level_values(0).nunique())


def _read(path: Path, cols=None):
    return pd.read_parquet(path, columns=cols) if path.exists() else None


def run_step(step: str, since: str | None, targets: list[str], write: bool,
             report: Path | None, tmp_dir: str | None) -> int:
    import pyarrow.parquet as pq

    prior: dict = {}
    if since:
        # A --since with no manifest excludes nothing, so step (a) would silently
        # absorb the fetch and step (b) would attribute zero rows to it: the two
        # causes this command exists to separate land in one change, unreported.
        if not (RUNS / f"since_{since}.parquet").exists():
            have = sorted(f.stem[len("since_"):] for f in RUNS.glob("since_*.parquet"))
            print(f"no fetch manifest {RUNS / f'since_{since}.parquet'}; manifests on disk: "
                  f"{', '.join(have) or 'none'}. Pass the --since the fetch ran with.",
                  file=sys.stderr)
            return 1
        stamp_path = VERIFIED / STAMP
        prior = json.loads(stamp_path.read_text()) if stamp_path.exists() else {}
        if prior.get("step") == "additions" and prior.get("since") == since:
            # A redo of (b) is only sound if the manifest gained no URLs since
            # (b) was written: a new URL would be absorbed without its own (a).
            n_now = len(excluded_urls(since))
            if step == "additions" and n_now != prior.get("manifest_urls", n_now):
                print(f"manifest since_{since} grew from {prior.get('manifest_urls')} to "
                      f"{n_now} URLs after (b) was written; fetch new URLs under a new --since",
                      file=sys.stderr)
                return 1
        if (step == "filters" and prior.get("step") == "additions"
                and prior.get("since") == since):
            # The additions of this manifest are already written. Re-running (a)
            # with it would REMOVE them again and report the removal as filter
            # drift; a later fetch must use a new --since (its own manifest).
            print(f"step (b) for --since {since} is already written "
                  f"({prior.get('written_at')}); step (a) with the same manifest would roll "
                  f"its additions back. Fetch the next range with a new --since.",
                  file=sys.stderr)
            return 1
    # (b) may be repeated for one manifest: its targets are fetched again by
    # --no-abandon without adding URLs, so (a) is unchanged and is not re-run.
    redo_b = bool(since) and step == "additions" and prior.get("step") == "additions" \
        and prior.get("since") == since
    exclude = excluded_urls(since)
    print(f"step {step}: {len(targets)} enabled pair(s); since {since or '-'}; "
          f"{len(exclude):,} URL(s) targeted by the date-bounded fetch "
          f"({'excluded' if step == 'filters' else 'included'})")
    if step == "additions" and not since:
        print("--step additions needs --since: the additions are that fetch", file=sys.stderr)
        return 1
    t0 = time.time()
    rows, refusals, summary, digests = [], [], [], {}
    # Staged next to the destination so the final swap is a same-volume rename;
    # nothing in VERIFIED changes unless every pair passed.
    staging = Path(tempfile.mkdtemp(dir=VERIFIED, prefix=".staging-")) if write else None
    try:
        with tempfile.TemporaryDirectory(dir=tmp_dir) as td:
            sl = slice_texts(targets, exclude, Path(td))
            print(f"sliced {TEXTS} once in {time.time() - t0:.0f}s")
            for slug in targets:
                paths = sl["paths"].get(slug)
                if not paths:
                    print(f"  {slug}: no fetched text on disk")
                    continue
                old_v = _read(VERIFIED / f"{slug}.parquet", ["url", "date", "month", "variant"])
                old_s = _read(VERIFIED / f"{slug}_series.parquet")
                va, sa, _ = build_from(paths["a"], slug)
                used = paths["a"] if step == "filters" else paths["full"]
                digests[slug] = (texts_digest(pd.read_parquet(used, columns=["url", "date", "text_hash"]))
                                 if used is not None else {})
                if step == "filters":
                    new_v, new_s = va, sa
                    r = {"pair": slug, **delta(old_v, va)}
                else:
                    # An untouched pair's (a) slice is its full slice: one build serves
                    # both. A full rebuild of every pair took 1,590 s (2026-10-19).
                    new_v, new_s = ((va, sa) if paths["a"] == paths["full"]
                                    else build_from(paths["full"], slug)[:2])
                    r = {"pair": slug, **delta(va, new_v)}
                    r["disk_is_a"] = same_rows(old_v, va)
                    if not r["disk_is_a"] and redo_b:
                        # A re-run of (b) for the same manifest, after a further
                        # fetch of its own targets (--no-abandon): the disk is the
                        # earlier (b), and outside the manifest's URLs it must
                        # still be exactly (a). Its own delta is reported below.
                        r["disk_is_a"] = same_rows(
                            old_v[~old_v.url.isin(exclude)] if old_v is not None else None, va)
                        r["added_vs_disk"] = delta(old_v, new_v)["added_post"]
                    cut = pd.Timestamp(since)
                    before_a = va[pd.to_datetime(va.date) < cut] if len(va) else va
                    before_b = new_v[pd.to_datetime(new_v.date) < cut] if len(new_v) else new_v
                    r["pre_since_unchanged"] = same_rows(before_a, before_b)
                    if not r["disk_is_a"]:
                        refusals.append(f"{slug}: files on disk are not step (a); "
                                        f"run --step filters --write first")
                    if not r["pre_since_unchanged"]:
                        refusals.append(f"{slug}: rows dated before {since} differ between (a) and (b)")
                r["series_months_changed"] = series_months_changed(old_s, new_s)
                r["excluded_text_rows"] = int(sl["excluded_rows"].get(slug, 0))
                if not len(new_v) and old_v is not None:
                    refusals.append(f"{slug}: the rebuild has no records but a verified file exists")
                if staging is not None and len(new_v):
                    new_v.to_parquet(staging / f"{slug}.parquet", compression="zstd", index=False)
                    new_s.to_parquet(staging / f"{slug}_series.parquet", compression="zstd", index=False)
                    vc = new_v.variant.value_counts()
                    ua, ru = int(vc.get("ukrainian", 0)), int(vc.get("russian", 0))
                    summary.append({"pair": slug, "records": len(new_v), "ua": ua, "ru": ru,
                                    "both": int(vc.get("both", 0)),
                                    "ua_%": round(ua / (ua + ru) * 100, 1) if ua + ru else None,
                                    "domains": new_v.domain.nunique(),
                                    "chartable": len(new_v) >= 30})
                rows.append(r)
                del new_v, new_s, va, sa
                print(f"  {slug:<22} done ({time.time() - t0:.0f}s)", flush=True)

        if not rows:
            print("no pair had fetched text; nothing to compare", file=sys.stderr)
            return 1
        t = pd.DataFrame(rows).set_index("pair")
        for part in ("hist", "post"):
            t[f"d_{part}"] = t[f"new_{part}"] - t[f"old_{part}"]
        num = list(t.select_dtypes("number").columns)
        t.loc["TOTAL", num] = t[num].sum()
        t[num] = t[num].astype("Int64")
        show = [c for c in ["old_hist", "new_hist", "d_hist", "added_hist", "removed_hist",
                            "old_post", "new_post", "d_post", "added_post", "removed_post",
                            "series_months_changed", "excluded_text_rows",
                            "disk_is_a", "pre_since_unchanged", "added_vs_disk"] if c in t.columns]
        base = "files on disk" if step == "filters" else "step (a)"
        print(f"\nper-pair delta against {base}; hist = month <= {cfg.TEXT_END}, post = after")
        with pd.option_context("display.width", 250, "display.max_columns", 30):
            print(t[show].to_string())
        if report:
            report.parent.mkdir(parents=True, exist_ok=True)
            report.write_text(json.dumps({"step": step, "since": since, "text_end": cfg.TEXT_END,
                                          "excluded_urls": len(exclude), "refusals": refusals,
                                          "pairs": t.reset_index().to_dict(orient="records")},
                                         indent=1, default=str))
            print(f"report -> {report}")
        if refusals:
            print("\nREFUSED:\n  " + "\n  ".join(refusals), file=sys.stderr)
            return 1
        if staging is None:
            print("\nreport only; nothing written (pass --write)")
            return 0
        for f in sorted(staging.glob("*.parquet")):
            os.replace(f, VERIFIED / f.name)
        if summary:
            pd.DataFrame(summary).sort_values("records", ascending=False).to_csv(
                VERIFIED / "_summary.csv", index=False)
        # Provenance for gdelt_paths_reconcile: which texts each corpus file was
        # built from, and the file's own hash so a later rebuild by another path
        # (gdelt_verified --all-pairs, pipeline.rebuild) reads as unstamped.
        stamp_path = VERIFIED / STAMP
        stamp = json.loads(stamp_path.read_text()) if stamp_path.exists() else {"pairs": {}}
        for r in rows:
            f = VERIFIED / f"{r['pair']}.parquet"
            if f.exists():
                stamp["pairs"][r["pair"]] = {
                    "step": step, "since": since, "corpus_sha1": file_sha1(f),
                    "new_hist": int(r["new_hist"]), "new_post": int(r["new_post"]),
                    "texts": digests.get(r["pair"], {})}
        stamp.update({"step": step, "since": since,
                      "written_at": pd.Timestamp.now(tz="UTC").isoformat(timespec="seconds"),
                      "texts_rows": pq.ParquetFile(TEXTS).metadata.num_rows,
                      "excluded_urls": len(exclude) if step == "filters" else 0,
                      "manifest_urls": len(exclude)})
        tmp_stamp = stamp_path.with_name(stamp_path.name + ".writing")
        tmp_stamp.write_text(json.dumps(stamp, indent=1))
        os.replace(tmp_stamp, stamp_path)
        print(f"\nwrote {len(summary)} pair(s) -> {VERIFIED} in {(time.time() - t0) / 60:.1f} min")
        return 0
    finally:
        if staging is not None:
            for f in staging.glob("*"):
                f.unlink()
            staging.rmdir()


# ── per-pair fetch + build ────────────────────────────────────────────────────

def workload() -> pd.DataFrame:
    """Urls per pair in each pool, so the run can be ordered and estimated."""
    att = pd.read_parquet(FINAL, columns=["url", "pair_slug"])
    m = pd.read_parquet(MATCHED, columns=["url", "pair_slug", "native_en", "url_match"])
    una = m[m.native_en & ~m.url_match].drop_duplicates("url")
    una = una[~una.url.isin(set(att.url)) & una.pair_slug.notna()]
    t = pd.DataFrame({"attested": att.groupby("pair_slug").size(),
                      "unattested": una.groupby("pair_slug").size()}).fillna(0).astype(int)
    t["total"] = t.attested + t.unattested
    return t


def run(cmd: list[str]) -> int:
    r = subprocess.run(cmd, capture_output=True, text=True)
    tail = [x for x in r.stdout.strip().split("\n") if x.strip()][-1:] or [""]
    if r.returncode:
        print(f"      FAILED: {(r.stderr or r.stdout).strip().split(chr(10))[-1][:160]}")
    else:
        print(f"      {tail[0][:150]}")
    return r.returncode


def run_per_pair(targets: list[str], a) -> int:
    if not (a.skip_fetch or a.fetch_all_dates or a.dry_run):
        print("this path fetches every unattempted URL of each pair, any year. Pass "
              "--fetch-all-dates to mean that, --skip-fetch to only rebuild, or use "
              "`gdelt_fetch_texts --since` + `--step` for a date-bounded change.", file=sys.stderr)
        return 1
    w = workload()
    plan = w.reindex(targets).fillna(0).astype(int).sort_values("total")   # smallest first
    plan["est_min"] = (plan.total / URL_PER_S / 60).round(1)
    print(plan.to_string())
    print(f"\n{len(plan)} pair(s), {int(plan.total.sum()):,} urls in the pools, "
          f"at most ~{plan.est_min.sum():.0f} min at {URL_PER_S} url/s (the ledger skips done urls)")
    if a.dry_run:
        return 0

    t0 = time.time()
    for i, slug in enumerate(plan.index, 1):
        n = int(plan.loc[slug, "total"])
        print(f"\n[{i}/{len(plan)}] {slug}  ({n:,} urls)", flush=True)
        if not a.skip_fetch:
            print("   attested…", flush=True)
            run([PY, "-W", "ignore", "-m", "pipeline.ingestion.gdelt_fetch_texts",
                 "--pair", slug, "--concurrency", str(a.concurrency)])
            print("   unattested…", flush=True)
            run([PY, "-W", "ignore", "-m", "pipeline.ingestion.gdelt_fetch_texts",
                 "--pair", slug, "--unattested", "--concurrency", str(a.concurrency)])
        print("   building verified record…", flush=True)
        run([PY, "-m", "pipeline.cl.corpus.gdelt_verified", "--pair", slug])

    print(f"\nfinished in {(time.time() - t0) / 60:.1f} min")
    rows = []
    for f in sorted(VERIFIED.glob("*.parquet")):
        if f.stem.endswith("_series"):
            continue
        d = pd.read_parquet(f)
        vc = d.variant.value_counts()
        ua, ru = int(vc.get("ukrainian", 0)), int(vc.get("russian", 0))
        rows.append({"pair": f.stem, "records": len(d), "ua": ua, "ru": ru,
                     "both": int(vc.get("both", 0)),
                     "ua_%": round(ua / (ua + ru) * 100, 1) if ua + ru else None,
                     "domains": d.domain.nunique(),
                     "chartable": len(d) >= 30})
    if rows:
        summary = pd.DataFrame(rows).sort_values("records", ascending=False)
        print("\n" + summary.to_string(index=False))
        VERIFIED.mkdir(parents=True, exist_ok=True)
        summary.to_csv(VERIFIED / "_summary.csv", index=False)
        print(f"\n{int(summary.records.sum()):,} verified records across {len(summary)} pair(s); "
              f"{int(summary.chartable.sum())} meet the 30-record chart threshold")
    print("\nnext: python -m pipeline.rebuild")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--pair")
    g.add_argument("--pairs", help="comma-separated slugs")
    g.add_argument("--all", action="store_true")
    ap.add_argument("--step", choices=["filters", "additions"],
                    help="attributable rebuild over every enabled pair (or --pair/--pairs)")
    ap.add_argument("--since", metavar="YYYY-MM-DD",
                    help="with --step: the date-bounded fetch whose URLs separate (a) from (b)")
    ap.add_argument("--write", action="store_true", help="with --step: write the files")
    ap.add_argument("--report", type=Path, help="with --step: also write the delta as JSON")
    ap.add_argument("--tmp-dir", help="with --step: where to slice article_texts (default: system temp)")
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--dry-run", action="store_true", help="print the plan and stop")
    ap.add_argument("--skip-fetch", action="store_true",
                    help="rebuild verified records from text already on disk")
    ap.add_argument("--fetch-all-dates", action="store_true",
                    help="per-pair fetch of every unattempted URL, any year (new-pair onboarding)")
    a = ap.parse_args()

    enabled = enabled_pairs()
    if a.pair:
        targets = [a.pair]
    elif a.pairs:
        targets = [x.strip() for x in a.pairs.split(",") if x.strip()]
    elif a.all or a.step:
        targets = enabled
    else:
        ap.error("give --step, or one of --pair / --pairs / --all")
    unknown = [t for t in targets if t not in enabled]
    if unknown:
        print(f"not enabled in config/pairs.yaml: {', '.join(unknown)}", file=sys.stderr)
        return 1

    if a.step:
        return run_step(a.step, a.since, targets, a.write, a.report, a.tmp_dir)
    return run_per_pair(targets, a)


if __name__ == "__main__":
    raise SystemExit(main())
