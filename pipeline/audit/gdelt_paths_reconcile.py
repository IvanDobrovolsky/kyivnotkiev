"""GDELT paths reconciliation: what each tier holds, per pair and month, and
whether the verified series can be plotted through series_end('gdelt').

Three data paths answer "how much GDELT does this pair have" and all are live
(CLAUDE.md). This audit puts them side by side for every enabled pair and month
from --from (default: the January before HEADLINE_YEAR + 1, i.e. 2025 and 2026):

  matched   gdelt_mentions_matched, native English + URL-attested ROWS (re-recordings
            counted; pair from flag_rows over all 47 pairs)
  final     gdelt_mentions_final, one row per attested URL, enabled-pair attribution
  dataset   dataset/raw_gdelt.parquet, sum(count): what export_site_data._load reads
  processed data/store/gdelt_processed.parquet rows
  pairfile  data/store/pairs/<slug>.parquet rows with source == gdelt (the verified
            corpus re-filtered and clamped by migrate)
  corpus    data/cl/corpus/gdelt_verified/<slug>.parquet rows
  series    data/cl/corpus/gdelt_verified/<slug>_series.parquet sum(articles): the chart

The tiers measure different things, so most differences are expected. Reported as
disagreements are the ones that are not: dataset != final (a lossless aggregation),
pairfile != corpus up to TEXT_END (filter drift, or a pair file built from another
corpus), any pair-file row after TEXT_END (keyness reads the pair files and is frozen
at TEXT_END), and series != corpus. Every tier's last month is printed too. Producers keep everything
they collected; the window is applied by consumers, so 2026 rows in a producer tier
are not a disagreement.

EXIT 1 -- the rule
------------------
Let S = pipeline.config.series_end('gdelt') (or --series-end), and the extension
months E = the months after TEXT_END up to S (empty while S <= TEXT_END). The
audit exits 1 if any of these holds for any enabled pair:

  R1 series/corpus. For some month <= S and variant, the series' `articles`
     differs from the number of corpus rows. The chart plots the series, and
     gdelt_verified.build() derives it from the corpus, so a difference means one
     file is stale. Fewer is the case that hides data; more is equally impossible
     by construction, so both fail.

  R2 a trailing partial month, any of:
     R2a collection. The matched parquet's max gkg_date is before the last GKG
         file of S (last_day(S) 23:45:00): the scan stops inside a plotted month.
         Today 20260913023000, so S = 2026-09 fails until the increment is appended.
     R2b fetch. Some URL of the pair's fetch pool whose earliest date falls in a
         month of E has no row in article_texts and is not in the ledger: it was
         never even queued. The pool is gdelt_fetch_texts.since_pool, the same
         function the date-bounded fetch uses, so after
         `gdelt_fetch_texts --since <first day of E>` this count is 0. Example: 22
         pairs stop on 2026-08-26 because the 2026-09-10 fetch read a matched file
         ending 08-26 23:45; 7,848 enabled-pair URLs dated from 2026-08-27 were
         never queued, so 2026-08 and 2026-09 fail once S reaches them. Months
         <= TEXT_END are not checked here: the historical backlog is a separate
         job (decided 2026-10-15).
     R2c build. For a month of E the corpus file was not written by
         pipeline.build_gdelt_verified (its sha1 does not match the rebuild stamp),
         or, for any month <= S of a stamped corpus, the text-bearing article_texts
         rows (count and a digest of url + text_hash) differ from the ones the
         corpus was built from. A corpus built before its texts were fetched or
         refilled reads as a dip, not as missing data.

     R2d derived tiers. An attested enabled-pair URL dated in E is in matched
         but not in gdelt_mentions_final (final was not re-run after an append,
         so R2b's attested pool cannot see it), or dataset/raw_gdelt differs from
         final in a month <= S (export not re-run).

Exit 0 otherwise. Disagreements outside these rules are printed but do not fail.

    python -m pipeline.audit.gdelt_paths_reconcile
    python -m pipeline.audit.gdelt_paths_reconcile --series-end 2026-09   # before bumping SOURCE_SERIES_END
    python -m pipeline.audit.gdelt_paths_reconcile --pair kyiv --json /path/out.json

R2b reads the url column of the matched parquet (about 1.3 GB in memory) when E
is not empty; run it under the heavy lock.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

import pandas as pd

import pipeline.config as cfg

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
MATCHED = ROOT / "data/raw/gdelt/mentions_v2/gdelt_mentions_matched.parquet"
FINAL = ROOT / "data/raw/gdelt/mentions_v2/gdelt_mentions_final.parquet"
TEXTS = ROOT / "data/raw/gdelt/texts/article_texts.parquet"
LEDGER = ROOT / "data/raw/gdelt/texts/fetched_urls.txt"
VERIFIED = ROOT / "data/cl/corpus/gdelt_verified"
STORE = ROOT / "data/store"
DATASET = ROOT / "dataset/raw_gdelt.parquet"
TIERS = ["matched", "final", "dataset", "processed", "pairfile", "corpus", "series"]


def _next_month(ym: str) -> str:
    return (pd.Period(ym, "M") + 1).strftime("%Y-%m")


def extension_months(series_end: str) -> list[str]:
    if series_end <= cfg.TEXT_END:
        return []
    return [p.strftime("%Y-%m") for p in pd.period_range(_next_month(cfg.TEXT_END), series_end, freq="M")]


def _month(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s).dt.strftime("%Y-%m")


# ── tiers ─────────────────────────────────────────────────────────────────────

def tier_counts(slugs: list[str], frm: str) -> pd.DataFrame:
    """(pair, month) x tier counts for months >= frm."""
    import pyarrow.parquet as pq
    parts = {}
    ts = pd.Timestamp(frm + "-01")
    if MATCHED.exists():
        m = pq.read_table(MATCHED, columns=["pair_slug", "date", "native_en", "url_match"],
                          filters=[("date", ">=", ts)]).to_pandas()
        m = m[m.native_en & m.url_match & m.pair_slug.isin(slugs)]
        parts["matched"] = m.groupby([m.pair_slug, _month(m.date)]).size()
    if FINAL.exists():
        f = pd.read_parquet(FINAL, columns=["pair_slug", "date"])
        f = f[(f.date >= ts) & f.pair_slug.isin(slugs)]
        parts["final"] = f.groupby([f.pair_slug, _month(f.date)]).size()
    if DATASET.exists():
        d = pd.read_parquet(DATASET, columns=["pair_slug", "date", "count"])
        d = d[d.pair_slug.isin(slugs)]
        d["month"] = d.date.astype(str).str[:7]
        d = d[d.month >= frm]
        parts["dataset"] = d.groupby(["pair_slug", "month"])["count"].sum()
    gp = STORE / "gdelt_processed.parquet"
    if gp.exists():
        p = pd.read_parquet(gp, columns=["pair_slug", "date"])
        p = p[p.pair_slug.isin(slugs)]
        p["month"] = p.date.astype(str).str[:7]
        p = p[p.month >= frm]
        parts["processed"] = p.groupby(["pair_slug", "month"]).size()
    pf, co, se = [], [], []
    for slug in slugs:
        f = STORE / "pairs" / f"{slug}.parquet"
        if f.exists():
            x = pd.read_parquet(f, columns=["source", "date"])
            x = x[x.source == "gdelt"]
            x = x.assign(month=x.date.astype(str).str[:7])
            pf.append(x[x.month >= frm].groupby("month").size().rename(slug))
        c = VERIFIED / f"{slug}.parquet"
        if c.exists():
            x = pd.read_parquet(c, columns=["month"])
            co.append(x[x.month >= frm].groupby("month").size().rename(slug))
        s = VERIFIED / f"{slug}_series.parquet"
        if s.exists():
            x = pd.read_parquet(s)
            se.append(x[x.month >= frm].groupby("month").articles.sum().rename(slug))
    for name, lst in (("pairfile", pf), ("corpus", co), ("series", se)):
        if lst:
            parts[name] = pd.concat({s.name: s for s in lst}, names=["pair_slug", "month"])
    out = pd.concat({k: v for k, v in parts.items()}, axis=1).fillna(0).astype("int64")
    out.index = out.index.set_names(["pair", "month"])
    return out.reindex(columns=[t for t in TIERS if t in out.columns]).sort_index()


def last_months(slugs: list[str]) -> pd.DataFrame:
    """Each tier's last month per pair, over the whole file (not just >= --from)."""
    rows = {s: {} for s in slugs}
    f = pd.read_parquet(FINAL, columns=["pair_slug", "date"]) if FINAL.exists() else None
    t = pd.read_parquet(TEXTS, columns=["pair_slug", "date"]) if TEXTS.exists() else None
    for s in slugs:
        if f is not None and (f.pair_slug == s).any():
            rows[s]["final"] = f.date[f.pair_slug == s].max().strftime("%Y-%m-%d")
        if t is not None and (t.pair_slug == s).any():
            rows[s]["texts"] = pd.Timestamp(t.date[t.pair_slug == s].max()).strftime("%Y-%m-%d")
        c = VERIFIED / f"{s}.parquet"
        if c.exists():
            x = pd.read_parquet(c, columns=["date"])
            if len(x):
                rows[s]["corpus"] = pd.Timestamp(x.date.max()).strftime("%Y-%m-%d")
        p = STORE / "pairs" / f"{s}.parquet"
        if p.exists():
            x = pd.read_parquet(p, columns=["source", "date"])
            x = x[x.source == "gdelt"]
            if len(x):
                rows[s]["pairfile"] = str(x.date.max())[:10]
    return pd.DataFrame.from_dict(rows, orient="index")


# ── exit rules ────────────────────────────────────────────────────────────────

def r1_series_vs_corpus(slugs: list[str], S: str) -> list[str]:
    out = []
    for slug in slugs:
        c, s = VERIFIED / f"{slug}.parquet", VERIFIED / f"{slug}_series.parquet"
        if not c.exists() and not s.exists():
            continue
        if c.exists() != s.exists():
            out.append(f"R1 {slug}: {'series' if c.exists() else 'corpus'} file missing")
            continue
        cc = pd.read_parquet(c, columns=["month", "variant"])
        cc = cc[cc.month <= S].groupby(["month", "variant"]).size()
        ss = pd.read_parquet(s)
        ss = ss[ss.month <= S].set_index(["month", "variant"]).articles
        j = pd.concat([cc.rename("corpus"), ss.rename("series")], axis=1).fillna(0).astype(int)
        bad = j[j.corpus != j.series]
        if len(bad):
            fewer = bad[bad.series < bad.corpus]
            out.append(f"R1 {slug}: series differs from its corpus in {bad.index.get_level_values(0).nunique()} "
                       f"month(s) <= {S} ({len(fewer)} with fewer rows than the corpus); "
                       f"first {bad.index[0][0]} {bad.index[0][1]}: series {bad.series.iloc[0]} "
                       f"vs corpus {bad.corpus.iloc[0]}")
    return out


def r2a_collection(S: str) -> list[str]:
    from pipeline.ingestion.gdelt_mentions import matched_watermark
    wm = matched_watermark(MATCHED)
    need = int(cfg.last_day(S).replace("-", "") + "234500")
    if wm is None:
        return [f"R2a no matched parquet at {MATCHED}"]
    if wm < need:
        return [f"R2a collection: matched watermark {wm} is before the last GKG file of {S} ({need}); "
                f"append an increment covering it"]
    return []


def r2b_fetch(slugs: list[str], E: list[str]) -> tuple[list[str], pd.DataFrame]:
    """Never-queued pool URLs per pair and month of E."""
    if not E:
        return [], pd.DataFrame()
    from pipeline.ingestion import gdelt_fetch_texts as ft
    since = E[0] + "-01"
    hi = pd.Timestamp(cfg.last_day(E[-1])) + pd.Timedelta(days=1)
    enabled = set(slugs)
    # Queued means a request was sent (the ledger) or text is held (browser and
    # wayback recoveries skip the ledger). A bare article_texts row is not
    # enough: host_abandoned / host_backoff rows are written for URLs that were
    # never requested and kept out of the ledger so they can be tried again,
    # and counting them as queued let a never-requested URL pass this gate.
    with _rooted(ft):
        ledger, with_text = ft.attempted_urls()
    queued = ledger | with_text
    del ledger, with_text
    frames = []
    for pool, unatt in (("attested", False), ("unattested", True)):
        with _rooted(ft):
            p = ft.since_pool(since, unatt, enabled)
        p = p[p.date < hi]
        frames.append(p.assign(pool=pool, queued=p.url.isin(queued)))
    pool = pd.concat(frames, ignore_index=True)
    pool["month"] = pool.date.dt.strftime("%Y-%m")
    miss = pool[~pool.queued]
    t = (pool.groupby(["pair_slug", "month", "pool"]).queued.agg(urls="size", queued="sum")
         .reset_index())
    t["never_queued"] = t["urls"] - t["queued"]
    out = []
    for slug, g in miss.groupby("pair_slug"):
        months = sorted(g.month.unique())
        out.append(f"R2b {slug}: {len(g):,} pool URL(s) never queued in {', '.join(months)} "
                   f"(attested {int((g.pool == 'attested').sum()):,}, "
                   f"unattested {int((g.pool == 'unattested').sum()):,}); "
                   f"run gdelt_fetch_texts --since {since}")
    return out, t


def r2d_derived(slugs: list[str], S: str, E: list[str], t: pd.DataFrame) -> list[str]:
    """The derived GDELT tiers are re-run after every append.

    `final` is what the attested fetch pool reads, so a final that predates an
    append hides the appended attested URLs from R2b: the gate passed with
    September attested URLs that were never fetched. Checked by URL, not by
    count: every native-English, URL-attested matched URL dated in E whose URL
    carries an ENABLED term must be in final (final drops only rows that belong
    to disabled pairs alone). dataset/raw_gdelt is a lossless aggregation of
    final and what the exporter's per-source numbers read, so it must equal
    final in every month <= S of the comparison table.
    """
    if not E:
        return []
    out = []
    if not (MATCHED.exists() and FINAL.exists()):
        return [f"R2d derived tiers: {'matched' if not MATCHED.exists() else 'final'} missing"]
    import re
    import pyarrow.parquet as pq
    from pipeline.ingestion.gdelt_mentions import _longest_first, load_terms
    on = set(slugs)
    rx = re.compile("(" + "|".join(_longest_first(f for s, _, _, f in load_terms() if s in on)) + ")")
    m = pq.read_table(MATCHED, columns=["url", "date"],
                      filters=[("date", ">=", pd.Timestamp(E[0] + "-01")),
                               ("date", "<", pd.Timestamp(cfg.last_day(E[-1])) + pd.Timedelta(days=1)),
                               ("native_en", "==", True), ("url_match", "==", True)]).to_pandas()
    urls = pd.Series(m.url.dropna().unique())
    urls = urls[urls.str.lower().str.contains(rx)]
    have = set(pd.read_parquet(FINAL, columns=["url"]).url)
    missing = urls[~urls.isin(have)]
    if len(missing):
        out.append(f"R2d final: {len(missing):,} attested enabled-pair URL(s) dated "
                   f"{E[0]}..{E[-1]} in matched are not in gdelt_mentions_final; re-run "
                   f"`gdelt_mentions final` and `export` after the append (e.g. {missing.iloc[0]})")
    if {"dataset", "final"} <= set(t.columns):
        month = t.index.get_level_values("month")
        bad = t[(month <= S) & (t.dataset != t["final"])]
        if len(bad):
            out.append(f"R2d dataset: dataset/raw_gdelt differs from final in {len(bad)} pair-month(s) "
                       f"<= {S} (first {bad.index[0][0]} {bad.index[0][1]}: final "
                       f"{int(bad['final'].iloc[0])}, dataset {int(bad.dataset.iloc[0])}); "
                       f"re-run `gdelt_mentions export`")
    return out


class _rooted:
    """gdelt_fetch_texts uses repo-relative paths; point them at ROOT."""
    def __init__(self, mod):
        self.mod = mod

    def __enter__(self):
        m = self.mod
        self.saved = (m.SRC, m.MATCHED, m.OUT, m.LEDGER, m.RESULTS)
        m.SRC, m.MATCHED = FINAL, MATCHED
        m.OUT, m.LEDGER, m.RESULTS = TEXTS.parent, LEDGER, TEXTS

    def __exit__(self, *a):
        m = self.mod
        m.SRC, m.MATCHED, m.OUT, m.LEDGER, m.RESULTS = self.saved


def r2c_build(slugs: list[str], S: str, E: list[str]) -> tuple[list[str], dict]:
    from pipeline.build_gdelt_verified import STAMP, file_sha1, texts_digest
    sp = VERIFIED / STAMP
    stamp = json.loads(sp.read_text()).get("pairs", {}) if sp.exists() else {}
    texts = (pd.read_parquet(TEXTS, columns=["pair_slug", "url", "date", "text_hash"])
             if stamp and TEXTS.exists() else None)
    out, status = [], {}
    for slug in slugs:
        c = VERIFIED / f"{slug}.parquet"
        st = stamp.get(slug)
        stamped = bool(st) and c.exists() and file_sha1(c) == st.get("corpus_sha1")
        status[slug] = "stamped" if stamped else "unstamped"
        if not stamped:
            if E:
                out.append(f"R2c {slug}: the corpus is not stamped by pipeline.build_gdelt_verified, so "
                           f"its {E[0]}..{E[-1]} months have no build provenance; rebuild with --step")
            continue
        if texts is None:
            continue
        now = texts_digest(texts[texts.pair_slug == slug])
        was = st.get("texts", {})
        bad = sorted(m for m in set(now) | set(was) if m <= S and now.get(m) != was.get(m))
        if bad:
            status[slug] = "stale"
            out.append(f"R2c {slug}: built from different texts than article_texts now holds in "
                       f"{len(bad)} month(s) <= {S}: {', '.join(bad[:6])}{' ...' if len(bad) > 6 else ''}")
    return out, status


# ── report ────────────────────────────────────────────────────────────────────

def disagreements(t: pd.DataFrame) -> dict:
    out = {}
    if {"dataset", "final"} <= set(t.columns):
        d = t[t.dataset != t["final"]]
        out["dataset_vs_final"] = d[["final", "dataset"]]
    if {"pairfile", "corpus"} <= set(t.columns):
        # The pair files feed keyness, which is frozen at TEXT_END, so inside it
        # they should equal the corpus (a gap is filter drift: today's filters
        # re-applied by migrate to a corpus built before them), and after it they
        # should hold nothing (anything there is 2026 text leaking into keyness).
        month = t.index.get_level_values("month")
        d = t[(month <= cfg.TEXT_END) & (t.pairfile != t.corpus)]
        out["pairfile_vs_corpus_text_window"] = d[["corpus", "pairfile"]]
        out["pairfile_after_text_end"] = t[(month > cfg.TEXT_END) & (t.pairfile > 0)][["corpus", "pairfile"]]
    if {"series", "corpus"} <= set(t.columns):
        d = t[t.series != t.corpus]
        out["series_vs_corpus"] = d[["corpus", "series"]]
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="frm", default=f"{cfg.HEADLINE_YEAR}-01",
                    help="first month of the per-month comparison (default HEADLINE_YEAR-01)")
    ap.add_argument("--series-end", help="judge as if series_end('gdelt') were this month")
    ap.add_argument("--pair", help="one enabled pair")
    ap.add_argument("--json", type=pathlib.Path, help="write the full table and verdicts here")
    a = ap.parse_args()

    slugs = [p["slug"] for p in cfg.get_enabled_pairs()]
    if a.pair:
        if a.pair not in slugs:
            print(f"{a.pair} is not an enabled pair", file=sys.stderr)
            return 2
        slugs = [a.pair]
    if a.series_end and not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", a.series_end):
        # Months compare as strings everywhere here; "2026-9" sorts after
        # "2026-10" and its last_day is "2026-9-30", which turned R2a off.
        print(f"--series-end must be YYYY-MM, got {a.series_end!r}", file=sys.stderr)
        return 2
    S = a.series_end or cfg.series_end("gdelt")
    E = extension_months(S)
    print(f"GDELT reconcile: {len(slugs)} enabled pair(s); series_end('gdelt') = {S}"
          f"{' (override)' if a.series_end else ''}; TEXT_END {cfg.TEXT_END}; "
          f"extension months {E[0] + '..' + E[-1] if E else 'none'}")

    t = tier_counts(slugs, a.frm)
    years = sorted({m[:4] for m in t.index.get_level_values("month")})
    print(f"\nrows per tier, by year (months >= {a.frm})")
    by_year = t.groupby([t.index.get_level_values("pair"), t.index.get_level_values("month").str[:4]]).sum()
    wide = by_year.unstack(1)
    wide.columns = [f"{tier}_{y}" for tier, y in wide.columns]
    cols = [f"{tier}_{y}" for y in years for tier in t.columns if f"{tier}_{y}" in wide.columns]
    with pd.option_context("display.width", 250, "display.max_columns", 40):
        print(wide.reindex(columns=cols).fillna(0).astype(int).to_string())
        print("\nlast month with rows, per tier (whole files)")
        print(last_months(slugs).to_string())

    dis = disagreements(t)
    print()
    for name, d in dis.items():
        if not len(d):
            print(f"{name}: agree in every pair-month >= {a.frm}")
            continue
        per = d.groupby(level="pair").size()
        print(f"{name}: {len(d):,} pair-month(s) disagree across {len(per)} pair(s): "
              + ", ".join(f"{p} {n}" for p, n in per.sort_values(ascending=False).items()))
        with pd.option_context("display.width", 250):
            print("  " + d.head(12).to_string().replace("\n", "\n  "))

    fails = r1_series_vs_corpus(slugs, S) + r2a_collection(S)
    r2b, pool = r2b_fetch(slugs, E)
    r2c, status = r2c_build(slugs, S, E)
    fails += r2b + r2c + r2d_derived(slugs, S, E, t)
    if len(pool):
        print("\nfetch coverage of the extension months (R2b)")
        print(pool.groupby("pair_slug")[["urls", "queued", "never_queued"]].sum().to_string())
    print("\nbuild provenance (R2c): " + ", ".join(f"{k} {v}" for k, v in
                                                  pd.Series(status).value_counts().items()))

    if a.json:
        a.json.parent.mkdir(parents=True, exist_ok=True)
        a.json.write_text(json.dumps({
            "series_end": S, "text_end": cfg.TEXT_END, "extension_months": E, "from": a.frm,
            "violations": fails, "build_status": status,
            "table": t.reset_index().to_dict(orient="records"),
            "disagreements": {k: v.reset_index().to_dict(orient="records") for k, v in dis.items()},
            "fetch_coverage": pool.to_dict(orient="records") if len(pool) else [],
        }, indent=1, default=str))
        print(f"-> {a.json}")

    if fails:
        print(f"\nFAIL ({len(fails)}):\n  " + "\n  ".join(fails))
        return 1
    print(f"\nPASS: the verified GDELT series can be plotted through {S}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
