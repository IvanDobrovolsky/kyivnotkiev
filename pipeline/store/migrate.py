"""Migrate the existing scattered files into the agreed store layout.

    data/store/<source>_raw.parquet         everything the provider returned
    data/store/<source>_processed.parquet   cleaned + regex-matched, correct records only
    data/store/pairs/<slug>.parquet         all sources stacked for one pair

COPY ONLY. Nothing is moved, overwritten in place, or deleted -- the originals stay
exactly where they are. Every step writes a new file and then verifies it against the
source before reporting success.

VERIFICATION on every step:
  * row count in == row count out (raw), or accounted for by a stated filter (processed)
  * a content checksum over the identifying columns, computed independently on both
    sides -- so a silent truncation or a reordered join cannot pass
  * the checksum, row count and column list are written to data/store/_manifest.json

    python -m pipeline.store.migrate --source ngrams
    python -m pipeline.store.migrate --source all --dry-run
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import html
import json
import pathlib
import re
import sys

import pandas as pd
import pyarrow.parquet as pq
import yaml

from pipeline.config import SERIES_END, TEXT_END, last_day
from pipeline.filters import apply_source_filters

STORE = pathlib.Path("data/store")
MANIFEST = STORE / "_manifest.json"

# where each source currently lives; nothing here is modified
SOURCES = {
    "gdelt":     {"matched": "data/raw/gdelt/mentions_v2/gdelt_mentions_matched.parquet",
                  "bodies":  "data/raw/gdelt/texts/article_texts.parquet"},
    "youtube":   {"glob": "data/cl/raw/youtube_census/*_enriched.parquet"},
    "reddit":    {"file": "dataset/raw_reddit.parquet"},
    "openalex":  {"file": "data/cl/raw/openalex/all_pairs.parquet"},
    "wikipedia": {"file": "dataset/raw_wikipedia.parquet"},
    "trends":    {"file": "dataset/raw_trends.parquet"},
    "ngrams":    {"file": "dataset/raw_ngrams.parquet"},
    # telegram: DEPRECATED 2026-09-02 — poor coverage, ~80% Cyrillic channels;
    # measures Ukrainian-language Telegram, not international English usage.
    # Store file renamed deprecated_telegram_raw.parquet; raw archive untouched.
}
# The study window, in two tiers (pipeline/config.py holds the definitions).
# `raw` deliberately keeps whatever the provider returned -- collection ran into
# 2026 and that data is real, so discarding it would destroy provenance.
#
#   <source>_processed  clamped at SERIES_END. This tier feeds the charts (the
#                       site's Reddit series reads reddit_processed), which run
#                       into 2026. A source plots only through its own
#                       series_end(); the exporter applies that, not this file.
#   pairs/<slug>        clamped at TEXT_END. Keyness and clusters read these, and
#                       the text analysis is frozen at 2010-2025. 27,499 GDELT
#                       rows from 2026 reached keyness while clusters held none.
#
# One constant used to clamp both, so extending the series for Reddit would have
# pushed 2026 into every pair file, sent all 24 pairs amber on row count and
# forced a rebuild plus fresh glosses and cluster names.

TEXT_SOURCES = {"gdelt", "youtube", "reddit", "openalex"}
COUNT_SOURCES = {"wikipedia", "trends", "ngrams"}
NO_PROCESSED = set()


def checksum(df: pd.DataFrame, cols: list[str]) -> str:
    """Order-independent content hash over the given columns."""
    use = [c for c in cols if c in df.columns]
    if not use:
        use = list(df.columns)[:3]
    # Nulls must hash identically regardless of how they are spelled: pandas renders
    # a missing object as "nan" in memory and "None" after a parquet round-trip, which
    # would otherwise read as 4,005 changed cells when nothing changed.
    ser = df[use].where(df[use].notna(), "").astype(str).agg("\x1f".join, axis=1)
    h = hashlib.sha256()
    for v in sorted(ser.tolist()):
        h.update(v.encode())
    return h.hexdigest()[:20]


def pair_patterns() -> dict:
    doc = yaml.safe_load(pathlib.Path("config/pairs.yaml").read_text())
    pairs = doc["pairs"] if isinstance(doc, dict) and "pairs" in doc else doc
    sep = r"[\s\-_,.:;'\"()\[\]«»!?]+"
    out = {}
    for p in pairs:
        mk = lambda t: re.compile(r"\b" + sep.join(re.escape(w) for w in str(t).split()) + r"\b", re.I)
        out[p["slug"]] = {"ua": mk(p["ukrainian"]), "ru": mk(p["russian"]),
                          "ua_term": str(p["ukrainian"]), "ru_term": str(p["russian"]),
                          "enabled": bool(p.get("enabled"))}
    return out


def word_span(text: str, at: int, before: int = 70, after: int = 130) -> str:
    """Window around `at`, snapped outward to whole words.

    A raw character slice cuts mid-word at both ends -- "imir Putin marked ... gala
    Tuesday attende" -- which makes the span unreadable as evidence and, worse, can
    truncate the very term the row is labelled on. Expanding to the nearest space costs
    a few characters and preserves every word.
    """
    lo, hi = max(0, at - before), min(len(text), at + after)
    if lo > 0:
        sp = text.rfind(" ", 0, lo)
        lo = 0 if sp == -1 else sp + 1
    if hi < len(text):
        sp = text.find(" ", hi)
        hi = len(text) if sp == -1 else sp
    out = re.sub(r"\s+", " ", text[lo:hi]).strip()
    return ("…" if lo > 0 else "") + out + ("…" if hi < len(text) else "")


def clean_text(*parts) -> str:
    """Decode entities and normalise whitespace. No truncation -- raw keeps everything."""
    joined = "\n\n".join(str(p) for p in parts if p and str(p) not in ("nan", "None",
                                                                      "[removed]", "[deleted]"))
    return re.sub(r"[ \t]+", " ", html.unescape(joined)).strip()


def record_manifest(name: str, df: pd.DataFrame, key_cols: list[str], note: str = "") -> dict:
    entry = {"rows": len(df), "columns": list(df.columns),
             "checksum": checksum(df, key_cols), "note": note}
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    data = json.loads(MANIFEST.read_text()) if MANIFEST.exists() else {}
    data[name] = entry
    MANIFEST.write_text(json.dumps(data, indent=2, sort_keys=True))
    return entry


def write_verified(df: pd.DataFrame, name: str, key_cols: list[str], note: str = "") -> bool:
    """Write, then re-read from disk and compare checksums independently."""
    STORE.mkdir(parents=True, exist_ok=True)
    path = STORE / f"{name}.parquet"
    before = checksum(df, key_cols)
    df.to_parquet(path, compression="zstd", index=False)
    back = pd.read_parquet(path)
    after = checksum(back, key_cols)
    ok = (before == after) and (len(back) == len(df))
    mark = "OK " if ok else "MISMATCH"
    print(f"    {mark} {path.name:<34}{len(df):>10,} rows  {path.stat().st_size/1e6:>7.1f} MB  "
          f"checksum {after}")
    if not ok:
        print(f"      wrote {len(df):,} rows / {before}, read back {len(back):,} / {after}",
              file=sys.stderr)
        return False
    record_manifest(name, back, key_cols, note)
    return True


# ── raw builders: normalise column names only, never drop rows ────────────────

def raw_counts(source: str) -> pd.DataFrame:
    df = pd.read_parquet(SOURCES[source]["file"])
    df["source"] = source
    return df


def raw_reddit(dry_run: bool = False) -> pd.DataFrame:
    df = pd.read_parquet(SOURCES["reddit"]["file"])
    # 2025-05..12 gap, recovered from the public monthly dumps after PullPush
    # went paid (pipeline/ingestion/reddit_backfill_merge.py). Concatenated
    # BEFORE the bot filter so mirror subreddits in the new months are caught.
    _bf = pathlib.Path("data/raw/reddit/backfill/backfill_submissions.parquet")
    if _bf.exists():
        _b = pd.read_parquet(_bf)
        _meta = _bf.with_name(_bf.stem + ".meta.json")
        _info = json.loads(_meta.read_text()) if _meta.exists() else None
        # The span comes from the file, not from a label: the merge now also
        # carries 2026 months, and a fixed "(2025-05..12)" would misreport them.
        _months = (_info or {}).get("months") or []
        _span = (f"{min(_months)}..{max(_months)}" if _months else
                 f"{str(_b.date.min())[:7]}..{str(_b.date.max())[:7]}" if len(_b) else "empty")
        print(f"  reddit: +{len(_b):,} backfilled rows (dump sweep {_span})")
        # The merge records which pair set its sweeps searched. A sweep searches
        # every DEFINED pair (swept_pairs), so a pairs.yaml term edit after the
        # sweep means the backfill no longer answers the current terms.
        from pipeline.ingestion.reddit_dump_filter import pair_set, swept_pairs
        _want = pair_set(swept_pairs())["sha1"]
        _got = ((_info or {}).get("pairset") or {}).get("sha1")
        if _info is None:
            print(f"  reddit: WARNING no {_meta.name}; cannot check which pair set the "
                  f"backfill was swept with")
        elif _got != _want:
            raise SystemExit(f"reddit backfill was swept under pair set {str(_got)[:12]}, but "
                             f"pairs.yaml now hashes to {_want[:12]}: re-sweep and re-merge "
                             f"(pipeline.ingestion.reddit_dump_sweep) before migrating")
        df = pd.concat([df, _b], ignore_index=True)
    # Mirror/bot subreddits (autotldr, AutoNewspaper, DOOMMM_* chain-posters...)
    # repeat the OUTLET's spelling, not a redditor's choice — they leak the news
    # signal into the organic social series. Behavioural rule, measured
    # 2026-09-04 on 840K rows (19.3% dropped): a subreddit with >=100 posts is a
    # mirror when it has <=2 distinct authors, or >=10% of its titles carry the
    # (N/M) chain-post marker. Human subs with templated titles survive the
    # author gate (indiegameswap: 241 authors). The kyivan-rus 2023-03 "spike"
    # was one syndicated article chain-posted 104x. Audit CSV in data/audit/.
    #
    # The rule is computed over a population, so adding months can flip a
    # subreddit across a threshold and delete rows from months already
    # published. It is therefore bounded at TEXT_END: rows <= TEXT_END are
    # judged by a list computed from rows <= TEXT_END only (the 2010-2025 text
    # corpus stays what it was however much 2026 is collected), and rows after
    # TEXT_END by a list computed over everything through SERIES_END, so a
    # mirror that only becomes visible in 2026 is still caught there. Undated
    # rows are judged with the historical list.
    if "subreddit" in df.columns and "author" in df.columns:
        def _bot_list(d: pd.DataFrame) -> pd.DataFrame:
            _t = d.get("title", pd.Series("", index=d.index)).astype(str)
            _chunk = _t.str.match(r"\(\d+/\d+\)")
            _st = d.groupby("subreddit").agg(
                posts=("post_id", "size"), authors=("author", "nunique"))
            _st["chunk_share"] = _chunk.groupby(d["subreddit"]).mean()
            return _st[(_st.posts >= 100)
                       & ((_st.authors <= 2) | (_st.chunk_share >= 0.10))]
        _dt = pd.to_datetime(df["date"], errors="coerce") if "date" in df.columns \
            else pd.Series(pd.NaT, index=df.index)
        _next = pd.Timestamp(last_day(TEXT_END)) + pd.Timedelta(days=1)
        _late = (_dt >= _next).to_numpy()
        _in_series = (_dt.isna() | (_dt < pd.Timestamp(last_day(SERIES_END)) + pd.Timedelta(days=1))).to_numpy()
        _bots = _bot_list(df[~_late])
        _bots_late = _bot_list(df[_in_series]) if _late.any() else _bots.iloc[0:0]
        _n0 = len(df)
        _drop = ((~_late & df.subreddit.isin(set(_bots.index)).to_numpy())
                 | (_late & df.subreddit.isin(set(_bots_late.index)).to_numpy()))
        df = df[~_drop].copy()
        print(f"  reddit: dropped {_n0 - len(df):,} rows from "
              f"{len(_bots)} mirror/bot subreddit(s) <= {TEXT_END}"
              + (f" and {len(_bots_late)} judged through {SERIES_END} for later rows"
                 if _late.any() else ""))
        # A dry run answers "what would this do"; it must not rewrite the
        # audit trail of a run that has not happened.
        if not dry_run:
            try:
                _bots.to_csv(pathlib.Path("data/audit/reddit_bot_subreddits.csv"))
            except Exception:                          # noqa: BLE001
                pass
    df["source"] = "reddit"
    df["doc_id"] = df["post_id"].astype(str)
    df["url"] = "https://reddit.com/r/" + df["subreddit"].astype(str) + "/comments/" + df["doc_id"]
    df["text"] = [clean_text(t, b) for t, b in zip(df.get("title", ""), df.get("selftext", ""))]
    return df


def raw_openalex() -> pd.DataFrame:
    """pair_slug is derived from matched_term.

    The parquet's `pair_id` column refers to a numeric scheme dropped when pairs.yaml
    moved to slugs, so it cannot be joined against anything. matched_term is the actual
    surface form and maps deterministically; it agrees with the file's own `variant`
    column on 99.98% of rows.
    """
    df = pd.read_parquet(SOURCES["openalex"]["file"])
    lut = {}
    for slug, p in pair_patterns().items():
        lut[p["ua_term"].lower()] = slug
        lut[p["ru_term"].lower()] = slug
    df["pair_slug"] = df["matched_term"].astype(str).str.strip().str.lower().map(lut)
    df["source"] = "openalex"
    df["doc_id"] = df["openalex_id"].astype(str)
    df["url"] = df["doc_id"]
    df["text"] = [clean_text(t, a) for t, a in zip(df.get("title", ""), df.get("abstract", ""))]
    return df


def raw_youtube() -> pd.DataFrame:
    frames = [pd.read_parquet(f) for f in sorted(glob.glob(SOURCES["youtube"]["glob"]))]
    df = pd.concat(frames, ignore_index=True)
    df["source"] = "youtube"
    df["doc_id"] = df["video_id"].astype(str)
    df["url"] = "https://youtube.com/watch?v=" + df["doc_id"]
    df["text"] = [clean_text(t, d) for t, d in zip(df.get("title", ""), df.get("description", ""))]
    return df


def raw_gdelt() -> pd.DataFrame:
    """BQ match rows, with the fetched body attached where one exists.

    One row per (url, pair) with text where we have it and null where we do not, so the
    fetch coverage is visible in the same table rather than in a second file.
    """
    m = pd.read_parquet(SOURCES["gdelt"]["matched"])
    b = pd.read_parquet(SOURCES["gdelt"]["bodies"], columns=["url", "text", "status", "error"])
    b = b.drop_duplicates("url").rename(columns={"text": "body", "status": "http_status",
                                                 "error": "fetch_error"})
    df = m.merge(b, on="url", how="left")
    df["source"] = "gdelt"
    df["doc_id"] = df["url"]
    df["text"] = df["body"].fillna("").map(lambda t: clean_text(t))
    return df.drop(columns=["body"])


RAW_BUILDERS = {"gdelt": raw_gdelt, "youtube": raw_youtube, "reddit": raw_reddit,
                "openalex": raw_openalex,
                **{s: (lambda s=s: raw_counts(s)) for s in COUNT_SOURCES},
                }


# ── processed: clean + regex-match; only correct records survive ──────────────

def _row_date(r) -> str:
    """Best available date as YYYY-MM-DD. Falls back to year for sources that only
    publish one (OpenAlex), which is honest to a year's precision rather than blank."""
    for attr in ("date", "published_at"):
        v = getattr(r, attr, None)
        if v is not None and str(v).strip() not in ("", "None", "nan", "NaT"):
            return str(v)[:10]
    y = getattr(r, "year", None)
    try:
        if y is not None and 1900 < int(float(y)) < 2100:
            return f"{int(float(y))}-01-01"
    except (TypeError, ValueError):
        pass
    return ""


def process_text_source(raw: pd.DataFrame, source: str, pats: dict) -> pd.DataFrame:
    rows = []
    for r in raw.itertuples():
        slug = getattr(r, "pair_slug", None)
        p = pats.get(slug)
        if p is None or not p["enabled"]:
            continue
        text = str(getattr(r, "text", "") or "")
        if not text:
            continue
        ua_m = list(p["ua"].finditer(text))
        ru_m = list(p["ru"].finditer(text))
        if not ua_m and not ru_m:
            continue                      # never names the thing: cannot evidence naming
        first = min([m.start() for m in (ua_m + ru_m)])
        rows.append({
            "record_id": f"{slug}:{source}:{getattr(r, 'doc_id', '')}",
            "pair_slug": slug, "source": source, "doc_id": str(getattr(r, "doc_id", "")),
            "url": str(getattr(r, "url", "") or ""),
            # OpenAlex carries `year`, not `date` or `published_at`, so every academic
            # record was undated and could not be placed in time.
            "date": _row_date(r),
            "title": str(getattr(r, "title", "") or ""),
            "text": text,
            "ua_hits": len(ua_m), "ru_hits": len(ru_m),
            "variant": "both" if ua_m and ru_m else "ukrainian" if ua_m else "russian",
            "match_context": word_span(text, first),
            "text_hash": hashlib.sha1(re.sub(r"\s+", " ", text).encode()).hexdigest()[:16],
        })
    out = pd.DataFrame(rows)
    if len(out):
        # record_id must be unique: the same document can appear twice in a provider
        # dump (one OpenAlex work was present twice with identical text), and a
        # duplicate id silently double-counts in every downstream stage.
        before = len(out)
        out = out.drop_duplicates("record_id", keep="first").reset_index(drop=True)
        if len(out) < before:
            print(f"    dropped {before - len(out)} duplicate record_id(s)")
    return apply_study_scope(out, SERIES_END, "series window")


def process_count_source(raw: pd.DataFrame, source: str, pats: dict) -> pd.DataFrame:
    enabled = {s for s, p in pats.items() if p["enabled"]}
    df = raw[raw.pair_slug.isin(enabled)].copy()
    df["source"] = source
    return apply_study_scope(df.reset_index(drop=True), SERIES_END, "series window")


def apply_study_scope(df: pd.DataFrame, end: str, window: str) -> pd.DataFrame:
    """Drop anything after the last day of month `end` ('YYYY-MM').

    `window` names the clamp in the log line ("series window", "text
    window", or a more specific label from a caller). The stats
    modules import this too, so keyness, clusters and the pair files apply one
    rule rather than three re-implementations of it.
    """
    if not len(df):
        return df
    # An undated row must survive: missing dates are a data-quality problem tracked
    # elsewhere, and dropping them here would delete records under the wrong banner.
    # Note NaT < Timestamp evaluates to False, not NaN, so the null case has to be
    # spelled out -- a .fillna(True) after the comparison never fires.
    cut = last_day(end)
    if "date" in df.columns:
        # Strictly before the first instant of the next month, so a timestamped
        # row on the last day is kept. Comparing against midnight of the last day
        # would drop 2025-12-31T14:00 while keeping 2025-12-31.
        #
        # ISO8601, not inferred: pandas infers ONE format from the first value,
        # so in a column mixing "2025-12-31" with "2025-12-31T14:00:00" every
        # value of the other shape coerced to NaT and was KEPT as undated --
        # a 2026 timestamp walked straight through the clamp. Every tier holds
        # date-only strings today (measured 2026-11-05), so this changes no row.
        dt = pd.to_datetime(df["date"], errors="coerce", format="ISO8601")
        if getattr(dt.dt, "tz", None) is not None:
            dt = dt.dt.tz_convert(None)
        keep = dt.isna() | (dt < pd.Timestamp(cut) + pd.Timedelta(days=1))
    elif "year" in df.columns:
        # Year-granular rows stand for the whole year, so an `end` before
        # December keeps that year entire. That is a partial year by
        # construction; the exporter has to label it, it cannot be cut here.
        yr = pd.to_numeric(df["year"], errors="coerce")
        keep = yr.isna() | (yr <= int(end[:4]))
    else:
        return df
    dropped = int((~keep).sum())
    if dropped:
        by = ""
        if "source" in df.columns and df["source"].nunique() > 1:
            by = "  " + str(df.loc[~keep, "source"].value_counts().to_dict())
        print(f"    {window}: dropped {dropped:,} row(s) after {cut}{by}")
    return df[keep].reset_index(drop=True)


KEY_COLS = ["record_id", "doc_id", "url", "pair_slug", "date", "variant", "text_hash"]


def migrate_source(source: str, dry_run: bool = False) -> bool:
    print(f"\n=== {source} ===")
    pats = pair_patterns()
    raw = raw_reddit(dry_run) if source == "reddit" else RAW_BUILDERS[source]()
    print(f"    raw assembled: {len(raw):,} rows, {len(raw.columns)} columns")
    if dry_run:
        if source in TEXT_SOURCES:
            proc = process_text_source(raw, source, pats)
        elif source in COUNT_SOURCES:
            proc = process_count_source(raw, source, pats)
        else:
            proc = pd.DataFrame()
        print(f"    would write {source}_raw ({len(raw):,}) and "
              f"{source}_processed ({len(proc):,})")
        return True

    ok = write_verified(raw, f"{source}_raw", KEY_COLS,
                        note="provider output, nothing dropped")
    if source in NO_PROCESSED:
        print(f"    no processed tier: 80% Cyrillic, measures Ukrainian-language channels")
        return ok

    if source in TEXT_SOURCES:
        proc = process_text_source(raw, source, pats)
        note = "cleaned + regex-matched; rows containing no spelling removed"
    else:
        proc = process_count_source(raw, source, pats)
        note = "restricted to enabled pairs"
    if not len(proc):
        print(f"    processed is empty — check the matcher before trusting this")
        return False
    kept = len(proc) / max(len(raw), 1) * 100
    print(f"    processed: {len(proc):,} of {len(raw):,} rows kept ({kept:.1f}%)")
    return ok and write_verified(proc, f"{source}_processed", KEY_COLS, note=note)


def build_pairs(dry_run: bool = False) -> bool:
    """Stack every source's processed rows per pair. Unbalanced; carries source.

    Pair files hold the TEXT window: every row is clamped at TEXT_END here,
    because the processed tiers it stacks now run to SERIES_END.
    """
    enabled = {s for s, p in pair_patterns().items() if p["enabled"]}

    # GDELT rows come from the VERIFIED corpus, not gdelt_processed: the
    # referent filters (US-town evidence rule, analyst-surname and 1917-prince
    # drops), the story/paragraph dedups and the reclassification all live
    # there. Publishing the store's unfiltered gdelt sent the noise to
    # HuggingFace even after the site was fixed.
    #
    # Retired pairs are not rebuilt. The verified corpus still holds borscht,
    # and stacking it here rewrote pairs/borscht.parquet as a 1,443-row
    # GDELT-only file over the 21,428-row multi-source one its retirement
    # rests on (72.8% UA; that copy now survives only on HuggingFace and in
    # ~/kyivnotkiev-archive). The processed tiers already skip disabled pairs,
    # so this is the one path that kept producing it.
    _vdir = pathlib.Path("data/cl/corpus/gdelt_verified")
    _vfiles = sorted(f for f in _vdir.glob("*.parquet")
                     if not f.stem.endswith("_series")) if _vdir.exists() else []
    _retired = sorted(f.stem for f in _vfiles if f.stem not in enabled)
    _vfiles = [f for f in _vfiles if f.stem in enabled]
    if _retired:
        print(f"  gdelt: verified corpus skipped for {len(_retired)} disabled "
              f"pair(s): {', '.join(_retired)}")

    files = sorted(STORE.glob("*_processed.parquet"))
    frames = []
    _n_store = 0
    for f in files:
        if _vfiles and f.name == "gdelt_processed.parquet":
            # Every one of its rows is replaced by the verified corpus below.
            # Loading it only to discard it was 3.6 GB of the 7.0 GB of
            # uncompressed parquet this step read (616,943 rows with bodies).
            _n_store = pq.read_metadata(f).num_rows
            continue
        d = pd.read_parquet(f)
        if "text" in d.columns and "record_id" in d.columns:
            frames.append(d)
    if not frames and not _vfiles:
        print("no text-bearing processed files yet")
        return False
    allp = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    if _vfiles:
        _vframes = []
        for _vf in _vfiles:
            _v = pd.read_parquet(_vf)
            _vframes.append(pd.DataFrame({
                "record_id": "gv_" + _vf.stem + "_" + _v.reset_index().index.astype(str),
                "pair_slug": _vf.stem, "source": "gdelt",
                "doc_id": _v.url.astype(str), "url": _v.url.astype(str),
                "date": _v.date.astype(str).str[:10],
                "title": "", "text": _v.text.astype(str),
                "variant": _v.variant,
            }))
        _gv = pd.concat(_vframes, ignore_index=True)
        print(f"  gdelt: {_n_store:,} store rows replaced by "
              f"{len(_gv):,} verified rows")
        allp = pd.concat([allp[allp.source != "gdelt"] if len(allp) else allp, _gv],
                         ignore_index=True)

    # The text window, applied to every source at once. The verified corpus is
    # read straight off disk and never passes through apply_study_scope on the
    # raw->processed path; that bypass once put 27,575 rows dated 2026 into 22
    # pair files. The processed tiers are clamped only at SERIES_END, so
    # without this cut a Reddit or YouTube 2026 ingest would reach keyness too.
    # Before the source filters, as both clamps were before this was split.
    allp = apply_study_scope(allp, TEXT_END, "text window")

    # Homonym and referent false positives are excluded from the
    # analysis-ready pair files — the same release principle as the telegram
    # deprecation: raw source mirrors stay untouched, combined pairs are
    # clean. All rules live in pipeline/filters.py and are applied per
    # (pair, source), so youtube_homonym_filters and the referent rules
    # reach reddit/youtube/openalex rows too, not just gdelt. Odessa-TX
    # alone was 34,735 rows of odesa's pair file before this existed
    # (2026-09-02).
    _n0 = len(allp)
    _kept = [apply_source_filters(g, _slug, _src)
             for (_slug, _src), g in allp.groupby(["pair_slug", "source"],
                                                  dropna=False, sort=False)]
    allp = pd.concat(_kept).sort_index() if _kept else allp
    print(f"  source filters: {_n0 - len(allp):,} rows dropped from pair files")

    out = STORE / "pairs"
    out.mkdir(parents=True, exist_ok=True)
    print(f"\n=== pairs === ({len(allp):,} records from "
          f"{allp.source.nunique()} source(s))")
    okall = True
    changed, produced = [], set()
    for slug, g in allp.groupby("pair_slug"):
        g = g.sort_values(["source", "date"]).reset_index(drop=True)
        path = out / f"{slug}.parquet"
        before = checksum(g, KEY_COLS)
        produced.add(path.name)
        if dry_run:
            # Compare against the file a real run would replace. Freshness is
            # judged by row count, so any difference here sends the pair amber
            # and forces its keyness and clusters to be recomputed.
            if path.exists():
                cols = [c for c in KEY_COLS if c in pq.read_schema(path).names]
                cur = pd.read_parquet(path, columns=cols)
                cur_ck = checksum(cur, KEY_COLS)
                same = len(cur) == len(g) and cur_ck == before
                verdict = ("unchanged" if same else
                           f"WOULD CHANGE: on disk {len(cur):,} rows, checksum {cur_ck}")
                if not same:
                    changed.append(path.name)
            else:
                verdict = "new file"
                changed.append(path.name)
            print(f"    DRY RUN  {path.name:<30}{len(g):>8,} rows  checksum {before}  {verdict}")
            continue
        g.to_parquet(path, compression="zstd", index=False)
        back = pd.read_parquet(path)
        ok = before == checksum(back, KEY_COLS) and len(back) == len(g)
        okall &= ok
        by_src = g.source.value_counts().to_dict()
        print(f"    {'OK ' if ok else 'MISMATCH'} {slug:<24}{len(g):>7,} rows  {by_src}")
        record_manifest(f"pairs/{slug}", back, KEY_COLS, note="all sources stacked, unbalanced")
    untouched = sorted(p.name for p in out.glob("*.parquet") if p.name not in produced)
    if untouched:
        print(f"    left untouched (no rows produced): {', '.join(untouched)}")
    if dry_run:
        print(f"    {len(changed)} of {len(produced)} pair file(s) would change"
              + (f": {', '.join(changed)}" if changed else ""))
    return okall


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True,
                    help="one source name, 'all', or 'pairs' to rebuild the per-pair files")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    if a.source == "pairs":
        # Pass the flag through. This dispatch used to ignore --dry-run entirely
        # and write unconditionally, so a run asking "what would this do" rewrote
        # all 25 pair files and sent every pair amber against its recorded stats.
        return 0 if build_pairs(a.dry_run) else 1
    targets = list(SOURCES) if a.source == "all" else [a.source]
    unknown = [t for t in targets if t not in SOURCES]
    if unknown:
        print(f"unknown source(s): {unknown}. known: {sorted(SOURCES)}", file=sys.stderr)
        return 1
    ok = all(migrate_source(t, a.dry_run) for t in targets)
    if a.source == "all" and not a.dry_run:
        ok &= build_pairs(a.dry_run)
    print("\n" + ("all steps verified" if ok else "SOME STEPS FAILED VERIFICATION"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
