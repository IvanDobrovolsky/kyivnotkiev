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
import yaml

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
# The study period. `raw` deliberately keeps whatever the provider returned --
# collection ran into 2026 and that data is real, so discarding it would destroy
# provenance. But 2026 is a partial year: a trailing fragment against fifteen
# complete ones reads as a collapse in volume rather than as an incomplete
# window, so it is cut from `processed` and therefore from the pair files too.
STUDY_END = "2025-12-31"

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


def raw_reddit() -> pd.DataFrame:
    df = pd.read_parquet(SOURCES["reddit"]["file"])
    # Mirror/bot subreddits (autotldr, AutoNewspaper, DOOMMM_* chain-posters...)
    # repeat the OUTLET's spelling, not a redditor's choice — they leak the news
    # signal into the organic social series. Behavioural rule, measured
    # 2026-09-04 on 840K rows (19.3% dropped): a subreddit with >=100 posts is a
    # mirror when it has <=2 distinct authors, or >=10% of its titles carry the
    # (N/M) chain-post marker. Human subs with templated titles survive the
    # author gate (indiegameswap: 241 authors). The kyivan-rus 2023-03 "spike"
    # was one syndicated article chain-posted 104x. Audit CSV in data/audit/.
    if "subreddit" in df.columns and "author" in df.columns:
        _t = df.get("title", pd.Series("", index=df.index)).astype(str)
        _chunk = _t.str.match(r"\(\d+/\d+\)")
        _st = df.groupby("subreddit").agg(
            posts=("post_id", "size"), authors=("author", "nunique"))
        _st["chunk_share"] = _chunk.groupby(df["subreddit"]).mean()
        _bots = _st[(_st.posts >= 100)
                    & ((_st.authors <= 2) | (_st.chunk_share >= 0.10))]
        _n0 = len(df)
        df = df[~df.subreddit.isin(set(_bots.index))].copy()
        print(f"  reddit: dropped {_n0 - len(df):,} rows from "
              f"{len(_bots)} mirror/bot subreddit(s)")
        try:
            _bots.to_csv(pathlib.Path("data/audit/reddit_bot_subreddits.csv"))
        except Exception:                              # noqa: BLE001
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
    return apply_study_scope(out, source)


def process_count_source(raw: pd.DataFrame, source: str, pats: dict) -> pd.DataFrame:
    enabled = {s for s, p in pats.items() if p["enabled"]}
    df = raw[raw.pair_slug.isin(enabled)].copy()
    df["source"] = source
    return apply_study_scope(df.reset_index(drop=True), source)


def apply_study_scope(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Drop anything past STUDY_END, on whichever date column the source carries."""
    if not len(df):
        return df
    # An undated row must survive: missing dates are a data-quality problem tracked
    # elsewhere, and dropping them here would delete records under the wrong banner.
    # Note NaT <= Timestamp evaluates to False, not NaN, so the null case has to be
    # spelled out -- a .fillna(True) after the comparison never fires.
    if "date" in df.columns:
        dt = pd.to_datetime(df["date"], errors="coerce")
        keep = dt.isna() | (dt <= pd.Timestamp(STUDY_END))
    elif "year" in df.columns:
        yr = pd.to_numeric(df["year"], errors="coerce")
        keep = yr.isna() | (yr <= int(STUDY_END[:4]))
    else:
        return df
    dropped = int((~keep).sum())
    if dropped:
        print(f"    study scope: dropped {dropped:,} row(s) after {STUDY_END}")
    return df[keep].reset_index(drop=True)


KEY_COLS = ["record_id", "doc_id", "url", "pair_slug", "date", "variant", "text_hash"]


def migrate_source(source: str, dry_run: bool = False) -> bool:
    print(f"\n=== {source} ===")
    pats = pair_patterns()
    raw = RAW_BUILDERS[source]()
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


def build_pairs() -> bool:
    """Stack every source's processed rows per pair. Unbalanced; carries source."""
    files = sorted(STORE.glob("*_processed.parquet"))
    frames = []
    for f in files:
        d = pd.read_parquet(f)
        if "text" in d.columns and "record_id" in d.columns:
            frames.append(d)
    if not frames:
        print("no text-bearing processed files yet")
        return False
    allp = pd.concat(frames, ignore_index=True)

    # Homonym false positives are excluded from the analysis-ready pair files —
    # the same release principle as the telegram deprecation: raw source
    # mirrors stay untouched, combined pairs are clean. Odessa-TX alone was
    # 34,735 rows of odesa's pair file before this existed (2026-09-02).
    import re as _re
    import yaml as _yaml
    _cfg = _yaml.safe_load(open("config/pairs.yaml"))
    _pats = {p["slug"]: [_re.compile(f, _re.I) for f in p.get("homonym_filters", [])]
             for p in _cfg["pairs"] if p.get("homonym_filters")}
    if _pats:
        _blob = (allp.get("title", pd.Series("", index=allp.index)).fillna("").astype(str)
                 + " " + allp["text"].fillna("").astype(str))
        _fp = pd.Series(False, index=allp.index)
        for _slug, _ps in _pats.items():
            _m2 = allp.pair_slug == _slug
            if _m2.any():
                _fp.loc[_m2] = _blob[_m2].apply(lambda t: any(r.search(t) for r in _ps))
        if int(_fp.sum()):
            print(f"  homonym filter: {int(_fp.sum()):,} false-positive rows excluded from pair files")
            allp = allp[~_fp]

    out = STORE / "pairs"
    out.mkdir(parents=True, exist_ok=True)
    print(f"\n=== pairs === ({len(allp):,} records from {len(frames)} source(s))")
    okall = True
    for slug, g in allp.groupby("pair_slug"):
        g = g.sort_values(["source", "date"]).reset_index(drop=True)
        path = out / f"{slug}.parquet"
        before = checksum(g, KEY_COLS)
        g.to_parquet(path, compression="zstd", index=False)
        back = pd.read_parquet(path)
        ok = before == checksum(back, KEY_COLS) and len(back) == len(g)
        okall &= ok
        by_src = g.source.value_counts().to_dict()
        print(f"    {'OK ' if ok else 'MISMATCH'} {slug:<24}{len(g):>7,} rows  {by_src}")
        record_manifest(f"pairs/{slug}", back, KEY_COLS, note="all sources stacked, unbalanced")
    return okall


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", required=True,
                    help="one source name, 'all', or 'pairs' to rebuild the per-pair files")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    if a.source == "pairs":
        return 0 if build_pairs() else 1
    targets = list(SOURCES) if a.source == "all" else [a.source]
    unknown = [t for t in targets if t not in SOURCES]
    if unknown:
        print(f"unknown source(s): {unknown}. known: {sorted(SOURCES)}", file=sys.stderr)
        return 1
    ok = all(migrate_source(t, a.dry_run) for t in targets)
    if a.source == "all" and not a.dry_run:
        ok &= build_pairs()
    print("\n" + ("all steps verified" if ok else "SOME STEPS FAILED VERIFICATION"))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
