"""Combine every text-bearing source for one pair into a single verifiable CSV.

Each row is one document with its text and the evidence for its label. Verification
is not a summary statistic here -- it is applied per row and recorded per row, so any
record can be checked by hand:

    ua_hits / ru_hits   word-boundary counts of each spelling in the text
    variant             read from those counts, never from the source's own label
    src_variant         what the source claimed, kept only so disagreement is visible
    verified            the text actually contains at least one spelling

A row that retrieves but contains no spelling is `verified=False`. It is written to
the CSV anyway rather than dropped, because a silent drop is unauditable -- filter on
the column to get the trainable set.

Sources without text (Wikipedia pageviews, Trends, Ngrams) contribute counts to the
adoption metric and cannot appear here. That is a property of the source, not a gap.

    python -m pipeline.cl.corpus.export_pair_csv --pair volodymyr-the-great
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import pathlib
import re

import pandas as pd
import yaml

OUT = pathlib.Path("data/cl/corpus/pair_csv")

# Documents run from a 132-char abstract to a 10k article body. Left uncapped, any
# unsupervised method keys on length before it keys on language. The window is taken
# AROUND the first match rather than from the start, so capping never removes the
# evidence the row is labelled on.
MAX_TEXT_CHARS = 1000


def patterns(slug: str):
    doc = yaml.safe_load(pathlib.Path("config/pairs.yaml").read_text())
    pairs = doc["pairs"] if isinstance(doc, dict) and "pairs" in doc else doc
    p = next(x for x in pairs if x["slug"] == slug)
    # Words may be separated by punctuation, not just spaces or hyphens:
    # "«Volodymyr, the Great!»" was being scored as containing no spelling at all.
    mk = lambda t: re.compile(r"\b" + r"[\s\-_,.:;'\"()\[\]«»]+".join(re.escape(w) for w in str(t).split()) + r"\b", re.I)
    return mk(p["ukrainian"]), mk(p["russian"]), str(p["ukrainian"]), str(p["russian"])


def _rows_gdelt(slug):
    f = pathlib.Path(f"data/cl/corpus/gdelt_verified/{slug}.parquet")
    if not f.exists():
        return []
    d = pd.read_parquet(f)
    return [{"source": "gdelt", "doc_id": r.url, "url": r.url, "date": str(r.date.date()),
             "title": "", "text": r.text, "src_variant": r.variant,
             "extra": f"domain={r.domain}"} for r in d.itertuples()]


def _rows_youtube(slug):
    f = pathlib.Path(f"data/cl/raw/youtube_census/{slug}_enriched.parquet")
    if not f.exists():
        return []
    d = pd.read_parquet(f).drop_duplicates("video_id")
    out = []
    for r in d.itertuples():
        text = f"{r.title}\n\n{getattr(r, 'description', '') or ''}".strip()
        out.append({"source": "youtube", "doc_id": r.video_id,
                    "url": f"https://youtube.com/watch?v={r.video_id}",
                    "date": str(r.published_at)[:10], "title": r.title, "text": text,
                    "src_variant": getattr(r, "form", None) or getattr(r, "variant", None),
                    "extra": f"channel={getattr(r, 'channel_title', '')}"})
    return out


def _rows_reddit(slug):
    f = pathlib.Path("dataset/raw_reddit.parquet")
    if not f.exists():
        return []
    d = pd.read_parquet(f)
    d = d[d.pair_slug == slug].drop_duplicates("post_id")
    out = []
    for r in d.itertuples():
        body = str(getattr(r, "selftext", "") or "")
        if body in ("[removed]", "[deleted]", "nan"):
            body = ""
        text = f"{r.title}\n\n{body}".strip()
        out.append({"source": "reddit", "doc_id": r.post_id,
                    "url": f"https://reddit.com/r/{r.subreddit}/comments/{r.post_id}",
                    "date": str(r.date)[:10], "title": r.title, "text": text,
                    "src_variant": r.variant,
                    "extra": f"subreddit=r/{r.subreddit};score={getattr(r, 'score', '')}"})
    return out


def _rows_openalex(slug, ua_term, ru_term):
    f = pathlib.Path("data/cl/raw/openalex/all_pairs.parquet")
    if not f.exists():
        return []
    d = pd.read_parquet(f)
    d = d[d.matched_term.astype(str).str.lower().isin({ua_term.lower(), ru_term.lower()})]
    d = d.drop_duplicates("openalex_id")
    out = []
    for r in d.itertuples():
        text = f"{r.title}\n\n{getattr(r, 'abstract', '') or ''}".strip()
        yr = r.year
        out.append({"source": "openalex", "doc_id": r.openalex_id, "url": r.openalex_id,
                    "date": f"{int(yr)}-01-01" if pd.notna(yr) else "",
                    "title": r.title, "text": text,
                    "src_variant": r.variant,
                    "extra": f"cited={getattr(r, 'cited_by_count', '')}"})
    return out


def window_on_match(text: str, ua_rx, ru_rx, limit: int = MAX_TEXT_CHARS) -> str:
    """Trim to `limit` chars centred on the first spelling, on word boundaries."""
    text = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(text) <= limit:
        return text
    hits = [m.start() for m in (ua_rx.search(text), ru_rx.search(text)) if m]
    centre = min(hits) if hits else 0
    lo = max(0, centre - limit // 2)
    hi = min(len(text), lo + limit)
    lo = max(0, hi - limit)
    out = text[lo:hi]
    if lo:
        out = out[out.find(" ") + 1:]
    if hi < len(text):
        out = out[:out.rfind(" ")]
    return ("…" if lo else "") + out.strip() + ("…" if hi < len(text) else "")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pair", required=True)
    a = ap.parse_args()
    ua_rx, ru_rx, ua_term, ru_term = patterns(a.pair)

    rows = (_rows_gdelt(a.pair) + _rows_youtube(a.pair)
            + _rows_reddit(a.pair) + _rows_openalex(a.pair, ua_term, ru_term))
    if not rows:
        print(f"no text-bearing records for {a.pair}")
        return 1

    for r in rows:
        full = str(r["text"] or "")
        r["full_len"] = len(re.sub(r"\s+", " ", full).strip())
        text = window_on_match(full, ua_rx, ru_rx)
        r["text"] = text
        ua, ru = len(ua_rx.findall(text)), len(ru_rx.findall(text))
        r["ua_hits"], r["ru_hits"] = ua, ru
        r["variant"] = ("both" if ua and ru else "ukrainian" if ua
                        else "russian" if ru else "none")
        r["verified"] = r["variant"] != "none"
        claimed = r["src_variant"] if r["src_variant"] in ("ukrainian", "russian", "both") else None
        r["src_variant"] = claimed
        r["agrees_with_source"] = (claimed == r["variant"]) if claimed else None
        r["text_len"] = len(text)
        r["text_hash"] = hashlib.sha1(re.sub(r"\s+", " ", text).strip().encode()).hexdigest()[:16]
        r["record_id"] = f"{a.pair}:{r['source']}:{r['doc_id']}"

    df = pd.DataFrame(rows)[[
        "record_id", "source", "doc_id", "url", "date", "title", "text", "text_len",
        "full_len", "variant", "ua_hits", "ru_hits", "verified", "src_variant",
        "agrees_with_source", "text_hash", "extra"]]
    df = df.sort_values(["source", "date"]).reset_index(drop=True)
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"{a.pair}.csv"
    df.to_csv(path, index=False)

    print(f"=== {a.pair} — {len(df):,} records -> {path} ({path.stat().st_size/1e6:.1f} MB)\n")
    t = df.groupby("source").agg(records=("record_id", "size"),
                                 verified=("verified", "sum"),
                                 dupes=("text_hash", lambda s: int(s.duplicated().sum())),
                                 median_chars=("text_len", "median"))
    t["verified_%"] = (t.verified / t.records * 100).round(1)
    print(t.to_string())
    print(f"\nvariant (from text): {df.variant.value_counts().to_dict()}")
    v = df[df.verified]
    print(f"verified records: {len(v):,} of {len(df):,} ({len(v)/len(df)*100:.1f}%)")
    print(f"unique texts among verified: {v.text_hash.nunique():,}")
    cmp = df[df.src_variant.notna() & df.verified & df.variant.isin(["ukrainian", "russian"])]
    if len(cmp):
        agree = (cmp.src_variant == cmp.variant).mean() * 100
        print(f"source label vs text: {agree:.1f}% agree over {len(cmp):,} comparable rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
