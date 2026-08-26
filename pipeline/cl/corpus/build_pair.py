"""Build a training corpus for ONE pair, from all sources, on exact matches only.

The previous corpus (data/corpus/toponyms-corpus.parquet) is void: 15% of it is
YouTube sampled under a truncation bug, 10% belongs to pairs since disabled, and
it spans a 45-pair set that no longer exists. This builds per pair from scratch.

DESIGN RULES

1. EXACT MATCH IS THE GROUND TRUTH. A document enters only if the pair's Russian
   or Ukrainian form appears as an exact, word-boundary match in its text. This
   also makes the builder independent of `pair_id`, a numeric key that several
   raw files still carry but config/pairs.yaml no longer defines — matching on
   the text is both more robust and the thing we actually mean.

   Word boundaries do the linguistic work: "Kyivska", "Kyivskyi", "Odeska" and
   "Lvivska" are transliterated Ukrainian inflections and are correctly rejected,
   as are "Kyivan Rus"/"Kievan Rus" (a separate pair), while "Kyiv's" and
   "Chicken Kiev" are kept. The study measures Latin-script adoption, not
   transliterated Ukrainian.

2. SOURCE IS NEVER A TRAINING FEATURE. The training frame carries text, variant,
   date and pair — nothing else. Source, document id and URL go to a SEPARATE
   manifest joined by record_id. A classifier given the source can shortcut on
   register (academic abstract vs video description) instead of learning
   discourse, and the source mix is heavily skewed. Keep source for auditing and
   for GROUPED cross-validation folds, never in the row the model sees.

3. CONTEXT WINDOW, NOT THE WHOLE FIELD. Descriptions run to 5,000 characters and
   the longest 5% of documents would otherwise supply ~14% of all training text.
   Take a sentence-aware window around the match instead. Keyword-stuffed
   boilerplate collapses to a short fragment and then fails the minimum-prose
   check on its own.

4. ONE RECORD PER DOCUMENT. 18% of documents mention the term more than once.
   Emitting one record per mention would let a single verbose author outvote
   several independent ones. One document is one authorial choice.

5. VARIANT COMES FROM THE TEXT. Which form the author actually wrote — not which
   query happened to surface the document.

Usage:
    python -m pipeline.cl.corpus.build_pair --pair volodymyr-the-great
"""

import argparse
import hashlib
import logging
import re
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
CL_RAW = ROOT / "data" / "cl" / "raw"
OUT_DIR = ROOT / "data" / "corpus" / "pairs"

CONTEXT_CHARS = 400        # window around the match, sentence-snapped
MIN_PROSE_CHARS = 60       # below this a context is a fragment, not prose
MAX_TEXT_CHARS = 1000      # hard cap on any single training text

# source directory -> (text columns in priority order, id column, url column)
SOURCES = {
    "youtube_census":     (["title", "description"], "video_id", None),
    "reddit_full":        (["title", "selftext"],    "post_id",  "url"),
    "gdelt_articles":     (["text"],                 None,       "url"),
    "gdelt":              (["text"],                 None,       "url"),
    "openalex":           (["title", "abstract"],    "openalex_id", None),
    "religious":          (["title", "body", "text"], None,      "url"),
    "telegram":           (["text"],                 None,       None),
    "wikipedia_articles": (["text"],                 None,       None),
    "wikipedia":          (["text"],                 None,       None),
}


def sentence_window(text: str, start: int, end: int, width: int = CONTEXT_CHARS) -> str:
    """Context around [start,end), snapped outward to sentence edges where possible."""
    lo = max(0, start - width // 2)
    hi = min(len(text), end + width // 2)
    left = text.rfind(". ", lo, start)
    if left != -1 and start - left < width:
        lo = left + 2
    right = text.find(". ", end, hi)
    if right != -1:
        hi = right + 1
    return text[lo:hi].strip()


def build_url(source: str, doc_id: str, raw_url: str) -> str:
    """A resolvable link to the document, so every corpus record is checkable.

    Holdout tables on the site are generated from these, which is why a record
    without a link is close to useless for verification.
    """
    if raw_url and raw_url.startswith("http"):
        return raw_url
    if source == "youtube" and doc_id:
        return f"https://youtube.com/watch?v={doc_id}"
    if source == "openalex" and doc_id:
        return doc_id if doc_id.startswith("http") else f"https://openalex.org/{doc_id}"
    if source == "reddit" and doc_id:
        return f"https://reddit.com/{doc_id}"
    return ""


def load_source(name: str) -> pd.DataFrame | None:
    cols, id_col, url_col = SOURCES[name]
    d = CL_RAW / name
    if not d.exists():
        return None
    if name == "youtube_census":
        files = sorted(d.glob("*_enriched.parquet"))
    else:
        files = sorted(d.glob("*.parquet"))
    if not files:
        return None
    frames = []
    for f in files:
        try:
            frames.append(pd.read_parquet(f))
        except Exception as e:
            log.warning(f"  {name}/{f.name}: unreadable ({e})")
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    df["_source"] = name.replace("_full", "").replace("_articles", "").replace("_census", "")
    df["_id_col"] = id_col or ""
    df["_url_col"] = url_col or ""
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    ap.add_argument("--context-chars", type=int, default=CONTEXT_CHARS)
    args = ap.parse_args()

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    pair = next((p for p in cfg["pairs"] if p["slug"] == args.pair), None)
    if pair is None:
        log.error(f"pair '{args.pair}' not in config")
        return
    ru, ua = pair["russian"], pair["ukrainian"]
    ru_re = re.compile(rf"\b{re.escape(ru)}\b", re.IGNORECASE)
    ua_re = re.compile(rf"\b{re.escape(ua)}\b", re.IGNORECASE)

    log.info(f"pair {args.pair}: '{ru}' vs '{ua}'")
    rows, manifest, stats = [], [], []

    for name, (cols, id_col, url_col) in SOURCES.items():
        df = load_source(name)
        if df is None or not len(df):
            stats.append({"source": name, "scanned": 0, "matched": 0, "kept": 0})
            continue

        present = [c for c in cols if c in df.columns]
        if not present:
            log.warning(f"  {name}: none of {cols} present — skipped")
            stats.append({"source": name, "scanned": len(df), "matched": 0, "kept": 0})
            continue

        blob = df[present[0]].fillna("").astype(str)
        for c in present[1:]:
            blob = blob + "\n\n" + df[c].fillna("").astype(str)

        m_ru = blob.str.contains(ru_re, regex=True, na=False)
        m_ua = blob.str.contains(ua_re, regex=True, na=False)
        hit = m_ru | m_ua
        matched = int(hit.sum())

        kept = 0
        for idx in df.index[hit]:
            text = blob.loc[idx]
            has_ru, has_ua = bool(ru_re.search(text)), bool(ua_re.search(text))
            variant = "both" if (has_ru and has_ua) else ("ukrainian" if has_ua else "russian")

            m = (ua_re.search(text) if has_ua else ru_re.search(text))
            ctx = sentence_window(text, m.start(), m.end(), args.context_chars)[:MAX_TEXT_CHARS]
            if len(ctx) < MIN_PROSE_CHARS:
                continue

            r = df.loc[idx]
            doc_id = str(r[id_col]) if id_col and id_col in df.columns else ""
            url = str(r[url_col]) if url_col and url_col in df.columns else ""
            date = ""
            for dc in ("date", "published_at", "created_utc", "year"):
                if dc in df.columns and pd.notna(r.get(dc)):
                    date = str(r[dc])[:10]
                    break

            rid = hashlib.sha1(f"{args.pair}|{r['_source']}|{doc_id}|{ctx[:120]}".encode()).hexdigest()[:16]
            rows.append({"record_id": rid, "pair_slug": args.pair,
                         "text": ctx, "variant": variant, "date": date})
            manifest.append({"record_id": rid, "source": r["_source"], "doc_id": doc_id,
                             "url": build_url(r["_source"], doc_id, url),
                             "raw_chars": len(text),
                             "n_mentions": len(ru_re.findall(text)) + len(ua_re.findall(text))})
            kept += 1
        stats.append({"source": name, "scanned": len(df), "matched": matched, "kept": kept})
        log.info(f"  {name:<20} scanned={len(df):>8,}  exact-match={matched:>6,}  kept={kept:>6,}")

    if not rows:
        log.error("no records — nothing written")
        return

    corpus = pd.DataFrame(rows).drop_duplicates(subset=["text"]).reset_index(drop=True)
    man = pd.DataFrame(manifest)
    man = man[man.record_id.isin(set(corpus.record_id))].drop_duplicates(subset=["record_id"])

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    corpus.to_parquet(OUT_DIR / f"{args.pair}.parquet", index=False)
    man.to_parquet(OUT_DIR / f"{args.pair}_manifest.parquet", index=False)
    pd.DataFrame(stats).to_csv(OUT_DIR / f"{args.pair}_sources.csv", index=False)

    log.info("")
    log.info(f"TRAINING FRAME  {len(corpus):,} records -> {args.pair}.parquet")
    log.info(f"  columns (source deliberately absent): {list(corpus.columns)}")
    log.info(f"  variant: {corpus.variant.value_counts().to_dict()}")
    log.info(f"  text chars: median={int(corpus.text.str.len().median())}, "
             f"p90={int(corpus.text.str.len().quantile(0.9))}, max={int(corpus.text.str.len().max())}")
    log.info(f"MANIFEST        {len(man):,} rows -> {args.pair}_manifest.parquet")
    log.info(f"  source mix (audit only): {man.source.value_counts().to_dict()}")
    linked = (man.url.str.len() > 0)
    log.info(f"  records with a resolvable link: {linked.sum():,}/{len(man):,} "
             f"({100*linked.mean():.1f}%)")
    for src, grp in man.groupby("source"):
        lk = (grp.url.str.len() > 0).mean()
        log.info(f"    {src:<12} {len(grp):>5} records, {100*lk:>5.1f}% linked")
    mx = man.source.value_counts()
    if len(mx):
        log.info(f"  LARGEST SOURCE SHARE: {mx.index[0]} at {100*mx.iloc[0]/len(man):.1f}% "
                 f"— balance before training if this dominates")


if __name__ == "__main__":
    main()
