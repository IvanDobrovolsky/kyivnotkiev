"""Generate per-source holdout tables from the verified corpus.

The site's holdout tables were inconsistent and partly unusable: the `news`
entries carried only domain percentages with no link at all, and the YouTube
entries were drawn from raw search output, so they included videos that never
contained the term.

These are generated from data/corpus/pairs/{slug}.parquet instead, which means
every row shown has passed exact word-boundary matching and carries a resolvable
link to the document. Both variants are shown so the table is checkable in both
directions rather than presenting one side.

Usage:
    python -m pipeline.cl.corpus.holdouts            # all pairs with a corpus
    python -m pipeline.cl.corpus.holdouts --pair volodymyr-the-great
"""

import argparse
import json
import logging
import re
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent.parent
PAIRS_DIR = ROOT / "data" / "corpus" / "pairs"
OUT_PATH = ROOT / "site" / "src" / "data" / "corpus_holdouts.json"

PER_SOURCE = 15          # rows per source per variant
SNIPPET = 320            # long enough to read a whole clause


def clean(text: str) -> str:
    """Collapse whitespace and truncate at a WORD boundary.

    Cutting at a fixed character count sliced mid-word and produced unreadable
    fragments like "baptizing the city into Christian…".
    """
    t = re.sub(r"\s+", " ", str(text)).strip()
    if len(t) <= SNIPPET:
        return t
    cut = t.rfind(" ", 0, SNIPPET)
    if cut < SNIPPET * 0.6:      # no sensible break — fall back to the hard cut
        cut = SNIPPET
    return t[:cut].rstrip(" ,;:—-") + "…"


def build_pair(slug: str) -> dict | None:
    cpath = PAIRS_DIR / f"{slug}.parquet"
    mpath = PAIRS_DIR / f"{slug}_manifest.parquet"
    if not cpath.exists() or not mpath.exists():
        return None
    corpus = pd.read_parquet(cpath)
    man = pd.read_parquet(mpath)
    df = corpus.merge(man, on="record_id", how="inner")
    df = df[df.url.str.len() > 0]
    # show readable records first; flagged ones remain in the corpus regardless
    if "short_context" in df.columns:
        df = df[~df.short_context.fillna(False)]
    if "duplicate_context" in df.columns:
        df = df[~df.duplicate_context.fillna(False)]
    if not len(df):
        return None

    out = {}
    for source, grp in df.groupby("source"):
        items = []
        # both variants, so the table can be checked in either direction
        for variant in ("ukrainian", "russian", "both"):
            sub = grp[grp.variant == variant].sort_values("date", ascending=False)
            for _, r in sub.head(PER_SOURCE).iterrows():
                items.append({"text": clean(r.text), "url": r.url,
                              "variant": r.variant, "date": r.date})
        if items:
            out[source] = items
    return out or None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", default=None)
    args = ap.parse_args()

    slugs = ([args.pair] if args.pair
             else sorted(p.stem for p in PAIRS_DIR.glob("*.parquet")
                         if not p.stem.endswith("_manifest")))
    payload = {}
    for slug in slugs:
        got = build_pair(slug)
        if got:
            payload[slug] = got
            counts = {k: len(v) for k, v in got.items()}
            log.info(f"  {slug}: {counts}")
        else:
            log.warning(f"  {slug}: no corpus or no linked records — skipped")

    if not payload:
        log.error("nothing generated")
        return
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(payload))
    total = sum(len(v) for p in payload.values() for v in p.values())
    log.info(f"wrote {OUT_PATH.name}: {len(payload)} pair(s), {total} linked rows")


if __name__ == "__main__":
    main()
