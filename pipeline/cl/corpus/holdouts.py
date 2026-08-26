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

# A holdout is evidence of CONTINUED RUSSIAN-FORM USE, so only russian and
# `both` rows qualify — a Ukrainian-form record is the opposite of a holdout.
# 100 per source gives enough surface to actually validate against.
PER_SOURCE = 100
HOLDOUT_VARIANTS = ("russian", "both")
SNIPPET = 320            # long enough to read a whole clause

# A holdout is a claim about CURRENT behaviour: "this outlet/channel still uses
# the Russian form". Evidence from 2018 does not support that — many of those
# outlets switched in the 2019 style-guide wave or after Feb 2022. Restricting
# to recent years keeps the table honest.
HOLDOUT_SINCE = "2022-01-01"

# Ranking YouTube holdouts by view count makes the table far more useful, but
# the YouTube API Terms restrict retention of API data other than IDs. So view
# counts are fetched at generation time, used ONLY to order the rows, and never
# written to disk — the output keeps the video ID and the resulting order.
RANK_BY_VIEWS = True


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


def youtube_view_order(video_ids: list[str], key: str) -> dict:
    """Return {video_id: rank_key} from live view counts. NOTHING is persisted.

    videos.list part=statistics costs 1 unit per 50 ids against the 110,000-unit
    pool that search never touches. The counts are used to sort and are then
    discarded; only the ordering survives into the output.
    """
    import requests
    order = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        try:
            r = requests.get("https://www.googleapis.com/youtube/v3/videos",
                             params={"part": "statistics", "id": ",".join(batch), "key": key},
                             timeout=30)
            if r.status_code != 200:
                continue
            for item in r.json().get("items", []):
                vc = item.get("statistics", {}).get("viewCount")
                if vc is not None:
                    order[item["id"]] = int(vc)
        except Exception as e:
            log.warning(f"  view fetch failed for a batch: {e}")
    return order


def build_pair(slug: str, view_key: str | None = None) -> dict | None:
    cpath = PAIRS_DIR / f"{slug}.parquet"
    mpath = PAIRS_DIR / f"{slug}_manifest.parquet"
    if not cpath.exists() or not mpath.exists():
        return None
    corpus = pd.read_parquet(cpath)
    man = pd.read_parquet(mpath)
    df = corpus.merge(man, on="record_id", how="inner")
    df = df[df.url.str.len() > 0]
    before = len(df)
    df = df[df.date.fillna("") >= HOLDOUT_SINCE]
    if before and not len(df):
        log.warning(f"  {slug}: no records since {HOLDOUT_SINCE} ({before} older ones excluded)")
    # show readable records first; flagged ones remain in the corpus regardless
    if "short_context" in df.columns:
        df = df[~df.short_context.fillna(False)]
    if "duplicate_context" in df.columns:
        df = df[~df.duplicate_context.fillna(False)]
    # Prefer contexts that read as prose. A window landing inside a description's
    # link dump ("Yandex, etc: https://band.link/...") is a technically valid
    # match but useless as displayed evidence, and view-ranking surfaces exactly
    # those because promo-heavy videos are the high-view ones.
    url_heavy = df.text.str.count(r"https?://") >= 2
    if (~url_heavy).sum() > 0:
        df = df[~url_heavy]
    if not len(df):
        return None

    views = {}
    if RANK_BY_VIEWS and view_key:
        yt = df[(df.source == "youtube") & (df.doc_id.str.len() > 0)]
        if len(yt):
            views = youtube_view_order(yt.doc_id.tolist(), view_key)
            log.info(f"  {slug}: ranked {len(views)} youtube rows by live view count "
                     f"(counts discarded, order kept)")

    out = {}
    for source, grp in df.groupby("source"):
        items = []
        # both variants, so the table can be checked in either direction
        for variant in HOLDOUT_VARIANTS:
            sub = grp[grp.variant == variant]
            if source == "youtube" and views:
                sub = sub.assign(_v=sub.doc_id.map(views).fillna(-1)).sort_values("_v", ascending=False)
            else:
                sub = sub.sort_values("date", ascending=False)
            for _, r in sub.head(PER_SOURCE).iterrows():
                # Prefer the document's real title; fall back to the matched
                # context only where the source stores no title (GDELT bodies).
                label = str(r.get("title") or "").strip() or r.text
                items.append({"text": clean(label), "url": r.url,
                              "variant": r.variant, "date": r.date})
        if items:
            out[source] = items
    return out or None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", default=None)
    ap.add_argument("--api-key", default=None,
                    help="YouTube key: ranks youtube holdouts by live view count. "
                         "Counts are used for ordering only and never stored.")
    args = ap.parse_args()

    slugs = ([args.pair] if args.pair
             else sorted(p.stem for p in PAIRS_DIR.glob("*.parquet")
                         if not p.stem.endswith(("_manifest", "_analysis"))))
    payload = {}
    for slug in slugs:
        got = build_pair(slug, view_key=args.api_key)
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
