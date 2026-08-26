"""Add full descriptions to census parquets and verify the term actually appears.

`search.list` returns only a TRUNCATED description (~160 chars), which is not
enough to check whether the toponym is really present. `videos.list` returns the
full snippet at **1 unit per 50 videos**, charged against the 110,000-unit daily
pool that search never touches — so this is effectively free.

NOTHING IS EVER DROPPED. Verification is expressed purely as columns, so every
filter is reversible and can be re-cut without re-fetching. The raw per-year
parquets remain exactly as search returned them.

What counts as a mention: this study measures an ORTHOGRAPHIC CHOICE, not a
referent. If someone names a rabbit "Vladimir the Great" rather than "Volodymyr
the Great", they made the choice being measured — that is a data point, not
noise. The referent is irrelevant; a planet would count.

The one exception is a coincidental string span, where the phrase never occurs
as a name at all: "Vladimir the Great Dane" is a dog called Vladimir of the
Great Dane breed, so "Vladimir the Great" is an artifact of adjacency. Word
boundaries cannot catch this — "Great" ends on a boundary before "Dane". These
are flagged in `span_artifact` and left in the data for the caller to decide.

Every row is marked, never dropped:

    in_title        term appears in the title, word-boundary matched
    in_description  term appears in the full description
    verified        either of the above

Word boundaries matter: without them "Kievan" counts as a "Kiev" mention.

Usage:
    python -m pipeline.ingestion.youtube_enrich --pair volodymyr-the-great --api-key "$KEY"
"""

import argparse
import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests
import yaml

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
CENSUS_DIR = ROOT / "data" / "cl" / "raw" / "youtube_census"
API = "https://www.googleapis.com/youtube/v3/videos"


def fetch_details(video_ids, key):
    """Full snippet for each id. 1 unit per 50 ids."""
    out, units = {}, 0
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        for attempt in range(4):
            resp = requests.get(API, params={
                "part": "snippet", "id": ",".join(batch), "key": key,
            }, timeout=30)
            units += 1
            if resp.status_code == 429:
                wait = 15 * (attempt + 1)
                log.warning(f"  rate limited — backing off {wait}s")
                time.sleep(wait)
                continue
            break
        if resp.status_code != 200:
            log.warning(f"  batch {i//50}: HTTP {resp.status_code} — skipped")
            continue
        for item in resp.json().get("items", []):
            sn = item.get("snippet", {})
            out[item["id"]] = {
                "description": sn.get("description", ""),
                "channel_title": sn.get("channelTitle", ""),
                "tags": "|".join(sn.get("tags", []) or []),
            }
        if (i // 50) % 20 == 0:
            log.info(f"  {len(out):,}/{len(video_ids):,} enriched ({units} units)")
        time.sleep(0.05)
    return out, units


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    ap.add_argument("--api-key", required=True)
    args = ap.parse_args()

    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    pair = next(p for p in cfg["pairs"] if p["slug"] == args.pair)
    ru_re = re.compile(rf"\b{re.escape(pair['russian'])}\b", re.IGNORECASE)
    ua_re = re.compile(rf"\b{re.escape(pair['ukrainian'])}\b", re.IGNORECASE)

    files = sorted(CENSUS_DIR.glob(f"{args.pair}_*.parquet"))
    if not files:
        log.error(f"no census parquets for {args.pair}")
        return

    frames = [pd.read_parquet(f) for f in files]
    df = pd.concat(frames, ignore_index=True).drop_duplicates("video_id")
    log.info(f"{len(files)} files -> {len(df):,} unique videos")

    details, units = fetch_details(df.video_id.tolist(), args.api_key)
    log.info(f"enriched {len(details):,}/{len(df):,} using {units} units")

    df["description"] = df.video_id.map(lambda v: details.get(v, {}).get("description", ""))
    df["channel_title"] = df.video_id.map(lambda v: details.get(v, {}).get("channel_title", ""))
    df["tags"] = df.video_id.map(lambda v: details.get(v, {}).get("tags", ""))

    title = df.title.fillna("")
    desc = df.description.fillna("")
    df["ru_in_title"] = title.apply(lambda t: bool(ru_re.search(t)))
    df["ua_in_title"] = title.apply(lambda t: bool(ua_re.search(t)))
    df["ru_in_desc"] = desc.apply(lambda t: bool(ru_re.search(t)))
    df["ua_in_desc"] = desc.apply(lambda t: bool(ua_re.search(t)))
    # Coincidental spans: phrase followed by a word that makes it not-a-name.
    # Extend this list as new cases are found; it only ever LABELS rows.
    SPAN_ARTIFACTS = ["Dane", "Danes"]
    span_re = re.compile(
        rf"\b(?:{re.escape(pair['russian'])}|{re.escape(pair['ukrainian'])})"
        rf"\s+(?:{'|'.join(SPAN_ARTIFACTS)})\b", re.IGNORECASE)
    df["span_artifact"] = (title.apply(lambda t: bool(span_re.search(t)))
                           | desc.apply(lambda t: bool(span_re.search(t))))

    df["in_title"] = df.ru_in_title | df.ua_in_title
    df["in_description"] = df.ru_in_desc | df.ua_in_desc
    df["verified"] = df.in_title | df.in_description

    def form(r):
        ru = r.ru_in_title or r.ru_in_desc
        ua = r.ua_in_title or r.ua_in_desc
        if ru and ua: return "both"
        if ua: return "ukrainian"
        if ru: return "russian"
        return "unverified"
    df["form"] = df.apply(form, axis=1)

    out = CENSUS_DIR / f"{args.pair}_enriched.parquet"
    df.to_parquet(out, index=False)

    n = len(df)
    log.info(f"\nsaved {out.name}  ({n:,} videos)")
    log.info(f"  verified (term in title or description): {df.verified.sum():,} ({100*df.verified.mean():.1f}%)")
    log.info(f"    in title       : {df.in_title.sum():,} ({100*df.in_title.mean():.1f}%)")
    log.info(f"    in description : {df.in_description.sum():,} ({100*df.in_description.mean():.1f}%)")
    log.info(f"  form: {df.form.value_counts().to_dict()}")
    log.info(f"  UNVERIFIED (matched on tags/other signals): {(~df.verified).sum():,}")
    log.info(f"  span_artifact (e.g. 'the Great Dane'), flagged not dropped: {df.span_artifact.sum():,}")
    log.info("  NOTE: referent is irrelevant — a rabbit named 'Vladimir the Great' "
             "is a real use of the Russian form and is counted.")


if __name__ == "__main__":
    main()
