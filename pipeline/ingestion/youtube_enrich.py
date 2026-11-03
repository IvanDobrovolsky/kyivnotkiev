"""Add full descriptions to census parquets and verify the term actually appears.

`search.list` returns only a TRUNCATED description (~160 chars), which is not
enough to check whether the toponym is really present. `videos.list` returns the
full snippet at **1 unit per 50 videos**, charged against the 110,000-unit daily
pool that search never touches — so this is effectively free.

NOTHING IS EVER DROPPED for failing verification. Verification is expressed
purely as columns, so every filter is reversible and can be re-cut without
re-fetching. The raw per-year parquets remain exactly as search returned them.
The only rows ever dropped are orphans (below), and only on --orphans drop.

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

INCREMENTAL is the default. A full re-fetch (--full) asks again for every
video, and videos.list returns nothing for a video deleted or privated since
the last enrich. Incremental reuses the stored metadata of every video that has
it and fetches only ids that lack it: new ids, and stored rows whose
channel_title is blank because an earlier fetch never got them (videos.list
returns a channelTitle for every live video: of 87,872 blank-channel_title rows
in the 25 enriched files on disk, retired borscht included, 1 had a
description, measured 2026-10-19). In either mode stored
metadata is never replaced by a blank: an id a successful response omits keeps
what it had. --years limits the rebuild to rows published in those years and
copies every other row unchanged.

A FAILED FETCH WRITES NOTHING. Any batch that never got a 200 (dead key, quota,
network) stops the run with exit 1 and leaves the enriched file untouched. It
used to be logged as "skipped", the run carried on, and every id in it was
written with a blank description and exit 0. zaporizhzhia_enriched.parquet was
rewritten that way on 2026-09-14 at 16:05: all 87,734 rows have a blank
description and channel_title, verified fell from 17,721 (enrich_zap.log,
2026-09-12) to 14,361, and the all-years UA share moved from 56.5% to 63.8%.

ORPHANS are rows of the enriched file, within the years being rebuilt, that no
current per-year parquet holds: leftovers of a superseded collection. Dropping
them moves a published series (2,298 of chornobyl's 3,052 are verified
2010-2024 rows), so it is a decision: the run refuses unless --orphans keep
(copy them verbatim) or --orphans drop is given.

Usage:
    python -m pipeline.ingestion.youtube_enrich --pair kyiv --years 2026 --dry-run
    python -m pipeline.ingestion.youtube_enrich --pair kyiv --years 2026 --api-key "$KEY"
    python -m pipeline.ingestion.youtube_enrich --pair zaporizhzhia --orphans keep \
        --api-key "$KEY"            # re-fetch rows lacking metadata, every year
"""

import argparse
import logging
import os
import re
import sys
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

# Only the census's own per-year files are input. The input glob used to be
# `<pair>_*.parquet`, which also matches `<pair>_enriched.parquet`, the OUTPUT.
# It sorted first and won drop_duplicates, so every row any earlier collection had
# ever produced survived every later enrich, --force included. Measured
# 2026-10-19: 3,550 enriched rows on 6 pairs sat in no current per-year parquet;
# 2,298 of chornobyl's 3,052 were verified 2010-2024 rows on the site.
CENSUS_FILE = re.compile(r"^(?P<pair>.+)_(?P<variant>russian|ukrainian)_(?P<year>\d{4})\.parquet$")
BASE_COLS = ["video_id", "title", "channel", "channel_id", "published_at",
             "pair_slug", "variant", "matched_term"]
META_COLS = ["description", "channel_title", "tags"]
OUT_COLS = BASE_COLS + META_COLS + [
    "ru_in_title", "ua_in_title", "ru_in_desc", "ua_in_desc", "span_artifact",
    "description_duplicate", "in_title", "in_description", "verified", "form"]


def census_files(census_dir: Path, pair: str) -> list[Path]:
    out = []
    for f in sorted(census_dir.glob(f"{pair}_*.parquet")):
        m = CENSUS_FILE.match(f.name)
        if m and m["pair"] == pair:
            out.append(f)
    return out


def fetch_details(video_ids, key):
    """Full snippet for each id. 1 unit per 50 ids.

    Returns (details, units, unfetched). `details` holds every id a 200 response
    returned; an id a 200 response omits is deleted or private. `unfetched` is
    the number of ids in batches that never got a 200. The fetch stops at the
    first such batch: a dead key or an exhausted quota fails every later batch
    too, and the caller writes nothing when unfetched > 0.
    """
    out, units = {}, 0
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        resp, err = None, None
        for attempt in range(5):
            try:
                resp = requests.get(API, params={
                    "part": "snippet", "id": ",".join(batch), "key": key,
                }, timeout=30)
            except requests.exceptions.RequestException as e:
                # A read timeout used to escape this loop and kill the whole run:
                # chornobyl died at 52,035 of 71,321 videos, discarding an hour of work
                # because only HTTP 429 was retried and network errors were not caught.
                resp, err = None, type(e).__name__
                wait = 5 * (attempt + 1)
                log.warning(f"  {err} — retrying in {wait}s")
                time.sleep(wait)
                continue
            units += 1
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = 15 * (attempt + 1)
                log.warning(f"  HTTP {resp.status_code} — backing off {wait}s")
                time.sleep(wait)
                continue
            break
        if resp is None or resp.status_code != 200:
            why = err if resp is None else f"HTTP {resp.status_code}"
            if resp is not None:
                try:
                    reasons = sorted({e.get("reason") for e in
                                      resp.json().get("error", {}).get("errors", [])} - {None})
                    if reasons:
                        why += f" {reasons}"
                except ValueError:
                    pass
            left = len(video_ids) - i
            log.error(f"  batch {i//50}: {why} after retries; stopping with "
                      f"{left:,} of {len(video_ids):,} ids unfetched")
            return out, units, left
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
    return out, units, 0


def label(df: pd.DataFrame, pair: dict) -> pd.DataFrame:
    """Per-row verification columns. Deterministic in title, description and the
    pair's two terms, so a row with unchanged inputs gets unchanged labels."""
    ru_re = re.compile(rf"\b{re.escape(pair['russian'])}\b", re.IGNORECASE)
    ua_re = re.compile(rf"\b{re.escape(pair['ukrainian'])}\b", re.IGNORECASE)
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
    df["form"] = df.apply(form, axis=1) if len(df) else pd.Series(dtype=str)
    return df


def duplicate_descriptions(desc: pd.Series) -> set:
    # Copy-pasted keyword boilerplate: one channel pasted the same block into 44
    # unrelated videos, injecting 44 Russian "mentions" from a single authorial
    # choice. Labelled, never dropped — the ratio moves <2pp either way, but the
    # training corpus should not see 34 identical texts.
    desc = desc.fillna("")
    long_ = desc[desc.str.len() > 50]
    counts = long_.value_counts()
    return set(counts[counts > 1].index)


def parse_years(spec: str | None) -> set[str] | None:
    if not spec:
        return None
    years = {y.strip() for y in spec.split(",") if y.strip()}
    bad = [y for y in years if not re.fullmatch(r"\d{4}", y)]
    if bad or not years:
        raise SystemExit(f"--years expects comma-separated years, got {spec!r}")
    return years


def by_year(published_at: pd.Series) -> dict:
    return published_at.astype(str).str[:4].value_counts().sort_index().to_dict()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pair", required=True)
    ap.add_argument("--api-key", default=None, help="required unless --dry-run")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--incremental", action="store_true",
                      help="the default: reuse stored metadata; fetch only ids that "
                           "lack it (new ids, rows with a blank channel_title)")
    mode.add_argument("--full", action="store_true",
                      help="re-fetch every id. Stored metadata still survives for ids "
                           "the API no longer returns")
    ap.add_argument("--years", default=None,
                    help="rebuild only rows published in these years (e.g. 2026); every "
                         "other row is copied unchanged from the existing enriched file")
    ap.add_argument("--orphans", choices=["refuse", "keep", "drop"], default="refuse",
                    help="rows within --years that no current census parquet holds: "
                         "refuse to write (default), keep them verbatim, or drop them")
    ap.add_argument("--census-dir", default=None,
                    help="default: data/cl/raw/youtube_census. A separate census "
                         "directory (an experiment) is enriched in place")
    ap.add_argument("--dry-run", action="store_true",
                    help="report what would be fetched, reused, kept and dropped; "
                         "no API call, no write")
    args = ap.parse_args(argv)
    if not args.dry_run and not args.api_key:
        log.error("--api-key is required unless --dry-run")
        return 2
    incremental = not args.full

    census_dir = Path(args.census_dir).resolve() if args.census_dir else CENSUS_DIR
    with open(CONFIG_PATH) as f:
        cfg = yaml.safe_load(f)
    pair = next((p for p in cfg["pairs"] if p["slug"] == args.pair), None)
    if pair is None:
        log.error(f"pair '{args.pair}' not found")
        return 2
    years = parse_years(args.years)

    files = census_files(census_dir, args.pair)
    if not files:
        log.error(f"no census parquets for {args.pair} in {census_dir}")
        return 1
    out = census_dir / f"{args.pair}_enriched.parquet"
    prev = pd.read_parquet(out) if out.exists() else None
    if years and prev is None:
        log.error(f"--years needs an existing {out.name} to copy the other years from")
        return 1
    if prev is not None and prev.video_id.duplicated().any():
        if incremental or years:
            log.error(f"{out.name} repeats video_ids; it cannot key a reuse")
            return 1
        prev = prev.drop_duplicates("video_id")      # --full: only a fallback source

    cur = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    cur = cur.drop_duplicates("video_id")[BASE_COLS]
    log.info(f"{len(files)} census files -> {len(cur):,} unique videos")

    # Rows outside --years are copied verbatim from the previous enriched file,
    # orphans included, so a 2026 enrich cannot move a 2010-2025 point.
    if years:
        keep = prev[~prev.published_at.astype(str).str[:4].isin(years)].copy()
        cur = cur[cur.published_at.astype(str).str[:4].isin(years)].copy()
        cur = cur[~cur.video_id.isin(keep.video_id)]
    else:
        keep = None

    orphans = pd.DataFrame(columns=OUT_COLS)
    if prev is not None:
        scope = prev if not years else prev[prev.published_at.astype(str).str[:4].isin(years)]
        orphans = scope[~scope.video_id.isin(cur.video_id)]
    n_orph_ver = int(orphans["verified"].fillna(False).astype(bool).sum()) \
        if "verified" in orphans and len(orphans) else 0
    if len(orphans):
        what = {"refuse": "REFUSING to write over", "keep": "keeping verbatim",
                "drop": "dropping"}[args.orphans]
        log.warning(f"{what} {len(orphans):,} rows ({n_orph_ver:,} verified) that no "
                    f"current census parquet holds: {by_year(orphans.published_at)}")

    have = prev.set_index("video_id") if prev is not None else None
    known = cur.video_id.isin(have.index) if have is not None else \
        pd.Series(False, index=cur.index)
    if have is not None:
        stored_ct = cur.video_id.map(have["channel_title"]).fillna("").astype(str)
        has_meta = known & stored_ct.ne("")
    else:
        has_meta = pd.Series(False, index=cur.index)
    if incremental and have is not None:
        # A known id keeps its previous base columns too. The per-year parquet can
        # hold a newer search snippet of the same video: 0.25% of ihor-sikorsky's
        # reused rows carried a different `channel`, and pipeline.filters matches
        # homonyms over title + channel + channel_title + description, so taking
        # the newer snippet could move a 2010-2025 row in or out of the series.
        for col in BASE_COLS[1:]:
            cur[col] = cur.video_id.map(have[col]).where(known, cur[col])
        reuse = has_meta
    else:
        reuse = pd.Series(False, index=cur.index)
    to_fetch = cur.loc[~reuse, "video_id"].tolist()
    blank_refetch = known & ~has_meta
    log.info(f"rebuild {len(cur):,} rows: reuse metadata {int(reuse.sum()):,}, "
             f"fetch {len(to_fetch):,} (~{-(-len(to_fetch) // 50)} units)"
             + (f"; copy {len(keep):,} rows outside {sorted(years)} unchanged"
                if keep is not None else ""))
    if blank_refetch.any():
        log.warning(f"  of the fetch, {int(blank_refetch.sum()):,} are stored rows with no "
                    f"metadata (blank channel_title), by year "
                    f"{by_year(cur.loc[blank_refetch, 'published_at'])}. Filling them moves "
                    f"in_description, verified and form for those years")
    if len(orphans) and args.orphans == "refuse":
        log.error("orphans present: pass --orphans keep or --orphans drop. Nothing written")
        return 1
    if args.dry_run:
        log.info("dry run: nothing fetched, nothing written")
        return 0

    details, units, unfetched = fetch_details(to_fetch, args.api_key) \
        if to_fetch else ({}, 0, 0)
    if unfetched:
        log.error(f"{unfetched:,} ids never got a 200 from videos.list ({units} units "
                  f"spent). Nothing written; {out.name} is unchanged")
        return 1
    fetched = cur.video_id.isin(details.keys())
    absent = cur.video_id.isin(to_fetch) & ~fetched
    kept_stored = absent & has_meta
    log.info(f"fetched {len(details):,}/{len(to_fetch):,} using {units} units"
             + (f"; {int(absent.sum()):,} not returned (deleted or private)" if absent.any() else "")
             + (f", of which {int(kept_stored.sum()):,} keep their stored metadata"
                if kept_stored.any() else ""))
    # fetched -> the fresh snippet; otherwise the stored one if any; otherwise blank,
    # so verification of a video deleted before its first enrich rests on the title.
    for c in META_COLS:
        fresh = cur.video_id.map(lambda v, c=c: details.get(v, {}).get(c, ""))
        stored = cur.video_id.map(have[c]).fillna("") if have is not None else ""
        cur[c] = fresh.where(fetched, stored.where(known, "") if have is not None else "")
    cur = label(cur, pair)

    carried = [f for f in (keep, orphans if args.orphans == "keep" else None)
               if f is not None and len(f)]
    pool = pd.concat([f.description for f in carried] + [cur.description])
    dups = duplicate_descriptions(pool)
    cur["description_duplicate"] = cur.description.fillna("").isin(dups) & \
        (cur.description.fillna("").str.len() > 50)
    df = pd.concat([f[OUT_COLS] for f in carried] + [cur[OUT_COLS]], ignore_index=True)

    tmp = out.with_suffix(".parquet.tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, out)

    n = len(df)
    log.info(f"\nsaved {out.name}  ({n:,} videos)")
    log.info(f"  verified (term in title or description): {df.verified.sum():,} ({100*df.verified.mean():.1f}%)")
    log.info(f"    in title       : {df.in_title.sum():,} ({100*df.in_title.mean():.1f}%)")
    log.info(f"    in description : {df.in_description.sum():,} ({100*df.in_description.mean():.1f}%)")
    log.info(f"  form: {df.form.value_counts().to_dict()}")
    log.info(f"  UNVERIFIED (matched on tags/other signals): {(~df.verified).sum():,}")
    log.info(f"  span_artifact (e.g. 'the Great Dane'), flagged not dropped: {df.span_artifact.sum():,}")
    log.info(f"  description_duplicate (copy-pasted boilerplate): {df.description_duplicate.sum():,}")
    log.info("  NOTE: referent is irrelevant — a rabbit named 'Vladimir the Great' "
             "is a real use of the Russian form and is counted.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
