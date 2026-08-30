"""Score every pair-month by how much a single split recovers, and map to a depth.

THE MARKER
----------
    score = log2( unique_ids(two half-month windows) / unique_ids(one month window) )

Each split multiplies the number of windows. If a month's content is evenly
spread and the month window already returned it, splitting recovers nothing and
the score is ~0. If the month window was silently truncated, splitting recovers
what it withheld and the score climbs.

Bin boundaries come from the window ladder itself, which is a doubling sequence:

    windows   month 1  ->  week 4  ->  day 30  ->  hour 720
    log2      0            2           4.9         9.5

so a score is compared against the yield that would justify the extra windows.

WHY NOT totalResults
--------------------
Tried and discarded. `pageInfo.totalResults` scales sub-linearly with window
width and, within a single year, does not separate an event month from a quiet
one — chornobyl's HBO months read 321k against 319k for quiet months. Its
apparent 19x range across 2013-2022 was YouTube's index growing, not events.

WHY NOT THE MONTH'S OWN COUNT
-----------------------------
Anti-correlated with need. volodymyr-the-great 2012 recorded 429 videos in
2012-05 and 1-8 in every other month; on descent 2012-05 gained 1.08x while the
months showing 1-6 gained up to 432x. A low count means shallow sampling, not a
quiet month.

Usage:
    python -m pipeline.ingestion.youtube_depth_score --pair volodymyr-the-great
"""

import argparse
import calendar
import json
import math
import pathlib
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import pandas as pd

from pipeline.config import ROOT_DIR, get_pair_by_slug

OUT = ROOT_DIR / "data" / "cl" / "raw" / "youtube_depth"
KEY_ID = "029b0141-1889-4665-a569-36d75c0f6191"
KEY_PROJECT = "kyivnotkiev-yt"
MAX_PAGES = 10         # let windows exhaust naturally. At 3 the cap WAS the
                       # measurement: month windows pinned at 129-150 and their
                       # halves at 230-290, so the score reduced to log2(2) - the
                       # ratio of window counts, not of content. A month window
                       # exhausts on its own at ~5 pages / ~230 ids.
DELAY = 0.8            # 80/min ceiling, enforced per call

# Depth from score. Boundaries are the log2 window multipliers of the ladder.
BINS = [(1.0, "month"), (3.0, "week"), (6.0, "day"), (float("inf"), "hour")]


def api_key() -> str:
    r = subprocess.run(["gcloud", "services", "api-keys", "get-key-string", KEY_ID,
                        f"--project={KEY_PROJECT}", "--format=value(keyString)"],
                       capture_output=True, text=True)
    key = r.stdout.strip()
    if not key:
        print("could not retrieve the API key via gcloud", file=sys.stderr)
        sys.exit(1)
    return key


def window(key: str, term: str, after: str, before: str) -> tuple[set, int]:
    """Unique video ids in a window, and the calls it cost."""
    ids, token, calls = set(), None, 0
    for _ in range(MAX_PAGES):
        # Must match pipeline/ingestion/youtube_census.py exactly. It sends
        # relevanceLanguage=en and NO order param (so the API defaults to
        # relevance). Probing with order=date instead restricts the candidate set
        # before it returns: the same window gave 1 id under order=date against
        # 149 under the census parameters, which made every window look empty.
        p = {"key": key, "part": "snippet", "type": "video", "q": term,
             "maxResults": 50, "relevanceLanguage": "en",
             "publishedAfter": after, "publishedBefore": before}
        if token:
            p["pageToken"] = token
        ok = False
        for attempt in range(4):
            try:
                url = "https://www.googleapis.com/youtube/v3/search?" + urllib.parse.urlencode(p)
                with urllib.request.urlopen(url, timeout=45) as r:
                    d = json.loads(r.read())
                ok = True
                break
            except Exception:
                time.sleep(5 * (attempt + 1))     # 429 is transient, never terminal
        if not ok:
            break
        calls += 1
        ids |= {i["id"]["videoId"] for i in d.get("items", []) if i.get("id", {}).get("videoId")}
        token = d.get("nextPageToken")
        if not token:
            break
        time.sleep(DELAY)
    # `truncated` = we stopped because of MAX_PAGES, not because the API ran out.
    return ids, calls, bool(token)


def depth_for(score: float) -> str:
    for edge, name in BINS:
        if score < edge:
            return name
    return "hour"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    ap.add_argument("--year-start", type=int, default=2010)
    ap.add_argument("--year-end", type=int, default=2025)
    ap.add_argument("--variant", choices=["russian", "ukrainian"], default=None)
    a = ap.parse_args()

    pair = get_pair_by_slug(a.pair)
    if not pair:
        print(f"unknown pair {a.pair}", file=sys.stderr); sys.exit(1)
    OUT.mkdir(parents=True, exist_ok=True)
    out_path = OUT / f"{a.pair}_depth.parquet"

    key = api_key()
    variants = ({a.variant: pair[a.variant]} if a.variant
                else {"russian": pair["russian"], "ukrainian": pair["ukrainian"]})
    rows, spent = [], 0

    for variant, term in variants.items():
        for year in range(a.year_start, a.year_end + 1):
            for month in range(1, 13):
                last = calendar.monthrange(year, month)[1]
                mid = last // 2
                whole, c0, trunc = window(key, term, f"{year}-{month:02d}-01T00:00:00Z",
                                          f"{year}-{month:02d}-{last:02d}T23:59:59Z")
                h1, c1, _ = window(key, term, f"{year}-{month:02d}-01T00:00:00Z",
                                   f"{year}-{month:02d}-{mid:02d}T23:59:59Z")
                h2, c2, _ = window(key, term, f"{year}-{month:02d}-{mid+1:02d}T00:00:00Z",
                                   f"{year}-{month:02d}-{last:02d}T23:59:59Z")
                halves = h1 | h2
                spent += c0 + c1 + c2
                base = max(len(whole), 1)
                score = math.log2(max(len(halves), 1) / base)
                rows.append({
                    "pair_slug": a.pair, "variant": variant,
                    "month": f"{year}-{month:02d}",
                    "month_ids": len(whole), "half_ids": len(halves),
                    "new_ids": len(halves - whole),
                    "score": round(score, 3), "depth": depth_for(score),
                    "month_truncated": trunc,
                    "calls": c0 + c1 + c2,
                })
                time.sleep(DELAY)
            # Save each year: a run that dies late should not discard what it has.
            pd.DataFrame(rows).to_parquet(out_path, index=False)
            print(f"  {variant} {year}  ({spent} calls)", flush=True)

    df = pd.DataFrame(rows)
    df.to_parquet(out_path, index=False)
    print(f"\nwrote {len(df)} rows, {spent} api calls -> {out_path}")
    print(df.groupby(["variant", "depth"]).size().to_string())


if __name__ == "__main__":
    main()
