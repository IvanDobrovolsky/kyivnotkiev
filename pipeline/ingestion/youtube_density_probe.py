"""Rank every pair-month by search density, to decide how deep to collect it.

WHY
---
YouTube's search API truncates without signalling it: a day with 539,051 claimed
results served 41 items and no next-page token, indistinguishable from a day that
genuinely holds 41 videos. So the collector's descent trigger — a full last page —
never fires where it matters, and busy months come back as flat as quiet ones.

`pageInfo.totalResults` is the one field that does carry the signal. Measured on
chornobyl, all 1-day windows, Russian variant:

    2022-02-25  invasion +1              670,046
    2019-06-04  HBO finale               539,051
    2020-04-05  exclusion zone fires     476,258
    2016-04-26  30th anniversary         151,378
    2013-03-14  quiet baseline            35,976

19x between quiet and peak, and the ordering matches events known from other
sources.

WHAT IT IS NOT
--------------
It is not a count and must never be used as a denominator. It scales sub-linearly
with window width — a month reads 3.7x a day, not 30x — so only windows of the
SAME width are comparable. It is also bucketed (consecutive days returned
identical values) and non-deterministic: the same day, same term, returned 16,911
on one call and 35,976 on another, 2.1x apart.

That noise is survivable because the useful signal is 19x and the bands below are
coarse. It would not survive being treated as a measurement.

OUTPUT
------
One row per pair-month-variant with its totalResults and a depth rank:

    1  month   sparse; a single month window returns what is there
    2  week
    3  day
    4  hour    dense; a day window will truncate

Ranks are assigned from quantiles of the pair's own distribution, not fixed
thresholds, since absolute values differ by orders of magnitude between pairs.

Usage:
    python -m pipeline.ingestion.youtube_density_probe --pair chornobyl
"""

import argparse
import calendar
import json
import pathlib
import subprocess
import sys
import time
import urllib.parse
import urllib.request

import pandas as pd

from pipeline.config import ROOT_DIR, get_pair_by_slug

OUT = ROOT_DIR / "data" / "cl" / "raw" / "youtube_density"
KEY_ID = "029b0141-1889-4665-a569-36d75c0f6191"
KEY_PROJECT = "kyivnotkiev-yt"
LADDER = {1: "month", 2: "week", 3: "day", 4: "hour"}
# Absolute thresholds, per window width, calibrated from measured day-width probes:
# a quiet chornobyl day read ~36k and the invasion day ~670k. Absolute rather than
# quantile-based, so a uniformly sparse pair correctly gets no deep collection at
# all — quantiles would force the same band distribution onto every pair.
WEEK_DESCEND = 150_000
DAY_DESCEND = 300_000
DELAY = 0.8            # 80/min ceiling, enforced per call


def api_key() -> str:
    r = subprocess.run(["gcloud", "services", "api-keys", "get-key-string", KEY_ID,
                        f"--project={KEY_PROJECT}", "--format=value(keyString)"],
                       capture_output=True, text=True)
    key = r.stdout.strip()
    if not key:
        print("could not retrieve the API key via gcloud", file=sys.stderr)
        sys.exit(1)
    return key


def probe(key: str, term: str, year: int, month: int,
          span: tuple[str, str] | None = None, retries: int = 4) -> int | None:
    """totalResults for a window. `span` is (YYYY-MM-DD, YYYY-MM-DD) inclusive."""
    if span:
        after, before = f"{span[0]}T00:00:00Z", f"{span[1]}T23:59:59Z"
    else:
        last = calendar.monthrange(year, month)[1]
        after = f"{year}-{month:02d}-01T00:00:00Z"
        before = f"{year}-{month:02d}-{last}T23:59:59Z"
    params = {"key": key, "part": "snippet", "type": "video", "q": term,
              "maxResults": 1, "order": "date",
              "publishedAfter": after, "publishedBefore": before}
    url = "https://www.googleapis.com/youtube/v3/search?" + urllib.parse.urlencode(params)
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(url, timeout=45) as r:
                d = json.loads(r.read())
            return int(d.get("pageInfo", {}).get("totalResults", 0))
        except Exception:
            time.sleep(5 * (attempt + 1))       # 429 is transient, never terminal
    return None


def week_spans(year: int, month: int) -> list[tuple[str, str]]:
    last = calendar.monthrange(year, month)[1]
    out, d = [], 1
    while d <= last:
        e = min(d + 6, last)
        out.append((f"{year}-{month:02d}-{d:02d}", f"{year}-{month:02d}-{e:02d}"))
        d = e + 1
    return out


def descend(key: str, term: str, year: int, month: int, rows: list) -> int:
    """Probe month, then narrower windows only where density warrants it.

    Measuring at a single width does not work. totalResults scales sub-linearly
    with the window, so at month width the whole range compresses: chornobyl's top
    twelve Russian months span 1.13x, while the same signal at day width spans 19x.
    Saturation at one width is therefore the trigger to descend, not a ceiling.

    Each level is compared only against windows of its OWN width, which is the only
    comparison the API supports.
    """
    m = probe(key, term, year, month)
    rows.append({"level": "month", "window": f"{year}-{month:02d}", "total_results": m})
    if m is None:
        return 1
    time.sleep(DELAY)

    weeks = []
    for a, b in week_spans(year, month):
        w = probe(key, term, year, month, span=(a, b))
        rows.append({"level": "week", "window": f"{a}..{b}", "total_results": w})
        weeks.append((a, b, w or 0))
        time.sleep(DELAY)
    if not weeks:
        return 1
    top = max(w for _, _, w in weeks)
    if top < WEEK_DESCEND:
        return 2

    hot = [(a, b, w) for a, b, w in weeks if w >= WEEK_DESCEND]
    day_max = 0
    for a, b, _ in hot:
        d0 = int(a[-2:])
        d1 = int(b[-2:])
        for dd in range(d0, d1 + 1):
            day = f"{year}-{month:02d}-{dd:02d}"
            v = probe(key, term, year, month, span=(day, day))
            rows.append({"level": "day", "window": day, "total_results": v})
            day_max = max(day_max, v or 0)
            time.sleep(DELAY)
    return 4 if day_max >= DAY_DESCEND else 3


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    ap.add_argument("--year-start", type=int, default=2010)
    ap.add_argument("--year-end", type=int, default=2025)
    a = ap.parse_args()

    pair = get_pair_by_slug(a.pair)
    if not pair:
        print(f"unknown pair {a.pair}", file=sys.stderr); sys.exit(1)
    OUT.mkdir(parents=True, exist_ok=True)
    out_path = OUT / f"{a.pair}_density.parquet"

    key = api_key()
    variants = {"russian": pair["russian"], "ukrainian": pair["ukrainian"]}
    ranks, probes = [], []
    months = [(y, m) for y in range(a.year_start, a.year_end + 1) for m in range(1, 13)]
    print(f"{a.pair}: {len(months) * len(variants)} months, descending only where dense")

    for variant, term in variants.items():
        for i, (y, m) in enumerate(months, 1):
            rows: list = []
            rank = descend(key, term, y, m, rows)
            for r in rows:
                r.update({"pair_slug": a.pair, "variant": variant, "term": term,
                          "month": f"{y}-{m:02d}"})
            probes.extend(rows)
            ranks.append({"pair_slug": a.pair, "variant": variant,
                          "month": f"{y}-{m:02d}", "depth_rank": rank,
                          "depth": LADDER[rank],
                          "month_total": rows[0]["total_results"] if rows else None})
            if i % 12 == 0:
                pd.DataFrame(ranks).to_parquet(out_path, index=False)
                pd.DataFrame(probes).to_parquet(
                    OUT / f"{a.pair}_probes.parquet", index=False)
                print(f"  {variant} {y}  ({len(probes)} probes so far)", flush=True)

    rk = pd.DataFrame(ranks)
    rk.to_parquet(out_path, index=False)
    pd.DataFrame(probes).to_parquet(OUT / f"{a.pair}_probes.parquet", index=False)
    print(f"\nwrote {len(rk)} month ranks and {len(probes)} probes -> {OUT}")
    print(rk.groupby(["variant", "depth"]).size().to_string())
    cost = {1: 1, 2: 4, 3: 31, 4: 31 * 24}
    for v, g in rk.groupby("variant"):
        print(f"  {v:<10} collection windows at rank depth: "
              f"{sum(cost[r] for r in g.depth_rank):,}")


if __name__ == "__main__":
    main()
