"""Fetch article bodies and read the spelling off the prose, not the URL.

WHY
---
The mention metric currently reads the variant from the URL path, because that is
the only attested string GDELT gives us. But a slug is written once, at publication,
and never revised. An outlet that switched its style guide in 2022 still serves
2019 articles from /kiev-protests/, and a CMS can mint a slug from an older
headline than the body it carries. So URL-attestation can MISLABEL, not merely
under-count, and nothing in the current pipeline can detect it.

Body text settles it. This fetcher counts BOTH variants in the extracted prose and
records them separately, so disagreement is measured rather than assumed away:

    url_variant   what the slug claims
    body_ua / body_ru   word-boundary counts in the article text
    body_variant  ukrainian | russian | both | neither, from those counts
    agrees        whether body_variant matches url_variant

`both` is a real outcome, not a failure -- "Kyiv (formerly Kiev)" is exactly the
transitional usage this study is about, and collapsing it to one label destroys
the most interesting rows.

DESIGN
------
Nothing is inferred that can be recorded. Every attempt writes a row, including
failures, with the HTTP status or error class, so coverage is measurable and a
silent 404 never looks like an absent mention.

Resumable: a ledger of completed URLs is written alongside the results, so an
interrupted run resumes without refetching. Results are flushed per batch, never
accumulated in memory.

USAGE
-----
    python -m pipeline.ingestion.gdelt_fetch_texts --limit 600 --sample-per-year 50
    python -m pipeline.ingestion.gdelt_fetch_texts --all --concurrency 6
"""
from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import random
import hashlib
import re
import sys
from urllib.parse import urlparse
import time

import pandas as pd
import yaml

SRC = pathlib.Path("data/raw/gdelt/mentions_v2/gdelt_mentions_final.parquet")
MATCHED = pathlib.Path("data/raw/gdelt/mentions_v2/gdelt_mentions_matched.parquet")
OUT = pathlib.Path("data/raw/gdelt/texts")
LEDGER = OUT / "fetched_urls.txt"
RESULTS = OUT / "article_texts.parquet"
PARTS = OUT / "parts"

CONCURRENCY = 32         # global ceiling
PER_HOST = 2             # simultaneous requests to any ONE host
HOST_DELAY = 0.4         # seconds between requests to the same host

# The median is 3 urls per domain, but the median says nothing about the tail. On
# oleksandr-usyk a 5,000-url window held 256 urls on boxingnewsonline.net and 255 on
# boxingnews24.com; with only a global cap we opened dozens of simultaneous
# connections to each, drew 24% HTTP 403, and throughput collapsed from 11.8 to
# 0.4 url/s. Politeness has to be enforced per host, not on average.
TIMEOUT = 10           # a host that has not answered in 10s is not going to
BATCH = 200              # flush to disk this often
MIN_TEXT = 200           # below this, extraction failed rather than the page being short
UA = "Mozilla/5.0 (compatible; kyivnotkiev-research/1.0; +https://kyivnotkiev.org)"


def _lost_the_article(requested: str, final: str) -> bool:
    """True when a redirect landed somewhere that cannot be the requested article.

    Only fires when the destination path is effectively empty (homepage or section
    root) while the request asked for a deep path. Same-path http->https upgrades and
    ordinary canonical redirects are left alone.
    """
    a, b = urlparse(requested), urlparse(final)
    req_depth = len([x for x in a.path.split("/") if x])
    fin_depth = len([x for x in b.path.split("/") if x])
    return req_depth >= 2 and fin_depth <= 1


def pair_patterns() -> dict:
    """slug -> (ukrainian_regex, russian_regex), word-boundary, case-insensitive."""
    doc = yaml.safe_load(pathlib.Path("config/pairs.yaml").read_text())
    pairs = doc["pairs"] if isinstance(doc, dict) and "pairs" in doc else doc
    out = {}
    for p in pairs:
        if not p.get("enabled"):
            continue
        mk = lambda t: re.compile(r"\b" + r"[-_\s]+".join(re.escape(w) for w in str(t).split()) + r"\b", re.I)
        out[p["slug"]] = (mk(p["ukrainian"]), mk(p["russian"]))
    return out


def classify(text: str, pats: tuple) -> tuple:
    ua_rx, ru_rx = pats
    ua, ru = len(ua_rx.findall(text)), len(ru_rx.findall(text))
    if ua and ru:
        label = "both"
    elif ua:
        label = "ukrainian"
    elif ru:
        label = "russian"
    else:
        label = "neither"
    return ua, ru, label


async def fetch_one(client, row, pats, sem, host_sems, host_last):
    import trafilatura
    from urllib.parse import urlparse
    rec = {"url": row.url, "pair_slug": row.pair_slug, "url_variant": row.variant,
           "domain": row.domain, "date": row.date,
           "status": None, "error": None, "text": None, "text_len": 0,
           "body_ua": 0, "body_ru": 0, "body_variant": None, "agrees": None,
           "final_url": None, "redirected_off_article": False, "text_hash": None}
    host = urlparse(str(row.url)).netloc
    hs = host_sems.setdefault(host, asyncio.Semaphore(PER_HOST))
    async with sem, hs:
        # Space out requests to the same host; different hosts stay fully parallel.
        wait = HOST_DELAY - (asyncio.get_event_loop().time() - host_last.get(host, 0))
        if wait > 0:
            await asyncio.sleep(wait)
        host_last[host] = asyncio.get_event_loop().time()
        try:
            r = await client.get(row.url, follow_redirects=True,
                                 headers={"User-Agent": UA})
            rec["status"] = r.status_code
            rec["final_url"] = str(r.url)
            # A parked or restructured domain answers 200 with its CURRENT homepage.
            # uatoday.tv did exactly this: two 2015/2016 articles both came back as an
            # identical live war-casualty ticker dated 2026. That is worse than a 404
            # because it looks like data, so detect it by the redirect collapsing the
            # path away rather than by inspecting the text.
            if _lost_the_article(row.url, str(r.url)):
                rec["redirected_off_article"] = True
                rec["error"] = "redirected_off_article"
                return rec
            if r.status_code != 200:
                rec["error"] = f"http_{r.status_code}"
                return rec
            body = trafilatura.extract(r.text, include_comments=False,
                                       include_tables=False, no_fallback=False)
            if not body or len(body) < MIN_TEXT:
                rec["error"] = "extract_failed" if not body else "too_short"
                rec["text_len"] = len(body or "")
                return rec
            rec["text"] = body
            rec["text_len"] = len(body)
            # Syndicated wire copy runs under many domains and GDELT re-records the same
            # article under several dates. Hash the body so duplicates are identifiable
            # downstream without deciding here which copy is canonical.
            rec["text_hash"] = hashlib.sha1(re.sub(r"\s+", " ", body).strip().encode()).hexdigest()[:16]
            ua, ru, label = classify(body, pats)
            rec["body_ua"], rec["body_ru"], rec["body_variant"] = ua, ru, label
            rec["agrees"] = ((label == row.variant)
                             if (label in ("ukrainian", "russian") and row.variant) else None)
        except Exception as e:                       # noqa: BLE001 - error class IS the datum
            rec["error"] = type(e).__name__
    return rec


def flush(records, _first=None):
    """Append a part file. Rewriting one parquet per batch is quadratic -- at 900 rows
    it had already dragged throughput down to 2.8 url/s. Parts are globbed on read."""
    PARTS.mkdir(parents=True, exist_ok=True)
    n = len(list(PARTS.glob("part-*.parquet")))
    pd.DataFrame(records).to_parquet(PARTS / f"part-{n:06d}.parquet", compression="zstd", index=False)
    with LEDGER.open("a") as fh:
        for r in records:
            fh.write(r["url"] + "\n")


def consolidate() -> pathlib.Path:
    """Merge part files into the single results parquet."""
    parts = sorted(PARTS.glob("part-*.parquet"))
    if not parts:
        return RESULTS
    frames = [pd.read_parquet(f) for f in parts]
    if RESULTS.exists():
        frames.insert(0, pd.read_parquet(RESULTS))
    pd.concat(frames, ignore_index=True).to_parquet(RESULTS, compression="zstd", index=False)
    for f in parts:
        f.unlink()
    return RESULTS


async def run(targets, pats_by_pair, concurrency):
    import httpx
    sem = asyncio.Semaphore(concurrency)
    host_sems: dict = {}
    host_last: dict = {}
    done, batch, t0, first = 0, [], time.time(), not RESULTS.exists()
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(timeout=TIMEOUT, limits=limits, verify=False) as client:
        for i in range(0, len(targets), BATCH):
            chunk = targets[i:i + BATCH]
            recs = await asyncio.gather(*[
                fetch_one(client, r, pats_by_pair[r.pair_slug], sem, host_sems, host_last)
                for r in chunk])
            batch.extend(recs)
            flush(batch, first); first = False; batch = []
            done += len(recs)
            ok = sum(1 for r in recs if r["text"])
            print(f"  {done:,}/{len(targets):,}  batch_ok={ok}/{len(recs)}  "
                  f"{done/(time.time()-t0):.1f} url/s", flush=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pair", help="restrict to one pair slug, for a controlled trial run")
    ap.add_argument("--unattested", action="store_true",
                    help="fetch the English articles whose spelling is NOT in the url. "
                         "These contribute nothing to the metric today; the body is the "
                         "only way to learn which variant they used.")
    ap.add_argument("--limit", type=int, default=600)
    ap.add_argument("--sample-per-year", type=int, default=0,
                    help="stratify the sample evenly across years to expose link rot by age")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--concurrency", type=int, default=CONCURRENCY)
    a = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    if a.unattested:
        m = pd.read_parquet(MATCHED, columns=["url", "domain", "date", "pair_slug",
                                              "native_en", "url_match"])
        attested = set(pd.read_parquet(SRC, columns=["url"]).url)
        df = m[m.native_en & ~m.url_match].drop_duplicates("url")
        df = df[~df.url.isin(attested) & df.pair_slug.notna()].copy()
        df["variant"] = None          # unknown by definition -- that is the point
    else:
        df = pd.read_parquet(SRC)
    if a.pair:
        df = df[df.pair_slug == a.pair]
        if df.empty:
            print(f"no attested urls for pair '{a.pair}'", file=sys.stderr); return 1
    seen = set(LEDGER.read_text().split()) if LEDGER.exists() else set()
    df = df[~df.url.isin(seen)]
    if df.empty:
        print("nothing left to fetch"); return 0

    if a.sample_per_year:
        rng = random.Random(20260826)      # fixed seed: reruns pick the same sample
        parts = []
        for _, g in df.groupby(df.date.dt.year):
            idx = list(g.index)
            rng.shuffle(idx)
            parts.append(g.loc[idx[:a.sample_per_year]])
        df = pd.concat(parts)
    elif not a.all and not a.pair:
        df = df.head(a.limit)

    # Round-robin across hosts so a single batch never piles onto one site: sorting
    # by pair/date groups same-host urls together, which is the worst possible order.
    from urllib.parse import urlparse as _up
    df = df.assign(_host=df.url.map(lambda u: _up(str(u)).netloc))
    df = df.assign(_rank=df.groupby("_host").cumcount()).sort_values(["_rank", "_host"])
    targets = list(df.drop(columns=["_host", "_rank"]).itertuples())
    print(f"fetching {len(targets):,} urls at concurrency {a.concurrency} "
          f"({len(seen):,} already done)")
    asyncio.run(run(targets, pair_patterns(), a.concurrency))
    consolidate()
    df = pd.read_parquet(RESULTS)
    ok = df.text.notna().sum()
    print(f"-> {RESULTS}  {len(df):,} rows, {ok:,} with text ({ok/len(df)*100:.1f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
