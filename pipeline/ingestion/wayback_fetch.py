"""Recover paywalled and dead news articles from the Wayback Machine — STANDALONE.

NOT wired into the pipeline. Writes only under data/raw/gdelt/texts/wayback/ with
its own ledger; article_texts.parquet, the store and the verified corpus are not
touched. Merging happens later, deliberately, once the yield is validated.

WHY
---
188,558 fetched URLs returned 403 or a truncated body — paywalls, bot walls and
dead hosts. A 100-URL probe found 77% have a Wayback snapshot. Paywalled outlets
are not variant-neutral (the NYT tier adopted differently from wire services), so
excluding them biases per-outlet claims; archive.org is the standard, legitimate
recovery route.

MECHANICS
---------
The `available` API picks the snapshot CLOSEST TO THE ARTICLE'S GDELT DATE, not
the newest — a 2015 article's 2023 snapshot may carry a rewritten page. Content
is fetched via the `id_` form (original bytes, no toolbar), extracted with
trafilatura, and classified for both variants exactly as the main fetcher does.
A snapshot can be the paywall page itself, so extraction failure is an expected
outcome and is recorded, not retried forever.

Politeness: archive.org is a library. Concurrency 4, ~1 request/second overall.

Usage:
    python -m pipeline.ingestion.wayback_fetch --sample 500          # pilot
    python -m pipeline.ingestion.wayback_fetch --pair kyiv --limit 5000
"""

import argparse
import asyncio
import hashlib
import json
import pathlib
import re
import sys
import time
import urllib.parse

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
TEXTS = ROOT / "data" / "raw" / "gdelt" / "texts" / "article_texts.parquet"
OUT = ROOT / "data" / "raw" / "gdelt" / "texts" / "wayback"
LEDGER = OUT / "wayback_ledger.txt"
PARTS = OUT / "parts"

UA = "kyivnotkiev-research/1.0 (academic study of toponym adoption; contact: dobrovolsky94@gmail.com)"
CONCURRENCY = 4
DELAY = 0.9
MIN_TEXT = 300


def pair_patterns() -> dict:
    import yaml
    cfg = yaml.safe_load(open(ROOT / "config" / "pairs.yaml"))
    pats = {}
    for p in cfg["pairs"]:
        if not p.get("enabled", True):
            continue
        def rx(term):
            words = [re.escape(w) for w in str(term).split()]
            return re.compile(r"\b" + r"[\s\-]+".join(words) + r"\b", re.I)
        pats[p["slug"]] = {"ua": rx(p["ukrainian"]), "ru": rx(p["russian"])}
    return pats


def classify(body: str, slug: str, pats: dict):
    p = pats.get(slug)
    if not p:
        return 0, 0, None
    ua = len(p["ua"].findall(body))
    ru = len(p["ru"].findall(body))
    label = "both" if ua and ru else "ukrainian" if ua else "russian" if ru else "neither"
    return ua, ru, label


async def fetch_one(client, row, pats, sem):
    rec = {"url": row.url, "pair_slug": row.pair_slug, "domain": row.domain,
           "date": str(row.date)[:10], "snapshot": None, "status": None,
           "error": None, "text": None, "text_len": 0,
           "body_ua": 0, "body_ru": 0, "body_variant": None, "text_hash": None}
    import trafilatura
    async with sem:
        try:
            ts = str(row.date)[:10].replace("-", "") if pd.notna(row.date) else "2020"
            q = ("http://archive.org/wayback/available?url="
                 + urllib.parse.quote(str(row.url), safe="") + f"&timestamp={ts}")
            r = await client.get(q, timeout=25)
            snap = (r.json().get("archived_snapshots", {}) or {}).get("closest", {}) or {}
            if not snap.get("available"):
                rec["error"] = "no_snapshot"
                return rec
            snap_url = snap["url"].replace("http://", "https://")
            snap_url = re.sub(r"(/web/\d+)/", r"\1id_/", snap_url, count=1)
            rec["snapshot"] = snap_url
            await asyncio.sleep(DELAY)
            r2 = await client.get(snap_url, timeout=40, follow_redirects=True)
            rec["status"] = r2.status_code
            if r2.status_code != 200:
                rec["error"] = f"snap_http_{r2.status_code}"
                return rec
            body = trafilatura.extract(r2.text, include_comments=False,
                                       include_tables=False, no_fallback=False)
            if not body or len(body) < MIN_TEXT:
                rec["error"] = "extract_failed" if not body else "too_short"
                rec["text_len"] = len(body or "")
                return rec
            rec["text"] = body
            rec["text_len"] = len(body)
            rec["text_hash"] = hashlib.sha1(
                re.sub(r"\s+", " ", body).strip().encode()).hexdigest()[:16]
            ua, ru, label = classify(body, row.pair_slug, pats)
            rec["body_ua"], rec["body_ru"], rec["body_variant"] = ua, ru, label
        except Exception as e:                        # noqa: BLE001 — the class is the datum
            rec["error"] = type(e).__name__
    return rec


def flush(records):
    PARTS.mkdir(parents=True, exist_ok=True)
    n = len(list(PARTS.glob("part-*.parquet")))
    pd.DataFrame(records).to_parquet(PARTS / f"part-{n:06d}.parquet",
                                     compression="zstd", index=False)
    # Only actually-attempted URLs enter the ledger — the lesson of the main
    # fetcher's 60k lockout is baked in from the start here.
    with LEDGER.open("a") as fh:
        for r in records:
            fh.write(r["url"] + "\n")


async def run(targets, pats):
    import httpx
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(headers={"User-Agent": UA}, verify=False) as client:
        batch, done, t0 = [], 0, time.time()
        for i in range(0, len(targets), 40):
            chunk = targets[i:i + 40]
            recs = await asyncio.gather(*[fetch_one(client, r, pats, sem) for r in chunk])
            batch.extend(recs)
            done += len(chunk)
            if len(batch) >= 40:
                flush(batch); batch = []
            ok = sum(1 for x in recs if x.get("text"))
            print(f"  {done}/{len(targets)}  last-batch texts {ok}/{len(chunk)}  "
                  f"{done / max(time.time() - t0, 1):.1f} url/s", flush=True)
        if batch:
            flush(batch)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=0, help="random pilot of N urls")
    ap.add_argument("--pair")
    ap.add_argument("--limit", type=int, default=2000)
    a = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    d = pd.read_parquet(TEXTS, columns=["url", "pair_slug", "domain", "date",
                                        "status", "error"])
    pool = d[(d.status == 403) | (d.error == "too_short")].drop_duplicates("url")
    seen = set(LEDGER.read_text().split()) if LEDGER.exists() else set()
    pool = pool[~pool.url.isin(seen)]
    if a.pair:
        pool = pool[pool.pair_slug == a.pair]
    if a.sample:
        pool = pool.sample(min(a.sample, len(pool)), random_state=20260830)
    else:
        pool = pool.head(a.limit)
    print(f"wayback: {len(pool):,} urls (pool after ledger: honest, resumable)")
    asyncio.run(run(list(pool.itertuples()), pair_patterns()))

    frames = [pd.read_parquet(f) for f in sorted(PARTS.glob("part-*.parquet"))]
    allr = pd.concat(frames, ignore_index=True)
    allr.to_parquet(OUT / "wayback_texts.parquet", compression="zstd", index=False)
    got = allr[allr.text.notna()]
    print(f"\n{len(allr):,} attempted | {len(got):,} texts recovered "
          f"({len(got) / max(len(allr), 1) * 100:.1f}%)")
    if len(got):
        print(got.body_variant.value_counts().to_string())
    return 0


if __name__ == "__main__":
    main()
