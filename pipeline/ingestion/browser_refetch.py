"""Recover bot-blocked news articles by fetching as a real browser — STANDALONE.

The paywalled/too_short pool (188K URLs) is precisely the set of sites that
403 plain HTTP clients. Measured 2026-08-30: the same pool is unreachable via
archives too — web.archive.org rate-limits retrieval to ~2 good responses/min
(a 24h cooldown does not reset it), and Common Crawl's CCBot is itself blocked
at robots.txt by key domains (oaoa.com et al.), so its index holds no article
bodies for them. A Chrome-impersonated direct fetch (curl_cffi) recovered 44%
full-text in a 150-URL stratified pilot: soft bot-walls serve browsers.

Quarantined like wayback_fetch: writes only to texts/browser_refetch/, is NOT
part of the main pipeline, and nothing merges into article_texts.parquet until
the recovered stratum is audited. Records share wayback's schema (snapshot is
always None here) so merge tooling treats both uniformly.

Politeness: per-domain serial lane with a minimum interval — parallelism comes
from breadth across ~2,600 domains, never depth on one. 429 backs the domain
off and leaves the URL out of the ledger; 20 consecutive connection-level
failures abandon the domain (rows stay retryable — the main fetcher's 60k
ledger-lockout lesson).

    python -m pipeline.ingestion.browser_refetch --limit 200000
"""

import argparse
import asyncio
import hashlib
import pathlib
import re
import time

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
TEXTS = ROOT / "data" / "raw" / "gdelt" / "texts" / "article_texts.parquet"
OUT = ROOT / "data" / "raw" / "gdelt" / "texts" / "browser_refetch"
LEDGER = OUT / "refetch_ledger.txt"
PARTS = OUT / "parts"

CONCURRENCY = 24          # in-flight HTTP cap; breadth across domains
WORKERS = 48              # queue consumers; ones parked on lane backoffs are cheap
DOMAIN_INTERVAL = 1.5     # min seconds between hits on the SAME domain
DEAD_AFTER = 20           # consecutive connection failures -> abandon domain
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


class DomainLane:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.next_ok = 0.0        # monotonic time the next hit is allowed
        self.conn_fails = 0       # consecutive connection-level failures
        self.dead = False


async def fetch_one(session, row, pats, sem, lanes):
    rec = {"url": row.url, "pair_slug": row.pair_slug, "domain": row.domain,
           "date": str(row.date)[:10], "snapshot": None, "status": None,
           "error": None, "text": None, "text_len": 0,
           "body_ua": 0, "body_ru": 0, "body_variant": None, "text_hash": None}
    import trafilatura
    lane = lanes[row.domain]
    # Pacing sleeps hold only the domain lock; the global semaphore bounds
    # in-flight HTTP only — a backed-off domain must never occupy a slot.
    async with lane.lock:
        if lane.dead:
            rec["error"] = "domain_abandoned"
            rec["not_attempted"] = True
            return rec
        wait = lane.next_ok - time.monotonic()
        if wait > 0:
            await asyncio.sleep(wait)
        lane.next_ok = time.monotonic() + DOMAIN_INTERVAL
        async with sem:
            try:
                # Belt over curl_cffi's own timeout: one hung socket froze the
                # whole pool for an hour (0.1% CPU, zero completions). wait_for
                # guarantees the slot is returned no matter what libcurl does.
                r = await asyncio.wait_for(
                    session.get(row.url, timeout=25, allow_redirects=True), 35)
                rec["status"] = r.status_code
                lane.conn_fails = 0
                if r.status_code == 429:
                    # site pushed back: domain-level stand-down, URL stays retryable
                    rec["error"] = "http_429"
                    rec["not_attempted"] = True
                    lane.next_ok = time.monotonic() + 120
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
                rec["text_hash"] = hashlib.sha1(
                    re.sub(r"\s+", " ", body).strip().encode()).hexdigest()[:16]
                ua, ru, label = classify(body, row.pair_slug, pats)
                rec["body_ua"], rec["body_ru"], rec["body_variant"] = ua, ru, label
            except Exception as e:                    # noqa: BLE001 — the class is the datum
                rec["error"] = type(e).__name__
                rec["not_attempted"] = True
                lane.conn_fails += 1
                lane.next_ok = time.monotonic() + min(5 * lane.conn_fails, 60)
                if lane.conn_fails >= DEAD_AFTER:
                    lane.dead = True
    return rec


def flush(records):
    PARTS.mkdir(parents=True, exist_ok=True)
    n = len(list(PARTS.glob("part-*.parquet")))
    pd.DataFrame(records).to_parquet(PARTS / f"part-{n:06d}.parquet",
                                     compression="zstd", index=False)
    with LEDGER.open("a") as fh:
        for r in records:
            if not r.pop("not_attempted", False):
                fh.write(r["url"] + "\n")


async def run(targets, pats):
    # Worker pool, no barriers: the old 200-chunk gather waited on each chunk's
    # slowest member (a 25s timeout or a 60s lane backoff), capping the whole
    # run at the stragglers' pace. Workers pull from a shared queue instead.
    from curl_cffi.requests import AsyncSession
    sem = asyncio.Semaphore(CONCURRENCY)
    lanes = {}
    for r in targets:
        lanes.setdefault(r.domain, DomainLane())
    q = asyncio.Queue()
    for r in targets:
        q.put_nowait(r)
    batch, stats = [], {"done": 0, "texts": 0, "t0": time.time()}
    blk = asyncio.Lock()
    async with AsyncSession(impersonate="chrome") as session:
        async def worker():
            while True:
                try:
                    r = q.get_nowait()
                except asyncio.QueueEmpty:
                    return
                rec = await fetch_one(session, r, pats, sem, lanes)
                async with blk:
                    batch.append(rec)
                    stats["done"] += 1
                    if rec.get("text") is not None:
                        stats["texts"] += 1
                    if len(batch) >= 400:
                        out = batch[:]
                        batch.clear()
                        flush(out)
                    if stats["done"] % 200 == 0:
                        dead = sum(1 for l in lanes.values() if l.dead)
                        print(f"  {stats['done']}/{len(targets)}  "
                              f"texts {stats['texts']:,} "
                              f"({stats['texts'] / max(stats['done'], 1) * 100:.0f}%)  "
                              f"{stats['done'] / max(time.time() - stats['t0'], 1):.1f} url/s  "
                              f"dead-domains {dead}", flush=True)
        await asyncio.gather(*[worker() for _ in range(WORKERS)])
    if batch:
        flush(batch)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--limit", type=int, default=200000)
    a = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    d = pd.read_parquet(TEXTS, columns=["url", "pair_slug", "domain", "date",
                                        "status", "error"])
    pool = d[(d.status == 403) | (d.error == "too_short")].drop_duplicates("url")
    seen = set(LEDGER.read_text().split()) if LEDGER.exists() else set()
    pool = pool[~pool.url.isin(seen)]
    if a.sample:
        pool = pool.sample(min(a.sample, len(pool)), random_state=20260830)
    else:
        pool = pool.head(a.limit)
    # Interleave domains: rank-within-domain as primary sort key, so every
    # domain's lane drains concurrently instead of big domains serializing.
    pool = pool.assign(_rk=pool.groupby("domain").cumcount()).sort_values(
        ["_rk", "domain"]).drop(columns="_rk")
    print(f"browser_refetch: {len(pool):,} urls across "
          f"{pool.domain.nunique():,} domains (ledger-resumable)")
    asyncio.run(run(list(pool.itertuples()), pair_patterns()))

    frames = [pd.read_parquet(f) for f in sorted(PARTS.glob("part-*.parquet"))]
    allr = pd.concat(frames, ignore_index=True)
    allr = allr.drop_duplicates("url", keep="last")
    allr.to_parquet(OUT / "browser_texts.parquet", compression="zstd", index=False)
    got = allr[allr.text.notna()]
    print(f"\n{len(allr):,} attempted | {len(got):,} texts recovered "
          f"({len(got) / max(len(allr), 1) * 100:.1f}%)")
    if len(got):
        print(got.body_variant.value_counts().to_string())
    return 0


if __name__ == "__main__":
    main()
