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

DATE-BOUNDED RUNS
-----------------
`--since YYYY-MM-DD` fetches only URLs whose EARLIEST GDELT date is on or after
that day, across every enabled pair, skipping any URL already in the ledger or
already carrying text. A URL whose earlier attempt never sent a request
(host_abandoned / host_backoff, kept out of the ledger) is fetched: inside the
window that is a first request, not a retry. That is the slice the verified tier
has never seen -- from 2026-08-27 for 22 pairs, because the 2026-09-10 fetch ran
against a matched file that ended 2026-08-26 23:45 -- plus whatever increments
were appended since.

It deliberately does NOT retry the historical backlog. 55,722 enabled-pair URLs
outside the ledger are dated before 2026-08-27 (host_abandoned / host_backoff are
kept out of the ledger by design, see flush()); retrying them in the same run
would move the 2015-2025 series inside the change that adds 2026, and nobody
could then say which part of a shift was new data (decided 2026-10-15). They are
a separate job with its own reported delta.

A URL first seen in 2019 and re-recorded in 2026-09 is a 2019 article: the
earliest date across every matched row decides, the same rule `final` uses.

Each date-bounded run records the URLs it targeted in runs/since_<day>.parquet
before fetching, so `pipeline.build_gdelt_verified --step filters` can rebuild
the verified tier without them and attribute every change to one cause.

This is a producer: --since bounds what is fetched, never what is kept, and no
study window is applied here. The exporter, migrate and analyze_pair clamp with
pipeline.config.

USAGE
-----
    python -m pipeline.ingestion.gdelt_fetch_texts --limit 600 --sample-per-year 50
    python -m pipeline.ingestion.gdelt_fetch_texts --all --concurrency 6
    python -m pipeline.ingestion.gdelt_fetch_texts --since 2026-01-01 --dry-run
    python -m pipeline.ingestion.gdelt_fetch_texts --since 2026-01-01
    python -m pipeline.ingestion.gdelt_fetch_texts --since 2026-01-01 --unattested
    python -m pipeline.ingestion.gdelt_fetch_texts --since 2026-01-01 --no-abandon   # the residue

Use the first day after TEXT_END, not the day the verified tier stops:
gdelt_paths_reconcile (R2b) needs every URL in the plotted 2026 months queued,
and 3 kyiv attested URLs from 2026-01 and 2026-03 never were. Measured 2026-10-19:
--since 2026-08-27 targets 3,649 attested + 4,199 unattested = 7,848 URLs;
--since 2026-01-01 targets 4,604 + 4,761 = 9,365, every one dated 2026.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
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
RUNS = OUT / "runs"

# zaporizhzhia, 2026-09-12: 58,194 urls in 65.1 min. The older 11.8 url/s figure
# predates the per-host AIMD limits below.
MEASURED_URL_PER_S = 14.9

CONCURRENCY = 96         # global ceiling
PER_HOST_START = 8       # starting simultaneous requests per host
PER_HOST_MAX = 16
PER_HOST_MIN = 1
HOST_DELAY = 0.0         # no blanket delay; back off only on evidence
HOST_FAIL_LIMIT = 8      # consecutive failures before a host is abandoned
HOST_429_BACKOFF = 30    # seconds to stand down from a host that rate-limits us
# --no-abandon: every target gets a real request. Abandonment and backoff-skips
# write rows that never sent a request, and a rerun only reaches about
# HOST_FAIL_LIMIT more URLs per dead host, so the residue never converges:
# after the 2026-11-05 --since 2026-01-01 runs, 4,665 of 36,388 targets on 99
# hosts were still unrequested (russiaherald 723, europesun 584, yahoo 508,
# interfax 392 -- interfax answered 2,740 of 3,145 in the same run). With the
# flag a failing host is still tried URL by URL and a 429 cooldown is waited
# out instead of skipped; per-host AIMD limits still apply.
NO_ABANDON = False

# A per-host cap makes dead hosts far more expensive than they look: a ConnectTimeout
# holds a slot for the full timeout, so with ~17 urls per host in a batch and
# PER_HOST=2 a single unresponsive host serialises the batch to ~85s. donbas is the
# worst case -- 12,097 remaining urls across 12 hosts, 75% of them on three. Hosts
# that have proven dead are abandoned rather than retried url by url; the rows are
# still written, carrying the reason, so coverage stays measurable.

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


async def fetch_one(client, row, pats, sem, host_sems, host_last, host_fails, host_until,
                    host_permits, ok_streak):
    import trafilatura
    from urllib.parse import urlparse
    rec = {"url": row.url, "pair_slug": row.pair_slug, "url_variant": row.variant,
           "domain": row.domain, "date": row.date,
           "status": None, "error": None, "text": None, "text_len": 0,
           "body_ua": 0, "body_ru": 0, "body_variant": None, "agrees": None,
           "final_url": None, "redirected_off_article": False, "text_hash": None}
    host = urlparse(str(row.url)).netloc
    if host_fails.get(host, 0) >= HOST_FAIL_LIMIT and not NO_ABANDON:
        rec["error"] = "host_abandoned"
        return rec
    # Additive-increase / multiplicative-decrease per host, the same shape as TCP
    # congestion control: assume a host can take real traffic and reduce only when it
    # says otherwise. A fixed conservative cap throttles hosts that were never the
    # problem -- with 12 hosts and PER_HOST=2 the whole run was capped at 24 in flight.
    hs = host_sems.setdefault(host, asyncio.Semaphore(PER_HOST_START))
    host_permits.setdefault(host, PER_HOST_START)
    async with sem, hs:
        now = asyncio.get_event_loop().time()
        if host_until.get(host, 0) > now:
            if not NO_ABANDON:
                rec["error"] = "host_backoff"
                return rec
            await asyncio.sleep(host_until[host] - now)
            now = asyncio.get_event_loop().time()
        host_last[host] = now
        try:
            r = await client.get(row.url, follow_redirects=True,
                                 headers={"User-Agent": UA})
            rec["status"] = r.status_code
            rec["final_url"] = str(r.url)
            if r.status_code == 429:
                host_until[host] = asyncio.get_event_loop().time() + HOST_429_BACKOFF
                host_fails[host] = host_fails.get(host, 0) + 1
                cur = host_permits.get(host, PER_HOST_START)
                host_permits[host] = max(PER_HOST_MIN, cur // 2)      # multiplicative decrease
                for _ in range(cur - host_permits[host]):
                    await hs.acquire()                                # retire permits
                rec["error"] = "http_429"
                return rec
            host_fails[host] = 0
            ok_streak[host] = ok_streak.get(host, 0) + 1
            cur = host_permits.get(host, PER_HOST_START)
            if ok_streak[host] >= 20 and cur < PER_HOST_MAX:          # additive increase
                host_permits[host] = cur + 1
                hs.release()
                ok_streak[host] = 0
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
            host_fails[host] = host_fails.get(host, 0) + 1
    return rec


def flush(records, _first=None):
    """Append a part file. Rewriting one parquet per batch is quadratic -- at 900 rows
    it had already dragged throughput down to 2.8 url/s. Parts are globbed on read."""
    PARTS.mkdir(parents=True, exist_ok=True)
    n = len(list(PARTS.glob("part-*.parquet")))
    pd.DataFrame(records).to_parquet(PARTS / f"part-{n:06d}.parquet", compression="zstd", index=False)
    # Only URLs we actually requested go in the ledger. host_abandoned and
    # host_backoff mean no request was ever made -- the host had tripped
    # HOST_FAIL_LIMIT, or was inside its 429 cooldown, and the URL was recorded
    # without being tried. Writing those as done turned a transient host failure
    # into permanent loss: 60,419 URLs were locked out of every later run, and the
    # loss is domain-correlated (yahoo.com, sputniknews.com, unian.info,
    # 112.international), so it skews per-outlet results rather than thinning them
    # evenly. Roughly a third of them return 200 on retry.
    NOT_ATTEMPTED = {"host_abandoned", "host_backoff"}
    with LEDGER.open("a") as fh:
        for r in records:
            if r.get("error") in NOT_ATTEMPTED:
                continue
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
    host_fails: dict = {}
    host_until: dict = {}
    host_permits: dict = {}
    ok_streak: dict = {}
    done, batch, t0, first = 0, [], time.time(), not RESULTS.exists()
    limits = httpx.Limits(max_connections=concurrency * 2, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(timeout=TIMEOUT, limits=limits, verify=False) as client:
        for i in range(0, len(targets), BATCH):
            chunk = targets[i:i + BATCH]
            recs = await asyncio.gather(*[
                fetch_one(client, r, pats_by_pair[r.pair_slug], sem, host_sems, host_last,
                          host_fails, host_until, host_permits, ok_streak)
                for r in chunk])
            batch.extend(recs)
            flush(batch, first); first = False; batch = []
            done += len(recs)
            ok = sum(1 for r in recs if r["text"])
            dead = sum(1 for h, n in host_fails.items() if n >= HOST_FAIL_LIMIT)
            print(f"  {done:,}/{len(targets):,}  batch_ok={ok}/{len(recs)}  "
                  f"{done/(time.time()-t0):.1f} url/s  hosts_abandoned={dead}", flush=True)


def since_pool(since: str, unattested: bool, enabled: set[str]) -> pd.DataFrame:
    """One pool's URLs whose earliest matched date is on or after `since`.

    Columns: url, pair_slug, variant, domain, date (the earliest one).

    Attested: final already carries one row per URL at its earliest date. The
    unattested pool is read with a date filter, and a URL is dropped if any
    matched row of it is dated before `since` -- checked against the url column
    of the older rows only, so the 14.4M-row frame is never materialised.
    """
    ts = pd.Timestamp(since)
    if not unattested:
        df = pd.read_parquet(SRC)
        return df[(df.date >= ts) & df.pair_slug.isin(enabled)].copy()

    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq
    new = pq.read_table(MATCHED, columns=["url", "domain", "date", "pair_slug", "native_en", "url_match"],
                        filters=[("date", ">=", ts)]).to_pandas()
    cand = pa.array(new.url.dropna().unique())
    old = pq.read_table(MATCHED, columns=["url"], filters=[("date", "<", ts)]).column("url")
    seen_before = set(old.filter(pc.is_in(old, value_set=cand)).to_pylist()) if len(cand) else set()
    del old
    attested = set(pd.read_parquet(SRC, columns=["url"]).url)
    df = new[~new.url.isin(seen_before) & new.native_en & ~new.url_match]
    # Earliest row per URL: its date is the article's, and its AllNames decided
    # the pair. The undated path keeps file order; this one must not, because an
    # appended increment sits at the end of the file.
    df = df.sort_values("date", kind="stable").drop_duplicates("url")
    df = df[~df.url.isin(attested) & df.pair_slug.isin(enabled)].copy()
    df["variant"] = None
    return df[["url", "pair_slug", "variant", "domain", "date"]]


def attempted_urls() -> tuple[set, set]:
    """(urls in the ledger, urls with text in the results). Merged browser and
    wayback recoveries carry text without passing through the ledger."""
    ledger = set(LEDGER.read_text().split()) if LEDGER.exists() else set()
    with_text = set()
    if RESULTS.exists():
        r = pd.read_parquet(RESULTS, columns=["url", "text_hash"])
        with_text = set(r.url[r.text_hash.notna()])
    return ledger, with_text


def manifest_path(since: str) -> pathlib.Path:
    return RUNS / f"since_{since}.parquet"


def record_manifest(since: str, df: pd.DataFrame, pool: str) -> pathlib.Path:
    """Add this run's targets to runs/since_<day>.parquet before any fetch, so a
    crash mid-run still leaves every row it wrote attributable to the run."""
    path = manifest_path(since)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = df[["url", "pair_slug", "date"]].assign(
        pool=pool, planned_at=pd.Timestamp.now(tz="UTC").isoformat(timespec="seconds"))
    if path.exists():
        old = pd.read_parquet(path)
        rows = pd.concat([old, rows[~rows.url.isin(set(old.url))]], ignore_index=True)
    tmp = path.with_name(path.name + ".writing")
    rows.to_parquet(tmp, compression="zstd", index=False)
    os.replace(tmp, path)
    return path


def report_plan(pool: pd.DataFrame, done: pd.Series, label: str) -> None:
    t = pd.DataFrame({"pool": pool.groupby("pair_slug").size(),
                      "requested": pool[done].groupby("pair_slug").size()}).fillna(0).astype(int)
    t["to_fetch"] = t["pool"] - t["requested"]
    print(t.sort_values("to_fetch", ascending=False).to_string())
    todo = pool[~done]
    by_month = todo.date.dt.to_period("M").astype(str).value_counts().sort_index()
    print(f"\n{label}: pool {len(pool):,} urls, already requested or with text {int(done.sum()):,}, "
          f"to fetch {len(todo):,} across {todo.pair_slug.nunique()} pair(s)")
    if len(by_month):
        print("to fetch by month: " + " · ".join(f"{k} {v:,}" for k, v in by_month.items()))
    print(f"~{len(todo) / MEASURED_URL_PER_S / 60:.0f} min at the measured {MEASURED_URL_PER_S} url/s "
          f"(arithmetic, not a timed run)")


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
    ap.add_argument("--since", metavar="YYYY-MM-DD",
                    help="only URLs first dated on or after this day, every enabled pair; "
                         "never retries older URLs")
    ap.add_argument("--dry-run", action="store_true", help="print the plan and write nothing")
    ap.add_argument("--no-abandon", action="store_true",
                    help="never abandon a host or skip a 429 cooldown: every target is requested")
    ap.add_argument("--concurrency", type=int, default=CONCURRENCY)
    a = ap.parse_args()
    global NO_ABANDON
    NO_ABANDON = a.no_abandon

    if a.since:
        if a.sample_per_year:
            print("--since and --sample-per-year do not combine", file=sys.stderr); return 1
        pd.Timestamp(a.since)                      # fail early on a malformed day
        pool = since_pool(a.since, a.unattested, set(pair_patterns()))
        if a.pair:
            pool = pool[pool.pair_slug == a.pair]
        ledger, with_text = attempted_urls()
        done = pool.url.isin(ledger) | pool.url.isin(with_text)
        report_plan(pool, done, f"{'unattested' if a.unattested else 'attested'} since {a.since}")
        if a.dry_run:
            return 0
        df, seen = pool[~done], ledger
        if df.empty:
            print("nothing left to fetch"); return 0
        OUT.mkdir(parents=True, exist_ok=True)
        print(f"targets recorded -> {record_manifest(a.since, df, 'unattested' if a.unattested else 'attested')}")
    else:
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
        if a.dry_run:
            print(f"would fetch {len(df):,} urls ({len(seen):,} already in the ledger)")
            return 0
        OUT.mkdir(parents=True, exist_ok=True)

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
    # Two columns, not the 1.6 GB text column, to report coverage.
    df = pd.read_parquet(RESULTS, columns=["url", "pair_slug", "text_hash"])
    ok = df.text_hash.notna().sum()
    print(f"-> {RESULTS}  {len(df):,} rows, {ok:,} with text ({ok/len(df)*100:.1f}%)")
    if a.since:
        mine = df[df.url.isin({t.url for t in targets})]
        per = mine.groupby("pair_slug").text_hash.agg(rows="size", with_text="count")
        print(f"this run: {len(mine):,} rows for {len(targets):,} targets\n{per.to_string()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
