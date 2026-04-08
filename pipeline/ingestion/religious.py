"""Collect religious-press data on toponym adoption.

The goal is to add a 9th source to the existing 8-source kyivnotkiev study:
how do major world religious bodies refer to Ukrainian places, and when
did they switch (or refuse to switch)? The Orthodox world is actively
split — Constantinople granted autocephaly to the Orthodox Church of
Ukraine in 2019, Moscow Patriarchate refuses to recognize it, the Vatican
shifted after 2022, the WCC followed.

For each source we crawl the English-language portion of the official site,
extract the publication year for each page (from sitemap lastmod or URL),
filter to pages mentioning the toponym pair, and count by year.

Sources implemented in this MVP:
  - Constantinople (Ecumenical Patriarchate, ec-patr.org) — WordPress
    sitemap with lastmod, ~7000 English-language posts
  - WCC (World Council of Churches, oikoumene.org) — Drupal sitemap
  - Vatican (vatican.va) — JS-driven, falls back to year-indexed crawl
    of speeches/messages by pope
  - Moscow Patriarchate (patriarchia.ru/en) — Next.js SPA, may need
    Playwright (TODO)
  - RISU (risu.ua) — TODO

Output: one JSON per source + a unified CSV in the standard format used
by openalex.py and the other ingestion modules so the religious source
slots into the existing master aggregation pipeline.

Usage:
    python -m pipeline.ingestion.religious                    # all sources, all pairs
    python -m pipeline.ingestion.religious --sources constantinople
    python -m pipeline.ingestion.religious --pair-ids 1
"""

import argparse
import csv
import json
import logging
import re
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import unquote

import requests

from pipeline.config import DATA_DIR, PROCESSED_DIR, ensure_dirs, load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

RAW_DIR = DATA_DIR / "raw" / "religious"
USER_AGENT = "kyivnotkiev-research/1.0 (academic study; contact: ivan@kyivnotkiev.org)"

START_YEAR = 2010
END_YEAR = 2026

# How long to wait between requests for a single source. Be polite — these
# are official church sites, not commercial APIs.
REQUEST_DELAY = 0.5


# ── Source: Ecumenical Patriarchate (Constantinople) ──

EC_PATR_SITEMAP_INDEX = "https://ec-patr.org/sitemap.xml"
EC_PATR_POST_SITEMAP_RE = re.compile(r"post-sitemap\d*\.xml")


def fetch_xml(url: str) -> str | None:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        log.warning(f"  fetch failed: {url}: {e}")
        return None


def parse_sitemap_entries(xml: str) -> list[tuple[str, str]]:
    """Return (url, lastmod) pairs from a sitemap XML."""
    out = []
    # Match <url><loc>...</loc>(<lastmod>...</lastmod>)?</url>
    for m in re.finditer(
        r"<url>\s*<loc>(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?</loc>"
        r"(?:\s*<lastmod>(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?</lastmod>)?",
        xml,
    ):
        out.append((m.group(1).strip(), (m.group(2) or "").strip()))
    return out


def parse_sitemap_index(xml: str) -> list[str]:
    """Return list of child sitemap URLs from a <sitemapindex>."""
    out = []
    for m in re.finditer(
        r"<sitemap>\s*<loc>(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?</loc>",
        xml,
    ):
        out.append(m.group(1).strip())
    return out


def crawl_constantinople() -> list[dict]:
    """Crawl ec-patr.org for English posts. Returns list of {url, year, html}.

    Strategy: walk the sitemap index → all post-sitemap*.xml children →
    every <url> entry. Filter to /en/ paths. Use sitemap <lastmod> as the
    publication year (this is when the post was created or last edited;
    for our adoption analysis the difference is small).
    """
    log.info("Constantinople: fetching sitemap index")
    idx_xml = fetch_xml(EC_PATR_SITEMAP_INDEX)
    if not idx_xml:
        return []
    children = [u for u in parse_sitemap_index(idx_xml) if EC_PATR_POST_SITEMAP_RE.search(u)]
    log.info(f"  found {len(children)} post sitemaps")

    en_entries = []  # (url, year)
    for child_url in children:
        time.sleep(REQUEST_DELAY)
        xml = fetch_xml(child_url)
        if not xml:
            continue
        for url, lastmod in parse_sitemap_entries(xml):
            if "/en/" not in url:
                continue
            year = None
            if lastmod and len(lastmod) >= 4 and lastmod[:4].isdigit():
                year = int(lastmod[:4])
            if year is None or year < START_YEAR or year > END_YEAR:
                continue
            en_entries.append((url, year))
    log.info(f"  collected {len(en_entries)} English-language posts in {START_YEAR}-{END_YEAR}")
    return [{"url": u, "year": y, "html": None} for u, y in en_entries]


# ── Source: WCC (World Council of Churches) ──

WCC_SITEMAP_INDEX = "https://www.oikoumene.org/sitemap.xml"


def crawl_wcc() -> list[dict]:
    log.info("WCC: fetching sitemap index")
    idx_xml = fetch_xml(WCC_SITEMAP_INDEX)
    if not idx_xml:
        return []
    children = parse_sitemap_index(idx_xml)
    log.info(f"  found {len(children)} child sitemaps")

    en_entries = []
    for child_url in children:
        time.sleep(REQUEST_DELAY)
        xml = fetch_xml(child_url)
        if not xml:
            continue
        for url, lastmod in parse_sitemap_entries(xml):
            # WCC English content is at /en/... but the default is also English
            year = None
            if lastmod and len(lastmod) >= 4 and lastmod[:4].isdigit():
                year = int(lastmod[:4])
            if year is None or year < START_YEAR or year > END_YEAR:
                continue
            en_entries.append((url, year))
    log.info(f"  collected {len(en_entries)} posts in {START_YEAR}-{END_YEAR}")
    return [{"url": u, "year": y, "html": None} for u, y in en_entries]


# ── Source: Vatican (vatican.va) ──
#
# Vatican.va has no useful sitemap (the front-end is a JS app), but the
# canonical content is organized by pope/section/year. Crawl per (pope,
# section, year) where each combination has a yearly index page.
#
# Example: https://www.vatican.va/content/francesco/en/speeches/2022/index.html
#
# Each yearly index lists every speech that year as <a href="...html">.
# Same shape for messages, audiences, homilies, angelus.

VATICAN_BASE = "https://www.vatican.va/content"
VATICAN_POPES = {
    "francesco":     (2013, 2025),   # Francis
    "leo-xiv":       (2025, END_YEAR),
    "benedict-xvi":  (2005, 2013),
    "john-paul-ii":  (1978, 2005),
}
VATICAN_SECTIONS = ["speeches", "messages", "homilies", "angelus", "audiences"]


def crawl_vatican() -> list[dict]:
    """For each (pope, section, year) combination in the study window,
    fetch the yearly index page and extract individual document URLs.
    """
    log.info("Vatican: crawling per-year indexes")
    en_entries = []

    for pope, (pope_start, pope_end) in VATICAN_POPES.items():
        ys = max(pope_start, START_YEAR)
        ye = min(pope_end, END_YEAR)
        for section in VATICAN_SECTIONS:
            for year in range(ys, ye + 1):
                url = f"{VATICAN_BASE}/{pope}/en/{section}/{year}.index.html"
                time.sleep(REQUEST_DELAY)
                try:
                    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
                except requests.RequestException as e:
                    log.warning(f"    {pope}/{section}/{year}: {e}")
                    continue
                if r.status_code != 200:
                    # No index for that combination — totally normal
                    continue
                # Extract document links from the year index
                doc_re = re.compile(rf'href="(/content/{pope}/en/{section}/{year}/[^"]+\.html)"')
                doc_urls = set()
                for m in doc_re.finditer(r.text):
                    href = m.group(1)
                    if "index.html" in href:
                        continue
                    doc_urls.add("https://www.vatican.va" + href)
                if doc_urls:
                    log.info(f"  {pope}/{section}/{year}: {len(doc_urls)} docs")
                for u in doc_urls:
                    en_entries.append({"url": u, "year": year, "html": None})

    log.info(f"  total {len(en_entries)} Vatican documents")
    return en_entries


# ── Generic content fetch + count ──


def fetch_html(url: str) -> str | None:
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        return r.text
    except requests.RequestException as e:
        log.debug(f"  fetch failed: {url}: {e}")
        return None


WORD_BOUNDARY_CACHE: dict[str, re.Pattern] = {}
def _term_re(term: str) -> re.Pattern:
    if term not in WORD_BOUNDARY_CACHE:
        # Word-boundary match, case-insensitive
        WORD_BOUNDARY_CACHE[term] = re.compile(rf"\b{re.escape(term)}\b", re.IGNORECASE)
    return WORD_BOUNDARY_CACHE[term]


def count_terms_in_html(html: str, ru_term: str, ua_term: str) -> tuple[int, int]:
    """Return (russian_count, ukrainian_count) word-boundary counts."""
    # Strip HTML tags so we don't double-count attribute values etc.
    text = re.sub(r"<[^>]+>", " ", html)
    text = unquote(text)
    return (
        len(_term_re(ru_term).findall(text)),
        len(_term_re(ua_term).findall(text)),
    )


def fetch_all_bodies(source_key: str, source_pages: list[dict]) -> dict[str, dict]:
    """Fetch every page once and cache its body text + year on disk.

    Returns a dict {url: {year, text}}. Skips pages already in the cache.
    """
    cache_path = RAW_DIR / f"{source_key}_bodies.json"
    cache: dict[str, dict] = {}
    if cache_path.exists():
        with open(cache_path) as f:
            cache = json.load(f)
        log.info(f"  loaded body cache: {len(cache)} pages")

    to_fetch = [p for p in source_pages if p["url"] not in cache]
    if not to_fetch:
        return cache

    log.info(f"  fetching {len(to_fetch)} new page bodies (this is the slow step)")
    saved_at = time.time()
    for i, entry in enumerate(to_fetch, 1):
        time.sleep(REQUEST_DELAY)
        html = fetch_html(entry["url"])
        if html is None:
            cache[entry["url"]] = {"year": entry["year"], "text": None}
            continue
        # Strip tags so we count body text only
        text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.S | re.I)
        text = re.sub(r"<style[^>]*>.*?</style>",   " ", text, flags=re.S | re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        text = unquote(text)
        # Light whitespace normalization (keeps file size sane)
        text = re.sub(r"\s+", " ", text).strip()
        cache[entry["url"]] = {"year": entry["year"], "text": text}

        # Periodic save so we don't lose progress on Ctrl-C
        if i % 50 == 0 or time.time() - saved_at > 30:
            with open(cache_path, "w") as f:
                json.dump(cache, f)
            saved_at = time.time()
            log.info(f"    fetched {i}/{len(to_fetch)} (cache: {cache_path})")

    with open(cache_path, "w") as f:
        json.dump(cache, f)
    log.info(f"  saved body cache: {cache_path} ({len(cache)} pages)")
    return cache


def collect_source_for_pair(source_key: str, bodies: dict[str, dict],
                            pair: dict) -> dict | None:
    """Scan cached page bodies for a single pair, aggregate counts by year."""
    if pair.get("is_control", False):
        return None
    pid = pair["id"]
    ru = pair["russian"]
    ua = pair["ukrainian"]

    log.info(f"  Pair {pid}: {ru!r} vs {ua!r}")

    yearly = defaultdict(lambda: {"russian_count": 0, "ukrainian_count": 0, "n_pages": 0})
    matched_pages = 0
    for url, entry in bodies.items():
        text = entry.get("text")
        if not text:
            continue
        ru_n = len(_term_re(ru).findall(text))
        ua_n = len(_term_re(ua).findall(text))
        if ru_n == 0 and ua_n == 0:
            continue
        y = entry["year"]
        yearly[y]["russian_count"] += ru_n
        yearly[y]["ukrainian_count"] += ua_n
        yearly[y]["n_pages"] += 1
        matched_pages += 1

    if not yearly:
        log.info(f"    no mentions in {len(bodies)} cached pages")
        return None

    rows = []
    for year in sorted(yearly.keys()):
        d = yearly[year]
        rows.append({
            "year": year,
            "russian_count": d["russian_count"],
            "ukrainian_count": d["ukrainian_count"],
            "n_pages": d["n_pages"],
            "total": d["russian_count"] + d["ukrainian_count"],
        })

    total_ru = sum(r["russian_count"] for r in rows)
    total_ua = sum(r["ukrainian_count"] for r in rows)
    log.info(f"    {matched_pages} pages → {total_ru + total_ua} mentions "
             f"({total_ua} UA, {total_ru} RU)")

    return {
        "pair_id": pid,
        "russian_term": ru,
        "ukrainian_term": ua,
        "category": pair.get("category", ""),
        "source": source_key,
        "yearly": rows,
        "total_russian": total_ru,
        "total_ukrainian": total_ua,
    }


# ── Source registry ──

CRAWLERS = {
    "constantinople": crawl_constantinople,
    "wcc":            crawl_wcc,
    "vatican":        crawl_vatican,
    # Moscow patriarchate and RISU TODO — both need different strategies
    # (Moscow uses a Next.js SPA, RISU has no sitemap)
}


def collect_all(sources: list[str] | None = None,
                pair_ids: list[int] | None = None) -> None:
    ensure_dirs()
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_pairs()
    pairs = [p for p in cfg["pairs"]
             if p.get("enabled", True) and not p.get("is_control", False)]
    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    sources_to_run = sources or list(CRAWLERS.keys())
    log.info(f"Religious ingest: sources={sources_to_run}, pairs={len(pairs)}")

    all_results = []

    for source_key in sources_to_run:
        if source_key not in CRAWLERS:
            log.error(f"unknown source: {source_key}")
            continue

        log.info(f"\n=== {source_key} ===")

        # 1) Build (or load cached) URL inventory for this source
        inventory_path = RAW_DIR / f"{source_key}_inventory.json"
        if inventory_path.exists():
            log.info(f"  loading cached inventory: {inventory_path}")
            with open(inventory_path) as f:
                pages = json.load(f)
        else:
            pages = CRAWLERS[source_key]()
            with open(inventory_path, "w") as f:
                json.dump(pages, f, indent=2)
            log.info(f"  saved inventory: {inventory_path} ({len(pages)} pages)")

        # 2) Fetch all page bodies once (cached on disk for replays)
        bodies = fetch_all_bodies(source_key, pages)

        # 3) For each pair, scan the cached bodies
        source_results = []
        for pair in pairs:
            data = collect_source_for_pair(source_key, bodies, pair)
            if data:
                source_results.append(data)

        # 3) Save raw JSON for this source
        out_path = RAW_DIR / f"{source_key}_results.json"
        with open(out_path, "w") as f:
            json.dump(source_results, f, indent=2)
        log.info(f"  saved {out_path} ({len(source_results)} pairs with mentions)")

        all_results.extend(source_results)

    # 4) Write a unified CSV in the same shape as openalex_summary.csv
    csv_path = PROCESSED_DIR / "religious_summary.csv"
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "source", "pair_id", "category", "russian_term", "ukrainian_term",
            "year", "russian_count", "ukrainian_count", "n_pages", "total",
            "adoption_ratio",
        ])
        for r in all_results:
            for yr in r["yearly"]:
                total = yr["total"]
                ratio = yr["ukrainian_count"] / total if total else None
                w.writerow([
                    r["source"], r["pair_id"], r["category"],
                    r["russian_term"], r["ukrainian_term"], yr["year"],
                    yr["russian_count"], yr["ukrainian_count"],
                    yr["n_pages"], total,
                    round(ratio, 4) if ratio is not None else "",
                ])
    log.info(f"\nSaved unified CSV: {csv_path}")

    # 5) Print headline summary
    log.info("\nHeadline summary:")
    by_source: dict[str, dict] = {}
    for r in all_results:
        bs = by_source.setdefault(r["source"], {"ru": 0, "ua": 0, "n": 0})
        bs["ru"] += r["total_russian"]
        bs["ua"] += r["total_ukrainian"]
        bs["n"] += 1
    log.info(f"{'Source':18s} {'pairs':>6} {'RU':>8} {'UA':>8} {'UA%':>7}")
    log.info("-" * 50)
    for src, d in sorted(by_source.items()):
        tot = d["ru"] + d["ua"]
        pct = d["ua"] / tot * 100 if tot else 0
        log.info(f"{src:18s} {d['n']:>6} {d['ru']:>8} {d['ua']:>8} {pct:>6.1f}%")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sources", nargs="*", default=None,
                        help=f"sources to crawl (default: {list(CRAWLERS.keys())})")
    parser.add_argument("--pair-ids", type=str, default=None,
                        help="comma-separated pair IDs to collect (default: all)")
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    collect_all(sources=args.sources, pair_ids=pair_ids)


if __name__ == "__main__":
    main()
