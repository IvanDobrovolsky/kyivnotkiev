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
    """Return (url, lastmod) pairs from a sitemap XML.

    Tolerates <url> blocks where <lastmod> is not immediately adjacent to
    </loc> (e.g. WCC's Drupal sitemap puts <xhtml:link> in between).
    """
    out = []
    for url_block in re.finditer(r"<url>(.*?)</url>", xml, flags=re.S):
        block = url_block.group(1)
        loc_m = re.search(r"<loc>(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?</loc>", block)
        if not loc_m:
            continue
        url = loc_m.group(1).strip()
        lastmod_m = re.search(
            r"<lastmod>(?:<!\[CDATA\[)?([^<\]]+)(?:\]\]>)?</lastmod>", block
        )
        lastmod = lastmod_m.group(1).strip() if lastmod_m else ""
        out.append((url, lastmod))
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
# WCC has 160k+ URLs across 4 sitemaps. Most are translated /resources/
# pages we don't want. Focus on /news/ which is English by default and
# contains the press releases / official statements that matter for our
# question. ~30k news URLs with lastmod dates.
WCC_KEEP_PREFIXES = ("https://www.oikoumene.org/news/",)
# Skip URLs that have a language prefix (translations of news pages)
WCC_LANG_PREFIXES = ("/ar/", "/de/", "/el/", "/es/", "/fr/", "/he/", "/hu/",
                     "/id/", "/it/", "/ja/", "/ko/", "/nb/", "/pt-pt/",
                     "/ru/", "/sv/", "/sw/", "/uk/", "/zh-hans/")


def crawl_wcc() -> list[dict]:
    log.info("WCC: fetching sitemap index")
    idx_xml = fetch_xml(WCC_SITEMAP_INDEX)
    if not idx_xml:
        return []
    children = parse_sitemap_index(idx_xml)
    log.info(f"  found {len(children)} child sitemaps")

    en_entries = []
    seen = set()
    for child_url in children:
        time.sleep(REQUEST_DELAY)
        xml = fetch_xml(child_url)
        if not xml:
            continue
        for url, lastmod in parse_sitemap_entries(xml):
            if not any(url.startswith(p) for p in WCC_KEEP_PREFIXES):
                continue
            if any(p in url for p in WCC_LANG_PREFIXES):
                continue
            if url in seen:
                continue
            seen.add(url)
            year = None
            if lastmod and len(lastmod) >= 4 and lastmod[:4].isdigit():
                year = int(lastmod[:4])
            if year is None or year < START_YEAR or year > END_YEAR:
                continue
            en_entries.append((url, year))
    log.info(f"  collected {len(en_entries)} English news posts in {START_YEAR}-{END_YEAR}")
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


# ── Source: Moscow Patriarchate DECR (mospat.ru/en) ──
#
# The Department for External Church Relations of the Moscow Patriarchate
# publishes English-language news at mospat.ru/en/news/. Their Bitrix CMS
# exposes a clean AJAX endpoint at /en/ajax/news.php that paginates back
# to ~September 2015 (~500 pages × ~12 articles = ~6000 English articles
# spanning a decade).
#
# This is much better than patriarchia.ru/en which is a Next.js SPA with
# no pagination and only ~5 valid English articles per 100 ID range.
#
# Each AJAX page returns HTML tiles with `data-jsdate="YYYY-MM-DD"` so we
# get the publication date for free without rendering each article.

MOSPAT_AJAX_URL = "https://mospat.ru/en/ajax/news.php"
MOSPAT_DEFAULT_MAX_PAGE = 600  # currently ~500 pages exist; allow headroom


def crawl_mospat(max_page: int = MOSPAT_DEFAULT_MAX_PAGE) -> list[dict]:
    """Walk mospat.ru/en/news/ via the AJAX endpoint, collecting (url, date)
    pairs from the listing tiles. Returns standard inventory entries with
    the URL only — bodies are fetched in a second pass via fetch_all_bodies.
    """
    log.info(f"mospat: paginating /en/ajax/news.php up to page {max_page}")
    seen: set[str] = set()
    entries: list[tuple[str, str]] = []
    consecutive_empty = 0

    # Match a tile: an /en/news/<id>/ href followed (later in the markup)
    # by a data-jsdate="YYYY-MM-DD" attribute. We collect both via a
    # single multiline regex on each page's HTML.
    tile_re = re.compile(
        r'href="(/en/news/\d+/)"[^<]*(?:[^d]*d)*?data-jsdate="(\d{4}-\d{2}-\d{2})"',
        re.S,
    )

    for page_n in range(1, max_page + 1):
        time.sleep(REQUEST_DELAY)
        try:
            r = requests.post(
                MOSPAT_AJAX_URL,
                data={"page": str(page_n)},
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            r.raise_for_status()
        except requests.RequestException as e:
            log.warning(f"  page {page_n}: {e}")
            consecutive_empty += 1
            if consecutive_empty >= 5:
                log.info(f"  page {page_n}: 5 consecutive failures, stopping")
                break
            continue

        # Walk every tile to find (url, date) pairs. Use a fresh, simpler
        # approach: find every tile block, then extract the first href and
        # the first data-jsdate within it.
        page_added = 0
        for tile_match in re.finditer(r'<div class="in-tile[^"]*"[^>]*>(.*?)</div>\s*</div>', r.text, re.S):
            block = tile_match.group(1)
            href_m = re.search(r'href="(/en/news/\d+/)"', block)
            date_m = re.search(r'data-jsdate="(\d{4}-\d{2}-\d{2})"', block)
            if href_m and date_m:
                url = "https://mospat.ru" + href_m.group(1)
                if url in seen:
                    continue
                seen.add(url)
                entries.append((url, date_m.group(1)))
                page_added += 1

        if page_added == 0:
            consecutive_empty += 1
            if consecutive_empty >= 5:
                log.info(f"  page {page_n}: 5 consecutive empty pages, stopping")
                break
        else:
            consecutive_empty = 0

        if page_n % 25 == 0:
            log.info(f"  page {page_n}: total {len(entries)} unique articles")

    out = []
    for url, date in entries:
        try:
            year = int(date[:4])
        except ValueError:
            continue
        if START_YEAR <= year <= END_YEAR:
            out.append({"url": url, "year": year})
    log.info(f"  collected {len(out)} mospat articles in {START_YEAR}-{END_YEAR}")
    return out


# ── Source: Moscow Patriarchate (patriarchia.ru/en) ──
#
# Patriarchia.ru is a Next.js SPA — no useful sitemap, no RSS, no archive
# index. The site uses sequential numeric article IDs (most recent ~120k+).
# We use Playwright (already a dependency for the dictionary scrapers) to
# render each article page and extract the date + body.
#
# Strategy: scan a configurable range of article IDs from the most recent
# downward. Default samples the latest ~500 IDs (~6 months of activity)
# which is enough to answer the headline question "does Moscow Patriarchate
# currently use Kyiv?" — the killer finding for the paper is whether they
# refused to switch, not a fine-grained temporal trajectory.

PATRIARCHIA_DEFAULT_TOP = 120424   # latest known published English article
PATRIARCHIA_DEFAULT_RANGE = 1500   # ~1500 most recent → ~1.5 years of activity


def crawl_patriarchia(top_id: int = PATRIARCHIA_DEFAULT_TOP,
                      sample_size: int = PATRIARCHIA_DEFAULT_RANGE) -> list[dict]:
    """Render the latest N article pages via Playwright. Returns inventory
    entries with the article URL, year (parsed from in-page date), and
    full text body. Detects bad pages by absence of the "Version for print"
    marker, which is present on every English article and absent on the
    homepage / Russian-only fallback.

    Slow scraper: ~2-3s per article via Playwright. 1500 articles ≈ 60 min.
    Auto-checkpoints to inventory.json every 25 entries so it survives
    interruption.
    """
    try:
        from playwright.sync_api import sync_playwright
        from playwright_stealth import Stealth
    except ImportError:
        log.error("Patriarchia: playwright not installed")
        return []

    inventory_path = RAW_DIR / "patriarchia_inventory.json"
    entries: list[dict] = []
    seen_ids: set[int] = set()
    if inventory_path.exists():
        try:
            with open(inventory_path) as f:
                entries = json.load(f)
            seen_ids = {int(e["url"].rstrip("/").split("/")[-1]) for e in entries
                         if "/article/" in e["url"]}
            log.info(f"Patriarchia: resuming from {len(entries)} cached entries")
        except Exception as e:
            log.warning(f"  bad cache: {e}, starting fresh")
            entries = []
            seen_ids = set()

    log.info(f"Patriarchia: rendering article IDs {top_id - sample_size + 1}..{top_id}")
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context()
        Stealth().apply_stealth_sync(ctx)
        page = ctx.new_page()

        scanned = 0
        consecutive_invalid = 0
        for art_id in range(top_id, top_id - sample_size, -1):
            scanned += 1
            if art_id in seen_ids:
                continue
            url = f"https://www.patriarchia.ru/en/article/{art_id}"
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=20000)
                page.wait_for_timeout(1500)
            except Exception as e:
                log.debug(f"  {art_id}: load failed: {e}")
                consecutive_invalid += 1
                if consecutive_invalid >= 50:
                    log.info(f"  {art_id}: 50 consecutive load failures, stopping")
                    break
                continue

            body = page.eval_on_selector("body", "el => el.innerText") or ""

            # Validity check: real English article pages always have the
            # "Version for print" link near the article header. Homepage /
            # redirected pages don't.
            if "Version for print" not in body:
                consecutive_invalid += 1
                if consecutive_invalid >= 50:
                    log.info(f"  {art_id}: 50 consecutive invalid pages, stopping")
                    break
                continue
            consecutive_invalid = 0

            # Extract date: "Month D, YYYY HH:MM" preceding "Version for print"
            year = None
            m = re.search(
                r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+(\d{4})\s+\d{1,2}:\d{2}',
                body,
            )
            if m:
                year = int(m.group(2))
            if year is None or year < START_YEAR or year > END_YEAR:
                continue

            parts = body.split("Version for print", 1)
            article_text = parts[1].strip() if len(parts) > 1 else body

            entries.append({
                "url": url,
                "year": year,
                "text": article_text[:5000],
            })

            if len(entries) % 25 == 0:
                log.info(f"  collected {len(entries)} (scanned {scanned}, latest id {art_id})")
                with open(inventory_path, "w") as f:
                    json.dump(entries, f)

        browser.close()

    with open(inventory_path, "w") as f:
        json.dump(entries, f)
    log.info(f"  collected {len(entries)} valid English articles in {START_YEAR}-{END_YEAR}")
    return entries


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
    "patriarchia":    crawl_patriarchia,   # Moscow Patriarchate via Playwright (sparse)
    "mospat":         crawl_mospat,        # Moscow Patriarchate DECR via AJAX (dense)
    # RISU still TODO
}

# Sources where the crawler already returns full body text in the inventory
# (vs. the default pattern of returning URLs and then fetching bodies via
# requests). Patriarchia uses Playwright to render an SPA so it captures
# the body text in the same pass as the URL inventory.
SELF_CONTAINED_CRAWLERS = {"patriarchia"}


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
        if source_key in SELF_CONTAINED_CRAWLERS:
            # Patriarchia-style: inventory already has body text
            bodies = {p["url"]: {"year": p["year"], "text": p.get("text")} for p in pages}
        else:
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
