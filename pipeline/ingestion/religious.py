"""Scrape religious institution websites for toponym adoption analysis.

Targets:
1. mospat.ru/en/ - Moscow Patriarchate DECR (6,000+ EN articles)
2. patriarchia.ru/en/ - Russian Orthodox Church main site
3. oikoumene.org - World Council of Churches

Usage:
    python -m pipeline.ingestion.religious [--site mospat]
"""

import argparse
import logging
import re
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup

from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = ROOT_DIR / "data" / "raw" / "religious"
UA = "KyivNotKiev-Research/1.0 (academic research)"

with open(ROOT_DIR / "config" / "pairs.yaml") as f:
    _cfg = yaml.safe_load(f)

PAIRS = []
for p in _cfg["pairs"]:
    if not p.get("enabled") or p.get("is_control"):
        continue
    PAIRS.append({
        "id": p["id"], "russian": p["russian"], "ukrainian": p["ukrainian"],
        "ru_re": re.compile(r"\b" + re.escape(p["russian"]) + r"\b", re.IGNORECASE),
        "ua_re": re.compile(r"\b" + re.escape(p["ukrainian"]) + r"\b", re.IGNORECASE),
    })


def match_text(text):
    matches = []
    for p in PAIRS:
        if p["ru_re"].search(text):
            matches.append((p["id"], "russian", p["russian"]))
        if p["ua_re"].search(text):
            matches.append((p["id"], "ukrainian", p["ukrainian"]))
    return matches


def fetch(url, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers={"User-Agent": UA}, timeout=15)
            if resp.status_code == 200:
                return resp.text
        except Exception as e:
            if attempt == retries - 1:
                log.warning(f"  Failed: {url}: {e}")
        time.sleep(1)
    return None


def save_checkpoint(results, name):
    if results:
        df = pd.DataFrame(results).drop_duplicates(subset=["pair_id", "url", "variant"])
        df.to_parquet(OUT_DIR / f"{name}_checkpoint.parquet", index=False)
        log.info(f"    checkpoint: {len(df):,} records")


def scrape_mospat():
    log.info("=== mospat.ru/en/ ===")
    results = []
    seen = set()
    for section in ["/news/", "/the-head-of-the-decr/", "/documents/"]:
        empty = 0
        page = 1
        while empty < 3 and page < 300:
            url = f"https://mospat.ru/en{section}" if page == 1 else f"https://mospat.ru/en{section}page/{page}/"
            log.info(f"  {section} p{page}")
            html = fetch(url)
            if not html:
                empty += 1; page += 1; continue
            soup = BeautifulSoup(html, "html.parser")
            links = []
            for a in soup.find_all("a", href=True):
                href = a["href"]
                m = re.search(r"/en/\S+/(\d{4})/(\d{2})/(\d{2})/", href)
                if m:
                    full = href if href.startswith("http") else f"https://mospat.ru{href}"
                    if full not in seen:
                        seen.add(full)
                        links.append((full, f"{m.group(1)}-{m.group(2)}-{m.group(3)}"))
            if not links:
                empty += 1; page += 1; continue
            empty = 0
            for art_url, date_str in links:
                art_html = fetch(art_url)
                if not art_html: continue
                body = BeautifulSoup(art_html, "html.parser").find("div", class_="entry-content") or \
                       BeautifulSoup(art_html, "html.parser").find("article")
                if not body: continue
                text = body.get_text(separator=" ", strip=True)
                if len(text) < 50: continue
                for pid, var, term in match_text(text):
                    results.append({"pair_id": pid, "source": "mospat", "source_domain": "mospat.ru",
                                    "url": art_url, "date": date_str, "variant": var,
                                    "matched_term": term, "text": text[:3000]})
                time.sleep(0.3)
            if page % 10 == 0:
                save_checkpoint(results, "mospat")
            page += 1; time.sleep(0.5)
    return results


def scrape_patriarchia():
    log.info("=== patriarchia.ru/en/ ===")
    results = []
    seen = set()
    offset = 0
    while offset < 4000:
        url = f"https://patriarchia.ru/en/db/news/?p={offset}" if offset > 0 else "https://patriarchia.ru/en/db/news/"
        log.info(f"  offset {offset}")
        html = fetch(url)
        if not html: break
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/en/db/text/" in href:
                full = f"https://patriarchia.ru{href}" if not href.startswith("http") else href
                if full not in seen:
                    seen.add(full)
                    links.append(full)
        if not links: break
        for art_url in links:
            art_html = fetch(art_url)
            if not art_html: continue
            art_soup = BeautifulSoup(art_html, "html.parser")
            body = art_soup.find("div", class_="text") or art_soup.find("article")
            if not body: continue
            text = body.get_text(separator=" ", strip=True)
            if len(text) < 50: continue
            date_str = ""
            date_el = art_soup.find("time") or art_soup.find("span", class_="date")
            if date_el:
                dt = date_el.get("datetime", "") or date_el.get_text(strip=True)
                for fmt in ["%Y-%m-%dT%H:%M:%S", "%B %d, %Y", "%d %B %Y", "%Y-%m-%d"]:
                    try: date_str = datetime.strptime(dt[:19], fmt).strftime("%Y-%m-%d"); break
                    except ValueError: continue
            for pid, var, term in match_text(text):
                results.append({"pair_id": pid, "source": "patriarchia", "source_domain": "patriarchia.ru",
                                "url": art_url, "date": date_str, "variant": var,
                                "matched_term": term, "text": text[:3000]})
            time.sleep(0.3)
        if offset % 100 == 0:
            save_checkpoint(results, "patriarchia")
        offset += 20; time.sleep(0.5)
    return results


def scrape_wcc():
    log.info("=== oikoumene.org ===")
    results = []
    seen = set()
    for page in range(0, 100):
        url = f"https://www.oikoumene.org/news?page={page}"
        log.info(f"  page {page + 1}")
        html = fetch(url)
        if not html: break
        soup = BeautifulSoup(html, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/news/" in href and href != "/news" and "page=" not in href:
                full = href if href.startswith("http") else f"https://www.oikoumene.org{href}"
                if full not in seen:
                    seen.add(full)
                    links.append(full)
        if not links: break
        for art_url in links:
            art_html = fetch(art_url)
            if not art_html: continue
            art_soup = BeautifulSoup(art_html, "html.parser")
            body = art_soup.find("div", class_="field--body") or art_soup.find("article")
            if not body: continue
            text = body.get_text(separator=" ", strip=True)
            if len(text) < 50: continue
            date_str = ""
            time_el = art_soup.find("time")
            if time_el: date_str = (time_el.get("datetime", "") or "")[:10]
            for pid, var, term in match_text(text):
                results.append({"pair_id": pid, "source": "wcc", "source_domain": "oikoumene.org",
                                "url": art_url, "date": date_str, "variant": var,
                                "matched_term": term, "text": text[:3000]})
            time.sleep(0.3)
        time.sleep(0.5)
    return results


SCRAPERS = {"mospat": scrape_mospat, "patriarchia": scrape_patriarchia, "wcc": scrape_wcc}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--site", default="all")
    args = parser.parse_args()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    sites = list(SCRAPERS.keys()) if args.site == "all" else [args.site]
    all_results = []
    for site in sites:
        results = SCRAPERS[site]()
        log.info(f"\n{site}: {len(results):,} matches")
        if results:
            df = pd.DataFrame(results).drop_duplicates(subset=["pair_id", "url", "variant"])
            df.to_parquet(OUT_DIR / f"{site}.parquet", index=False)
            log.info(f"  Saved: {len(df):,} records, {df['pair_id'].nunique()} pairs")
            all_results.extend(results)
    if all_results:
        combined = pd.DataFrame(all_results).drop_duplicates(subset=["pair_id", "url", "variant"])
        combined.to_parquet(OUT_DIR / "all_religious.parquet", index=False)
        log.info(f"\nTotal: {len(combined):,} records, {combined['pair_id'].nunique()} pairs")
        log.info(f"Variants: {combined['variant'].value_counts().to_dict()}")


if __name__ == "__main__":
    main()
