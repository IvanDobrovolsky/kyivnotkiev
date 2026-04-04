"""Dictionary scraper for captcha-free dictionaries: Britannica + Wiktionary.

Checks both Russian and Ukrainian forms for each pair.
Captures: exists, redirects, article title, origin mentions, screenshots.

Usage:
    python -m pipeline.ingestion.playwright_audits.dict_nocaptcha
"""

import json
import logging
import re
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

from pipeline.config import load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "screenshots"
OUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "dictionaries"
SITE_DATA = Path(__file__).resolve().parent.parent.parent.parent / "site" / "src" / "data"

DICTIONARIES = {
    "britannica": {
        "name": "Britannica",
        "urls": [
            "https://www.britannica.com/place/{term}",
            "https://www.britannica.com/topic/{term}",
            "https://www.britannica.com/biography/{term}",
        ],
    },
    "wiktionary": {
        "name": "Wiktionary",
        "urls": [
            "https://en.wiktionary.org/wiki/{term}",
        ],
    },
}


def check_entry(page, dict_key: str, dict_name: str, urls: list, term: str,
                pair_id: int, variant: str) -> dict:
    result = {
        "dictionary": dict_name,
        "dict_key": dict_key,
        "term": term,
        "pair_id": pair_id,
        "variant": variant,
        "exists": False,
        "redirected": False,
        "final_url": None,
        "page_title": None,
        "snippet": None,
        "mentions_russian": False,
        "mentions_ukrainian": False,
        "mentions_origin": None,
        "screenshot": None,
    }

    url_term = term.replace(" ", "-") if dict_key == "britannica" else term.replace(" ", "_")

    for url_tmpl in urls:
        url = url_tmpl.format(term=url_term)
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=10000)
            time.sleep(1)

            if resp and resp.status == 200:
                final_url = page.url
                result["exists"] = True
                result["final_url"] = final_url

                # Check redirect
                if url_term.lower().replace("-", "") not in final_url.lower().replace("-", "").replace("_", "").replace("%20", ""):
                    result["redirected"] = True

                # Page title
                h1 = page.query_selector("h1")
                if h1:
                    result["page_title"] = h1.inner_text().strip()[:150]

                # Get text content for analysis
                content = page.content()
                text = re.sub(r'<[^>]+>', ' ', content).lower()

                result["mentions_russian"] = bool(re.search(r'\brussian\b', text))
                result["mentions_ukrainian"] = bool(re.search(r'\bukrainian\b', text))

                # Origin detection
                if re.search(r'(?:origin|derived|from|etymology).*?\brussian\b', text[:3000]):
                    result["mentions_origin"] = "russian"
                elif re.search(r'(?:origin|derived|from|etymology).*?\bukrainian\b', text[:3000]):
                    result["mentions_origin"] = "ukrainian"

                # Extract first paragraph/definition
                if dict_key == "britannica":
                    p_el = page.query_selector(".topic-paragraph, .md-article p")
                    if p_el:
                        result["snippet"] = p_el.inner_text().strip()[:300]
                elif dict_key == "wiktionary":
                    def_el = page.query_selector(".mw-parser-output > ol > li, .mw-parser-output dd")
                    if def_el:
                        result["snippet"] = def_el.inner_text().strip()[:300]

                # Screenshot
                ss_dir = BASE_DIR / dict_key
                ss_dir.mkdir(parents=True, exist_ok=True)
                safe = re.sub(r'[^\w]', '_', term)[:30]
                ss_path = ss_dir / f"pair{pair_id}_{variant}_{safe}.png"
                page.screenshot(path=str(ss_path), full_page=False)
                result["screenshot"] = f"{dict_key}/{ss_path.name}"

                break  # Found it, stop trying other URL patterns

        except Exception as e:
            continue

    status = "✓" if result["exists"] else "✗"
    redir = " (redirected)" if result["redirected"] else ""
    title = result["page_title"] or ""
    log.info(f"      {dict_name}: {status} {term} → {title[:40]}{redir}")
    return result


def main():
    log.info("=" * 60)
    log.info("DICTIONARY SCRAPER (Britannica + Wiktionary — no captcha)")
    log.info("=" * 60)

    cfg = load_pairs()
    all_pairs = [(p["id"], p["russian"], p["ukrainian"])
                 for p in cfg["pairs"]
                 if p.get("enabled", True) and not p.get("is_control", False)]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = context.new_page()

        for pair_id, russian, ukrainian in all_pairs:
            log.info(f"  Pair {pair_id}: {russian} / {ukrainian}")

            for dict_key, dict_info in DICTIONARIES.items():
                for term, variant in [(russian, "russian"), (ukrainian, "ukrainian")]:
                    r = check_entry(page, dict_key, dict_info["name"], dict_info["urls"],
                                    term, pair_id, variant)
                    results.append(r)
                    time.sleep(0.5)

        browser.close()

    # Save raw
    out_path = OUT_DIR / "dict_nocaptcha_results.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    log.info(f"\nSaved: {out_path}")

    # Analysis
    log.info("\nSUMMARY:")
    for dict_key, dict_info in DICTIONARIES.items():
        dr = [r for r in results if r["dict_key"] == dict_key]
        ru_exists = [r for r in dr if r["variant"] == "russian" and r["exists"]]
        uk_exists = [r for r in dr if r["variant"] == "ukrainian" and r["exists"]]
        ru_only = [r for r in dr if r["variant"] == "russian" and r["exists"] and
                   not any(r2["variant"] == "ukrainian" and r2["exists"] and r2["pair_id"] == r["pair_id"] for r2 in dr)]
        uk_only = [r for r in dr if r["variant"] == "ukrainian" and r["exists"] and
                   not any(r2["variant"] == "russian" and r2["exists"] and r2["pair_id"] == r["pair_id"] for r2 in dr)]

        log.info(f"  {dict_info['name']}:")
        log.info(f"    Russian form found: {len(ru_exists)}")
        log.info(f"    Ukrainian form found: {len(uk_exists)}")
        log.info(f"    Russian-only (no UA entry): {len(ru_only)}")
        log.info(f"    Ukrainian-only (no RU entry): {len(uk_only)}")
        log.info(f"    Mentions 'Russian' in text: {sum(1 for r in dr if r['mentions_russian'])}")
        log.info(f"    Mentions 'Ukrainian' in text: {sum(1 for r in dr if r['mentions_ukrainian'])}")

    # Interesting findings
    log.info("\nKEY FINDINGS:")
    for r in results:
        if r["redirected"] and r["exists"]:
            log.info(f"  REDIRECT: {r['dictionary']} '{r['term']}' → {r['page_title']}")
        if r["mentions_origin"] == "russian" and r["variant"] == "russian":
            log.info(f"  ORIGIN: {r['dictionary']} '{r['term']}' mentions Russian origin")

    # Save site data
    site_data = {
        "britannica": {},
        "wiktionary": {},
    }
    for r in results:
        dk = r["dict_key"]
        pid = r["pair_id"]
        if pid not in site_data[dk]:
            site_data[dk][pid] = {}
        site_data[dk][pid][r["variant"]] = {
            "exists": r["exists"],
            "title": r["page_title"],
            "snippet": r["snippet"],
            "redirected": r["redirected"],
            "mentions_russian": r["mentions_russian"],
            "mentions_ukrainian": r["mentions_ukrainian"],
        }

    site_path = SITE_DATA / "dictionary_detail.json"
    with open(site_path, "w") as f:
        json.dump(site_data, f, indent=2)
    log.info(f"Saved: {site_path}")


if __name__ == "__main__":
    main()
