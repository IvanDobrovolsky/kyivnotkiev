"""DuckDuckGo search audit using Playwright.

DuckDuckGo has no captcha and shows Wikipedia knowledge panels.
Captures autocorrect behavior, Wikipedia refs, and screenshots.

Usage:
    python -m pipeline.ingestion.playwright_audits.ddg_autocorrect
"""

import asyncio
import json
import logging
import re
from pathlib import Path

from playwright.sync_api import sync_playwright

from pipeline.config import load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SCREENSHOT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "screenshots" / "search"
OUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "search_audit"
SITE_DATA = Path(__file__).resolve().parent.parent.parent.parent / "site" / "src" / "data"

# Key pairs to test
TEST_PAIRS = [
    (1, "Kiev", "Kyiv"),
    (2, "Kharkov", "Kharkiv"),
    (3, "Odessa", "Odesa"),
    (10, "Chernobyl", "Chornobyl"),
    (21, "Chicken Kiev", "Chicken Kyiv"),
    (23, "borscht", "borshch"),
    (35, "Kievan Rus", "Kyivan Rus"),
    (36, "Cossack", "Kozak"),
    (54, "Babi Yar", "Babyn Yar"),
    (60, "Alexander Usyk", "Oleksandr Usyk"),
    (61, "Vladimir Zelensky", "Volodymyr Zelenskyy"),
    (70, "Vladimir the Great", "Volodymyr the Great"),
    (71, "Prince of Kiev", "Prince of Kyiv"),
    (72, "Bakhmut", "Artemovsk"),
]


def search_ddg(page, query: str, pair_id: int, variant: str) -> dict:
    """Search DuckDuckGo and capture results."""
    result = {
        "query": query,
        "variant": variant,
        "pair_id": pair_id,
        "autocorrected": False,
        "autocorrect_suggestion": None,
        "wikipedia_panel": None,
        "wikipedia_title": None,
        "wikipedia_snippet": None,
        "page_title": None,
        "screenshot": None,
    }

    try:
        url = f"https://duckduckgo.com/?q={query.replace(' ', '+')}&ia=web"
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(3000)

        content = page.content()
        result["page_title"] = page.title()

        # Check for "Did you mean" or spelling correction
        did_you_mean = page.query_selector('.js-spelling-suggestion, [data-testid="spelling-suggestion"]')
        if did_you_mean:
            text = did_you_mean.inner_text()
            result["autocorrected"] = True
            result["autocorrect_suggestion"] = text

        # Also check if DDG shows different term in results
        if variant == "ukrainian":
            russian_in_results = re.search(r'(?i)\b' + re.escape(query.split()[0]) + r'\b', content)

        # Wikipedia/knowledge panel
        wiki_panel = page.query_selector('.module--about, [data-testid="about-panel"]')
        if wiki_panel:
            panel_text = wiki_panel.inner_text()
            result["wikipedia_panel"] = panel_text[:300]
            # Extract title
            title_el = wiki_panel.query_selector('h2, .module__title, a[href*="wikipedia"]')
            if title_el:
                result["wikipedia_title"] = title_el.inner_text().strip()

        # Check for Wikipedia link in results
        wiki_match = re.search(r'en\.wikipedia\.org/wiki/([^"&\s]+)', content)
        if wiki_match:
            wiki_article = wiki_match.group(1).replace('_', ' ').replace('%27', "'")
            result["wikipedia_snippet"] = wiki_article

        # Screenshot
        SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r'[^\w]', '_', query)[:40]
        ss_path = SCREENSHOT_DIR / f"pair{pair_id}_{variant}_{safe_name}.png"
        page.screenshot(path=str(ss_path), full_page=False)
        result["screenshot"] = str(ss_path.name)

        log.info(f"    {variant}: wiki={result['wikipedia_snippet']}, autocorrect={result['autocorrected']}")

    except Exception as e:
        log.warning(f"    Error: {e}")

    return result


def main():
    log.info("=" * 60)
    log.info("SEARCH AUDIT (DuckDuckGo — no captcha)")
    log.info("=" * 60)

    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=300)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = context.new_page()

        for pair_id, russian, ukrainian in TEST_PAIRS:
            log.info(f"  Pair {pair_id}: {russian} / {ukrainian}")

            ru_result = search_ddg(page, russian, pair_id, "russian")
            results.append(ru_result)
            page.wait_for_timeout(2000)

            uk_result = search_ddg(page, ukrainian, pair_id, "ukrainian")
            results.append(uk_result)
            page.wait_for_timeout(2000)

        browser.close()

    # Save
    out_path = OUT_DIR / "search_audit.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    log.info(f"\nSaved: {out_path}")

    # Analysis: which Wikipedia articles does each form lead to?
    log.info("\nWIKIPEDIA ARTICLE MAPPING:")
    for pair_id, russian, ukrainian in TEST_PAIRS:
        ru_res = next((r for r in results if r["pair_id"] == pair_id and r["variant"] == "russian"), {})
        uk_res = next((r for r in results if r["pair_id"] == pair_id and r["variant"] == "ukrainian"), {})
        ru_wiki = ru_res.get("wikipedia_snippet", "—")
        uk_wiki = uk_res.get("wikipedia_snippet", "—")
        same = "✓ SAME" if ru_wiki == uk_wiki else "✗ DIFFERENT"
        log.info(f"  {russian:>25} → wiki: {ru_wiki}")
        log.info(f"  {ukrainian:>25} → wiki: {uk_wiki}  [{same}]")
        log.info("")

    # Save site data
    site_path = SITE_DATA / "search_audit.json"
    with open(site_path, "w") as f:
        json.dump({
            "source": "DuckDuckGo",
            "total_searches": len(results),
            "results": results,
            "wikipedia_mapping": [
                {
                    "pair_id": pid,
                    "russian_query": ru,
                    "ukrainian_query": uk,
                    "russian_wiki": next((r.get("wikipedia_snippet") for r in results if r["pair_id"] == pid and r["variant"] == "russian"), None),
                    "ukrainian_wiki": next((r.get("wikipedia_snippet") for r in results if r["pair_id"] == pid and r["variant"] == "ukrainian"), None),
                }
                for pid, ru, uk in TEST_PAIRS
            ],
        }, f, indent=2)
    log.info(f"Saved: {site_path}")


if __name__ == "__main__":
    main()
