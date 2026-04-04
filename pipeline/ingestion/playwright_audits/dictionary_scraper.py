"""Dictionary scraper using Playwright.

Visits Oxford, Cambridge, Merriam-Webster, and Britannica for each pair.
Captures:
1. Does the article exist under Russian form, Ukrainian form, or both?
2. Does it redirect? (e.g., Kyiv → Kiev)
3. What origin/attribution does it give?
4. Screenshots of each entry.

Usage:
    python -m pipeline.ingestion.playwright_audits.dictionary_scraper
"""

import asyncio
import json
import logging
import re
from pathlib import Path

from playwright.async_api import async_playwright

from pipeline.config import load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "screenshots"
OUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "dictionaries"
SITE_DATA = Path(__file__).resolve().parent.parent.parent.parent / "site" / "src" / "data"


DICTIONARIES = {
    "oxford": {
        "name": "Oxford Learner's Dictionary",
        "url_template": "https://www.oxfordlearnersdictionaries.com/definition/english/{term}",
        "screenshot_dir": "oxford",
    },
    "cambridge": {
        "name": "Cambridge Dictionary",
        "url_template": "https://dictionary.cambridge.org/dictionary/english/{term}",
        "screenshot_dir": "cambridge",
    },
    "merriam_webster": {
        "name": "Merriam-Webster",
        "url_template": "https://www.merriam-webster.com/dictionary/{term}",
        "screenshot_dir": "merriam_webster",
    },
    "britannica": {
        "name": "Britannica",
        "url_template": "https://www.britannica.com/topic/{term}",
        "screenshot_dir": "britannica",
    },
}

# Pairs worth checking in dictionaries (single-word or well-known terms)
DICT_TERMS = [
    (1, "Kiev", "Kyiv"),
    (10, "Chernobyl", "Chornobyl"),
    (3, "Odessa", "Odesa"),
    (23, "borscht", "borshch"),
    (36, "Cossack", "Kozak"),
    (54, "Babi-Yar", "Babyn-Yar"),
    (35, "Kievan-Rus", "Kyivan-Rus"),
    (70, "Vladimir-the-Great", "Volodymyr-the-Great"),
    (72, "Bakhmut", "Artemovsk"),
]


async def check_dictionary(page, dict_key: str, dict_info: dict, term: str,
                           pair_id: int, variant: str) -> dict:
    """Check a single term in a single dictionary."""
    result = {
        "dictionary": dict_info["name"],
        "dict_key": dict_key,
        "term": term,
        "pair_id": pair_id,
        "variant": variant,
        "exists": False,
        "redirected": False,
        "redirected_to": None,
        "final_url": None,
        "page_title": None,
        "first_definition": None,
        "mentions_russian": False,
        "mentions_ukrainian": False,
        "screenshot": None,
    }

    url_term = term.lower().replace(" ", "-")
    url = dict_info["url_template"].format(term=url_term)

    try:
        response = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(2000)

        final_url = page.url
        result["final_url"] = final_url

        # Check if we were redirected
        if url_term not in final_url.lower().replace("-", "").replace("_", ""):
            result["redirected"] = True
            result["redirected_to"] = final_url

        # Check if page exists (not 404)
        if response and response.status < 400:
            result["exists"] = True

            # Get page title
            title_el = await page.query_selector("h1, .headword, .hw, title")
            if title_el:
                result["page_title"] = (await title_el.inner_text()).strip()[:100]

            # Get first definition text
            content = await page.content()
            def_match = re.search(r'class="[^"]*def[^"]*"[^>]*>([^<]+)', content)
            if def_match:
                result["first_definition"] = def_match.group(1).strip()[:200]

            # Check for Russian/Ukrainian mentions
            text = content.lower()
            result["mentions_russian"] = "russian" in text or "russia" in text
            result["mentions_ukrainian"] = "ukrainian" in text or "ukraine" in text

        # Screenshot
        ss_dir = BASE_DIR / dict_info["screenshot_dir"]
        ss_dir.mkdir(parents=True, exist_ok=True)
        safe_term = re.sub(r'[^\w]', '_', term)[:30]
        ss_path = ss_dir / f"pair{pair_id}_{variant}_{safe_term}.png"
        await page.screenshot(path=str(ss_path), full_page=False)
        result["screenshot"] = str(ss_path.relative_to(BASE_DIR))

    except Exception as e:
        log.warning(f"      Error: {e}")

    status = "✓" if result["exists"] else "✗"
    redir = f" → {result['redirected_to'][-40:]}" if result["redirected"] else ""
    log.info(f"      {dict_info['name']}: {status} {term}{redir}")

    return result


async def run_scraper():
    log.info("=" * 60)
    log.info("DICTIONARY SCRAPER (Playwright)")
    log.info("=" * 60)

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    all_results = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
        )
        page = await context.new_page()

        for pair_id, russian, ukrainian in DICT_TERMS:
            log.info(f"\n  Pair {pair_id}: {russian} / {ukrainian}")

            for dict_key, dict_info in DICTIONARIES.items():
                # Check Russian form
                ru_result = await check_dictionary(page, dict_key, dict_info, russian, pair_id, "russian")
                all_results.append(ru_result)
                await page.wait_for_timeout(2000)

                # Check Ukrainian form
                uk_result = await check_dictionary(page, dict_key, dict_info, ukrainian, pair_id, "ukrainian")
                all_results.append(uk_result)
                await page.wait_for_timeout(2000)

        await browser.close()

    # Save results
    out_path = OUT_DIR / "dictionary_scraper_results.json"
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    log.info(f"\nSaved: {out_path}")

    # Summary
    log.info("\nSUMMARY:")
    for dict_key, dict_info in DICTIONARIES.items():
        dict_results = [r for r in all_results if r["dict_key"] == dict_key]
        ru_exists = sum(1 for r in dict_results if r["variant"] == "russian" and r["exists"])
        uk_exists = sum(1 for r in dict_results if r["variant"] == "ukrainian" and r["exists"])
        redirects = sum(1 for r in dict_results if r["redirected"])
        log.info(f"  {dict_info['name']}:")
        log.info(f"    Russian form: {ru_exists}/{len(DICT_TERMS)} entries found")
        log.info(f"    Ukrainian form: {uk_exists}/{len(DICT_TERMS)} entries found")
        log.info(f"    Redirects: {redirects}")

    # Save to site data
    site_data = {
        "dictionaries": {},
        "results": all_results,
    }
    for dict_key, dict_info in DICTIONARIES.items():
        dict_results = [r for r in all_results if r["dict_key"] == dict_key]
        site_data["dictionaries"][dict_key] = {
            "name": dict_info["name"],
            "russian_entries": sum(1 for r in dict_results if r["variant"] == "russian" and r["exists"]),
            "ukrainian_entries": sum(1 for r in dict_results if r["variant"] == "ukrainian" and r["exists"]),
            "redirects": [
                {"from": r["term"], "to": r["redirected_to"], "pair_id": r["pair_id"]}
                for r in dict_results if r["redirected"]
            ],
        }

    site_path = SITE_DATA / "dictionary_scraper.json"
    with open(site_path, "w") as f:
        json.dump(site_data, f, indent=2)
    log.info(f"Saved: {site_path}")


def main():
    asyncio.run(run_scraper())


if __name__ == "__main__":
    main()
