"""Dictionary scraper using Playwright (sync + stealth).

Visits Merriam-Webster, Britannica, Cambridge, and Oxford for each pair.
Stealth mode + captcha pause for Cambridge.

Usage:
    python -m pipeline.ingestion.playwright_audits.dictionary_scraper
"""

import json
import logging
import re
import time
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import Stealth

from pipeline.config import load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "screenshots"
OUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "dictionaries"
SITE_DATA = Path(__file__).resolve().parent.parent.parent.parent / "site" / "src" / "data"

DICTIONARIES = {
    "merriam_webster": {
        "name": "Merriam-Webster",
        "url_template": "https://www.merriam-webster.com/dictionary/{term}",
    },
    "britannica": {
        "name": "Britannica",
        "url_template": "https://www.britannica.com/topic/{term}",
    },
    "cambridge": {
        "name": "Cambridge Dictionary",
        "url_template": "https://dictionary.cambridge.org/dictionary/english/{term}",
    },
    "oxford": {
        "name": "Oxford Learner's Dictionary",
        "url_template": "https://www.oxfordlearnersdictionaries.com/definition/english/{term}",
    },
}

# Terms worth checking — single words or well-known compounds
DICT_TERMS = [
    (1, "Kiev", "Kyiv"),
    (2, "Kharkov", "Kharkiv"),
    (3, "Odessa", "Odesa"),
    (10, "Chernobyl", "Chornobyl"),
    (23, "borscht", "borshch"),
    (36, "Cossack", "Kozak"),
    (54, "Babi-Yar", "Babyn-Yar"),
    (72, "Bakhmut", "Artemovsk"),
    (61, "Zelensky", "Zelenskyy"),
]


def check_for_captcha(page) -> bool:
    """Check if page shows a captcha — strict detection to avoid false positives."""
    content = page.content().lower()
    # Only trigger on actual captcha elements, not mentions of "robot" in privacy policies
    return any(w in content for w in ["recaptcha", "unusual traffic", "verify you are human", "g-recaptcha", "captcha-container"])


def check_dictionary(page, dict_key: str, dict_info: dict, term: str,
                     pair_id: int, variant: str) -> dict:
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
        response = page.goto(url, wait_until="domcontentloaded", timeout=15000)
        page.wait_for_timeout(2000)

        # Captcha check — pause for manual solve
        if check_for_captcha(page):
            log.warning(f"      ⚠️  CAPTCHA on {dict_info['name']}! Solve manually...")
            # Wait for URL to change (user solves captcha)
            try:
                page.wait_for_timeout(10000)  # 60s to solve
            except:
                pass

        final_url = page.url
        result["final_url"] = final_url

        if url_term.replace("-", "") not in final_url.lower().replace("-", "").replace("_", "").replace("%20", ""):
            result["redirected"] = True
            result["redirected_to"] = final_url

        if response and response.status < 400:
            result["exists"] = True

            title_el = page.query_selector("h1, .headword, .hw, .entry-title")
            if title_el:
                result["page_title"] = title_el.inner_text().strip()[:100]

            content = page.content()
            def_match = re.search(r'class="[^"]*(?:def|sense|topic-paragraph)[^"]*"[^>]*>([^<]{10,})', content)
            if def_match:
                result["first_definition"] = re.sub(r'<[^>]+>', '', def_match.group(1)).strip()[:300]

            text = content.lower()
            result["mentions_russian"] = "russian" in text or "russia" in text
            result["mentions_ukrainian"] = "ukrainian" in text or "ukraine" in text

        # Screenshot
        ss_dir = BASE_DIR / dict_key
        ss_dir.mkdir(parents=True, exist_ok=True)
        safe_term = re.sub(r'[^\w]', '_', term)[:30]
        ss_path = ss_dir / f"pair{pair_id}_{variant}_{safe_term}.png"
        page.screenshot(path=str(ss_path), full_page=False)
        result["screenshot"] = f"{dict_key}/{ss_path.name}"

    except Exception as e:
        log.warning(f"      Error: {e}")

    status = "✓" if result["exists"] else "✗"
    redir = f" → {result['redirected_to'][-50:]}" if result["redirected"] else ""
    log.info(f"      {dict_info['name']}: {status} {term}{redir}")
    return result


def main():
    log.info("=" * 60)
    log.info("DICTIONARY SCRAPER (Playwright + Stealth)")
    log.info("=" * 60)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    all_results = []

    stealth = Stealth()

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, slow_mo=100,
            args=["--disable-blink-features=AutomationControlled"]
        )
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        page = context.new_page()
        stealth.apply_stealth_sync(page)

        for pair_id, russian, ukrainian in DICT_TERMS:
            log.info(f"\n  Pair {pair_id}: {russian} / {ukrainian}")

            for dict_key, dict_info in DICTIONARIES.items():
                ru_result = check_dictionary(page, dict_key, dict_info, russian, pair_id, "russian")
                all_results.append(ru_result)
                page.wait_for_timeout(1500)

                uk_result = check_dictionary(page, dict_key, dict_info, ukrainian, pair_id, "ukrainian")
                all_results.append(uk_result)
                page.wait_for_timeout(1500)

        browser.close()

    # Save
    out_path = OUT_DIR / "dictionary_scraper_results.json"
    with open(out_path, "w") as f:
        json.dump(all_results, f, indent=2)
    log.info(f"\nSaved: {out_path}")

    # Summary per dictionary
    log.info("\nSUMMARY:")
    for dict_key, dict_info in DICTIONARIES.items():
        dr = [r for r in all_results if r["dict_key"] == dict_key]
        ru_exists = sum(1 for r in dr if r["variant"] == "russian" and r["exists"])
        uk_exists = sum(1 for r in dr if r["variant"] == "ukrainian" and r["exists"])
        redirects = sum(1 for r in dr if r["redirected"])
        ru_mention = sum(1 for r in dr if r["mentions_russian"])
        uk_mention = sum(1 for r in dr if r["mentions_ukrainian"])
        log.info(f"  {dict_info['name']}:")
        log.info(f"    Russian form entries: {ru_exists}/{len(DICT_TERMS)}")
        log.info(f"    Ukrainian form entries: {uk_exists}/{len(DICT_TERMS)}")
        log.info(f"    Redirects: {redirects}")
        log.info(f"    Mentions 'Russian': {ru_mention}, 'Ukrainian': {uk_mention}")

    # Save site data
    site_data = {"dictionaries": {}, "results": all_results}
    for dict_key, dict_info in DICTIONARIES.items():
        dr = [r for r in all_results if r["dict_key"] == dict_key]
        site_data["dictionaries"][dict_key] = {
            "name": dict_info["name"],
            "russian_entries": sum(1 for r in dr if r["variant"] == "russian" and r["exists"]),
            "ukrainian_entries": sum(1 for r in dr if r["variant"] == "ukrainian" and r["exists"]),
            "redirects": [
                {"from": r["term"], "to": r.get("page_title", r.get("redirected_to", "")), "pair_id": r["pair_id"]}
                for r in dr if r["redirected"]
            ],
            "mentions_russian": sum(1 for r in dr if r["mentions_russian"]),
            "mentions_ukrainian": sum(1 for r in dr if r["mentions_ukrainian"]),
        }

    site_path = SITE_DATA / "dictionary_scraper.json"
    with open(site_path, "w") as f:
        json.dump(site_data, f, indent=2)
    log.info(f"Saved: {site_path}")


if __name__ == "__main__":
    main()
