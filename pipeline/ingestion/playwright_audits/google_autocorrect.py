"""Google Search autocorrect audit using Playwright.

For each toponym pair, searches Google for both Russian and Ukrainian forms.
Captures:
1. Does Google autocorrect (e.g., "Volodymyr the Great" → "Vladimir the Great")?
2. What does the knowledge panel show?
3. What Wikipedia article does the snippet reference?

Saves screenshots + structured data.

Usage:
    python -m pipeline.ingestion.playwright_audits.google_autocorrect
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

SCREENSHOT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "screenshots" / "google"
OUT_DIR = Path(__file__).resolve().parent.parent.parent.parent / "data" / "raw" / "google_audit"
SITE_DATA = Path(__file__).resolve().parent.parent.parent.parent / "site" / "src" / "data"


async def search_google(page, query: str, pair_id: int, variant: str) -> dict:
    """Search Google and capture autocorrect behavior."""
    result = {
        "query": query,
        "variant": variant,
        "pair_id": pair_id,
        "autocorrected": False,
        "autocorrect_to": None,
        "showing_results_for": None,
        "knowledge_panel": None,
        "wikipedia_ref": None,
        "screenshot": None,
    }

    try:
        url = f"https://www.google.com/search?q={query.replace(' ', '+')}&hl=en"
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(2000)

        # Check for "Showing results for" or "Did you mean"
        content = await page.content()

        # Autocorrect detection
        showing_for = await page.query_selector('a.gL9Hy')  # "Showing results for" link
        if showing_for:
            text = await showing_for.inner_text()
            result["autocorrected"] = True
            result["autocorrect_to"] = text
            result["showing_results_for"] = text

        # Also check for "Did you mean"
        did_you_mean = await page.query_selector('a.gL9Hy, p.card-section span')
        if did_you_mean:
            text = await did_you_mean.inner_text()
            if text and text != query:
                result["autocorrected"] = True
                result["autocorrect_to"] = text

        # Check for spelling suggestion
        spell_el = await page.query_selector('#fprs a, .spell_orig a')
        if spell_el:
            spell_text = await spell_el.inner_text()
            if spell_text:
                result["autocorrected"] = True
                result["autocorrect_to"] = spell_text

        # Knowledge panel
        kp = await page.query_selector('[data-attrid="title"], .kno-ecr-pt')
        if kp:
            result["knowledge_panel"] = await kp.inner_text()

        # Wikipedia reference in results
        if "wikipedia" in content.lower():
            wiki_match = re.search(r'en\.wikipedia\.org/wiki/([^"&]+)', content)
            if wiki_match:
                result["wikipedia_ref"] = wiki_match.group(1).replace('_', ' ')

        # Screenshot
        safe_name = re.sub(r'[^\w]', '_', query)[:50]
        screenshot_path = SCREENSHOT_DIR / f"pair{pair_id}_{variant}_{safe_name}.png"
        await page.screenshot(path=str(screenshot_path), full_page=False)
        result["screenshot"] = str(screenshot_path.name)
        log.info(f"    {variant}: autocorrect={result['autocorrected']}, wiki={result['wikipedia_ref']}")

    except Exception as e:
        log.warning(f"    Error searching '{query}': {e}")

    return result


async def run_audit():
    cfg = load_pairs()
    # Key pairs to test — focus on ones where autocorrect matters
    test_pairs = [p for p in cfg["pairs"]
                  if p.get("enabled", True) and not p.get("is_control", False)
                  and p["id"] in [1, 2, 3, 10, 21, 23, 35, 36, 54, 60, 61, 70, 71, 72]]

    if not test_pairs:
        test_pairs = [p for p in cfg["pairs"]
                      if p.get("enabled", True) and not p.get("is_control", False)][:15]

    log.info(f"Google autocorrect audit: {len(test_pairs)} pairs")

    SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    results = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            locale="en-US",
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        )
        page = await context.new_page()

        for pair in test_pairs:
            log.info(f"  Pair {pair['id']}: {pair['russian']} / {pair['ukrainian']}")

            # Search Russian form
            ru_result = await search_google(page, pair["russian"], pair["id"], "russian")
            results.append(ru_result)
            await page.wait_for_timeout(3000)

            # Search Ukrainian form
            uk_result = await search_google(page, pair["ukrainian"], pair["id"], "ukrainian")
            results.append(uk_result)
            await page.wait_for_timeout(3000)

        await browser.close()

    # Save results
    out_path = OUT_DIR / "google_autocorrect_audit.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    log.info(f"\nSaved: {out_path}")

    # Summary
    autocorrected = [r for r in results if r["autocorrected"]]
    log.info(f"\nSUMMARY:")
    log.info(f"  Total searches: {len(results)}")
    log.info(f"  Autocorrected: {len(autocorrected)}")
    for r in autocorrected:
        log.info(f"    '{r['query']}' → '{r['autocorrect_to']}'")

    # Save site data
    site_path = SITE_DATA / "google_audit.json"
    with open(site_path, "w") as f:
        json.dump({
            "total_searches": len(results),
            "autocorrected_count": len(autocorrected),
            "autocorrections": [
                {"query": r["query"], "corrected_to": r["autocorrect_to"],
                 "pair_id": r["pair_id"], "variant": r["variant"]}
                for r in autocorrected
            ],
            "results": results,
        }, f, indent=2)
    log.info(f"Saved: {site_path}")


def main():
    asyncio.run(run_audit())


if __name__ == "__main__":
    main()
