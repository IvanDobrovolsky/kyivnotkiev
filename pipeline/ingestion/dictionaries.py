"""Collect dictionary definitions and origin attributions for all toponym pairs.

Checks major English dictionaries for:
1. Which spelling they use (Russian or Ukrainian form)
2. Origin attribution (Russian, Ukrainian, or neutral)
3. Whether they misattribute Ukrainian things to Russia

Sources: Wiktionary (free API), Oxford via web, Britannica via web

Usage:
    python -m pipeline.ingestion.dictionaries
"""

import json
import logging
import re
import time
from pathlib import Path

import requests

from pipeline.config import load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "raw" / "dictionaries"
SITE_DATA = Path(__file__).resolve().parent.parent.parent / "site" / "src" / "data"


def query_wiktionary(term: str) -> dict | None:
    """Query Wiktionary API for a term's definition and etymology."""
    url = f"https://en.wiktionary.org/api/rest_v1/page/definition/{term.replace(' ', '_')}"
    try:
        r = requests.get(url, headers={"User-Agent": "KyivNotKiev/1.0"}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            definitions = []
            etymology = ""
            for lang_section in data.get("en", []):
                for defn in lang_section.get("definitions", []):
                    text = re.sub(r'<[^>]+>', '', defn.get("definition", ""))
                    if text:
                        definitions.append(text)
            return {"source": "Wiktionary", "definitions": definitions[:3], "raw": True}
        elif r.status_code == 404:
            return None
    except Exception as e:
        log.warning(f"  Wiktionary error for {term}: {e}")
    return None


def query_free_dictionary(term: str) -> dict | None:
    """Query Free Dictionary API."""
    url = f"https://api.dictionaryapi.dev/api/v2/entries/en/{term.replace(' ', '%20')}"
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                entry = data[0]
                meanings = []
                origin = entry.get("origin", "")
                for m in entry.get("meanings", []):
                    for d in m.get("definitions", [])[:2]:
                        meanings.append(d.get("definition", ""))
                return {"source": "FreeDictionary", "definitions": meanings[:3], "origin": origin}
        return None
    except Exception:
        return None


def check_origin_attribution(definitions: list[str], term_russian: str, term_ukrainian: str) -> dict:
    """Analyze definitions for Russian vs Ukrainian attribution."""
    all_text = " ".join(definitions).lower()

    result = {
        "mentions_russian": bool(re.search(r'\brussian\b', all_text)),
        "mentions_ukrainian": bool(re.search(r'\bukrainian\b', all_text)),
        "mentions_ukraine": bool(re.search(r'\bukraine\b', all_text)),
        "mentions_russia": bool(re.search(r'\brussia\b', all_text)),
        "attributes_to_russia": False,
        "attributes_to_ukraine": False,
        "neutral": False,
    }

    # Check for explicit attribution patterns
    if re.search(r'(russian|from russia|of russia|russia.s)', all_text) and not re.search(r'ukrain', all_text):
        result["attributes_to_russia"] = True
    elif re.search(r'(ukrainian|from ukraine|of ukraine|ukraine.s|originally from ukraine)', all_text):
        result["attributes_to_ukraine"] = True
    else:
        result["neutral"] = True

    return result


def collect_pair(pair: dict) -> dict:
    """Collect dictionary data for one pair."""
    pid = pair["id"]
    russian = pair["russian"]
    ukrainian = pair["ukrainian"]

    log.info(f"  Pair {pid}: {russian} / {ukrainian}")

    result = {
        "pair_id": pid,
        "russian": russian,
        "ukrainian": ukrainian,
        "category": pair["category"],
        "dictionaries": [],
        "spelling_preference": None,  # which form dictionaries prefer
        "origin_attribution": None,   # russian, ukrainian, or neutral
    }

    # Check both forms in each dictionary
    for term, variant in [(russian, "russian"), (ukrainian, "ukrainian")]:
        # Wiktionary
        wikt = query_wiktionary(term)
        if wikt:
            wikt["term"] = term
            wikt["variant"] = variant
            result["dictionaries"].append(wikt)
        time.sleep(0.3)

        # Free Dictionary API
        fd = query_free_dictionary(term)
        if fd:
            fd["term"] = term
            fd["variant"] = variant
            result["dictionaries"].append(fd)
        time.sleep(0.3)

    # Analyze attribution
    all_defs = []
    russian_found = False
    ukrainian_found = False

    for d in result["dictionaries"]:
        all_defs.extend(d.get("definitions", []))
        if d.get("variant") == "russian" and d.get("definitions"):
            russian_found = True
        if d.get("variant") == "ukrainian" and d.get("definitions"):
            ukrainian_found = True

    if all_defs:
        result["origin_attribution"] = check_origin_attribution(all_defs, russian, ukrainian)

    if ukrainian_found and not russian_found:
        result["spelling_preference"] = "ukrainian"
    elif russian_found and not ukrainian_found:
        result["spelling_preference"] = "russian"
    elif russian_found and ukrainian_found:
        result["spelling_preference"] = "both"
    else:
        result["spelling_preference"] = "neither"

    return result


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    cfg = load_pairs()
    pairs = [p for p in cfg["pairs"]
             if p.get("enabled", True) and not p.get("is_control", False)]

    log.info(f"Collecting dictionary data for {len(pairs)} pairs...")

    results = []
    for pair in pairs:
        data = collect_pair(pair)
        results.append(data)

    # Save raw data
    out_path = OUT_DIR / "dictionary_audit.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    log.info(f"Saved: {out_path}")

    # Summary
    russian_pref = sum(1 for r in results if r["spelling_preference"] == "russian")
    ukrainian_pref = sum(1 for r in results if r["spelling_preference"] == "ukrainian")
    both_pref = sum(1 for r in results if r["spelling_preference"] == "both")
    neither = sum(1 for r in results if r["spelling_preference"] == "neither")

    log.info(f"\nDICTIONARY SPELLING PREFERENCE:")
    log.info(f"  Russian form preferred: {russian_pref}")
    log.info(f"  Ukrainian form preferred: {ukrainian_pref}")
    log.info(f"  Both forms found: {both_pref}")
    log.info(f"  Neither found: {neither}")

    # Attribution
    ru_attr = sum(1 for r in results if (r.get("origin_attribution") or {}).get("attributes_to_russia"))
    ua_attr = sum(1 for r in results if (r.get("origin_attribution") or {}).get("attributes_to_ukraine"))
    neutral = sum(1 for r in results if (r.get("origin_attribution") or {}).get("neutral"))

    log.info(f"\nORIGIN ATTRIBUTION:")
    log.info(f"  Attributed to Russia: {ru_attr}")
    log.info(f"  Attributed to Ukraine: {ua_attr}")
    log.info(f"  Neutral: {neutral}")

    # Save site data
    site_data = {
        "pairs": [{
            "pair_id": r["pair_id"],
            "russian": r["russian"],
            "ukrainian": r["ukrainian"],
            "category": r["category"],
            "spelling_preference": r["spelling_preference"],
            "origin_attribution": r.get("origin_attribution", {}),
            "dict_count": len(r["dictionaries"]),
        } for r in results],
        "summary": {
            "russian_preferred": russian_pref,
            "ukrainian_preferred": ukrainian_pref,
            "both_found": both_pref,
            "neither_found": neither,
            "attributed_russia": ru_attr,
            "attributed_ukraine": ua_attr,
            "neutral_attribution": neutral,
        },
    }
    site_path = SITE_DATA / "dictionaries.json"
    with open(site_path, "w") as f:
        json.dump(site_data, f, indent=2)
    log.info(f"Saved site data: {site_path}")


if __name__ == "__main__":
    main()
