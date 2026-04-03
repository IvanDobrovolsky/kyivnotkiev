"""Regex precision evaluation: sample GDELT matches and check for false positives.

Samples N matches per pair from GDELT, checks context for common false positive patterns:
- "Odessa, Texas" / "Odessa, FL" (US city, not Ukraine)
- "Kiev, Idaho" (US town)
- Recipe contexts for city-pair matches ("chicken kiev recipe" in city data)
- Russian media .ru domains inflating Russian variant counts

Usage:
    python -m pipeline.analysis.regex_precision [--sample-size 50]
"""

import json
import logging
import re
from collections import Counter
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT = "kyivnotkiev-research"
DATASET = "kyivnotkiev"
client = bigquery.Client(project=PROJECT)
SITE_DATA = Path(__file__).resolve().parent.parent.parent / "site" / "src" / "data"

# Known false positive patterns
FP_PATTERNS = {
    3: {  # Odessa/Odesa
        "patterns": [
            re.compile(r"odessa.*?(texas|tx|florida|fl|missouri|mo|new york|ny|delaware|de|minnesota|mn|washington|wa)", re.IGNORECASE),
            re.compile(r"(texas|tx|florida|fl|missouri|mo).*?odessa", re.IGNORECASE),
        ],
        "description": "US cities named Odessa (TX, FL, MO, etc.)",
    },
    1: {  # Kiev/Kyiv
        "patterns": [
            re.compile(r"chicken\s+kiev|kiev\s+cake|kiev\s+recipe|recipe.*kiev", re.IGNORECASE),
        ],
        "description": "Food terms leaking into city pair (Chicken Kiev, Kiev cake)",
    },
    7: {  # Dnepropetrovsk/Dnipro (city)
        "patterns": [
            re.compile(r"dnipro\s*(river|reservoir|dam|hydroelectric|rapids|water|basin|tributary|banks)", re.IGNORECASE),
            re.compile(r"(river|reservoir|dam|hydroelectric|rapids|across|along|near)\s*dnipro", re.IGNORECASE),
        ],
        "description": "River 'Dnipro' leaking into city pair — shared Ukrainian term",
    },
    15: {  # Dnieper/Dnipro (river)
        "patterns": [
            re.compile(r"dnipro\s*(city|fc|football|club|airport|metro|station|university|oblast)", re.IGNORECASE),
            re.compile(r"(city|fc|football|club|airport|metro|station|university)\s*(of\s*)?dnipro", re.IGNORECASE),
        ],
        "description": "City 'Dnipro' leaking into river pair — shared Ukrainian term",
    },
    27: {  # the Ukraine / Ukraine
        "patterns": [
            re.compile(r"university\s+of\s+ukraine|ukraine\s+university|national\s+university", re.IGNORECASE),
        ],
        "description": "Institutional names containing 'Ukraine'",
    },
}

# Cross-contamination pairs: same Ukrainian term used in multiple pairs
CROSS_CONTAMINATION = {
    7: {"shared_term": "Dnipro", "conflicts_with": 15, "note": "City pair shares 'Dnipro' with river pair"},
    15: {"shared_term": "Dnipro", "conflicts_with": 7, "note": "River pair shares 'Dnipro' with city pair"},
}

# Russian media domains (may inflate Russian variant counts)
RU_DOMAINS = re.compile(r"\.(ru|su)$|tass\.|ria\.|sputnik|rt\.com|interfax\.ru|lenta\.ru|gazeta\.ru")


def sample_matches(pair_id: int, n: int = 50) -> list[dict]:
    """Sample N random GDELT matches for a pair."""
    sql = f"""
        SELECT pair_id, source_domain, source_url, matched_term, variant, date
        FROM `{DATASET}.raw_gdelt`
        WHERE pair_id = {pair_id}
        ORDER BY RAND()
        LIMIT {n}
    """
    return [dict(r) for r in client.query(sql).result()]


def check_false_positives(pair_id: int, matches: list[dict]) -> dict:
    """Check a sample for false positive patterns."""
    fp_count = 0
    ru_domain_count = 0
    fp_examples = []

    patterns = FP_PATTERNS.get(pair_id, {}).get("patterns", [])

    for m in matches:
        url = m.get("source_url", "")
        domain = m.get("source_domain", "")

        # Check URL against FP patterns
        is_fp = False
        for pat in patterns:
            if pat.search(url):
                is_fp = True
                fp_count += 1
                if len(fp_examples) < 5:
                    fp_examples.append({"url": url[:100], "reason": "URL pattern match"})
                break

        # Check for Russian media domain
        if RU_DOMAINS.search(domain):
            ru_domain_count += 1

    return {
        "total_sampled": len(matches),
        "false_positives": fp_count,
        "precision": round((len(matches) - fp_count) / len(matches) * 100, 1) if matches else 0,
        "ru_domain_pct": round(ru_domain_count / len(matches) * 100, 1) if matches else 0,
        "fp_examples": fp_examples,
    }


def check_domain_distribution(pair_id: int) -> dict:
    """Check top domains for a pair — identifies systematic biases."""
    sql = f"""
        SELECT source_domain, variant, COUNT(*) as cnt
        FROM `{DATASET}.raw_gdelt`
        WHERE pair_id = {pair_id}
        GROUP BY source_domain, variant
        ORDER BY cnt DESC
        LIMIT 20
    """
    rows = [dict(r) for r in client.query(sql).result()]

    ru_domains = [r for r in rows if RU_DOMAINS.search(r["source_domain"])]
    ua_domains = [r for r in rows if r["source_domain"].endswith(".ua")]

    return {
        "top_domains": [{"domain": r["source_domain"], "variant": r["variant"], "count": r["cnt"]} for r in rows[:10]],
        "ru_domain_mentions": sum(r["cnt"] for r in ru_domains),
        "ua_domain_mentions": sum(r["cnt"] for r in ua_domains),
    }


def main():
    import yaml
    with open(Path(__file__).resolve().parent.parent.parent / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)

    pairs_with_gdelt = [p for p in cfg["pairs"]
                        if p.get("enabled", True) and not p.get("is_control", False)]

    # Check which pairs have GDELT data
    sql = f"SELECT DISTINCT pair_id FROM `{DATASET}.raw_gdelt`"
    gdelt_pairs = {r["pair_id"] for r in client.query(sql).result()}

    log.info("=" * 70)
    log.info("REGEX PRECISION EVALUATION")
    log.info("=" * 70)

    results = []
    overall_fp = 0
    overall_total = 0

    # Focus on pairs with most data (most impactful if FP rate is high)
    target_pairs = [p for p in pairs_with_gdelt if p["id"] in gdelt_pairs]
    target_pairs.sort(key=lambda p: p["id"])

    for pair in target_pairs:
        pid = pair["id"]
        log.info(f"\nPair {pid}: {pair['russian']} / {pair['ukrainian']}")

        matches = sample_matches(pid, n=50)
        if not matches:
            log.info("  No GDELT data")
            continue

        fp_result = check_false_positives(pid, matches)
        domain_info = check_domain_distribution(pid)

        overall_fp += fp_result["false_positives"]
        overall_total += fp_result["total_sampled"]

        if fp_result["false_positives"] > 0 or fp_result["ru_domain_pct"] > 30:
            log.info(f"  Precision: {fp_result['precision']}% ({fp_result['false_positives']}/{fp_result['total_sampled']} FP)")
            log.info(f"  Russian domains: {fp_result['ru_domain_pct']}%")
            if fp_result["fp_examples"]:
                for ex in fp_result["fp_examples"][:2]:
                    log.info(f"    FP: {ex['url']}")
        else:
            log.info(f"  Precision: {fp_result['precision']}% — clean")

        results.append({
            "pair_id": pid,
            "russian": pair["russian"],
            "ukrainian": pair["ukrainian"],
            **fp_result,
            "domain_distribution": domain_info,
        })

    # Summary
    overall_precision = round((overall_total - overall_fp) / overall_total * 100, 1) if overall_total else 0

    log.info("\n" + "=" * 70)
    log.info("SUMMARY")
    log.info("=" * 70)
    log.info(f"Pairs evaluated: {len(results)}")
    log.info(f"Total sampled: {overall_total}")
    log.info(f"False positives: {overall_fp}")
    log.info(f"Overall precision: {overall_precision}%")

    # Pairs with lowest precision
    low_prec = [r for r in results if r["precision"] < 95]
    if low_prec:
        log.info(f"\nPairs with <95% precision:")
        for r in sorted(low_prec, key=lambda x: x["precision"]):
            log.info(f"  {r['pair_id']} {r['russian']}/{r['ukrainian']}: {r['precision']}%")

    # Save
    output = {
        "overall_precision": overall_precision,
        "total_sampled": overall_total,
        "total_false_positives": overall_fp,
        "pairs_evaluated": len(results),
        "pairs": results,
    }

    out_path = SITE_DATA / "regex_precision.json"
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    log.info(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
