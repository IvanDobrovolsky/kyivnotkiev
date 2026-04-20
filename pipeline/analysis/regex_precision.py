"""Regex precision evaluation: sample GDELT matches and check for false positives.

Reads from HuggingFace parquet files — no BigQuery required.

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

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
DATASET_DIR = ROOT / "dataset"
SITE_DATA = ROOT / "site" / "src" / "data"

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
}

# Cross-contamination pairs
CROSS_CONTAMINATION = {
    7: {"shared_term": "Dnipro", "conflicts_with": 15, "note": "City pair shares 'Dnipro' with river pair"},
    15: {"shared_term": "Dnipro", "conflicts_with": 7, "note": "River pair shares 'Dnipro' with city pair"},
}

# Russian media domains
RU_DOMAINS = re.compile(r"\.(ru|su)$|tass\.|ria\.|sputnik|rt\.com|interfax\.ru|lenta\.ru|gazeta\.ru")


def _load_gdelt() -> pd.DataFrame:
    path = DATASET_DIR / "raw_gdelt.parquet"
    if not path.exists():
        log.error(f"GDELT parquet not found: {path}")
        return pd.DataFrame()
    import pyarrow.parquet as pq
    import pyarrow as pa
    table = pq.read_table(path)
    for i, field in enumerate(table.schema):
        if "date" in str(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
    table = table.replace_schema_metadata({})
    return table.to_pandas()


def sample_matches(gdelt: pd.DataFrame, pair_id: int, n: int = 50) -> pd.DataFrame:
    """Sample N random GDELT matches for a pair."""
    pair_data = gdelt[gdelt["pair_id"] == pair_id]
    if len(pair_data) == 0:
        return pd.DataFrame()
    return pair_data.sample(n=min(n, len(pair_data)), random_state=42)


def check_false_positives(pair_id: int, matches: pd.DataFrame) -> dict:
    """Check a sample for false positive patterns."""
    fp_count = 0
    ru_domain_count = 0
    fp_examples = []
    n = len(matches)

    patterns = FP_PATTERNS.get(pair_id, {}).get("patterns", [])

    for _, m in matches.iterrows():
        domain = str(m.get("source_domain", ""))
        matched_term = str(m.get("matched_term", ""))

        # Check matched_term against FP patterns (no URLs in parquet)
        for pat in patterns:
            if pat.search(matched_term) or pat.search(domain):
                fp_count += 1
                if len(fp_examples) < 5:
                    fp_examples.append({"domain": domain, "term": matched_term, "reason": "Pattern match"})
                break

        if RU_DOMAINS.search(domain):
            ru_domain_count += 1

    return {
        "total_sampled": n,
        "false_positives": fp_count,
        "precision": round((n - fp_count) / n * 100, 1) if n else 0,
        "ru_domain_pct": round(ru_domain_count / n * 100, 1) if n else 0,
        "fp_examples": fp_examples,
    }


def check_domain_distribution(gdelt: pd.DataFrame, pair_id: int) -> dict:
    """Check top domains for a pair."""
    pair_data = gdelt[gdelt["pair_id"] == pair_id]
    if len(pair_data) == 0:
        return {"top_domains": [], "ru_domain_mentions": 0, "ua_domain_mentions": 0}

    g = pair_data.groupby(["source_domain", "variant"])["count"].sum().reset_index()
    g = g.sort_values("count", ascending=False).head(20)

    ru_total = g[g["source_domain"].str.contains(r"\.(?:ru|su)$", regex=True)]["count"].sum()
    ua_total = g[g["source_domain"].str.endswith(".ua")]["count"].sum()

    return {
        "top_domains": [{"domain": r["source_domain"], "variant": r["variant"], "count": int(r["count"])} for _, r in g.head(10).iterrows()],
        "ru_domain_mentions": int(ru_total),
        "ua_domain_mentions": int(ua_total),
    }


def main():
    import yaml
    with open(ROOT / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)

    pairs_list = [p for p in cfg["pairs"]
                  if p.get("enabled", True) and not p.get("is_control", False)]

    log.info("=" * 70)
    log.info("REGEX PRECISION EVALUATION")
    log.info("=" * 70)

    log.info("Loading GDELT parquet...")
    gdelt = _load_gdelt()
    if len(gdelt) == 0:
        log.error("No GDELT data. Exiting.")
        return

    gdelt_pair_ids = set(gdelt["pair_id"].unique())

    results = []
    overall_fp = 0
    overall_total = 0

    target_pairs = [p for p in pairs_list if p["id"] in gdelt_pair_ids]
    target_pairs.sort(key=lambda p: p["id"])

    for pair in target_pairs:
        pid = pair["id"]
        log.info(f"\nPair {pid}: {pair['russian']} / {pair['ukrainian']}")

        matches = sample_matches(gdelt, pid, n=50)
        if len(matches) == 0:
            log.info("  No GDELT data")
            continue

        fp_result = check_false_positives(pid, matches)
        domain_info = check_domain_distribution(gdelt, pid)

        overall_fp += fp_result["false_positives"]
        overall_total += fp_result["total_sampled"]

        if fp_result["false_positives"] > 0 or fp_result["ru_domain_pct"] > 30:
            log.info(f"  Precision: {fp_result['precision']}% ({fp_result['false_positives']}/{fp_result['total_sampled']} FP)")
            log.info(f"  Russian domains: {fp_result['ru_domain_pct']}%")
        else:
            log.info(f"  Precision: {fp_result['precision']}% — clean")

        results.append({
            "pair_id": pid,
            "russian": pair["russian"],
            "ukrainian": pair["ukrainian"],
            **fp_result,
            "domain_distribution": domain_info,
        })

    overall_precision = round((overall_total - overall_fp) / overall_total * 100, 1) if overall_total else 0

    log.info("\n" + "=" * 70)
    log.info("SUMMARY")
    log.info("=" * 70)
    log.info(f"Pairs evaluated: {len(results)}")
    log.info(f"Total sampled: {overall_total}")
    log.info(f"False positives: {overall_fp}")
    log.info(f"Overall precision: {overall_precision}%")

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
