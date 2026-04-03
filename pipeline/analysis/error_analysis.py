"""Cross-source error analysis: where do sources disagree and why?

Identifies pairs where different sources give contradictory adoption signals.
Outputs disagreements for the paper and site.

Usage:
    python -m pipeline.analysis.error_analysis
"""

import json
import logging
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT = "kyivnotkiev-research"
DATASET = "kyivnotkiev"
client = bigquery.Client(project=PROJECT)
SITE_DATA = Path(__file__).resolve().parent.parent.parent / "site" / "src" / "data"


def query(sql: str) -> list[dict]:
    return [dict(r) for r in client.query(sql).result()]


def main():
    log.info("=" * 60)
    log.info("Cross-source error analysis")
    log.info("=" * 60)

    # Get recent adoption ratio per pair per source
    rows = query(f"""
        WITH src AS (
            SELECT pair_id, 'trends' as source,
                SAFE_DIVIDE(SUM(IF(variant='ukrainian', interest, 0)),
                            SUM(interest)) as adoption
            FROM `{DATASET}.raw_trends`
            WHERE (geo='' OR geo IS NULL) AND date >= '2024-01-01'
            GROUP BY pair_id
            UNION ALL
            SELECT pair_id, 'gdelt',
                SAFE_DIVIDE(COUNTIF(variant='ukrainian'), COUNT(*))
            FROM `{DATASET}.raw_gdelt` WHERE date >= '2024-01-01'
            GROUP BY pair_id
            UNION ALL
            SELECT pair_id, 'wikipedia',
                SAFE_DIVIDE(SUM(IF(variant='ukrainian', pageviews, 0)), SUM(pageviews))
            FROM `{DATASET}.raw_wikipedia` WHERE date >= '2024-01-01'
            GROUP BY pair_id
            UNION ALL
            SELECT pair_id, 'reddit',
                SAFE_DIVIDE(COUNTIF(variant='ukrainian'), COUNT(*))
            FROM `{DATASET}.raw_reddit` WHERE DATE(created_utc) >= '2024-01-01'
            GROUP BY pair_id
            UNION ALL
            SELECT pair_id, 'youtube',
                SAFE_DIVIDE(COUNTIF(variant='ukrainian'), COUNT(*))
            FROM `{DATASET}.raw_youtube` WHERE DATE(published_at) >= '2024-01-01'
            GROUP BY pair_id
        )
        SELECT pair_id, source, ROUND(adoption * 100, 1) as adoption_pct
        FROM src
        WHERE adoption IS NOT NULL
        ORDER BY pair_id, source
    """)

    # Build pair → {source: adoption} map
    from collections import defaultdict
    pair_sources = defaultdict(dict)
    for r in rows:
        pair_sources[r["pair_id"]][r["source"]] = r["adoption_pct"]

    # Load pair names
    import yaml
    with open(Path(__file__).resolve().parent.parent.parent / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)
    names = {p["id"]: (p["russian"], p["ukrainian"], p["category"])
             for p in cfg["pairs"] if p.get("enabled", True)}

    # Also add OpenAlex
    oa_path = Path(__file__).resolve().parent.parent.parent / "data" / "raw" / "openalex" / "openalex_all_pairs.json"
    if oa_path.exists():
        with open(oa_path) as f:
            oa_data = json.load(f)
        for p in oa_data:
            pid = p["pair_id"]
            recent = [yr for yr in p["yearly"] if yr["year"] >= 2024]
            if recent:
                total = sum(yr["total"] for yr in recent)
                ukr = sum(yr["ukrainian_count"] for yr in recent)
                if total > 0:
                    pair_sources[pid]["openalex"] = round(ukr / total * 100, 1)

    # Find disagreements: pairs where sources differ by >30pp
    log.info("\nDISAGREEMENTS (>30pp spread between sources):")
    log.info(f"{'ID':<4} {'Pair':<35} {'Sources':>50} {'Spread':>8}")
    log.info("-" * 100)

    disagreements = []
    for pid in sorted(pair_sources.keys()):
        if pid not in names:
            continue
        sources = pair_sources[pid]
        if len(sources) < 2:
            continue

        vals = list(sources.values())
        spread = max(vals) - min(vals)

        if spread > 30:
            ru, uk, cat = names[pid]
            src_str = " | ".join(f"{s}:{v:.0f}%" for s, v in sorted(sources.items()))
            log.info(f"{pid:<4} {ru}/{uk:<33} {src_str:>50} {spread:>7.1f}pp")

            # Find which source is the outlier
            avg = sum(vals) / len(vals)
            outliers = {s: v for s, v in sources.items() if abs(v - avg) > 20}

            disagreements.append({
                "pair_id": pid,
                "russian": ru,
                "ukrainian": uk,
                "category": cat,
                "sources": sources,
                "spread_pp": round(spread, 1),
                "outliers": outliers,
                "explanation": classify_disagreement(pid, sources, cat),
            })

    # Pairs where ALL sources agree (spread < 10pp)
    agreements = []
    for pid in sorted(pair_sources.keys()):
        if pid not in names:
            continue
        sources = pair_sources[pid]
        if len(sources) < 3:
            continue
        vals = list(sources.values())
        spread = max(vals) - min(vals)
        if spread < 10:
            ru, uk, cat = names[pid]
            avg = sum(vals) / len(vals)
            agreements.append({
                "pair_id": pid,
                "russian": ru,
                "ukrainian": uk,
                "category": cat,
                "avg_adoption": round(avg, 1),
                "spread_pp": round(spread, 1),
                "n_sources": len(sources),
            })

    log.info(f"\nAGREEMENTS (<10pp spread, 3+ sources): {len(agreements)} pairs")
    for a in sorted(agreements, key=lambda x: -x["avg_adoption"])[:10]:
        log.info(f"  {a['russian']}/{a['ukrainian']}: {a['avg_adoption']}% ±{a['spread_pp']}pp ({a['n_sources']} sources)")

    # Summary statistics
    all_spreads = []
    for pid in pair_sources:
        if pid in names and len(pair_sources[pid]) >= 2:
            vals = list(pair_sources[pid].values())
            all_spreads.append(max(vals) - min(vals))

    import statistics
    log.info(f"\nSUMMARY:")
    log.info(f"  Pairs with 2+ sources: {len(all_spreads)}")
    log.info(f"  Median spread: {statistics.median(all_spreads):.1f}pp")
    log.info(f"  Mean spread: {statistics.mean(all_spreads):.1f}pp")
    log.info(f"  Max spread: {max(all_spreads):.1f}pp")
    log.info(f"  Disagreements (>30pp): {len(disagreements)}")
    log.info(f"  Agreements (<10pp): {len(agreements)}")

    # Save for site
    result = {
        "disagreements": disagreements,
        "agreements": agreements,
        "summary": {
            "pairs_analyzed": len(all_spreads),
            "median_spread_pp": round(statistics.median(all_spreads), 1),
            "mean_spread_pp": round(statistics.mean(all_spreads), 1),
            "max_spread_pp": round(max(all_spreads), 1),
            "n_disagreements": len(disagreements),
            "n_agreements": len(agreements),
        },
    }

    out_path = SITE_DATA / "error_analysis.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, default=str)
    log.info(f"\nSaved: {out_path}")


def classify_disagreement(pid: int, sources: dict, category: str) -> str:
    """Generate explanation for why sources disagree."""
    explanations = []

    # Wikipedia vs others: Wikipedia measures page views (consumption), others measure production
    if "wikipedia" in sources:
        wiki = sources["wikipedia"]
        others = [v for s, v in sources.items() if s != "wikipedia"]
        if others and abs(wiki - sum(others) / len(others)) > 20:
            if wiki > sum(others) / len(others):
                explanations.append("Wikipedia article under Ukrainian name gets more views (consumption bias)")
            else:
                explanations.append("Wikipedia redirect from old name inflates Russian variant views")

    # Trends vs GDELT: public search vs news media
    if "trends" in sources and "gdelt" in sources:
        if abs(sources["trends"] - sources["gdelt"]) > 20:
            if sources["trends"] < sources["gdelt"]:
                explanations.append("Public search lags behind news media adoption (media-public gap)")
            else:
                explanations.append("Public search adopts faster than news URLs (URL stickiness)")

    # Food/cultural terms: search behavior differs from editorial style
    if category == "food":
        explanations.append("Food terms: recipe searches use familiar spelling, media uses updated form")

    # Historical terms: academic vs public framing
    if category == "historical":
        explanations.append("Historical terms: scholarship preserves traditional naming, public follows media")

    if not explanations:
        explanations.append("Multi-domain adoption varies by audience and editorial control")

    return "; ".join(explanations)


if __name__ == "__main__":
    main()
