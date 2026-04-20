"""Cross-source error analysis: where do sources disagree and why?

Identifies pairs where different sources give contradictory adoption signals.
Reads from HuggingFace parquet files — no BigQuery required.

Usage:
    python -m pipeline.analysis.error_analysis
"""

import json
import logging
import statistics
from collections import defaultdict
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
DATASET_DIR = ROOT / "dataset"
SITE_DATA = ROOT / "site" / "src" / "data"


def _load(name: str) -> pd.DataFrame:
    path = DATASET_DIR / f"raw_{name}.parquet"
    if not path.exists():
        path = DATASET_DIR / f"{name}.parquet"
    if not path.exists():
        return pd.DataFrame()
    import pyarrow.parquet as pq
    import pyarrow as pa
    table = pq.read_table(path)
    for i, field in enumerate(table.schema):
        if "date" in str(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
    table = table.replace_schema_metadata({})
    return table.to_pandas()


def _safe_div(a, b):
    return a / b if b > 0 else None


def compute_source_adoption(cutoff="2024-01-01"):
    """Compute adoption ratio per pair per source from parquets."""
    pair_sources = defaultdict(dict)

    # Trends
    df = _load("trends")
    if len(df):
        t = df[(df["geo"] == "") | (df["geo"].isna())].copy()
        t = t[pd.to_datetime(t["date"]) >= cutoff]
        g = t.groupby(["pair_id", "variant"])["interest"].sum().reset_index()
        p = g.pivot_table(index="pair_id", columns="variant", values="interest", fill_value=0).reset_index()
        for _, r in p.iterrows():
            ukr = float(r.get("ukrainian", 0))
            rus = float(r.get("russian", 0))
            total = ukr + rus
            if total > 0:
                pair_sources[int(r["pair_id"])]["trends"] = round(ukr / total * 100, 1)

    # GDELT
    df = _load("gdelt")
    if len(df):
        d = df[pd.to_datetime(df["date"]) >= cutoff]
        g = d.groupby(["pair_id", "variant"])["count"].sum().reset_index()
        p = g.pivot_table(index="pair_id", columns="variant", values="count", fill_value=0).reset_index()
        for _, r in p.iterrows():
            ukr = float(r.get("ukrainian", 0))
            rus = float(r.get("russian", 0))
            total = ukr + rus
            if total > 0:
                pair_sources[int(r["pair_id"])]["gdelt"] = round(ukr / total * 100, 1)

    # Wikipedia
    df = _load("wikipedia")
    if len(df):
        d = df[pd.to_datetime(df["date"]) >= cutoff]
        g = d.groupby(["pair_id", "variant"])["pageviews"].sum().reset_index()
        p = g.pivot_table(index="pair_id", columns="variant", values="pageviews", fill_value=0).reset_index()
        for _, r in p.iterrows():
            ukr = float(r.get("ukrainian", 0))
            rus = float(r.get("russian", 0))
            total = ukr + rus
            if total > 0:
                pair_sources[int(r["pair_id"])]["wikipedia"] = round(ukr / total * 100, 1)

    # Reddit
    df = _load("reddit")
    if len(df):
        d = df[pd.to_datetime(df["date"]) >= cutoff]
        g = d.groupby(["pair_id", "variant"]).size().reset_index(name="cnt")
        p = g.pivot_table(index="pair_id", columns="variant", values="cnt", fill_value=0).reset_index()
        for _, r in p.iterrows():
            ukr = float(r.get("ukrainian", 0))
            rus = float(r.get("russian", 0))
            total = ukr + rus
            if total > 0:
                pair_sources[int(r["pair_id"])]["reddit"] = round(ukr / total * 100, 1)

    # YouTube
    df = _load("youtube")
    if len(df):
        d = df[pd.to_datetime(df["date"]) >= cutoff]
        g = d.groupby(["pair_id", "variant"]).size().reset_index(name="cnt")
        p = g.pivot_table(index="pair_id", columns="variant", values="cnt", fill_value=0).reset_index()
        for _, r in p.iterrows():
            ukr = float(r.get("ukrainian", 0))
            rus = float(r.get("russian", 0))
            total = ukr + rus
            if total > 0:
                pair_sources[int(r["pair_id"])]["youtube"] = round(ukr / total * 100, 1)

    # OpenAlex (from local JSON)
    oa_path = ROOT / "data" / "raw" / "openalex" / "openalex_all_pairs.json"
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

    return pair_sources


def classify_disagreement(pid: int, sources: dict, category: str) -> str:
    explanations = []

    if "wikipedia" in sources:
        wiki = sources["wikipedia"]
        others = [v for s, v in sources.items() if s != "wikipedia"]
        if others and abs(wiki - sum(others) / len(others)) > 20:
            if wiki > sum(others) / len(others):
                explanations.append("Wikipedia article under Ukrainian name gets more views (consumption bias)")
            else:
                explanations.append("Wikipedia redirect from old name inflates Russian variant views")

    if "trends" in sources and "gdelt" in sources:
        if abs(sources["trends"] - sources["gdelt"]) > 20:
            if sources["trends"] < sources["gdelt"]:
                explanations.append("Public search lags behind news media adoption (media-public gap)")
            else:
                explanations.append("Public search adopts faster than news URLs (URL stickiness)")

    if category == "food":
        explanations.append("Food terms: recipe searches use familiar spelling, media uses updated form")
    if category == "historical":
        explanations.append("Historical terms: scholarship preserves traditional naming, public follows media")

    if not explanations:
        explanations.append("Multi-domain adoption varies by audience and editorial control")

    return "; ".join(explanations)


def main():
    import yaml
    with open(ROOT / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)
    names = {p["id"]: (p["russian"], p["ukrainian"], p["category"])
             for p in cfg["pairs"] if p.get("enabled", True) and not p.get("is_control", False)}

    log.info("=" * 60)
    log.info("Cross-source error analysis")
    log.info("=" * 60)

    pair_sources = compute_source_adoption()

    # Disagreements (>30pp spread)
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
            avg = sum(vals) / len(vals)
            outliers = {s: v for s, v in sources.items() if abs(v - avg) > 20}
            disagreements.append({
                "pair_id": pid, "russian": ru, "ukrainian": uk, "category": cat,
                "sources": sources, "spread_pp": round(spread, 1), "outliers": outliers,
                "explanation": classify_disagreement(pid, sources, cat),
            })

    # Agreements (<10pp spread, 3+ sources)
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
                "pair_id": pid, "russian": ru, "ukrainian": uk, "category": cat,
                "avg_adoption": round(avg, 1), "spread_pp": round(spread, 1),
                "n_sources": len(sources),
            })

    # Summary
    all_spreads = []
    for pid in pair_sources:
        if pid in names and len(pair_sources[pid]) >= 2:
            vals = list(pair_sources[pid].values())
            all_spreads.append(max(vals) - min(vals))

    log.info(f"Pairs with 2+ sources: {len(all_spreads)}")
    log.info(f"Median spread: {statistics.median(all_spreads):.1f}pp")
    log.info(f"Disagreements (>30pp): {len(disagreements)}")
    log.info(f"Agreements (<10pp): {len(agreements)}")

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
    log.info(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
