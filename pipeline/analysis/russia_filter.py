"""Russia filter: classify GDELT domains by origin.

Uses the EU sanctions list (Council Regulation 833/2014, Article 2f) to
identify Russian state media. This is a legally verifiable classification,
not a subjective editorial judgment.

Three tiers:
  1. EU-sanctioned state media (RT, Sputnik, RIA, Izvestia, etc.)
  2. Russian-domain (.ru/.su) — not sanctioned but Russian-origin
  3. All other domains (global Latin-script media)

Usage:
    python -m pipeline.analysis.russia_filter
"""

import json
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
DATASET_DIR = ROOT / "dataset"
SITE_DATA = ROOT / "site" / "src" / "data"

# ── EU-Sanctioned Russian State Media ────────────────────────────────────────
# Each wave cites the specific Council Decision / Regulation.

EU_SANCTIONED_DOMAINS = {
    # Wave 1: Decision 2022/351 (1 March 2022) — RT + Sputnik
    "rt.com", "russian.rt.com", "arabic.rt.com", "actualidad.rt.com",
    "de.rt.com", "francais.rt.com", "rt.rs",
    "sputniknews.com", "sputnikglobe.com", "sputnikportal.rs",
    # Wave 3: Decision 2022/2478 (16 Dec 2022) — NTV, REN TV, Channel One
    "ntv.ru", "ren.tv", "1tv.ru",
    # Wave 4: Decision 2023/1217 (23 June 2023)
    "tsargrad.tv", "orientalreview.org", "journal-neo.su", "katehon.com",
    # Wave 5: Decision 2023/2874 (18 Dec 2023) — Spas TV (ROC channel)
    "spastv.ru",
    # Wave 6: Regulation 2024/1428 (17 May 2024) — RIA, Izvestia, Rossiyskaya Gazeta
    "ria.ru", "iz.ru", "rg.ru", "voiceofeurope.com",
    # Wave 7: Decision 2025/394 (24 Feb 2025)
    "lenta.ru", "news-front.su", "rubaltic.ru", "southfront.press",
    "strategic-culture.su", "tvzvezda.ru", "redstar.ru", "eadaily.com", "fondsk.ru",
}

# RT mirror domains documented by ISD (March 2025 audit)
RT_MIRROR_DOMAINS = {
    "freedert.online", "dert.online", "rtde.live", "rtde.world",
    "rtde.me", "rtde.xyz", "rtde.tech", "rtde.team", "rtde.site",
    "rurtnews.com", "swentr.site", "actualidad-rt.com",
    "esrt.site", "esrt.press", "rtenfrance.tv",
}

ALL_STATE_MEDIA = EU_SANCTIONED_DOMAINS | RT_MIRROR_DOMAINS


def classify_domain(domain: str) -> str:
    """Classify a domain into one of three tiers."""
    d = domain.lower().strip()
    if d in ALL_STATE_MEDIA:
        return "eu_sanctioned"
    if d.endswith(".ru") or d.endswith(".su"):
        return "russian_domain"
    return "other"


def _load_gdelt() -> pd.DataFrame:
    import pyarrow.parquet as pq
    import pyarrow as pa
    path = DATASET_DIR / "raw_gdelt.parquet"
    table = pq.read_table(path)
    for i, field in enumerate(table.schema):
        if "date" in str(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
    table = table.replace_schema_metadata({})
    df = table.to_pandas()
    df["source_domain"] = df["source_domain"].fillna("").astype(str)
    return df


def main():
    log.info("=" * 60)
    log.info("Russia Filter Analysis")
    log.info("=" * 60)

    df = _load_gdelt()
    df["tier"] = df["source_domain"].apply(classify_domain)

    # Per-tier stats
    tiers = {}
    for tier_name, group in df.groupby("tier"):
        g = group.groupby("variant")["count"].sum()
        total = int(g.sum())
        ua = int(g.get("ukrainian", 0))
        ru = int(g.get("russian", 0))
        tiers[tier_name] = {
            "total": total,
            "russian": ru,
            "ukrainian": ua,
            "ua_pct": round(ua / total * 100, 1) if total > 0 else 0,
            "n_domains": int(group["source_domain"].nunique()),
        }
        log.info(f"  {tier_name}: {total:,} mentions, {tiers[tier_name]['ua_pct']}% Ukrainian, {tiers[tier_name]['n_domains']} domains")

    # Top sanctioned domains
    sanctioned = df[df["tier"] == "eu_sanctioned"]
    top_sanctioned = (
        sanctioned.groupby(["source_domain", "variant"])["count"]
        .sum().reset_index()
        .pivot_table(index="source_domain", columns="variant", values="count", fill_value=0)
        .reset_index()
    )
    if len(top_sanctioned):
        top_sanctioned["total"] = top_sanctioned.get("russian", 0) + top_sanctioned.get("ukrainian", 0)
        top_sanctioned["ua_pct"] = (top_sanctioned.get("ukrainian", 0) / top_sanctioned["total"] * 100).round(1)
        top_sanctioned = top_sanctioned.sort_values("total", ascending=False)

    result = {
        "method": "EU Council Regulation 833/2014, Article 2f — domains sanctioned under Decisions 2022/351 through 2025/394",
        "source": "EUR-Lex (eur-lex.europa.eu)",
        "tiers": tiers,
        "sanctioned_domains": [
            {"domain": r["source_domain"], "total": int(r["total"]), "ua_pct": float(r["ua_pct"])}
            for _, r in top_sanctioned.iterrows()
        ] if len(top_sanctioned) else [],
        "n_sanctioned_domains_in_data": int(sanctioned["source_domain"].nunique()),
        "n_sanctioned_domains_in_list": len(ALL_STATE_MEDIA),
    }

    out_path = SITE_DATA / "russia_filter.json"
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2)
    log.info(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
