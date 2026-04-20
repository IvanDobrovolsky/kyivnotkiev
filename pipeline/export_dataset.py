"""Export the full KyivNotKiev dataset for public release.

NOTE: This script was used during development to export data from BigQuery
to the HuggingFace parquet format. It requires GCP credentials and the
BigQuery dataset to be populated. For reproduction, use the pre-exported
parquet files in dataset/ instead (see: make reproduce).

Creates a publishable dataset package with:
- Parquet files per source (efficient, typed, standard)
- metadata.json with dataset card info
- README.md for Hugging Face Datasets hub

Usage:
    python -m pipeline.export_dataset [--output-dir ./dataset]
    # Then: huggingface-cli upload IvanDobrovolsky/kyivnotkiev-dataset ./dataset
"""

import argparse
import json
import logging
from datetime import datetime
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT = "kyivnotkiev-research"
DATASET = "kyivnotkiev"
client = bigquery.Client(project=PROJECT)


def export_table(table: str, output_dir: Path, query_override: str = None):
    """Export a BQ table to Parquet."""
    sql = query_override or f"SELECT * FROM `{DATASET}.{table}`"
    log.info(f"  Exporting {table}...")
    df = client.query(sql).to_dataframe()
    path = output_dir / f"{table}.parquet"
    df.to_parquet(path, index=False)
    log.info(f"    {len(df):,} rows → {path.name} ({path.stat().st_size / 1024 / 1024:.1f} MB)")
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Export dataset for public release")
    parser.add_argument("--output-dir", type=str, default="./dataset")
    args = parser.parse_args()

    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)

    log.info("=" * 60)
    log.info("Exporting KyivNotKiev dataset for public release")
    log.info("=" * 60)

    stats = {}

    # Core tables — anonymized (no raw URLs for GDELT, no post bodies for Reddit)
    stats["trends"] = export_table("raw_trends", out, f"""
        SELECT pair_id, date, term, variant, interest, geo
        FROM `{DATASET}.raw_trends`
        ORDER BY pair_id, date, geo
    """)

    stats["gdelt"] = export_table("raw_gdelt", out, f"""
        SELECT pair_id, date, source_domain, matched_term, variant, count
        FROM `{DATASET}.raw_gdelt`
        ORDER BY pair_id, date
    """)

    stats["wikipedia"] = export_table("raw_wikipedia", out, f"""
        SELECT pair_id, page_title, variant, date, pageviews
        FROM `{DATASET}.raw_wikipedia`
        ORDER BY pair_id, date
    """)

    stats["reddit"] = export_table("raw_reddit", out, f"""
        SELECT pair_id, subreddit, variant, matched_term,
            DATE(created_utc) as date, score
        FROM `{DATASET}.raw_reddit`
        ORDER BY pair_id, date
    """)

    stats["youtube"] = export_table("raw_youtube", out, f"""
        SELECT pair_id, channel_title, variant, matched_term,
            DATE(published_at) as date, view_count
        FROM `{DATASET}.raw_youtube`
        ORDER BY pair_id, date
    """)

    stats["ngrams"] = export_table("raw_ngrams", out, f"""
        SELECT pair_id, year, term, variant, frequency
        FROM `{DATASET}.raw_ngrams`
        ORDER BY pair_id, year
    """)

    # OpenAlex from local file
    oa_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "openalex" / "openalex_all_pairs.json"
    if oa_path.exists():
        import pandas as pd
        with open(oa_path) as f:
            oa_data = json.load(f)
        rows = []
        for p in oa_data:
            for yr in p["yearly"]:
                rows.append({
                    "pair_id": p["pair_id"],
                    "year": yr["year"],
                    "russian_term": p["russian_term"],
                    "ukrainian_term": p["ukrainian_term"],
                    "russian_count": yr["russian_count"],
                    "ukrainian_count": yr["ukrainian_count"],
                })
        df = pd.DataFrame(rows)
        path = out / "openalex.parquet"
        df.to_parquet(path, index=False)
        stats["openalex"] = len(df)
        log.info(f"    {len(df):,} rows → openalex.parquet")

    # Pairs metadata
    import yaml
    pairs_path = Path(__file__).resolve().parent.parent / "config" / "pairs.yaml"
    with open(pairs_path) as f:
        cfg = yaml.safe_load(f)
    pairs = [
        {"id": p["id"], "category": p["category"], "russian": p["russian"],
         "ukrainian": p["ukrainian"], "enabled": p.get("enabled", True),
         "is_control": p.get("is_control", False)}
        for p in cfg["pairs"]
    ]
    with open(out / "pairs.json", "w") as f:
        json.dump(pairs, f, indent=2)

    # Analysis results
    analysis_path = Path(__file__).resolve().parent.parent / "site" / "src" / "data" / "analysis.json"
    if analysis_path.exists():
        import shutil
        shutil.copy(analysis_path, out / "analysis.json")

    # Dataset metadata
    total_rows = sum(stats.values())
    metadata = {
        "name": "kyivnotkiev",
        "version": "2.0.0",
        "description": "Multi-source computational dataset tracking Ukrainian toponym adoption in English (2010-2026)",
        "license": "CC-BY-4.0",
        "citation": "Dobrovolskyi, I. (2026). Measuring Real-Time Toponymic Change: A Multi-Source Computational Framework for Tracking Ukrainian Spelling Adoption. Computational Linguistics.",
        "homepage": "https://kyivnotkiev.org",
        "repository": "https://github.com/IvanDobrovolsky/kyivnotkiev",
        "paper": "https://github.com/IvanDobrovolsky/KyivNotKiev-paper",
        "date_created": datetime.now().strftime("%Y-%m-%d"),
        "total_rows": total_rows,
        "sources": {
            "trends": {"rows": stats["trends"], "description": "Google Trends search interest (global + 55 countries)", "time_range": "2010-2026"},
            "gdelt": {"rows": stats["gdelt"], "description": "GDELT news article mentions (domain-level, no raw URLs)", "time_range": "2015-2026"},
            "wikipedia": {"rows": stats["wikipedia"], "description": "Wikipedia article pageviews", "time_range": "2015-2026"},
            "reddit": {"rows": stats["reddit"], "description": "Reddit post mentions (no body text, privacy-preserving)", "time_range": "2007-2026"},
            "youtube": {"rows": stats["youtube"], "description": "YouTube video title/channel mentions", "time_range": "2006-2026"},
            "ngrams": {"rows": stats["ngrams"], "description": "Google Books Ngram frequency", "time_range": "1900-2019"},
            "openalex": {"rows": stats.get("openalex", 0), "description": "OpenAlex academic paper title mentions", "time_range": "2010-2026"},
        },
        "pairs": {"total": len(pairs), "enabled": sum(1 for p in pairs if p["enabled"]),
                   "analyzable": sum(1 for p in pairs if p["enabled"] and not p["is_control"])},
    }
    with open(out / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    # README for Hugging Face
    readme = f"""---
license: cc-by-4.0
task_categories:
  - text-classification
language:
  - en
tags:
  - linguistics
  - sociolinguistics
  - language-policy
  - ukraine
  - toponymy
  - corpus-linguistics
size_categories:
  - 1M<n<10M
---

# KyivNotKiev Dataset

**The world's largest computational dataset tracking how English-language media adopts Ukrainian spellings.**

Part of the research project: [kyivnotkiev.org](https://kyivnotkiev.org)

## Dataset Description

This dataset measures the adoption of Ukrainian-derived English spellings (e.g., "Kyiv" instead of "Kiev") across 7 independent data sources spanning 2010-2026. It covers {metadata['pairs']['analyzable']} toponym pairs across 8 categories.

## Sources

| Source | Rows | Description | Time Range |
|--------|------|-------------|------------|
| Google Trends | {stats['trends']:,} | Search interest (global + 55 countries) | 2010-2026 |
| GDELT | {stats['gdelt']:,} | News article mentions | 2015-2026 |
| Wikipedia | {stats['wikipedia']:,} | Article pageviews | 2015-2026 |
| Reddit | {stats['reddit']:,} | Post mentions | 2007-2026 |
| YouTube | {stats['youtube']:,} | Video title mentions | 2006-2026 |
| Google Books Ngrams | {stats['ngrams']:,} | Book frequency | 1900-2019 |
| OpenAlex | {stats.get('openalex', 0):,} | Academic paper mentions | 2010-2026 |

**Total: {total_rows:,} rows**

## Files

- `raw_trends.parquet` — Google Trends data (pair_id, date, term, variant, interest, geo)
- `raw_gdelt.parquet` — GDELT news mentions (pair_id, date, source_domain, variant, count)
- `raw_wikipedia.parquet` — Wikipedia pageviews (pair_id, date, page_title, variant, pageviews)
- `raw_reddit.parquet` — Reddit posts (pair_id, date, subreddit, variant, score)
- `raw_youtube.parquet` — YouTube videos (pair_id, date, channel_title, variant, view_count)
- `raw_ngrams.parquet` — Google Books frequency (pair_id, year, term, variant, frequency)
- `openalex.parquet` — Academic papers (pair_id, year, russian_count, ukrainian_count)
- `pairs.json` — Toponym pair definitions with categories
- `analysis.json` — Statistical test results
- `metadata.json` — Dataset metadata

## Categories

| Category | Pairs | Example |
|----------|-------|---------|
| Geographical | 24 | Kiev → Kyiv |
| Food & Cuisine | 5 | Chicken Kiev → Chicken Kyiv |
| Landmarks | 6 | Kiev Pechersk Lavra → Kyiv Pechersk Lavra |
| Country-Level | 1 | the Ukraine → Ukraine |
| Institutional | 6 | Kiev National University → Kyiv National University |
| Sports | 5 | Dynamo Kiev → Dynamo Kyiv |
| Historical | 6 | Vladimir the Great → Volodymyr the Great |
| People | 2 | Vladimir Zelensky → Volodymyr Zelenskyy |

## Privacy

- GDELT: domain-level aggregation only (no raw article URLs)
- Reddit: no post body text, only metadata (subreddit, date, score)
- YouTube: no video URLs, only channel name and metadata

## Citation

```bibtex
@article{{dobrovolskyi2026kyivnotkiev,
  title={{Measuring Real-Time Toponymic Change: A Multi-Source Computational Framework for Tracking Ukrainian Spelling Adoption}},
  author={{Dobrovolskyi, Ivan}},
  journal={{Computational Linguistics}},
  year={{2026}},
  publisher={{MIT Press}}
}}
```

## License

CC-BY-4.0
"""
    with open(out / "README.md", "w") as f:
        f.write(readme)

    log.info("=" * 60)
    log.info(f"Dataset exported to {out}/")
    log.info(f"Total: {total_rows:,} rows across 7 sources")
    log.info(f"Files: {len(list(out.glob('*')))} files")
    log.info("")
    log.info("To publish on Hugging Face:")
    log.info("  pip install huggingface_hub")
    log.info("  huggingface-cli login")
    log.info(f"  huggingface-cli upload IvanDobrovolsky/kyivnotkiev-dataset {out}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
