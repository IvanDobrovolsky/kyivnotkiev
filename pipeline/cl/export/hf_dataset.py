"""Export balanced labeled corpus to Hugging Face dataset format.

Creates a proper HF dataset with:
- Train/val/test splits
- Dataset card (README.md)
- Balance report
- Fetch log for transparency

Usage:
    python -m pipeline.cl.export.hf_dataset [--repo IvanDobrovolsky/kyivnotkiev-cl]
"""

import argparse
import json
import logging
import shutil
from pathlib import Path

import pandas as pd

from pipeline.cl.config import (
    CL_BALANCED_DIR, CL_CLASSIFIED_DIR, CL_EXPORT_DIR, CL_RAW_DIR,
    CONTEXT_LABELS, SENTIMENT_LABELS, ensure_cl_dirs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def generate_dataset_card(df, report, output_dir):
    """Generate README.md dataset card."""
    n_pairs = df["pair_id"].nunique()
    n_sources = df["source"].nunique()

    card = f"""---
language:
- en
license: cc-by-4.0
task_categories:
- text-classification
task_ids:
- topic-classification
- sentiment-classification
tags:
- linguistics
- ukraine
- toponyms
- language-policy
- kyivnotkiev
size_categories:
- 10K<n<100K
---

# KyivNotKiev Computational Linguistics Corpus

A balanced, labeled corpus of texts containing Ukrainian and Russian toponym variants
(e.g., "Kyiv" vs "Kiev"), annotated with context categories and sentiment.

## Dataset Description

- **Curated by:** Ivan Dobrovolskyi
- **Language:** Primarily English
- **License:** CC-BY 4.0
- **Paper:** [forthcoming]
- **Website:** https://kyivnotkiev.org

## Dataset Summary

{len(df):,} texts across {n_pairs} Ukrainian-Russian toponym pairs from {n_sources} sources
(Reddit, YouTube, GDELT news articles). Each text is labeled with:
- **Context category**: {', '.join(CONTEXT_LABELS)}
- **Sentiment**: positive, neutral, negative
- **Variant**: which toponym form (russian/ukrainian) appears in the text

## Intended Uses

- Studying language policy adoption in media and social platforms
- Training toponym context classifiers
- Analyzing sentiment differences between spelling variants
- Cross-source and temporal analysis of naming conventions

## Dataset Structure

### Data Fields
- `pair_id`: Integer ID of the toponym pair
- `text`: The full text content
- `variant`: "russian" or "ukrainian" — which spelling form appears
- `source`: Data source (reddit, youtube, gdelt)
- `year`: Publication year
- `context_label`: Annotated context category
- `context_confidence`: Annotation confidence (0-1)
- `sentiment_label`: Sentiment annotation
- `sentiment_score`: Sentiment score (-1 to 1)
- `word_count`: Number of words in text
- `matched_term`: The specific toponym form found in text

### Splits
| Split | Count |
|-------|-------|
| train | {int(len(df) * 0.8):,} |
| validation | {int(len(df) * 0.1):,} |
| test | {int(len(df) * 0.1):,} |

## Balance Report

See `balance_report.json` for detailed per-pair, per-source, per-variant distributions
and documented shortfalls.

## Collection Methodology

1. **Reddit**: Titles and bodies from Arctic Shift API + Reddit search (2010-2026)
2. **YouTube**: Video titles and descriptions via yt-dlp (2010-2026)
3. **GDELT**: News article bodies fetched from URLs using trafilatura (2010-2026)
4. **Balancing**: Stratified sampling by pair × source × variant × year stratum
5. **Annotation**: Llama 3.1 70B-Instruct with human validation on 200 random samples
6. **Fetch transparency**: All GDELT URL fetch attempts logged in `fetch_log.parquet`

## Citation

```bibtex
@article{{dobrovolskyi2026kyivnotkiev,
  title={{#KyivNotKiev: A Large-Scale Computational Study of Ukrainian Toponym Adoption}},
  author={{Dobrovolskyi, Ivan}},
  year={{2026}}
}}
```
"""

    with open(output_dir / "README.md", "w") as f:
        f.write(card)
    log.info(f"Dataset card: {output_dir / 'README.md'}")


def export_dataset(repo_id=None):
    ensure_cl_dirs()

    corpus_path = CL_CLASSIFIED_DIR / "corpus_labeled.parquet"
    if not corpus_path.exists():
        raise FileNotFoundError("Labeled corpus not found.")

    df = pd.read_parquet(corpus_path)
    output_dir = CL_EXPORT_DIR / "dataset"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Select columns for export
    export_cols = [
        "pair_id", "text", "variant", "source", "year", "matched_term",
        "word_count", "context_label", "context_confidence", "context_reason",
        "sentiment_label", "sentiment_score",
    ]
    available_cols = [c for c in export_cols if c in df.columns]
    export_df = df[available_cols].copy()

    # Train/val/test splits
    from sklearn.model_selection import train_test_split
    train_df, temp_df = train_test_split(export_df, test_size=0.2, random_state=42)
    val_df, test_df = train_test_split(temp_df, test_size=0.5, random_state=42)

    train_df.to_parquet(output_dir / "train.parquet", index=False)
    val_df.to_parquet(output_dir / "validation.parquet", index=False)
    test_df.to_parquet(output_dir / "test.parquet", index=False)

    # Copy balance report
    report_path = CL_BALANCED_DIR / "balance_report.json"
    if report_path.exists():
        shutil.copy(report_path, output_dir / "balance_report.json")
        with open(report_path) as f:
            report = json.load(f)
    else:
        report = {}

    # Copy fetch log
    fetch_log_path = CL_RAW_DIR / "gdelt_articles" / "fetch_log.parquet"
    if fetch_log_path.exists():
        shutil.copy(fetch_log_path, output_dir / "fetch_log.parquet")

    # Dataset card
    generate_dataset_card(export_df, report, output_dir)

    log.info(f"Dataset exported: {output_dir}")
    log.info(f"  Train: {len(train_df)}, Val: {len(val_df)}, Test: {len(test_df)}")

    # Push to HF if repo specified
    if repo_id:
        log.info(f"Pushing to Hugging Face: {repo_id}")
        from huggingface_hub import HfApi
        api = HfApi()
        api.upload_folder(
            folder_path=str(output_dir),
            repo_id=repo_id,
            repo_type="dataset",
        )
        log.info(f"Published: https://huggingface.co/datasets/{repo_id}")

    return output_dir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", type=str, default=None,
                        help="HF repo ID (e.g., IvanDobrovolsky/kyivnotkiev-cl)")
    args = parser.parse_args()
    export_dataset(repo_id=args.repo)


if __name__ == "__main__":
    main()
