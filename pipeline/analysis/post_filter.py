"""Unified post-filter pipeline for all sources.

Reads matching rules from pairs.yaml and applies them consistently
to any parquet dataset. Handles:
1. Word-boundary enforcement (for sources that used substring matching)
2. Homonym filters (Odessa TX, Nikolaev surname, etc.)
3. NER-based disambiguation (removes records where term is PERSON not LOC)

Usage:
    python -m pipeline.analysis.post_filter [--ner]

Without --ner: applies regex filters only (fast, seconds)
With --ner: also runs spaCy NER on flagged pairs (slow, minutes)
"""

import argparse
import json
import logging
import re
from pathlib import Path

import pandas as pd
import yaml

from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = ROOT_DIR / "data"
DATASET_DIR = ROOT_DIR / "dataset"


def load_pair_rules() -> dict:
    """Load matching rules from pairs.yaml."""
    with open(ROOT_DIR / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)

    rules = {}
    for p in cfg["pairs"]:
        if not p.get("enabled") or p.get("is_control"):
            continue
        pid = p["id"]
        rules[pid] = {
            "russian": p["russian"],
            "ukrainian": p["ukrainian"],
            "match_mode": p.get("match_mode", "word_boundary"),
            "homonym_filters": [re.compile(f, re.IGNORECASE) for f in p.get("homonym_filters", [])],
            "homonym_type": p.get("homonym_type"),
            "ner_filter": p.get("ner_filter", False),
        }
    return rules


def apply_homonym_filters(df: pd.DataFrame, rules: dict, text_col: str = "text") -> pd.DataFrame:
    """Apply regex-based homonym filters. Removes false positives."""
    if text_col not in df.columns:
        # Try URL or other text fields
        for alt in ["url", "source_domain", "matched_term"]:
            if alt in df.columns:
                text_col = alt
                break

    initial = len(df)
    removed_per_pair = {}

    for pid, rule in rules.items():
        if not rule["homonym_filters"]:
            continue

        mask = df["pair_id"] == pid
        if not mask.any():
            continue

        pair_df = df[mask]
        search_text = pair_df[text_col].fillna("").astype(str)

        # If we have multiple text columns, concat them for broader matching
        if "url" in df.columns and text_col != "url":
            search_text = search_text + " " + pair_df["url"].fillna("").astype(str)
        if "source_domain" in df.columns and text_col != "source_domain":
            search_text = search_text + " " + pair_df["source_domain"].fillna("").astype(str)

        filter_mask = pd.Series(False, index=pair_df.index)
        for pat in rule["homonym_filters"]:
            filter_mask |= search_text.apply(lambda x: bool(pat.search(x)))

        n_removed = filter_mask.sum()
        if n_removed > 0:
            df = df[~(mask & filter_mask)]
            removed_per_pair[pid] = int(n_removed)

    total_removed = initial - len(df)
    if total_removed > 0:
        log.info(f"  Homonym filters removed {total_removed:,} records: {removed_per_pair}")
    return df


def apply_ner_filter(df: pd.DataFrame, rules: dict) -> pd.DataFrame:
    """Run spaCy NER to remove records where the term is PERSON not LOC."""
    import spacy

    # Only process pairs that need NER
    ner_pairs = {pid: r for pid, r in rules.items() if r["ner_filter"]}
    if not ner_pairs:
        return df

    # Check which pairs actually have data
    ner_pids_with_data = [pid for pid in ner_pairs if (df["pair_id"] == pid).any()]
    if not ner_pids_with_data:
        return df

    log.info(f"  Loading spaCy for NER on {len(ner_pids_with_data)} pairs...")
    try:
        nlp = spacy.load("en_core_web_trf")
    except OSError:
        try:
            nlp = spacy.load("en_core_web_lg")
        except OSError:
            nlp = spacy.load("en_core_web_sm")
    log.info(f"  Model: {nlp.meta['name']}")

    if "text" not in df.columns:
        log.info("  No text column — skipping NER")
        return df

    initial = len(df)
    removed_per_pair = {}
    contamination_rates = {}

    for pid in ner_pids_with_data:
        rule = ner_pairs[pid]
        mask = df["pair_id"] == pid
        pair_df = df[mask].copy()

        if len(pair_df) == 0:
            continue

        terms = [rule["russian"].lower(), rule["ukrainian"].lower()]
        person_indices = []

        for idx, row in pair_df.iterrows():
            text = str(row.get("text", ""))[:5000]
            doc = nlp(text)

            is_person = False
            for ent in doc.ents:
                if any(t in ent.text.lower() for t in terms):
                    if ent.label_ in ("PERSON", "PER"):
                        is_person = True
                        break

            if is_person:
                person_indices.append(idx)

        n_person = len(person_indices)
        pct = n_person / len(pair_df) * 100 if len(pair_df) > 0 else 0
        contamination_rates[pid] = {
            "total": len(pair_df),
            "person": n_person,
            "pct": round(pct, 1),
        }

        if n_person > 0:
            df = df.drop(person_indices)
            removed_per_pair[pid] = n_person

        log.info(f"  Pair {pid} ({rule['russian']}): {n_person}/{len(pair_df)} person ({pct:.1f}%) — removed")

    total_removed = initial - len(df)
    if total_removed > 0:
        log.info(f"  NER removed {total_removed:,} records: {removed_per_pair}")

    # Save contamination report
    report_path = DATA_DIR / "cl" / "annotation" / "ner_contamination_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w") as f:
        json.dump(contamination_rates, f, indent=2)
    log.info(f"  NER report: {report_path}")

    return df


def filter_dataset(name: str, path: Path, rules: dict, run_ner: bool = False) -> None:
    """Apply all filters to a single dataset file."""
    if not path.exists():
        return

    log.info(f"\n{name}: {path}")

    # Handle BQ date type
    try:
        df = pd.read_parquet(path)
    except Exception:
        import pyarrow.parquet as pq
        import pyarrow as pa
        table = pq.read_table(path)
        for i, field in enumerate(table.schema):
            if "date" in str(field.type):
                table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
        table = table.replace_schema_metadata({})
        df = table.to_pandas()

    if "pair_id" not in df.columns:
        log.info("  No pair_id column — skipping")
        return

    df["pair_id"] = df["pair_id"].astype(int)
    initial = len(df)
    log.info(f"  Loaded: {initial:,} rows, {df['pair_id'].nunique()} pairs")

    # Step 1: Homonym filters
    df = apply_homonym_filters(df, rules)

    # Step 2: NER (if requested and data has text)
    if run_ner and "text" in df.columns:
        df = apply_ner_filter(df, rules)

    final = len(df)
    if final < initial:
        log.info(f"  Cleaned: {initial:,} → {final:,} ({initial - final:,} removed)")
        df.to_parquet(path, index=False)
        log.info(f"  Saved: {path}")
    else:
        log.info(f"  Clean: no records removed")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ner", action="store_true", help="Run NER disambiguation (slow)")
    args = parser.parse_args()

    rules = load_pair_rules()
    log.info(f"Loaded rules for {len(rules)} pairs")
    log.info(f"  Homonym filters: {sum(1 for r in rules.values() if r['homonym_filters'])} pairs")
    log.info(f"  NER filter: {sum(1 for r in rules.values() if r['ner_filter'])} pairs")

    # Apply to all data sources
    # 1. Main GDELT dataset (time series)
    filter_dataset("GDELT (timeseries)", DATASET_DIR / "raw_gdelt.parquet", rules)

    # 2. CL corpus sources
    cl_raw = DATA_DIR / "cl" / "raw"
    for src_dir in sorted(cl_raw.iterdir()):
        if not src_dir.is_dir():
            continue
        for f in sorted(src_dir.glob("*.parquet")):
            if any(skip in f.name for skip in ["checkpoint", "session", "fetch", "all_posts"]):
                continue
            filter_dataset(f"CL/{src_dir.name}/{f.name}", f, rules, run_ner=args.ner)

    # 3. Other datasets
    for name in ["raw_reddit", "raw_youtube", "raw_wikipedia", "raw_trends", "raw_ngrams"]:
        path = DATASET_DIR / f"{name}.parquet"
        if path.exists():
            filter_dataset(f"Dataset/{name}", path, rules)

    log.info("\n=== Post-filter complete ===")


if __name__ == "__main__":
    main()
