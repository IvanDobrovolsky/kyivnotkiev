"""Normalize, merge, and validate raw data from all sources.

Produces a unified dataset with consistent schema for downstream analysis.

Usage:
    python -m src.pipeline.preprocess [--source gdelt|trends|ngrams|all]
"""

import argparse
import logging

import pandas as pd

from src.config import (
    GDELT_RAW_DIR,
    NGRAMS_RAW_DIR,
    PROCESSED_DIR,
    TRENDS_RAW_DIR,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def preprocess_gdelt() -> pd.DataFrame | None:
    """Merge and normalize all raw GDELT parquet files."""
    parquet_files = sorted(GDELT_RAW_DIR.glob("pair_*.parquet"))
    if not parquet_files:
        log.warning("No GDELT parquet files found")
        return None

    log.info(f"Processing {len(parquet_files)} GDELT files")

    frames = []
    for f in parquet_files:
        df = pd.read_parquet(f)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)

    # Normalize column names
    combined.columns = combined.columns.str.lower().str.replace(" ", "_")

    # Ensure week is datetime
    if "week" in combined.columns:
        combined["week"] = pd.to_datetime(combined["week"])

    # Compute adoption ratio: ukrainian / (ukrainian + russian)
    combined["adoption_ratio"] = combined["ukrainian_count"] / (
        combined["ukrainian_count"] + combined["russian_count"]
    ).replace(0, float("nan"))

    combined["source"] = "gdelt"

    out_path = PROCESSED_DIR / "gdelt_merged.parquet"
    combined.to_parquet(out_path, index=False)
    log.info(f"GDELT processed: {len(combined)} rows -> {out_path}")

    return combined


def preprocess_trends() -> pd.DataFrame | None:
    """Merge and normalize all raw Google Trends CSV files."""
    csv_files = sorted(TRENDS_RAW_DIR.glob("pair_*.csv"))
    if not csv_files:
        log.warning("No Trends CSV files found")
        return None

    log.info(f"Processing {len(csv_files)} Trends files")

    pairs_lookup = {p["id"]: p for p in get_all_pairs()}
    frames = []

    for f in csv_files:
        df = pd.read_csv(f, parse_dates=["date"])

        pair_id = df["pair_id"].iloc[0] if "pair_id" in df.columns else None
        if pair_id is None:
            # Try to extract from filename
            try:
                pair_id = int(f.stem.split("_")[1])
            except (IndexError, ValueError):
                log.warning(f"Cannot determine pair_id for {f}, skipping")
                continue

        pair = pairs_lookup.get(pair_id)
        if pair is None:
            continue

        russian = pair["russian"]
        ukrainian = pair["ukrainian"]

        # Normalize to common schema
        if russian in df.columns and ukrainian in df.columns:
            df_norm = df[["date", russian, ukrainian]].copy()
            df_norm = df_norm.rename(columns={
                "date": "week",
                russian: "russian_interest",
                ukrainian: "ukrainian_interest",
            })
        elif russian in df.columns:
            df_norm = df[["date", russian]].copy()
            df_norm = df_norm.rename(columns={"date": "week", russian: "russian_interest"})
            df_norm["ukrainian_interest"] = 0
        else:
            log.warning(f"Unexpected columns in {f}: {df.columns.tolist()}")
            continue

        df_norm["pair_id"] = pair_id
        df_norm["category"] = pair["category"]
        df_norm["russian_term"] = russian
        df_norm["ukrainian_term"] = ukrainian
        df_norm["geo"] = df["geo"].iloc[0] if "geo" in df.columns else "worldwide"

        # Compute adoption ratio
        total = df_norm["russian_interest"] + df_norm["ukrainian_interest"]
        df_norm["adoption_ratio"] = df_norm["ukrainian_interest"] / total.replace(0, float("nan"))

        frames.append(df_norm)

    if not frames:
        log.warning("No valid Trends data processed")
        return None

    combined = pd.concat(frames, ignore_index=True)
    combined["source"] = "trends"

    out_path = PROCESSED_DIR / "trends_merged.parquet"
    combined.to_parquet(out_path, index=False)
    log.info(f"Trends processed: {len(combined)} rows -> {out_path}")

    return combined


def preprocess_ngrams() -> pd.DataFrame | None:
    """Merge and normalize all raw Ngrams CSV files."""
    csv_files = sorted(NGRAMS_RAW_DIR.glob("pair_*.csv"))
    if not csv_files:
        log.warning("No Ngrams CSV files found")
        return None

    log.info(f"Processing {len(csv_files)} Ngrams files")

    pairs_lookup = {p["id"]: p for p in get_all_pairs()}
    frames = []

    for f in csv_files:
        df = pd.read_csv(f)
        pair_id = df["pair_id"].iloc[0] if "pair_id" in df.columns else None
        if pair_id is None:
            continue

        pair = pairs_lookup.get(pair_id)
        if pair is None:
            continue

        # Pivot variants into columns
        if "variant" in df.columns:
            russian_data = df[df["variant"] == "russian"][["year", "frequency"]].rename(
                columns={"frequency": "russian_freq"}
            )
            ukrainian_data = df[df["variant"] == "ukrainian"][["year", "frequency"]].rename(
                columns={"frequency": "ukrainian_freq"}
            )

            if not russian_data.empty and not ukrainian_data.empty:
                merged = russian_data.merge(ukrainian_data, on="year", how="outer").fillna(0)
            elif not russian_data.empty:
                merged = russian_data
                merged["ukrainian_freq"] = 0
            else:
                merged = ukrainian_data
                merged["russian_freq"] = 0
        else:
            continue

        merged["pair_id"] = pair_id
        merged["category"] = pair["category"]
        merged["russian_term"] = pair["russian"]
        merged["ukrainian_term"] = pair["ukrainian"]

        total = merged.get("russian_freq", 0) + merged.get("ukrainian_freq", 0)
        if "ukrainian_freq" in merged.columns:
            merged["adoption_ratio"] = merged["ukrainian_freq"] / total.replace(0, float("nan"))

        frames.append(merged)

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)
    combined["source"] = "ngrams"

    out_path = PROCESSED_DIR / "ngrams_merged.parquet"
    combined.to_parquet(out_path, index=False)
    log.info(f"Ngrams processed: {len(combined)} rows -> {out_path}")

    return combined


def validate(df: pd.DataFrame, source: str) -> bool:
    """Run basic validation checks on processed data."""
    issues = []

    if df.empty:
        issues.append(f"{source}: empty dataframe")

    if "adoption_ratio" in df.columns:
        invalid = df["adoption_ratio"].dropna()
        out_of_range = ((invalid < 0) | (invalid > 1)).sum()
        if out_of_range > 0:
            issues.append(f"{source}: {out_of_range} adoption_ratio values outside [0, 1]")

    if "pair_id" in df.columns:
        n_pairs = df["pair_id"].nunique()
        log.info(f"{source}: {n_pairs} pairs, {len(df)} rows")

    if issues:
        for issue in issues:
            log.error(f"VALIDATION: {issue}")
        return False

    log.info(f"{source}: validation passed")
    return True


def preprocess_all(sources: list[str] | None = None):
    """Run preprocessing for all (or selected) data sources."""
    ensure_dirs()

    if sources is None:
        sources = ["gdelt", "trends", "ngrams"]

    for source in sources:
        log.info(f"Preprocessing: {source}")
        if source == "gdelt":
            df = preprocess_gdelt()
        elif source == "trends":
            df = preprocess_trends()
        elif source == "ngrams":
            df = preprocess_ngrams()
        else:
            log.error(f"Unknown source: {source}")
            continue

        if df is not None:
            validate(df, source)


def main():
    parser = argparse.ArgumentParser(description="Preprocess raw data from all sources")
    parser.add_argument("--source", type=str, default="all",
                        choices=["gdelt", "trends", "ngrams", "all"],
                        help="Which data source to preprocess (default: all)")
    args = parser.parse_args()

    sources = None if args.source == "all" else [args.source]
    preprocess_all(sources=sources)


if __name__ == "__main__":
    main()
