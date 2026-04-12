"""Stratified balanced sampling across pairs, sources, variants, and years.

Takes raw extracted texts and produces a balanced corpus where no single
pair/source/variant/year dominates. Documents all shortfalls.

Usage:
    python -m pipeline.cl.balance.sampler
"""

import json
import logging
from collections import defaultdict

import pandas as pd

from pipeline.cl.config import (
    CL_BALANCED_DIR, CL_RAW_DIR,
    MAX_PER_VARIANT_PER_SOURCE, MIN_TEXTS_PER_PAIR,
    YEAR_STRATA, ensure_cl_dirs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SOURCES = ["reddit", "youtube", "gdelt_articles", "openalex", "religious"]


def load_raw_texts():
    """Load all raw extracted texts from parquet files.

    Reads both all_pairs.parquet (legacy) and individual pair_XX.parquet
    files, deduplicating by pair_id + text content.
    """
    frames = []
    for source_dir in SOURCES:
        src_path = CL_RAW_DIR / source_dir

        # Legacy single-file format
        all_path = src_path / "all_pairs.parquet"
        if all_path.exists():
            df = pd.read_parquet(all_path)
            if "source" not in df.columns:
                df["source"] = source_dir.replace("_articles", "")
            frames.append(df)
            log.info(f"Loaded {len(df)} texts from {source_dir}/all_pairs.parquet")

        # Per-pair files (new pairs + supplements)
        per_pair_files = sorted(src_path.glob("pair_*.parquet"))
        if per_pair_files:
            pair_frames = []
            for f in per_pair_files:
                try:
                    pdf = pd.read_parquet(f)
                    if "source" not in pdf.columns:
                        pdf["source"] = source_dir.replace("_articles", "")
                    pair_frames.append(pdf)
                except Exception as e:
                    log.warning(f"  Error reading {f}: {e}")
            if pair_frames:
                combined_pairs = pd.concat(pair_frames, ignore_index=True)
                frames.append(combined_pairs)
                log.info(f"Loaded {len(combined_pairs)} texts from {source_dir}/ ({len(per_pair_files)} pair files)")

        if not all_path.exists() and not per_pair_files:
            log.warning(f"No data found for {source_dir}")

    if not frames:
        raise FileNotFoundError("No raw text data found. Run extract scripts first.")

    combined = pd.concat(frames, ignore_index=True)
    log.info(f"Total raw texts before dedup: {len(combined)}")

    # Deduplicate: same pair_id + text content = same document
    before_dedup = len(combined)
    combined = combined.drop_duplicates(subset=["pair_id", "text"], keep="first")
    log.info(f"Dedup: {before_dedup} → {len(combined)} (removed {before_dedup - len(combined)})")

    # Filter to enabled pairs only
    from pipeline.config import get_enabled_pairs
    enabled_ids = {p["id"] for p in get_enabled_pairs()}
    before_filter = len(combined)
    combined = combined[combined["pair_id"].isin(enabled_ids)].copy()
    log.info(f"Enabled pairs filter: {before_filter} → {len(combined)} (removed {before_filter - len(combined)} disabled pair texts)")

    log.info(f"Total raw texts: {len(combined)}")

    # Filter to Latin-script texts only
    # Cyrillic/other-script texts confound English-language collocation and context analysis
    import re
    def is_latin_majority(text):
        if not text or len(str(text)) < 10:
            return False
        t = str(text)
        latin = len(re.findall(r'[a-zA-Z]', t))
        total_alpha = latin + len(re.findall(r'[\u0400-\u04FF\u4e00-\u9fff\u0600-\u06FF]', t))
        return latin > total_alpha * 0.5 if total_alpha > 0 else False

    before = len(combined)
    combined = combined[combined["text"].apply(is_latin_majority)].copy()
    removed = before - len(combined)
    log.info(f"Latin-script filter: {before} → {len(combined)} (removed {removed}, {removed/before*100:.1f}%)")

    # Remove very short texts (< 20 chars)
    before2 = len(combined)
    combined = combined[combined["text"].str.len() >= 20].copy()
    log.info(f"Short text filter: {before2} → {len(combined)} (removed {before2 - len(combined)})")

    # Remove Odessa Texas contamination
    before3 = len(combined)
    texas_mask = (combined["pair_id"] == 3) & combined["text"].str.contains(
        r"Texas|Permian Basin|Midland.{0,10}Odessa|Odessa.{0,10}Texas",
        case=False, na=False, regex=True
    )
    combined = combined[~texas_mask].copy()
    log.info(f"Odessa TX filter: {before3} → {len(combined)} (removed {before3 - len(combined)})")

    return combined


def assign_year_stratum(year):
    """Map a year to its stratum label."""
    for start, end in YEAR_STRATA:
        if start <= year <= end:
            return f"{start}-{end}"
    return "unknown"


def balanced_sample(df, max_per_cell=MAX_PER_VARIANT_PER_SOURCE):
    """Stratified sampling: pair × source × variant × year_stratum.

    For each cell, take min(available, max_per_cell / num_strata) texts.
    This ensures temporal balance within each pair-source-variant group.
    """
    df = df.copy()
    df["year_stratum"] = df["year"].apply(assign_year_stratum)

    num_strata = len(YEAR_STRATA)
    per_stratum = max(max_per_cell // num_strata, 10)

    sampled = []
    shortfalls = []
    distribution = defaultdict(lambda: defaultdict(int))

    groups = df.groupby(["pair_id", "source", "variant", "year_stratum"])
    for (pair_id, source, variant, stratum), group in groups:
        available = len(group)
        target = per_stratum
        take = min(available, target)

        sample = group.sample(n=take, random_state=42)
        sampled.append(sample)

        distribution[f"{pair_id}_{source}_{variant}"][stratum] = take

        if available < target:
            shortfalls.append({
                "pair_id": pair_id,
                "source": source,
                "variant": variant,
                "year_stratum": stratum,
                "target": target,
                "available": available,
                "shortfall": target - available,
            })

    result = pd.concat(sampled, ignore_index=True)
    return result, shortfalls


def generate_balance_report(df, shortfalls):
    """Generate comprehensive balance report."""
    report = {
        "total_texts": len(df),
        "timestamp": pd.Timestamp.now().isoformat(),
        "by_source": df.groupby("source").size().to_dict(),
        "by_variant": df.groupby("variant").size().to_dict(),
        "by_pair": {},
        "shortfalls": shortfalls,
        "year_distribution": df.groupby("year_stratum").size().to_dict() if "year_stratum" in df.columns else {},
    }

    for pair_id in sorted(df["pair_id"].unique()):
        pdf = df[df["pair_id"] == pair_id]
        report["by_pair"][int(pair_id)] = {
            "total": len(pdf),
            "by_variant": pdf.groupby("variant").size().to_dict(),
            "by_source": pdf.groupby("source").size().to_dict(),
            "by_year_stratum": pdf.groupby("year_stratum").size().to_dict() if "year_stratum" in pdf.columns else {},
        }

    return report


def run_balancing():
    ensure_cl_dirs()

    # Load raw
    raw_df = load_raw_texts()

    # Filter pairs with too few texts
    pair_counts = raw_df.groupby("pair_id").size()
    valid_pairs = pair_counts[pair_counts >= MIN_TEXTS_PER_PAIR].index.tolist()
    skipped_pairs = pair_counts[pair_counts < MIN_TEXTS_PER_PAIR].index.tolist()

    if skipped_pairs:
        log.warning(f"Skipping {len(skipped_pairs)} pairs with <{MIN_TEXTS_PER_PAIR} texts: {skipped_pairs}")

    raw_df = raw_df[raw_df["pair_id"].isin(valid_pairs)]

    # Balance
    balanced_df, shortfalls = balanced_sample(raw_df)

    # Report
    report = generate_balance_report(balanced_df, shortfalls)

    # Save
    out_path = CL_BALANCED_DIR / "corpus.parquet"
    balanced_df.to_parquet(out_path, index=False)
    log.info(f"Balanced corpus: {len(balanced_df)} texts → {out_path}")

    report_path = CL_BALANCED_DIR / "balance_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    log.info(f"Balance report: {report_path}")

    # Summary
    log.info(f"\nBALANCE SUMMARY:")
    log.info(f"  Pairs included: {len(valid_pairs)}")
    log.info(f"  Total texts: {len(balanced_df)}")
    log.info(f"  By source: {dict(balanced_df.groupby('source').size())}")
    log.info(f"  By variant: {dict(balanced_df.groupby('variant').size())}")
    log.info(f"  Shortfalls: {len(shortfalls)} cells below target")

    if shortfalls:
        total_shortfall = sum(s["shortfall"] for s in shortfalls)
        log.info(f"  Total shortfall: {total_shortfall} texts")

    return balanced_df, report


if __name__ == "__main__":
    run_balancing()
