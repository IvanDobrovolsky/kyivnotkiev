"""Category-level aggregation and statistical comparison.

Computes mean adoption metrics per category and tests whether adoption
speed differs significantly between categories.

Usage:
    python -m pipeline.analysis.categories [--source gdelt|trends]
"""

import argparse
import logging

import pandas as pd
from scipy import stats

from pipeline.config import (
    PROCESSED_DIR,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def compute_category_adoption_curves(df: pd.DataFrame) -> pd.DataFrame:
    """Compute mean adoption ratio per category per week."""
    pairs = get_all_pairs()
    pair_categories = {p["id"]: p["category"] for p in pairs}

    df = df.copy()
    df["category"] = df["pair_id"].map(pair_categories)

    time_col = "week" if "week" in df.columns else "year"

    curves = df.groupby(["category", time_col]).agg(
        adoption_ratio_mean=("adoption_ratio", "mean"),
        adoption_ratio_median=("adoption_ratio", "median"),
        adoption_ratio_std=("adoption_ratio", "std"),
        n_pairs=("pair_id", "nunique"),
    ).reset_index()

    return curves


def compute_category_crossover_stats(source: str = "gdelt") -> pd.DataFrame:
    """Compute crossover date statistics per category from change-point results."""
    cp_path = PROCESSED_DIR / f"changepoints_{source}.parquet"
    if not cp_path.exists():
        log.error(f"Change-point results not found: {cp_path}")
        return pd.DataFrame()

    df = pd.read_parquet(cp_path)

    if "crossover_date" in df.columns:
        df["crossover_date"] = pd.to_datetime(df["crossover_date"])

    category_stats = df.groupby("category").agg(
        mean_crossover=("crossover_date", "mean"),
        median_crossover=("crossover_date", "median"),
        earliest_crossover=("crossover_date", "min"),
        latest_crossover=("crossover_date", "max"),
        n_crossed=("crossover_date", "count"),
        n_total=("pair_id", "count"),
        mean_confidence=("confidence", "mean"),
        mean_ratio_before=("adoption_ratio_before", "mean"),
        mean_ratio_after=("adoption_ratio_after", "mean"),
    ).reset_index()

    category_stats["pct_crossed"] = category_stats["n_crossed"] / category_stats["n_total"]

    return category_stats


def test_category_differences(source: str = "gdelt") -> dict:
    """Test whether crossover dates differ significantly between categories.

    Uses Kruskal-Wallis H-test (non-parametric) since we don't assume
    normal distribution of crossover dates.
    """
    cp_path = PROCESSED_DIR / f"changepoints_{source}.parquet"
    if not cp_path.exists():
        return {}

    df = pd.read_parquet(cp_path)
    df["crossover_date"] = pd.to_datetime(df["crossover_date"])

    # Convert dates to ordinal for statistical testing
    df = df.dropna(subset=["crossover_date"])
    df["crossover_ordinal"] = df["crossover_date"].apply(lambda x: x.toordinal())

    categories = df["category"].unique()
    groups = [df[df["category"] == cat]["crossover_ordinal"].values for cat in categories]
    groups = [g for g in groups if len(g) >= 2]

    results = {"n_categories": len(categories)}

    if len(groups) >= 2:
        h_stat, p_value = stats.kruskal(*groups)
        results["kruskal_h"] = float(h_stat)
        results["kruskal_p"] = float(p_value)
        results["significant"] = p_value < 0.05
        log.info(f"Kruskal-Wallis: H={h_stat:.2f}, p={p_value:.4f}")
    else:
        log.warning("Not enough categories with data for statistical testing")

    # Pairwise Mann-Whitney U tests
    pairwise = []
    cat_groups = {cat: df[df["category"] == cat]["crossover_ordinal"].values for cat in categories}
    for i, cat1 in enumerate(categories):
        for cat2 in categories[i + 1:]:
            g1, g2 = cat_groups[cat1], cat_groups[cat2]
            if len(g1) >= 2 and len(g2) >= 2:
                u_stat, p_val = stats.mannwhitneyu(g1, g2, alternative="two-sided")
                pairwise.append({
                    "category_1": cat1,
                    "category_2": cat2,
                    "u_statistic": float(u_stat),
                    "p_value": float(p_val),
                    "significant": p_val < 0.05,
                })

    results["pairwise_tests"] = pairwise
    return results


def rank_categories(source: str = "gdelt") -> pd.DataFrame:
    """Rank categories by adoption speed (earliest mean crossover = fastest)."""
    category_stats = compute_category_crossover_stats(source)
    if category_stats.empty:
        return category_stats

    category_stats = category_stats.sort_values("mean_crossover")
    category_stats["rank"] = range(1, len(category_stats) + 1)

    log.info("Category ranking by adoption speed:")
    for _, row in category_stats.iterrows():
        log.info(f"  {row['rank']}. {row['category']}: mean crossover = {row['mean_crossover']}")

    return category_stats


def analyze_all(source: str = "gdelt"):
    """Run full category analysis."""
    ensure_dirs()

    # Adoption curves
    data_path = PROCESSED_DIR / f"{source}_merged.parquet"
    if data_path.exists():
        df = pd.read_parquet(data_path)
        curves = compute_category_adoption_curves(df)
        curves_path = PROCESSED_DIR / f"category_curves_{source}.parquet"
        curves.to_parquet(curves_path, index=False)
        log.info(f"Category curves saved: {curves_path}")

    # Crossover stats
    stats_df = compute_category_crossover_stats(source)
    if not stats_df.empty:
        stats_path = PROCESSED_DIR / f"category_stats_{source}.parquet"
        stats_df.to_parquet(stats_path, index=False)
        log.info(f"Category stats saved: {stats_path}")

    # Statistical tests
    test_results = test_category_differences(source)
    if test_results:
        log.info(f"Statistical tests: {test_results.get('kruskal_p', 'N/A')}")

    # Ranking
    ranking = rank_categories(source)

    return {
        "stats": stats_df,
        "tests": test_results,
        "ranking": ranking,
    }


def main():
    parser = argparse.ArgumentParser(description="Category-level adoption analysis")
    parser.add_argument("--source", type=str, default="gdelt", choices=["gdelt", "trends"])
    args = parser.parse_args()
    analyze_all(source=args.source)


if __name__ == "__main__":
    main()
