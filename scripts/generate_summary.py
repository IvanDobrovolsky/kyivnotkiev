"""Generate cross-source summary table for the paper.

Combines GDELT, Google Trends, and Ngrams findings into a unified
comparison table showing adoption status per pair across all sources.

Usage:
    python scripts/generate_summary.py
"""

import json
import logging

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.config import FIGURES_DIR, PROCESSED_DIR, ensure_dirs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_config():
    with open("data/toponym_pairs.json") as f:
        return json.load(f)


def get_latest_ratio(df, pair_id, n_weeks=8):
    """Get mean adoption ratio from the last n weeks of data."""
    pair_data = df[df["pair_id"] == pair_id].sort_values("week" if "week" in df.columns else "year")
    if pair_data.empty:
        return np.nan
    return pair_data.tail(n_weeks)["adoption_ratio"].mean()


def get_crossover_date(cp_df, pair_id):
    """Get crossover date from change-point results."""
    row = cp_df[cp_df["pair_id"] == pair_id]
    if row.empty:
        return None
    val = row.iloc[0].get("crossover_date")
    if pd.isna(val):
        return None
    return str(val)[:10]


def main():
    ensure_dirs()
    config = load_config()

    # Load all processed data
    sources = {}
    for name in ["gdelt_merged", "trends_merged", "ngrams_merged"]:
        path = PROCESSED_DIR / f"{name}.parquet"
        if path.exists():
            sources[name] = pd.read_parquet(path)
            log.info(f"Loaded {name}: {len(sources[name])} rows")

    # Load change-point results
    cp_sources = {}
    for name in ["gdelt", "trends"]:
        path = PROCESSED_DIR / f"changepoints_{name}.parquet"
        if path.exists():
            cp_sources[name] = pd.read_parquet(path)

    # Build summary table
    rows = []
    for pair in config["pairs"]:
        pid = pair["id"]
        if pair["is_control"] and pair["russian"] == pair["ukrainian"]:
            continue

        row = {
            "id": pid,
            "russian": pair["russian"],
            "ukrainian": pair["ukrainian"],
            "category": pair["category"],
        }

        # GDELT latest ratio
        if "gdelt_merged" in sources:
            row["gdelt_ratio"] = get_latest_ratio(sources["gdelt_merged"], pid)

        # Trends latest ratio
        if "trends_merged" in sources:
            row["trends_ratio"] = get_latest_ratio(sources["trends_merged"], pid)

        # Ngrams latest ratio (use last 5 years)
        if "ngrams_merged" in sources:
            ngrams = sources["ngrams_merged"]
            pair_ng = ngrams[ngrams["pair_id"] == pid]
            if not pair_ng.empty and "year" in pair_ng.columns:
                recent = pair_ng[pair_ng["year"] >= 2018]
                if not recent.empty:
                    row["ngrams_ratio"] = recent["adoption_ratio"].mean()
                else:
                    row["ngrams_ratio"] = np.nan
            else:
                row["ngrams_ratio"] = np.nan

        # Crossover dates
        if "gdelt" in cp_sources:
            row["gdelt_crossover"] = get_crossover_date(cp_sources["gdelt"], pid)
        if "trends" in cp_sources:
            row["trends_crossover"] = get_crossover_date(cp_sources["trends"], pid)

        # Overall adoption status
        ratios = [row.get("gdelt_ratio", np.nan), row.get("trends_ratio", np.nan)]
        valid = [r for r in ratios if not np.isnan(r)]
        if valid:
            mean_ratio = np.mean(valid)
            if mean_ratio >= 0.8:
                row["status"] = "Adopted"
            elif mean_ratio >= 0.5:
                row["status"] = "Crossing"
            elif mean_ratio >= 0.2:
                row["status"] = "Emerging"
            else:
                row["status"] = "Resistant"
        else:
            row["status"] = "No data"

        rows.append(row)

    summary = pd.DataFrame(rows)

    # Save as CSV and parquet
    csv_path = PROCESSED_DIR / "cross_source_summary.csv"
    summary.to_csv(csv_path, index=False)
    log.info(f"Saved: {csv_path}")

    parquet_path = PROCESSED_DIR / "cross_source_summary.parquet"
    summary.to_parquet(parquet_path, index=False)

    # Print formatted table
    log.info("\n" + "=" * 120)
    log.info("CROSS-SOURCE ADOPTION SUMMARY")
    log.info("=" * 120)
    log.info(f"{'ID':>3} {'Russian':<25} {'Ukrainian':<20} {'Category':<15} "
             f"{'GDELT':>6} {'Trends':>7} {'Ngrams':>7} {'Status':<10} {'Crossover (GDELT)':<20}")
    log.info("-" * 120)

    for _, r in summary.iterrows():
        log.info(
            f"{r['id']:3d} {r['russian']:<25} {r['ukrainian']:<20} {r['category']:<15} "
            f"{r.get('gdelt_ratio', float('nan')):6.3f} "
            f"{r.get('trends_ratio', float('nan')):7.3f} "
            f"{r.get('ngrams_ratio', float('nan')):7.3f} "
            f"{r['status']:<10} "
            f"{str(r.get('gdelt_crossover', 'N/A') or 'N/A'):<20}"
        )

    # Category summary
    log.info("\n" + "=" * 80)
    log.info("CATEGORY SUMMARY")
    log.info("=" * 80)

    for cat in summary["category"].unique():
        cat_data = summary[summary["category"] == cat]
        gdelt_mean = cat_data["gdelt_ratio"].mean()
        trends_mean = cat_data["trends_ratio"].mean()
        n_adopted = (cat_data["status"] == "Adopted").sum()
        n_total = len(cat_data)
        log.info(f"  {cat:<20} GDELT={gdelt_mean:.3f}  Trends={trends_mean:.3f}  "
                 f"Adopted: {n_adopted}/{n_total}")

    # Generate cross-source comparison chart
    fig, axes = plt.subplots(1, 2, figsize=(18, 10))

    # Left: GDELT vs Trends scatter
    ax = axes[0]
    valid = summary.dropna(subset=["gdelt_ratio", "trends_ratio"])
    colors = {"geographical": "#0057B8", "food": "#E67E22", "landmarks": "#8E44AD",
              "country": "#2ECC71", "institutional": "#3498DB", "sports": "#E74C3C",
              "historical": "#95A5A6"}

    for cat, color in colors.items():
        cat_data = valid[valid["category"] == cat]
        if not cat_data.empty:
            ax.scatter(cat_data["gdelt_ratio"], cat_data["trends_ratio"],
                      c=color, s=80, label=cat, alpha=0.8, edgecolors="black", linewidth=0.5)
            for _, r in cat_data.iterrows():
                ax.annotate(r["ukrainian"], (r["gdelt_ratio"], r["trends_ratio"]),
                           fontsize=6, alpha=0.7, ha="left")

    ax.plot([0, 1], [0, 1], "k--", alpha=0.3)
    ax.set_xlabel("GDELT adoption ratio", fontsize=12)
    ax.set_ylabel("Google Trends adoption ratio", fontsize=12)
    ax.set_title("Cross-Source Validation: GDELT vs Trends", fontsize=13, fontweight="bold")
    ax.legend(fontsize=9)
    ax.set_xlim(-0.05, 1.05)
    ax.set_ylim(-0.05, 1.05)
    ax.grid(True, alpha=0.3)

    # Right: Category bar chart
    ax = axes[1]
    cats = summary.groupby("category").agg(
        gdelt=("gdelt_ratio", "mean"),
        trends=("trends_ratio", "mean"),
    ).reindex(colors.keys()).dropna(how="all")

    x = range(len(cats))
    width = 0.35
    ax.bar([i - width/2 for i in x], cats["gdelt"], width, label="GDELT", color="#0057B8", alpha=0.8)
    ax.bar([i + width/2 for i in x], cats["trends"], width, label="Trends", color="#FFD700", alpha=0.8)
    ax.set_xticks(list(x))
    ax.set_xticklabels(cats.index, rotation=30, ha="right", fontsize=10)
    ax.set_ylabel("Mean adoption ratio", fontsize=12)
    ax.set_title("Adoption by Category & Source", fontsize=13, fontweight="bold")
    ax.legend(fontsize=10)
    ax.axhline(0.5, color="gray", linestyle=":", alpha=0.5)
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    fig_path = FIGURES_DIR / "cross_source_comparison.png"
    fig.savefig(fig_path, dpi=300, bbox_inches="tight")
    log.info(f"\nSaved: {fig_path}")
    plt.close(fig)


if __name__ == "__main__":
    main()
