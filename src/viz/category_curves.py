"""Category adoption curves: one line per category showing mean adoption over time.

Usage:
    python -m src.viz.category_curves [--source gdelt|trends]
"""

import argparse
import logging

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

from src.config import (
    PROCESSED_DIR,
    FIGURES_DIR,
    VIZ_DPI,
    VIZ_FIGSIZE,
    VIZ_STYLE,
    ensure_dirs,
    get_categories,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CATEGORY_COLORS = {
    "geographical": "#0057B8",
    "food": "#E67E22",
    "landmarks": "#8E44AD",
    "country": "#2ECC71",
    "institutional": "#3498DB",
    "sports": "#E74C3C",
    "historical": "#95A5A6",
}


def plot_category_curves(source: str = "gdelt", save: bool = True) -> plt.Figure:
    """Plot mean adoption ratio over time for each category."""
    ensure_dirs()
    plt.style.use(VIZ_STYLE)

    curves_path = PROCESSED_DIR / f"category_curves_{source}.parquet"
    if not curves_path.exists():
        log.error(f"Category curves not found: {curves_path}")
        return None

    df = pd.read_parquet(curves_path)
    time_col = "week" if "week" in df.columns else "year"
    df[time_col] = pd.to_datetime(df[time_col])

    categories = get_categories()
    cat_names = {c["id"]: c["name"] for c in categories}

    fig, ax = plt.subplots(figsize=VIZ_FIGSIZE)

    for cat_id, cat_name in cat_names.items():
        cat_data = df[df["category"] == cat_id].sort_values(time_col)
        if cat_data.empty:
            continue

        color = CATEGORY_COLORS.get(cat_id, "#666666")

        # Smooth with rolling mean
        smoothed = cat_data["adoption_ratio_mean"].rolling(window=4, min_periods=1).mean()

        ax.plot(cat_data[time_col], smoothed,
                color=color, linewidth=2, label=cat_name, alpha=0.9)

        # Confidence band
        if "adoption_ratio_std" in cat_data.columns:
            std = cat_data["adoption_ratio_std"].rolling(window=4, min_periods=1).mean()
            ax.fill_between(
                cat_data[time_col],
                smoothed - std,
                smoothed + std,
                color=color, alpha=0.1,
            )

    ax.axhline(0.5, color="#666666", linestyle=":", alpha=0.5, label="50% crossover")

    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel("Mean adoption ratio", fontsize=12)
    ax.set_title(f"Adoption by Category — {source.upper()} (2015-2026)",
                fontsize=14, fontweight="bold")
    ax.legend(fontsize=10, loc="upper left")
    ax.set_ylim(-0.05, 1.05)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if save:
        out_path = FIGURES_DIR / f"category_curves_{source}.png"
        fig.savefig(out_path, dpi=VIZ_DPI, bbox_inches="tight")
        log.info(f"Saved: {out_path}")

    return fig


def main():
    parser = argparse.ArgumentParser(description="Generate category adoption curves")
    parser.add_argument("--source", type=str, default="gdelt", choices=["gdelt", "trends"])
    args = parser.parse_args()
    plot_category_curves(source=args.source)


if __name__ == "__main__":
    main()
