"""Toponym adoption heatmap: all pairs x time.

Color represents adoption ratio (0=fully Russian, 1=fully Ukrainian).
Pairs ordered by crossover date to show the wave of adoption.

Usage:
    python -m src.viz.heatmap [--source gdelt|trends]
"""

import argparse
import logging

import matplotlib.colors as mcolors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from src.config import (
    COLOR_RUSSIAN,
    COLOR_UKRAINIAN,
    FIGURES_DIR,
    PROCESSED_DIR,
    VIZ_DPI,
    VIZ_STYLE,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def build_heatmap_matrix(df: pd.DataFrame) -> tuple[np.ndarray, list[str], list[str]]:
    """Build the heatmap matrix: pairs (rows) x time (columns).

    Returns (matrix, pair_labels, time_labels).
    """
    pairs = [p for p in get_all_pairs() if not (p["is_control"] and p["russian"] == p["ukrainian"])]
    time_col = "week" if "week" in df.columns else "year"

    # Get all unique time periods
    all_times = sorted(df[time_col].unique())

    # Build matrix
    matrix = np.full((len(pairs), len(all_times)), np.nan)
    pair_labels = []

    for i, pair in enumerate(pairs):
        pair_data = df[df["pair_id"] == pair["id"]]
        if pair_data.empty:
            pair_labels.append(f"{pair['russian']} -> {pair['ukrainian']}")
            continue

        for _, row in pair_data.iterrows():
            t = row[time_col]
            if t in all_times:
                j = all_times.index(t)
                matrix[i, j] = row.get("adoption_ratio", np.nan)

        pair_labels.append(f"{pair['russian']} -> {pair['ukrainian']}")

    # Format time labels (show every Nth for readability)
    time_labels = [str(t)[:10] for t in all_times]

    return matrix, pair_labels, time_labels


def sort_by_crossover(matrix: np.ndarray, pair_labels: list[str]) -> tuple[np.ndarray, list[str]]:
    """Sort rows by the time column when adoption ratio first exceeds 0.5."""
    crossover_indices = []
    for i in range(matrix.shape[0]):
        row = matrix[i]
        crossed = np.where(row >= 0.5)[0]
        if len(crossed) > 0:
            crossover_indices.append(crossed[0])
        else:
            crossover_indices.append(matrix.shape[1])  # never crossed -> sort to end

    order = np.argsort(crossover_indices)
    return matrix[order], [pair_labels[i] for i in order]


def plot_heatmap(source: str = "gdelt", save: bool = True) -> plt.Figure:
    """Generate the toponym adoption heatmap."""
    ensure_dirs()
    plt.style.use(VIZ_STYLE)

    data_path = PROCESSED_DIR / f"{source}_merged.parquet"
    if not data_path.exists():
        log.error(f"Data not found: {data_path}")
        return None

    df = pd.read_parquet(data_path)

    # Aggregate to pair x week level
    time_col = "week" if "week" in df.columns else "year"
    df_agg = df.groupby(["pair_id", time_col]).agg(
        adoption_ratio=("adoption_ratio", "mean"),
    ).reset_index()

    matrix, pair_labels, time_labels = build_heatmap_matrix(df_agg)
    matrix, pair_labels = sort_by_crossover(matrix, pair_labels)

    # Custom colormap: red (Russian) -> white -> blue (Ukrainian)
    cmap = mcolors.LinearSegmentedColormap.from_list(
        "adoption", [COLOR_RUSSIAN, "#FFFFFF", COLOR_UKRAINIAN]
    )

    fig, ax = plt.subplots(figsize=(20, max(10, len(pair_labels) * 0.4)))

    im = ax.imshow(matrix, aspect="auto", cmap=cmap, vmin=0, vmax=1, interpolation="nearest")

    # Y-axis: pair labels
    ax.set_yticks(range(len(pair_labels)))
    ax.set_yticklabels(pair_labels, fontsize=8)

    # X-axis: show every 26th label (~6 months)
    step = max(1, len(time_labels) // 20)
    ax.set_xticks(range(0, len(time_labels), step))
    ax.set_xticklabels([time_labels[i] for i in range(0, len(time_labels), step)],
                       rotation=45, ha="right", fontsize=8)

    # Colorbar
    cbar = fig.colorbar(im, ax=ax, shrink=0.8, pad=0.02)
    cbar.set_label("Adoption ratio (0=Russian, 1=Ukrainian)", fontsize=10)

    ax.set_title(f"Ukrainian Toponym Adoption Heatmap — {source.upper()} (2015-2026)",
                fontsize=14, fontweight="bold")
    ax.set_xlabel("Time", fontsize=12)

    plt.tight_layout()

    if save:
        out_path = FIGURES_DIR / f"heatmap_{source}.png"
        fig.savefig(out_path, dpi=VIZ_DPI, bbox_inches="tight")
        log.info(f"Saved: {out_path}")

    return fig


def main():
    parser = argparse.ArgumentParser(description="Generate toponym adoption heatmap")
    parser.add_argument("--source", type=str, default="gdelt", choices=["gdelt", "trends"])
    args = parser.parse_args()
    plot_heatmap(source=args.source)


if __name__ == "__main__":
    main()
