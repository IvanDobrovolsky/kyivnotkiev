"""Geographic choropleth maps showing adoption diffusion by country.

Usage:
    python -m src.viz.choropleth [--source gdelt|trends] [--pair-ids 1,2,3]
"""

import argparse
import logging

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import numpy as np
import pandas as pd

from src.config import (
    PROCESSED_DIR,
    FIGURES_DIR,
    VIZ_DPI,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Try geopandas; fall back gracefully
try:
    import geopandas as gpd
    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False
    log.warning("geopandas not installed; choropleth maps will be skipped")


def plot_choropleth(
    pair_id: int,
    source: str = "gdelt",
    save: bool = True,
) -> plt.Figure | None:
    """Plot a choropleth map showing crossover date by country for a pair."""
    if not HAS_GEOPANDAS:
        log.error("geopandas required for choropleth maps")
        return None

    ensure_dirs()

    geo_path = PROCESSED_DIR / f"geographic_{source}.parquet"
    if not geo_path.exists():
        log.error(f"Geographic data not found: {geo_path}")
        return None

    df = pd.read_parquet(geo_path)
    pair_data = df[df["pair_id"] == pair_id]

    if pair_data.empty:
        log.warning(f"No geographic data for pair {pair_id}")
        return None

    pairs = get_all_pairs()
    pair = next((p for p in pairs if p["id"] == pair_id), None)
    if pair is None:
        return None

    # Load world shapefile from geopandas datasets
    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))

    # Merge crossover data with world geometry
    pair_data = pair_data.copy()
    if "crossover_date" in pair_data.columns:
        pair_data["crossover_date"] = pd.to_datetime(pair_data["crossover_date"])
        pair_data["crossover_ordinal"] = pair_data["crossover_date"].apply(
            lambda x: x.toordinal() if pd.notna(x) else np.nan
        )

    merged = world.merge(pair_data, left_on="iso_a3", right_on="country", how="left")

    # Color: early adopters = green, late = red, no data = gray
    fig, ax = plt.subplots(figsize=(18, 10))

    # Plot base map
    world.plot(ax=ax, color="#E0E0E0", edgecolor="#CCCCCC", linewidth=0.5)

    # Plot countries with data
    if "crossover_ordinal" in merged.columns:
        has_data = merged.dropna(subset=["crossover_ordinal"])
        no_cross = merged[merged["has_crossed"] == False] if "has_crossed" in merged.columns else gpd.GeoDataFrame()

        if not has_data.empty:
            cmap = mcolors.LinearSegmentedColormap.from_list("adoption", ["#228B22", "#FFD700", "#DC143C"])
            has_data.plot(
                ax=ax,
                column="crossover_ordinal",
                cmap=cmap,
                edgecolor="#666666",
                linewidth=0.5,
                legend=True,
                legend_kwds={"label": "Crossover date (earlier = greener)", "shrink": 0.6},
            )

        if not no_cross.empty:
            no_cross.plot(ax=ax, color="#FF6B6B", edgecolor="#666666", linewidth=0.5, alpha=0.5)
    else:
        # Fall back to adoption_ratio_current
        has_data = merged.dropna(subset=["adoption_ratio_current"])
        if not has_data.empty:
            has_data.plot(
                ax=ax,
                column="adoption_ratio_current",
                cmap="RdYlGn",
                edgecolor="#666666",
                linewidth=0.5,
                legend=True,
                vmin=0, vmax=1,
                legend_kwds={"label": "Current adoption ratio", "shrink": 0.6},
            )

    ax.set_title(
        f'Geographic Adoption: "{pair["russian"]}" -> "{pair["ukrainian"]}" ({source.upper()})',
        fontsize=14, fontweight="bold",
    )
    ax.set_axis_off()

    plt.tight_layout()

    if save:
        out_path = FIGURES_DIR / f"choropleth_{pair_id:02d}_{source}.png"
        fig.savefig(out_path, dpi=VIZ_DPI, bbox_inches="tight")
        log.info(f"Saved: {out_path}")

    return fig


def plot_all(source: str = "gdelt", pair_ids: list[int] | None = None):
    """Generate choropleth maps for selected pairs."""
    if not HAS_GEOPANDAS:
        return

    # Default to top 5 pairs
    if pair_ids is None:
        pair_ids = [1, 2, 3, 4, 5]

    for pid in pair_ids:
        fig = plot_choropleth(pid, source=source)
        if fig:
            plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Generate geographic choropleth maps")
    parser.add_argument("--source", type=str, default="gdelt", choices=["gdelt", "trends"])
    parser.add_argument("--pair-ids", type=str, default=None)
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    plot_all(source=args.source, pair_ids=pair_ids)


if __name__ == "__main__":
    main()
