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

    # Load world shapefile with country boundaries
    ne_url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
    world = gpd.read_file(ne_url)

    # Merge crossover data with world geometry
    pair_data = pair_data.copy()
    if "crossover_date" in pair_data.columns:
        pair_data["crossover_date"] = pd.to_datetime(pair_data["crossover_date"])
        pair_data["crossover_ordinal"] = pair_data["crossover_date"].apply(
            lambda x: x.toordinal() if pd.notna(x) else np.nan
        )

    # Compute latest adoption ratio per country from raw geographic data
    geo_raw_path = PROCESSED_DIR / f"{source}_geographic.parquet"
    if geo_raw_path.exists():
        geo_raw = pd.read_parquet(geo_raw_path)
        pair_geo = geo_raw[geo_raw["pair_id"] == pair_id]
        # Get latest 6 months of data per country
        if not pair_geo.empty:
            pair_geo = pair_geo.copy()
            pair_geo["week"] = pd.to_datetime(pair_geo["week"])
            cutoff = pair_geo["week"].max() - pd.Timedelta(days=180)
            recent = pair_geo[pair_geo["week"] >= cutoff]
            country_ratios = recent.groupby("source_country")["adoption_ratio"].mean().reset_index()
            country_ratios.columns = ["country_code", "adoption_ratio"]
        else:
            country_ratios = pd.DataFrame(columns=["country_code", "adoption_ratio"])
    else:
        country_ratios = pd.DataFrame(columns=["country_code", "adoption_ratio"])

    # Try multiple join keys (GDELT uses FIPS, NE uses ISO)
    merged = world.copy()
    merged["adoption_ratio"] = np.nan

    for _, cr in country_ratios.iterrows():
        cc = cr["country_code"]
        ratio = cr["adoption_ratio"]
        # Try matching on ISO_A2, ISO_A3, FIPS_10_, or ADM0_A3
        for col in ["ISO_A2", "ISO_A3", "FIPS_10_", "ADM0_A3", "ISO_A2_EH"]:
            if col in merged.columns:
                mask = merged[col] == cc
                if mask.any():
                    merged.loc[mask, "adoption_ratio"] = ratio
                    break

    fig, ax = plt.subplots(figsize=(18, 10))

    # Plot base map
    world.plot(ax=ax, color="#E0E0E0", edgecolor="#CCCCCC", linewidth=0.5)

    # Plot countries with data
    has_data = merged.dropna(subset=["adoption_ratio"])
    if not has_data.empty:
        cmap = mcolors.LinearSegmentedColormap.from_list(
            "adoption", ["#E74C3C", "#FFFFFF", "#0057B8"]
        )
        has_data.plot(
            ax=ax,
            column="adoption_ratio",
            cmap=cmap,
            edgecolor="#666666",
            linewidth=0.5,
            legend=True,
            vmin=0, vmax=1,
            legend_kwds={"label": "Adoption ratio (0=Russian, 1=Ukrainian)", "shrink": 0.6},
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
