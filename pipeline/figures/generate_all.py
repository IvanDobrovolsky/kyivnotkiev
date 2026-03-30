"""Generate all publication figures from BigQuery data."""

import logging
from pathlib import Path

from pipeline.config import FIGURES_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
log = logging.getLogger(__name__)


def main():
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"Generating figures to {FIGURES_DIR}")

    log.info("[1/6] Crossover plots (per pair, per source)...")
    from pipeline.figures.crossover import generate_crossover_plots
    generate_crossover_plots()

    log.info("[2/6] Category curves...")
    from pipeline.figures.category_curves import generate_category_curves
    generate_category_curves()

    log.info("[3/6] Heatmaps...")
    from pipeline.figures.heatmap import generate_heatmaps
    generate_heatmaps()

    log.info("[4/6] Event overlays...")
    from pipeline.figures.event_overlay import generate_event_overlays
    generate_event_overlays()

    log.info("[5/6] Choropleth maps...")
    from pipeline.figures.choropleth import generate_choropleths
    generate_choropleths()

    log.info("[6/6] Modern dashboard figures...")
    from pipeline.figures.modern import generate_modern_figures
    generate_modern_figures()

    log.info(f"All figures saved to {FIGURES_DIR}")


if __name__ == "__main__":
    main()
