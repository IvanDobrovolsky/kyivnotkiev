"""Generate all publication-ready figures.

Usage:
    python scripts/run_viz.py [--source gdelt|trends] [--pair-ids 1,2,3]
"""

import argparse
import logging

import matplotlib

matplotlib.use("Agg")  # non-interactive backend for CLI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Generate all figures")
    parser.add_argument("--source", type=str, default="gdelt", choices=["gdelt", "trends"])
    parser.add_argument("--pair-ids", type=str, default=None)
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    source = args.source

    log.info(f"=== Crossover charts ({source}) ===")
    from src.viz.crossover import plot_all as crossover_all
    crossover_all(source=source, pair_ids=pair_ids)

    log.info(f"=== Heatmap ({source}) ===")
    from src.viz.heatmap import plot_heatmap
    plot_heatmap(source=source)

    log.info(f"=== Choropleth maps ({source}) ===")
    try:
        from src.viz.choropleth import plot_all as choropleth_all
        choropleth_all(source=source, pair_ids=pair_ids)
    except Exception as e:
        log.warning(f"Choropleth generation failed (non-critical): {e}")

    log.info(f"=== Category curves ({source}) ===")
    from src.viz.category_curves import plot_category_curves
    plot_category_curves(source=source)

    log.info(f"=== Event overlay charts ({source}) ===")
    from src.viz.event_overlay import plot_all as event_all
    event_all(source=source, pair_ids=pair_ids)

    log.info("=== All figures generated ===")


if __name__ == "__main__":
    main()
