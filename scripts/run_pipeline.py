"""End-to-end pipeline: collect -> preprocess -> analyze -> visualize.

Usage:
    python scripts/run_pipeline.py [--source gdelt|trends|all] [--skip-collect] [--skip-viz]
"""

import argparse
import logging
import time

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run full KyivNotKiev pipeline")
    parser.add_argument("--source", type=str, default="gdelt",
                        choices=["gdelt", "trends", "all"])
    parser.add_argument("--skip-collect", action="store_true",
                        help="Skip data collection (use existing raw data)")
    parser.add_argument("--skip-viz", action="store_true",
                        help="Skip visualization generation")
    parser.add_argument("--pair-ids", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    sources = ["gdelt", "trends"] if args.source == "all" else [args.source]
    start = time.time()

    # Step 1: Collection
    if not args.skip_collect:
        for source in sources:
            log.info(f"{'=' * 60}")
            log.info(f"STEP 1: DATA COLLECTION ({source.upper()})")
            log.info(f"{'=' * 60}")
            try:
                if source == "gdelt":
                    from src.pipeline.collect_gdelt import collect_all as collect_gdelt
                    collect_gdelt(pair_ids=pair_ids, dry_run=args.dry_run)
                elif source == "trends":
                    from src.pipeline.collect_trends import collect_all as collect_trends
                    collect_trends(pair_ids=pair_ids)
            except Exception as e:
                log.error(f"Collection failed for {source}: {e}")
                if source == "gdelt":
                    log.info("Continuing with Google Trends...")
                    continue
                raise

        # Also collect ngrams
        log.info(f"{'=' * 60}")
        log.info("STEP 1b: NGRAMS COLLECTION")
        log.info(f"{'=' * 60}")
        try:
            from src.pipeline.collect_ngrams import collect_all as collect_ngrams
            collect_ngrams(pair_ids=pair_ids)
        except Exception as e:
            log.warning(f"Ngrams collection failed (non-critical): {e}")

    # Step 2: Preprocessing
    log.info(f"{'=' * 60}")
    log.info("STEP 2: PREPROCESSING")
    log.info(f"{'=' * 60}")
    from src.pipeline.preprocess import preprocess_all
    preprocess_all()

    # Step 3: Analysis (per source)
    for source in sources:
        log.info(f"{'=' * 60}")
        log.info(f"STEP 3: ANALYSIS ({source.upper()})")
        log.info(f"{'=' * 60}")

        from src.analysis.changepoint import analyze_all as cp_analyze
        cp_analyze(source=source, pair_ids=pair_ids)

        from src.analysis.geographic import analyze_all as geo_analyze
        geo_analyze(source=source, pair_ids=pair_ids)

        from src.analysis.categories import analyze_all as cat_analyze
        cat_analyze(source=source)

        from src.analysis.events import analyze_all as event_analyze
        event_analyze(source=source, pair_ids=pair_ids)

    # Step 4: Visualization
    if not args.skip_viz:
        import matplotlib
        matplotlib.use("Agg")

        for source in sources:
            log.info(f"{'=' * 60}")
            log.info(f"STEP 4: VISUALIZATION ({source.upper()})")
            log.info(f"{'=' * 60}")

            from src.viz.crossover import plot_all as crossover_all
            crossover_all(source=source, pair_ids=pair_ids)

            from src.viz.heatmap import plot_heatmap
            plot_heatmap(source=source)

            from src.viz.choropleth import plot_all as choropleth_all
            choropleth_all(source=source, pair_ids=pair_ids)

            from src.viz.category_curves import plot_category_curves
            plot_category_curves(source=source)

            from src.viz.event_overlay import plot_all as event_overlay_all
            event_overlay_all(source=source, pair_ids=pair_ids)

    elapsed = time.time() - start
    log.info(f"{'=' * 60}")
    log.info(f"PIPELINE COMPLETE in {elapsed / 60:.1f} minutes")
    log.info(f"{'=' * 60}")


if __name__ == "__main__":
    main()
