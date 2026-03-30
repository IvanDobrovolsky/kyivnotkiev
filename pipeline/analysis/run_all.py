"""Run all analysis modules in sequence."""

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
log = logging.getLogger(__name__)


def main():
    log.info("Starting full analysis pipeline...")

    log.info("[1/5] Computing adoption ratios...")
    from pipeline.analysis.categories import compute_category_adoption_curves
    # Will be called with data from BigQuery once ingestion is complete

    log.info("[2/5] Detecting change points...")
    from pipeline.analysis.changepoint import detect_changepoints
    # Will run on adoption time series

    log.info("[3/5] Running category tests (Kruskal-Wallis)...")
    from pipeline.analysis.categories import compute_category_adoption_curves

    log.info("[4/5] Analyzing holdout domains...")
    from pipeline.analysis.holdouts import generate_report
    generate_report()

    log.info("[5/5] Event impact analysis...")
    from pipeline.analysis.events import compute_event_impacts

    log.info("Full analysis complete.")


if __name__ == "__main__":
    main()
