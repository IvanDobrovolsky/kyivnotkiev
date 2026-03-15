"""Run all analysis steps.

Usage:
    python scripts/run_analysis.py [--source gdelt|trends] [--pair-ids 1,2,3]
"""

import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run analysis pipeline")
    parser.add_argument("--source", type=str, default="gdelt", choices=["gdelt", "trends"])
    parser.add_argument("--pair-ids", type=str, default=None)
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    source = args.source

    log.info(f"=== Change-point detection ({source}) ===")
    from src.analysis.changepoint import analyze_all as cp_analyze
    cp_analyze(source=source, pair_ids=pair_ids)

    log.info(f"=== Geographic diffusion ({source}) ===")
    from src.analysis.geographic import analyze_all as geo_analyze
    geo_analyze(source=source, pair_ids=pair_ids)

    log.info(f"=== Category analysis ({source}) ===")
    from src.analysis.categories import analyze_all as cat_analyze
    cat_analyze(source=source)

    log.info(f"=== Event correlation ({source}) ===")
    from src.analysis.events import analyze_all as event_analyze
    event_analyze(source=source, pair_ids=pair_ids)

    log.info("=== Analysis complete ===")


if __name__ == "__main__":
    main()
