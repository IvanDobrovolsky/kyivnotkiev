"""Run all data collection steps.

Usage:
    python scripts/run_collect.py [--source gdelt|trends|ngrams|all] [--pair-ids 1,2,3] [--dry-run]
"""

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="Run data collection pipeline")
    parser.add_argument("--source", type=str, default="all",
                        choices=["gdelt", "trends", "ngrams", "all"])
    parser.add_argument("--pair-ids", type=str, default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    sources = ["gdelt", "trends", "ngrams"] if args.source == "all" else [args.source]

    for source in sources:
        log.info(f"=== Collecting: {source} ===")
        try:
            if source == "gdelt":
                from src.pipeline.collect_gdelt import collect_all
                collect_all(pair_ids=pair_ids, dry_run=args.dry_run)
            elif source == "trends":
                from src.pipeline.collect_trends import collect_all
                collect_all(pair_ids=pair_ids)
            elif source == "ngrams":
                from src.pipeline.collect_ngrams import collect_all
                collect_all(pair_ids=pair_ids)
        except Exception as e:
            log.error(f"Collection failed for {source}: {e}")
            if args.dry_run:
                raise
            continue

    log.info("=== Collection complete ===")

    # Preprocess
    log.info("=== Preprocessing ===")
    from src.pipeline.preprocess import preprocess_all
    preprocess_all(sources=sources)

    log.info("=== Done ===")


if __name__ == "__main__":
    main()
