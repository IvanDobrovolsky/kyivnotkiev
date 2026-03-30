"""Pipeline orchestrator: coordinates incremental ingestion across all sources.

The orchestrator reads pairs.yaml, checks watermarks, and only fetches
data for pairs that are new, stale, or failed. This is the main entry point
for `make ingest`.
"""

import argparse
import logging
import sys

from pipeline.config import get_enabled_pairs, get_pair_by_id, load_pipeline
from pipeline.ingestion.watermarks import is_stale, get_all_watermarks

logger = logging.getLogger(__name__)

SOURCES = ["gdelt", "trends", "ngrams", "wikipedia", "reddit", "youtube"]


def ingest_pair_all_sources(pair: dict, cfg: dict, force: bool = False):
    """Ingest a single pair across all sources, skipping fresh ones."""
    pair_id = pair["id"]
    staleness = cfg["pipeline"]["staleness_threshold_days"]

    for source in SOURCES:
        if not force and not is_stale(pair_id, source, max_age_days=staleness):
            logger.info(f"  [{source}] pair {pair_id} is fresh, skipping")
            continue

        logger.info(f"  [{source}] pair {pair_id}: ingesting...")
        try:
            _run_source(source, [pair_id], cfg)
        except Exception as e:
            logger.error(f"  [{source}] pair {pair_id}: FAILED — {e}")


def ingest_source_all_pairs(source: str, cfg: dict, force: bool = False):
    """Ingest all enabled pairs for a single source."""
    pairs = get_enabled_pairs()
    staleness = cfg["pipeline"]["staleness_threshold_days"]

    stale_pairs = []
    for pair in pairs:
        if force or is_stale(pair["id"], source, max_age_days=staleness):
            stale_pairs.append(pair["id"])

    if not stale_pairs:
        logger.info(f"[{source}] All pairs are fresh, nothing to do")
        return

    logger.info(f"[{source}] {len(stale_pairs)} pairs to ingest")
    _run_source(source, stale_pairs, cfg)


def _run_source(source: str, pair_ids: list[int], cfg: dict):
    """Dispatch to the appropriate source ingestion module."""
    if source == "gdelt":
        from pipeline.ingestion.gdelt import run
        run(pair_ids)
    elif source == "trends":
        from pipeline.ingestion.trends import run
        run(pair_ids)
    elif source == "ngrams":
        from pipeline.ingestion.ngrams import run
        run(pair_ids)
    elif source == "wikipedia":
        from pipeline.ingestion.wikipedia import run
        run(pair_ids)
    elif source == "reddit":
        from pipeline.ingestion.reddit import run
        run(pair_ids)
    elif source == "youtube":
        from pipeline.ingestion.youtube import run
        run(pair_ids)
    else:
        logger.warning(f"Unknown source: {source}")


def show_status():
    """Display current watermark status for all pairs and sources."""
    watermarks = get_all_watermarks()
    if not watermarks:
        print("No watermarks found — pipeline has not been run yet.")
        return

    print(f"{'Source':<12} {'Pair':<6} {'Last Fetched':<22} {'Rows':<10} {'Status'}")
    print("-" * 70)
    for wm in watermarks:
        print(f"{wm['source']:<12} {wm['pair_id']:<6} "
              f"{wm['last_fetched'].strftime('%Y-%m-%d %H:%M'):<22} "
              f"{wm['row_count']:<10} {wm['status']}")


def main():
    parser = argparse.ArgumentParser(description="KyivNotKiev pipeline orchestrator")
    parser.add_argument("--all", action="store_true", help="Ingest all enabled pairs, all sources")
    parser.add_argument("--pair-id", type=int, help="Ingest a single pair across all sources")
    parser.add_argument("--source", type=str, help="Ingest all pairs for a single source")
    parser.add_argument("--force", action="store_true", help="Ignore watermarks, force re-fetch")
    parser.add_argument("--status", action="store_true", help="Show pipeline status")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

    if args.status:
        show_status()
        return

    cfg = load_pipeline()

    if args.pair_id:
        pair = get_pair_by_id(args.pair_id)
        if not pair:
            logger.error(f"Pair {args.pair_id} not found in pairs.yaml")
            sys.exit(1)
        logger.info(f"Ingesting pair {args.pair_id}: {pair['russian']} → {pair['ukrainian']}")
        ingest_pair_all_sources(pair, cfg, force=args.force)

    elif args.source:
        if args.source not in SOURCES:
            logger.error(f"Unknown source: {args.source}. Valid: {SOURCES}")
            sys.exit(1)
        ingest_source_all_pairs(args.source, cfg, force=args.force)

    elif args.all:
        logger.info("Full incremental ingestion: all sources, all enabled pairs")
        for source in SOURCES:
            ingest_source_all_pairs(source, cfg, force=args.force)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
