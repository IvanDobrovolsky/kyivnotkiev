"""Run all analysis steps in order.

Usage:
    python -m pipeline.analysis.run_all [--step classify_language|cluster|all]
"""

import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

STEPS = [
    "classify_language",
    "cluster",
]


def run_classify_language():
    log.info("=== Step: classify_language ===")
    from pipeline.analysis.classify_language import classify_telegram, classify_corpus
    classify_telegram()
    classify_corpus()


def run_cluster():
    log.info("=== Step: cluster (all pairs) ===")
    from pipeline.analysis.embedding_clusters_kyiv import main as cluster_main
    cluster_main()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", default="all", choices=STEPS + ["all"])
    args = parser.parse_args()

    if args.step == "all":
        for step in STEPS:
            globals()[f"run_{step}"]()
    else:
        globals()[f"run_{args.step}"]()


if __name__ == "__main__":
    main()
