"""CL pipeline orchestrator — run individual steps or the full pipeline.

Usage:
    python -m pipeline.cl.run --step extract      # Phase 1: Extract texts
    python -m pipeline.cl.run --step balance       # Phase 1: Balance corpus
    python -m pipeline.cl.run --step classify      # Phase 2: Context + sentiment
    python -m pipeline.cl.run --step embed         # Phase 4: Embeddings + collocations
    python -m pipeline.cl.run --step finetune      # Phase 3: Train encoders
    python -m pipeline.cl.run --step evaluate      # Phase 3: Ablation studies
    python -m pipeline.cl.run --step export        # Phase 5: HF + site JSON
    python -m pipeline.cl.run --step all           # Everything
"""

import argparse
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

STEPS = ["extract", "balance", "classify", "embed", "finetune", "evaluate", "export", "all"]


def run_extract():
    log.info("=" * 60)
    log.info("STEP 1: EXTRACT TEXTS")
    log.info("=" * 60)

    from pipeline.cl.extract.reddit_texts import extract_reddit
    from pipeline.cl.extract.youtube_texts import extract_youtube

    extract_reddit()
    extract_youtube()

    # GDELT is optional and slow — run separately
    log.info("Note: GDELT article fetching is slow. Run separately:")
    log.info("  python -m pipeline.cl.extract.gdelt_articles [--pair-ids 1,3,10]")


def run_balance():
    log.info("=" * 60)
    log.info("STEP 2: BALANCE CORPUS")
    log.info("=" * 60)

    from pipeline.cl.balance.sampler import run_balancing
    run_balancing()


def run_classify(api_url=None, mode="llm"):
    log.info("=" * 60)
    log.info("STEP 3: CLASSIFY (CONTEXT + SENTIMENT)")
    log.info("=" * 60)

    from pipeline.cl.classify.context import run_classification
    from pipeline.cl.classify.sentiment import run_sentiment

    run_classification(mode=mode, api_url=api_url)
    run_sentiment(mode=mode, api_url=api_url)


def run_embed():
    log.info("=" * 60)
    log.info("STEP 4: EMBEDDINGS + COLLOCATIONS")
    log.info("=" * 60)

    from pipeline.cl.embeddings.collocations import run_collocations
    from pipeline.cl.embeddings.sentence import run_embeddings

    run_collocations()
    run_embeddings()


def run_finetune(model_keys=None, epochs=3):
    log.info("=" * 60)
    log.info("STEP 5: FINE-TUNE ENCODERS")
    log.info("=" * 60)

    from pipeline.cl.finetune.train import run_training
    run_training(model_keys=model_keys, epochs=epochs)


def run_evaluate(model_key="deberta-v3-large"):
    log.info("=" * 60)
    log.info("STEP 6: EVALUATE + ABLATION")
    log.info("=" * 60)

    from pipeline.cl.finetune.evaluate import run_evaluation
    for key in ["deberta-v3-large", "xlm-roberta-large", "mdeberta-v3-base"]:
        try:
            run_evaluation(key)
        except FileNotFoundError:
            log.warning(f"Model {key} not found, skipping evaluation")


def run_export(hf_dataset_repo=None, hf_model_repo=None):
    log.info("=" * 60)
    log.info("STEP 7: EXPORT")
    log.info("=" * 60)

    from pipeline.cl.export.hf_dataset import export_dataset
    from pipeline.cl.export.site_json import export_site_json

    export_dataset(repo_id=hf_dataset_repo)
    export_site_json()

    if hf_model_repo:
        from pipeline.cl.export.hf_model import push_model
        push_model("deberta-v3-large", hf_model_repo)


def main():
    parser = argparse.ArgumentParser(description="CL Pipeline Orchestrator")
    parser.add_argument("--step", choices=STEPS, required=True)
    parser.add_argument("--api-url", type=str, default=None,
                        help="vLLM API URL for LLM annotation")
    parser.add_argument("--mode", choices=["llm", "zero-shot", "encoder"],
                        default="llm", help="Classification mode")
    parser.add_argument("--model", type=str, default=None,
                        help="Specific encoder to train/evaluate")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--hf-dataset", type=str, default=None,
                        help="HF dataset repo for publishing")
    parser.add_argument("--hf-model", type=str, default=None,
                        help="HF model repo for publishing")
    args = parser.parse_args()

    if args.step == "extract":
        run_extract()
    elif args.step == "balance":
        run_balance()
    elif args.step == "classify":
        run_classify(api_url=args.api_url, mode=args.mode)
    elif args.step == "embed":
        run_embed()
    elif args.step == "finetune":
        model_keys = [args.model] if args.model else None
        run_finetune(model_keys=model_keys, epochs=args.epochs)
    elif args.step == "evaluate":
        run_evaluate()
    elif args.step == "export":
        run_export(hf_dataset_repo=args.hf_dataset, hf_model_repo=args.hf_model)
    elif args.step == "all":
        run_extract()
        run_balance()
        if args.api_url:
            run_classify(api_url=args.api_url, mode=args.mode)
        else:
            log.warning("No --api-url provided, skipping LLM classification")
            log.warning("Use: python -m pipeline.cl.run --step classify --api-url http://...")
        run_embed()
        # Fine-tuning requires classification to be done first
        if args.api_url:
            run_finetune(epochs=args.epochs)
            run_evaluate()
        run_export(hf_dataset_repo=args.hf_dataset, hf_model_repo=args.hf_model)


if __name__ == "__main__":
    main()
