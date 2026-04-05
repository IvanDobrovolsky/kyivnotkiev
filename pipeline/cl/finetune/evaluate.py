"""Evaluation and ablation studies for fine-tuned models.

Generates:
- Per-category performance breakdown
- Per-pair performance (do some pairs confuse the model?)
- Cross-lingual transfer analysis
- Confusion matrices
- Error analysis (what does the model get wrong?)

Usage:
    python -m pipeline.cl.finetune.evaluate [--model deberta-v3-large]
"""

import argparse
import json
import logging

import numpy as np
import pandas as pd

from pipeline.cl.config import (
    CL_CLASSIFIED_DIR, CL_MODEL_DIR, CONTEXT_LABELS, ENCODER_MODELS,
    ensure_cl_dirs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_model_and_predict(model_key, texts):
    """Load fine-tuned model and predict on texts."""
    from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

    model_path = CL_MODEL_DIR / model_key / "best"
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found at {model_path}")

    classifier = pipeline(
        "text-classification",
        model=str(model_path),
        tokenizer=str(model_path),
        device=-1,
        top_k=None,
        truncation=True,
        max_length=512,
    )

    results = []
    batch_size = 32
    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        outputs = classifier(batch)
        for out in outputs:
            best = max(out, key=lambda x: x["score"])
            results.append({
                "predicted_label": best["label"],
                "predicted_score": best["score"],
                "all_scores": {s["label"]: s["score"] for s in out},
            })

    return results


def ablation_by_pair(df, predictions):
    """Performance breakdown per pair."""
    from sklearn.metrics import f1_score

    df = df.copy()
    df["predicted"] = [p["predicted_label"] for p in predictions]

    results = {}
    for pair_id in sorted(df["pair_id"].unique()):
        pdf = df[df["pair_id"] == pair_id]
        if len(pdf) < 10:
            continue

        f1 = f1_score(
            pdf["context_label"], pdf["predicted"],
            labels=CONTEXT_LABELS, average="macro", zero_division=0,
        )
        accuracy = (pdf["context_label"] == pdf["predicted"]).mean()

        results[int(pair_id)] = {
            "n_texts": len(pdf),
            "accuracy": float(accuracy),
            "f1_macro": float(f1),
        }

    return results


def ablation_by_source(df, predictions):
    """Performance breakdown per source."""
    from sklearn.metrics import f1_score

    df = df.copy()
    df["predicted"] = [p["predicted_label"] for p in predictions]

    results = {}
    for source in df["source"].unique():
        sdf = df[df["source"] == source]
        f1 = f1_score(
            sdf["context_label"], sdf["predicted"],
            labels=CONTEXT_LABELS, average="macro", zero_division=0,
        )
        results[source] = {
            "n_texts": len(sdf),
            "accuracy": float((sdf["context_label"] == sdf["predicted"]).mean()),
            "f1_macro": float(f1),
        }

    return results


def error_analysis(df, predictions, n_examples=20):
    """Identify systematic errors."""
    df = df.copy()
    df["predicted"] = [p["predicted_label"] for p in predictions]
    df["correct"] = df["context_label"] == df["predicted"]

    errors = df[~df["correct"]].copy()
    confusion_pairs = errors.groupby(["context_label", "predicted"]).size().sort_values(ascending=False)

    top_confusions = []
    for (true_label, pred_label), count in confusion_pairs.head(10).items():
        examples = errors[
            (errors["context_label"] == true_label) & (errors["predicted"] == pred_label)
        ]["text"].str[:100].tolist()[:3]

        top_confusions.append({
            "true_label": true_label,
            "predicted_label": pred_label,
            "count": int(count),
            "examples": examples,
        })

    return top_confusions


def run_evaluation(model_key):
    ensure_cl_dirs()

    corpus_path = CL_CLASSIFIED_DIR / "corpus_labeled.parquet"
    df = pd.read_parquet(corpus_path)
    df = df[df["context_label"].isin(CONTEXT_LABELS)].copy()

    texts = df["text"].fillna("").str[:512].tolist()
    log.info(f"Evaluating {model_key} on {len(texts)} texts")

    predictions = load_model_and_predict(model_key, texts)

    # Ablations
    pair_results = ablation_by_pair(df, predictions)
    source_results = ablation_by_source(df, predictions)
    errors = error_analysis(df, predictions)

    eval_output = {
        "model_key": model_key,
        "total_texts": len(df),
        "by_pair": pair_results,
        "by_source": source_results,
        "top_confusions": errors,
    }

    out_path = CL_MODEL_DIR / model_key / "evaluation.json"
    with open(out_path, "w") as f:
        json.dump(eval_output, f, indent=2, default=str)
    log.info(f"Evaluation saved: {out_path}")

    return eval_output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, required=True)
    args = parser.parse_args()
    run_evaluation(args.model)


if __name__ == "__main__":
    main()
