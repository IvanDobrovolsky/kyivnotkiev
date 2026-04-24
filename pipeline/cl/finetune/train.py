"""Fine-tune encoder models on LLM-annotated context labels.

Trains multiple encoders on the same labeled data for benchmarking:
  - DeBERTa-v3-large
  - XLM-RoBERTa-large
  - mDeBERTa-v3-base (lightweight baseline)

Usage:
    python -m pipeline.cl.finetune.train [--model deberta-v3-large] [--epochs 3]
"""

import argparse
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from pipeline.cl.config import (
    CL_CLASSIFIED_DIR, CL_MODEL_DIR, CONTEXT_LABELS, ENCODER_MODELS,
    ensure_cl_dirs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def prepare_dataset(df):
    """Prepare train/val/test splits from labeled corpus."""
    # Filter to valid context labels (11 classes including religion)
    df = df[df["context_label"].isin(CONTEXT_LABELS)].copy()

    # Encode labels
    label2id = {label: i for i, label in enumerate(CONTEXT_LABELS)}
    df["label"] = df["context_label"].map(label2id)

    # Stratified split: 80/10/10
    train_df, temp_df = train_test_split(
        df, test_size=0.2, stratify=df["label"], random_state=42,
    )
    val_df, test_df = train_test_split(
        temp_df, test_size=0.5, stratify=temp_df["label"], random_state=42,
    )

    log.info(f"Splits: train={len(train_df)}, val={len(val_df)}, test={len(test_df)}")
    log.info(f"Label distribution (train):\n{train_df['context_label'].value_counts()}")

    return train_df, val_df, test_df, label2id


def train_encoder(model_key, train_df, val_df, test_df, label2id, epochs=3, batch_size=16):
    """Fine-tune a single encoder model."""
    from datasets import Dataset
    from transformers import (
        AutoModelForSequenceClassification,
        AutoTokenizer,
        Trainer,
        TrainingArguments,
    )
    from sklearn.metrics import classification_report, f1_score

    model_name = ENCODER_MODELS[model_key]
    output_dir = CL_MODEL_DIR / model_key
    output_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"\nTraining {model_key} ({model_name})")

    id2label = {v: k for k, v in label2id.items()}
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(
        model_name,
        num_labels=len(CONTEXT_LABELS),
        id2label=id2label,
        label2id=label2id,
    )

    def tokenize_fn(examples):
        return tokenizer(
            examples["text"], truncation=True, max_length=512, padding="max_length",
        )

    train_ds = Dataset.from_pandas(train_df[["text", "label"]]).map(tokenize_fn, batched=True)
    val_ds = Dataset.from_pandas(val_df[["text", "label"]]).map(tokenize_fn, batched=True)
    test_ds = Dataset.from_pandas(test_df[["text", "label"]]).map(tokenize_fn, batched=True)

    def compute_metrics(eval_pred):
        predictions, labels = eval_pred
        preds = np.argmax(predictions, axis=-1)
        f1_macro = f1_score(labels, preds, average="macro")
        f1_weighted = f1_score(labels, preds, average="weighted")
        accuracy = (preds == labels).mean()
        return {"accuracy": accuracy, "f1_macro": f1_macro, "f1_weighted": f1_weighted}

    training_args = TrainingArguments(
        output_dir=str(output_dir),
        num_train_epochs=epochs,
        per_device_train_batch_size=batch_size,
        per_device_eval_batch_size=batch_size * 2,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="f1_macro",
        logging_steps=50,
        warmup_ratio=0.1,
        weight_decay=0.01,
        learning_rate=2e-5,
        fp16=True,
        report_to="none",
    )

    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_ds,
        eval_dataset=val_ds,
        compute_metrics=compute_metrics,
        tokenizer=tokenizer,
    )

    trainer.train()

    # Evaluate on test set
    test_results = trainer.evaluate(test_ds)
    log.info(f"Test results for {model_key}: {test_results}")

    # Detailed classification report
    preds = trainer.predict(test_ds)
    pred_labels = np.argmax(preds.predictions, axis=-1)
    report = classification_report(
        test_df["label"].values, pred_labels,
        target_names=CONTEXT_LABELS, output_dict=True,
    )

    # Save results
    results = {
        "model_key": model_key,
        "model_name": model_name,
        "test_metrics": test_results,
        "classification_report": report,
        "train_size": len(train_df),
        "val_size": len(val_df),
        "test_size": len(test_df),
        "epochs": epochs,
        "label2id": label2id,
    }

    with open(output_dir / "results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)

    # Save model
    trainer.save_model(str(output_dir / "best"))
    tokenizer.save_pretrained(str(output_dir / "best"))
    log.info(f"Saved model: {output_dir / 'best'}")

    return results


def run_training(model_keys=None, epochs=3, batch_size=16):
    ensure_cl_dirs()

    # Prefer clean corpus (contaminated texts removed, 11 classes)
    corpus_path = CL_CLASSIFIED_DIR / "corpus_clean.parquet"
    if not corpus_path.exists():
        corpus_path = CL_CLASSIFIED_DIR / "corpus_labeled.parquet"
    if not corpus_path.exists():
        raise FileNotFoundError("Labeled corpus not found.")

    df = pd.read_parquet(corpus_path)
    train_df, val_df, test_df, label2id = prepare_dataset(df)

    if model_keys is None:
        model_keys = list(ENCODER_MODELS.keys())

    all_results = {}
    for model_key in model_keys:
        results = train_encoder(model_key, train_df, val_df, test_df, label2id, epochs, batch_size)
        all_results[model_key] = results

    # Comparison table
    benchmark_path = CL_MODEL_DIR / "benchmark.json"
    with open(benchmark_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    log.info(f"\nBenchmark saved: {benchmark_path}")

    log.info("\nBENCHMARK COMPARISON:")
    log.info(f"{'Model':<25} {'Accuracy':>10} {'F1-macro':>10} {'F1-weighted':>12}")
    log.info("-" * 60)
    for key, res in all_results.items():
        m = res["test_metrics"]
        log.info(
            f"{key:<25} {m.get('eval_accuracy', 0):>10.4f} "
            f"{m.get('eval_f1_macro', 0):>10.4f} {m.get('eval_f1_weighted', 0):>12.4f}"
        )

    return all_results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, default=None,
                        help="Single model key to train (default: all)")
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=16)
    args = parser.parse_args()

    model_keys = [args.model] if args.model else None
    run_training(model_keys=model_keys, epochs=args.epochs, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
