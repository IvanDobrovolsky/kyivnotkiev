"""Push fine-tuned model + benchmark results to Hugging Face.

Usage:
    python -m pipeline.cl.export.hf_model [--model deberta-v3-large] [--repo IvanDobrovolsky/toponym-context-classifier]
"""

import argparse
import json
import logging
from pathlib import Path

from pipeline.cl.config import CL_MODEL_DIR, CONTEXT_LABELS, ENCODER_MODELS, ensure_cl_dirs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def generate_model_card(model_key, results, output_dir):
    """Generate model card README.md."""
    benchmark_path = CL_MODEL_DIR / "benchmark.json"
    benchmark = {}
    if benchmark_path.exists():
        with open(benchmark_path) as f:
            benchmark = json.load(f)

    metrics = results.get("test_metrics", {})
    report = results.get("classification_report", {})

    # Build per-class table
    class_rows = ""
    for label in CONTEXT_LABELS:
        if label in report:
            r = report[label]
            class_rows += f"| {label} | {r.get('precision', 0):.3f} | {r.get('recall', 0):.3f} | {r.get('f1-score', 0):.3f} | {r.get('support', 0)} |\n"

    # Build benchmark comparison
    bench_rows = ""
    for key, res in benchmark.items():
        m = res.get("test_metrics", {})
        marker = " **" if key == model_key else ""
        bench_rows += f"| {key}{marker} | {m.get('eval_accuracy', 0):.4f} | {m.get('eval_f1_macro', 0):.4f} | {m.get('eval_f1_weighted', 0):.4f} |\n"

    card = f"""---
language:
- en
license: cc-by-4.0
tags:
- text-classification
- toponyms
- ukraine
- language-policy
- kyivnotkiev
datasets:
- IvanDobrovolsky/kyivnotkiev-cl
metrics:
- accuracy
- f1
pipeline_tag: text-classification
---

# Toponym Context Classifier ({model_key})

Classifies texts containing Ukrainian/Russian toponym variants into context categories.
Fine-tuned on the [KyivNotKiev CL Corpus](https://huggingface.co/datasets/IvanDobrovolsky/kyivnotkiev-cl).

## Model Description

- **Base model:** {ENCODER_MODELS.get(model_key, model_key)}
- **Task:** Multi-class text classification (10 categories)
- **Training data:** ~{results.get('train_size', 0):,} labeled texts
- **Labels:** {', '.join(CONTEXT_LABELS)}

## Performance

| Metric | Score |
|--------|-------|
| Accuracy | {metrics.get('eval_accuracy', 0):.4f} |
| F1 (macro) | {metrics.get('eval_f1_macro', 0):.4f} |
| F1 (weighted) | {metrics.get('eval_f1_weighted', 0):.4f} |

### Per-class Performance

| Category | Precision | Recall | F1 | Support |
|----------|-----------|--------|-----|---------|
{class_rows}

### Benchmark Comparison

| Model | Accuracy | F1 (macro) | F1 (weighted) |
|-------|----------|------------|---------------|
{bench_rows}

## Usage

```python
from transformers import pipeline

classifier = pipeline("text-classification", model="IvanDobrovolsky/toponym-context-classifier")
result = classifier("The Champions League final will be held in Kyiv this year")
print(result)  # [{{'label': 'sports', 'score': 0.95}}]
```

## Citation

```bibtex
@article{{dobrovolskyi2026kyivnotkiev,
  title={{Did the World Listen? A Large-Scale Computational Study of Ukrainian Toponym Adoption}},
  author={{Dobrovolskyi, Ivan}},
  year={{2026}}
}}
```
"""

    with open(output_dir / "README.md", "w") as f:
        f.write(card)


def push_model(model_key, repo_id):
    ensure_cl_dirs()

    model_dir = CL_MODEL_DIR / model_key / "best"
    if not model_dir.exists():
        raise FileNotFoundError(f"Model not found at {model_dir}")

    results_path = CL_MODEL_DIR / model_key / "results.json"
    results = {}
    if results_path.exists():
        with open(results_path) as f:
            results = json.load(f)

    generate_model_card(model_key, results, model_dir)

    if repo_id:
        from huggingface_hub import HfApi
        api = HfApi()
        api.upload_folder(
            folder_path=str(model_dir),
            repo_id=repo_id,
            repo_type="model",
        )
        log.info(f"Published: https://huggingface.co/models/{repo_id}")

    return model_dir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, default="deberta-v3-large")
    parser.add_argument("--repo", type=str, default=None)
    args = parser.parse_args()
    push_model(args.model, args.repo)


if __name__ == "__main__":
    main()
