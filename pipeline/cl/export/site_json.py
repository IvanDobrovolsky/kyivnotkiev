"""Export CL analysis results as JSON for site visualizations.

Generates data for:
- Context classification distribution per pair
- Sentiment comparison (RU vs UA variant)
- Top collocations per variant
- Embedding cluster metrics
- Temporal semantic shift
- Benchmark comparison table

Usage:
    python -m pipeline.cl.export.site_json
"""

import json
import logging
from pathlib import Path

import pandas as pd

from pipeline.cl.config import (
    CL_CLASSIFIED_DIR, CL_EMBEDDINGS_DIR, CL_MODEL_DIR, TOP_6_PAIR_IDS,
    ensure_cl_dirs,
)
from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SITE_DATA_DIR = ROOT_DIR / "site" / "src" / "data"


def export_site_json():
    ensure_cl_dirs()

    cl_data = {
        "context_distribution": {},
        "sentiment_comparison": {},
        "collocations": {},
        "embedding_clusters": {},
        "temporal_shift": {},
        "benchmark": {},
    }

    # Context distribution per pair
    corpus_path = CL_CLASSIFIED_DIR / "corpus_labeled.parquet"
    if corpus_path.exists():
        df = pd.read_parquet(corpus_path)

        for pair_id in sorted(df["pair_id"].unique()):
            pdf = df[df["pair_id"] == pair_id]
            pair_data = {}
            for variant in ["russian", "ukrainian"]:
                vdf = pdf[pdf["variant"] == variant]
                if not vdf.empty:
                    dist = vdf["context_label"].value_counts(normalize=True).to_dict()
                    pair_data[variant] = {k: round(v, 4) for k, v in dist.items()}
            if pair_data:
                cl_data["context_distribution"][str(pair_id)] = pair_data

        # Sentiment comparison
        for pair_id in sorted(df["pair_id"].unique()):
            pdf = df[df["pair_id"] == pair_id]
            pair_sent = {}
            for variant in ["russian", "ukrainian"]:
                vdf = pdf[pdf["variant"] == variant]
                if not vdf.empty and "sentiment_label" in vdf.columns:
                    sent_dist = vdf["sentiment_label"].value_counts(normalize=True).to_dict()
                    mean_score = vdf["sentiment_score"].mean() if "sentiment_score" in vdf.columns else 0
                    pair_sent[variant] = {
                        "distribution": {k: round(v, 4) for k, v in sent_dist.items()},
                        "mean_score": round(float(mean_score), 4),
                        "n_texts": len(vdf),
                    }
            if pair_sent:
                cl_data["sentiment_comparison"][str(pair_id)] = pair_sent

    # Collocations
    coll_path = CL_EMBEDDINGS_DIR / "collocations.json"
    if coll_path.exists():
        with open(coll_path) as f:
            cl_data["collocations"] = json.load(f)

    # Embedding clusters
    cluster_path = CL_EMBEDDINGS_DIR / "cluster_analysis.json"
    if cluster_path.exists():
        with open(cluster_path) as f:
            cl_data["embedding_clusters"] = json.load(f)

    # Temporal shift
    shift_path = CL_EMBEDDINGS_DIR / "temporal_shift.json"
    if shift_path.exists():
        with open(shift_path) as f:
            cl_data["temporal_shift"] = json.load(f)

    # Benchmark
    bench_path = CL_MODEL_DIR / "benchmark.json"
    if bench_path.exists():
        with open(bench_path) as f:
            raw = json.load(f)
        cl_data["benchmark"] = {
            k: {
                "accuracy": v.get("test_metrics", {}).get("eval_accuracy", 0),
                "f1_macro": v.get("test_metrics", {}).get("eval_f1_macro", 0),
                "f1_weighted": v.get("test_metrics", {}).get("eval_f1_weighted", 0),
            }
            for k, v in raw.items()
        }

    # Add top-6 flag
    cl_data["top_6_pair_ids"] = TOP_6_PAIR_IDS

    # Save
    out_path = SITE_DATA_DIR / "cl_analysis.json"
    with open(out_path, "w") as f:
        json.dump(cl_data, f, indent=2)
    log.info(f"Saved CL analysis for site: {out_path}")

    # Summary
    log.info(f"  Context distributions: {len(cl_data['context_distribution'])} pairs")
    log.info(f"  Sentiment comparisons: {len(cl_data['sentiment_comparison'])} pairs")
    log.info(f"  Collocations: {len(cl_data['collocations'])} pairs")
    log.info(f"  Embedding clusters: {len(cl_data['embedding_clusters'])} pairs")

    return cl_data


if __name__ == "__main__":
    export_site_json()
