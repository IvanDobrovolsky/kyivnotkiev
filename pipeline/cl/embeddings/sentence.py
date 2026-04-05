"""Sentence embeddings for variant clustering analysis.

Generates embeddings using multilingual-e5-large, then analyzes:
- How texts with 'Kiev' vs 'Kyiv' cluster in embedding space
- Semantic shift over time (pre/post 2022)
- Cross-pair embedding similarity

Usage:
    python -m pipeline.cl.embeddings.sentence [--model intfloat/multilingual-e5-large]
"""

import argparse
import logging

import numpy as np
import pandas as pd

from pipeline.cl.config import (
    CL_CLASSIFIED_DIR, CL_EMBEDDINGS_DIR, EMBEDDING_MODEL, ensure_cl_dirs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def compute_embeddings(texts, model_name=EMBEDDING_MODEL, batch_size=64):
    """Compute sentence embeddings using sentence-transformers."""
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    log.info(f"Computing embeddings with {model_name} (batch_size={batch_size})")

    # E5 models need "query: " prefix
    if "e5" in model_name.lower():
        texts = ["query: " + t[:512] for t in texts]
    else:
        texts = [t[:512] for t in texts]

    embeddings = model.encode(texts, batch_size=batch_size, show_progress_bar=True)
    return embeddings


def analyze_variant_clusters(df, embeddings):
    """Analyze embedding clusters by variant."""
    from sklearn.metrics.pairwise import cosine_similarity

    results = {}
    for pair_id in sorted(df["pair_id"].unique()):
        mask = df["pair_id"] == pair_id
        pair_emb = embeddings[mask]
        pair_df = df[mask]

        ru_mask = pair_df["variant"].values == "russian"
        ua_mask = pair_df["variant"].values == "ukrainian"

        if ru_mask.sum() < 5 or ua_mask.sum() < 5:
            continue

        ru_emb = pair_emb[ru_mask]
        ua_emb = pair_emb[ua_mask]

        # Centroid distance
        ru_centroid = ru_emb.mean(axis=0)
        ua_centroid = ua_emb.mean(axis=0)
        centroid_sim = cosine_similarity([ru_centroid], [ua_centroid])[0][0]

        # Within-variant similarity (cohesion)
        ru_cohesion = cosine_similarity(ru_emb).mean()
        ua_cohesion = cosine_similarity(ua_emb).mean()

        # Cross-variant similarity
        cross_sim = cosine_similarity(ru_emb, ua_emb).mean()

        results[int(pair_id)] = {
            "centroid_similarity": float(centroid_sim),
            "russian_cohesion": float(ru_cohesion),
            "ukrainian_cohesion": float(ua_cohesion),
            "cross_variant_similarity": float(cross_sim),
            "separation": float(ru_cohesion + ua_cohesion - 2 * cross_sim),
            "n_russian": int(ru_mask.sum()),
            "n_ukrainian": int(ua_mask.sum()),
        }

        log.info(
            f"  Pair {pair_id}: centroid_sim={centroid_sim:.3f}, "
            f"separation={results[int(pair_id)]['separation']:.4f}"
        )

    return results


def analyze_temporal_shift(df, embeddings):
    """Analyze how embeddings shift over time (pre/post invasion)."""
    from sklearn.metrics.pairwise import cosine_similarity

    results = {}
    for pair_id in sorted(df["pair_id"].unique()):
        mask = df["pair_id"] == pair_id
        pair_df = df[mask].copy()
        pair_emb = embeddings[mask]

        pre = pair_df["year"].values < 2022
        post = pair_df["year"].values >= 2022

        if pre.sum() < 5 or post.sum() < 5:
            continue

        pre_centroid = pair_emb[pre].mean(axis=0)
        post_centroid = pair_emb[post].mean(axis=0)
        shift = cosine_similarity([pre_centroid], [post_centroid])[0][0]

        results[int(pair_id)] = {
            "pre_post_similarity": float(shift),
            "semantic_shift": float(1 - shift),
            "n_pre": int(pre.sum()),
            "n_post": int(post.sum()),
        }

    return results


def run_embeddings(model_name=EMBEDDING_MODEL, batch_size=64):
    ensure_cl_dirs()

    corpus_path = CL_CLASSIFIED_DIR / "corpus_labeled.parquet"
    if not corpus_path.exists():
        raise FileNotFoundError("Labeled corpus not found. Run classification first.")

    df = pd.read_parquet(corpus_path)
    log.info(f"Computing embeddings for {len(df)} texts")

    texts = df["text"].fillna("").tolist()
    embeddings = compute_embeddings(texts, model_name, batch_size)

    # Save embeddings
    emb_path = CL_EMBEDDINGS_DIR / "embeddings.npy"
    np.save(emb_path, embeddings)
    log.info(f"Saved embeddings: {emb_path} (shape={embeddings.shape})")

    # Save index mapping
    index_path = CL_EMBEDDINGS_DIR / "embedding_index.parquet"
    df[["pair_id", "variant", "source", "year"]].to_parquet(index_path, index=False)

    # Analyze
    cluster_results = analyze_variant_clusters(df, embeddings)
    temporal_results = analyze_temporal_shift(df, embeddings)

    import json
    with open(CL_EMBEDDINGS_DIR / "cluster_analysis.json", "w") as f:
        json.dump(cluster_results, f, indent=2)

    with open(CL_EMBEDDINGS_DIR / "temporal_shift.json", "w") as f:
        json.dump(temporal_results, f, indent=2)

    log.info(f"Cluster analysis: {len(cluster_results)} pairs")
    log.info(f"Temporal shift: {len(temporal_results)} pairs")

    return embeddings, cluster_results, temporal_results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, default=EMBEDDING_MODEL)
    parser.add_argument("--batch-size", type=int, default=64)
    args = parser.parse_args()
    run_embeddings(model_name=args.model, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
