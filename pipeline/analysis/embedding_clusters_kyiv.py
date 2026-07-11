"""Unsupervised embedding analysis per pair.

Runs separately for Ukrainian and Russian variants.
Goal: detect false positives and outliers via HDBSCAN noise detection.

Usage:
    python -m pipeline.analysis.embedding_clusters_kyiv [--pair kyiv]
"""

import gc
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CORPUS_PATH = ROOT / "data" / "corpus" / "toponyms-corpus.parquet"
OUT_DIR = ROOT / "data" / "corpus" / "kyiv_clusters"


def embed_texts(texts: list[str], model_name: str = "all-MiniLM-L6-v2", batch_size: int = 256):
    from sentence_transformers import SentenceTransformer
    log.info(f"Loading model: {model_name}")
    model = SentenceTransformer(model_name)
    log.info(f"Embedding {len(texts):,} texts...")
    embeddings = model.encode(texts, batch_size=batch_size, show_progress_bar=True, normalize_embeddings=True)
    del model
    gc.collect()
    return embeddings


def reduce_umap(embeddings, n_components=2, n_neighbors=15, min_dist=0.1, metric="cosine"):
    import umap
    log.info(f"UMAP: {embeddings.shape} -> {n_components}D (metric={metric})")
    reducer = umap.UMAP(n_components=n_components, n_neighbors=n_neighbors,
                        min_dist=min_dist, metric=metric, low_memory=True,
                        random_state=42)
    return reducer.fit_transform(embeddings)


def cluster_hdbscan(embeddings, min_cluster_size=15, min_samples=5):
    import hdbscan
    log.info(f"HDBSCAN: min_cluster={min_cluster_size}, min_samples={min_samples}")
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples,
                                 metric="euclidean", cluster_selection_method="eom")
    labels = clusterer.fit_predict(embeddings)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = (labels == -1).sum()
    log.info(f"  Clusters: {n_clusters}, Noise: {n_noise} ({n_noise/len(labels)*100:.1f}%)")
    return labels, clusterer


def run_variant(df: pd.DataFrame, variant_label: str, max_texts: int = 15000):
    """Run full pipeline for one variant subset."""
    log.info(f"\n{'='*60}")
    log.info(f"VARIANT: {variant_label} ({len(df):,} texts)")
    log.info(f"{'='*60}")

    # Auto-sample if too large for laptop memory
    # UMAP on 40K+ texts needs 8-15 GB; 15K stays under 3 GB
    if max_texts > 0 and len(df) > max_texts:
        log.info(f"  Sampling {max_texts:,} from {len(df):,} (stratified by source)")
        frac = max_texts / len(df)
        df = df.groupby("source", group_keys=False).apply(
            lambda g: g.sample(max(1, round(len(g) * frac)), random_state=42)
        ).sample(frac=1, random_state=42).reset_index(drop=True)
        log.info(f"  Sampled: {len(df):,} texts")

    out = OUT_DIR / variant_label
    out.mkdir(parents=True, exist_ok=True)

    # Embed (model freed inside embed_texts)
    embeddings = embed_texts(df["text"].tolist())
    np.save(out / "embeddings.npy", embeddings)

    # Single UMAP pass: 384D → 10D for clustering (the expensive step)
    coords_10d = reduce_umap(embeddings, n_components=10, n_neighbors=30, min_dist=0.0)
    del embeddings
    gc.collect()
    log.info("  Freed embedding matrix")

    # HDBSCAN on 10D
    labels, clusterer = cluster_hdbscan(coords_10d, min_cluster_size=20)
    df = df.copy()
    df["cluster"] = labels
    df["outlier_score"] = clusterer.outlier_scores_

    # 10D → 2D for viz (fast — input is only 10-dim, not 384-dim)
    coords_2d = reduce_umap(coords_10d, n_components=2, n_neighbors=15, min_dist=0.1,
                            metric="euclidean")
    del coords_10d
    gc.collect()
    df["umap_x"] = coords_2d[:, 0]
    df["umap_y"] = coords_2d[:, 1]

    # Noise analysis
    noise_df = df[df["cluster"] == -1].copy()
    noise_df = noise_df.sort_values("outlier_score", ascending=False)

    log.info(f"\nNOISE/OUTLIERS: {len(noise_df)} texts ({len(noise_df)/len(df)*100:.1f}%)")
    log.info(f"  By source: {noise_df['source'].value_counts().to_dict()}")
    log.info(f"  Avg text len: {noise_df['text_len'].mean():.0f}")

    # Top outliers
    log.info(f"\n  TOP 20 OUTLIERS (highest outlier score):")
    for i, (_, r) in enumerate(noise_df.head(20).iterrows()):
        log.info(f"  {i+1:2d}. [score={r['outlier_score']:.3f}] [{r['source']}] {r['text'][:120]}...")

    # Cluster summary
    n_clusters = df["cluster"].nunique() - (1 if -1 in df["cluster"].values else 0)
    log.info(f"\nCLUSTERS: {n_clusters}")
    for cid in sorted(df["cluster"].unique()):
        if cid == -1:
            continue
        c = df[df["cluster"] == cid]
        log.info(f"  Cluster {cid}: {len(c)} texts | sources: {c['source'].value_counts().head(3).to_dict()}")
        sample = c.sample(min(2, len(c)), random_state=42)
        for _, r in sample.iterrows():
            log.info(f"    -> {r['text'][:100]}...")

    # Save outputs
    viz_df = df[["source", "variant", "text_len", "cluster", "outlier_score", "umap_x", "umap_y"]].copy()
    viz_df.to_parquet(out / "umap_coords.parquet", index=False)

    noise_df.to_parquet(out / "noise_texts.parquet", index=False)
    log.info(f"\nSaved {len(noise_df)} noise texts to {out / 'noise_texts.parquet'}")

    # Save cluster stats as JSON
    stats = {
        "variant": variant_label,
        "total_texts": len(df),
        "n_clusters": n_clusters,
        "n_noise": len(noise_df),
        "noise_pct": round(len(noise_df) / len(df) * 100, 1),
        "noise_by_source": noise_df["source"].value_counts().to_dict(),
        "clusters": [],
    }
    for cid in sorted(df["cluster"].unique()):
        c = df[df["cluster"] == cid]
        stats["clusters"].append({
            "id": int(cid),
            "size": len(c),
            "sources": c["source"].value_counts().to_dict(),
            "avg_text_len": round(c["text_len"].mean()),
            "avg_outlier_score": round(c["outlier_score"].mean(), 4),
        })
    with open(out / "cluster_stats.json", "w") as f:
        json.dump(stats, f, indent=2, default=str)

    log.info(f"Saved stats to {out / 'cluster_stats.json'}")
    return noise_df


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", default="kyiv", help="Pair slug to cluster")
    parser.add_argument("--max-texts", type=int, default=15000,
                        help="Max texts per variant (0=unlimited, default 15000)")
    args = parser.parse_args()

    pair_slug = args.pair

    global OUT_DIR
    OUT_DIR = ROOT / "data" / "corpus" / f"{pair_slug}_clusters"

    df = pd.read_parquet(CORPUS_PATH)
    pair_df = df[df["pair_slug"] == pair_slug].reset_index(drop=True)
    log.info(f"{pair_slug} pair: {len(pair_df):,} texts")

    if len(pair_df) == 0:
        log.error(f"No texts found for pair '{pair_slug}'")
        return

    ua = pair_df[pair_df["variant"] == "ukrainian"].reset_index(drop=True)
    ru = pair_df[pair_df["variant"] == "russian"].reset_index(drop=True)

    noise_ua = run_variant(ua, "ukrainian", args.max_texts) if len(ua) >= 20 else pd.DataFrame()
    noise_ru = run_variant(ru, "russian", args.max_texts) if len(ru) >= 20 else pd.DataFrame()

    log.info(f"\n{'='*60}")
    log.info(f"SUMMARY — {pair_slug}")
    log.info(f"{'='*60}")
    if len(ua) >= 20:
        log.info(f"Ukrainian: {len(ua):,} texts, {len(noise_ua)} outliers ({len(noise_ua)/max(len(ua),1)*100:.1f}%)")
    else:
        log.info(f"Ukrainian: {len(ua):,} texts (too few to cluster)")
    if len(ru) >= 20:
        log.info(f"Russian:   {len(ru):,} texts, {len(noise_ru)} outliers ({len(noise_ru)/max(len(ru),1)*100:.1f}%)")
    else:
        log.info(f"Russian:   {len(ru):,} texts (too few to cluster)")


if __name__ == "__main__":
    main()
