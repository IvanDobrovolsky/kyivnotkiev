"""Unsupervised embedding analysis of the CL corpus.

Embeds all texts with sentence-transformers, reduces with UMAP,
clusters with HDBSCAN, and outputs analysis for the site + paper.

Findings:
- Do Russian vs Ukrainian spellings occupy different discourse spaces?
- Do sources cluster independently of content?
- Can we detect trash/FP texts as outliers?

Usage:
    python -m pipeline.analysis.embedding_clusters [--sample 0 --model all-MiniLM-L6-v2]
"""

import argparse
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CORPUS_PATH = ROOT / "data" / "corpus" / "toponyms-corpus.parquet"
OUT_DIR = ROOT / "data" / "corpus"


def embed_texts(texts: list[str], model_name: str = "all-MiniLM-L6-v2", batch_size: int = 256):
    """Embed texts with sentence-transformers."""
    from sentence_transformers import SentenceTransformer
    log.info(f"Loading model: {model_name}")
    model = SentenceTransformer(model_name)
    log.info(f"Embedding {len(texts):,} texts (batch_size={batch_size})...")
    embeddings = model.encode(texts, batch_size=batch_size, show_progress_bar=True, normalize_embeddings=True)
    return embeddings


def reduce_umap(embeddings, n_components=2, n_neighbors=15, min_dist=0.1):
    """Reduce embeddings to 2D with UMAP."""
    import umap
    log.info(f"UMAP: {embeddings.shape} → {n_components}D")
    reducer = umap.UMAP(n_components=n_components, n_neighbors=n_neighbors,
                        min_dist=min_dist, metric="cosine", random_state=42)
    coords = reducer.fit_transform(embeddings)
    return coords


def cluster_hdbscan(embeddings, min_cluster_size=15, min_samples=5):
    """Cluster with HDBSCAN. Returns labels (-1 = noise/outlier)."""
    import hdbscan
    log.info(f"HDBSCAN: min_cluster={min_cluster_size}, min_samples={min_samples}")
    clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, min_samples=min_samples,
                                 metric="euclidean", cluster_selection_method="eom")
    labels = clusterer.fit_predict(embeddings)
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    n_noise = (labels == -1).sum()
    log.info(f"  Clusters: {n_clusters}, Noise: {n_noise} ({n_noise/len(labels)*100:.1f}%)")
    return labels, clusterer


def analyze_clusters(df: pd.DataFrame):
    """Analyze cluster composition by variant, source, pair."""
    results = {}

    # Overall stats
    n_clusters = df["cluster"].nunique() - (1 if -1 in df["cluster"].values else 0)
    noise = (df["cluster"] == -1).sum()
    results["n_clusters"] = n_clusters
    results["n_noise"] = int(noise)
    results["noise_pct"] = round(noise / len(df) * 100, 1)

    # Per-cluster breakdown
    clusters = []
    for cid in sorted(df["cluster"].unique()):
        c = df[df["cluster"] == cid]
        cluster_info = {
            "id": int(cid),
            "size": len(c),
            "variant_dist": c["variant"].value_counts().to_dict(),
            "top_sources": c["source"].value_counts().head(5).to_dict(),
            "top_pairs": c["pair_slug"].value_counts().head(5).to_dict(),
            "avg_text_len": round(c["text_len"].mean()),
        }

        # Dominant variant
        ru = c["variant"].eq("russian").sum()
        ua = c["variant"].eq("ukrainian").sum()
        if ru + ua > 0:
            cluster_info["ua_pct"] = round(ua / (ru + ua) * 100, 1)

        # Sample texts
        cluster_info["samples"] = c["text"].sample(min(3, len(c)), random_state=42).tolist()

        clusters.append(cluster_info)

    results["clusters"] = sorted(clusters, key=lambda x: -x["size"])

    # Variant separation score: how well do clusters separate variants?
    # If perfect separation: each cluster is 100% one variant
    # Measure: weighted average of max(ru%, ua%) per cluster
    weighted_purity = 0
    total_non_noise = 0
    for c in clusters:
        if c["id"] == -1:
            continue
        ru = c["variant_dist"].get("russian", 0)
        ua = c["variant_dist"].get("ukrainian", 0)
        both = c["variant_dist"].get("both", 0)
        total = ru + ua + both
        if total > 0:
            purity = max(ru, ua) / total
            weighted_purity += purity * total
            total_non_noise += total

    results["variant_separation"] = round(weighted_purity / max(total_non_noise, 1) * 100, 1)

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=0, help="Sample N texts (0=all)")
    parser.add_argument("--model", default="all-MiniLM-L6-v2", help="Sentence transformer model")
    parser.add_argument("--min-cluster", type=int, default=20, help="HDBSCAN min cluster size")
    args = parser.parse_args()

    # Load corpus
    df = pd.read_parquet(CORPUS_PATH)
    log.info(f"Corpus: {len(df):,} texts")

    if args.sample > 0 and args.sample < len(df):
        df = df.sample(args.sample, random_state=42).reset_index(drop=True)
        log.info(f"Sampled: {len(df):,}")

    # Embed
    embeddings = embed_texts(df["text"].tolist(), model_name=args.model)

    # Save embeddings
    emb_path = OUT_DIR / "embeddings.npy"
    np.save(emb_path, embeddings)
    log.info(f"Saved embeddings: {emb_path}")

    # UMAP
    coords_2d = reduce_umap(embeddings)
    df["umap_x"] = coords_2d[:, 0]
    df["umap_y"] = coords_2d[:, 1]

    # Also do UMAP to higher dim for clustering (better than 2D)
    coords_cluster = reduce_umap(embeddings, n_components=10, n_neighbors=30, min_dist=0.0)

    # HDBSCAN
    labels, clusterer = cluster_hdbscan(coords_cluster, min_cluster_size=args.min_cluster)
    df["cluster"] = labels

    # Analyze
    analysis = analyze_clusters(df)
    log.info(f"\n{'='*60}")
    log.info(f"RESULTS")
    log.info(f"{'='*60}")
    log.info(f"Clusters: {analysis['n_clusters']}")
    log.info(f"Noise/outliers: {analysis['n_noise']} ({analysis['noise_pct']}%)")
    log.info(f"Variant separation: {analysis['variant_separation']}%")

    # Noise texts are potential FP/trash
    noise_df = df[df["cluster"] == -1]
    if len(noise_df) > 0:
        log.info(f"\nNOISE ANALYSIS (potential FP):")
        log.info(f"  By source: {noise_df['source'].value_counts().to_dict()}")
        log.info(f"  By variant: {noise_df['variant'].value_counts().to_dict()}")
        log.info(f"  Avg text len: {noise_df['text_len'].mean():.0f}")
        log.info(f"  Sample noise texts:")
        for _, r in noise_df.sample(min(5, len(noise_df)), random_state=42).iterrows():
            log.info(f"    [{r['source']}] [{r['pair_slug']}] {r['text'][:100]}...")

    for c in analysis["clusters"][:10]:
        ua_pct = c.get("ua_pct", "?")
        log.info(f"\n  Cluster {c['id']}: {c['size']} texts, {ua_pct}% UA")
        log.info(f"    Sources: {c['top_sources']}")
        log.info(f"    Pairs: {c['top_pairs']}")
        log.info(f"    Sample: {c['samples'][0][:100]}...")

    # Save results
    analysis_path = OUT_DIR / "embedding_analysis.json"
    # Remove samples for JSON (too large)
    for c in analysis["clusters"]:
        del c["samples"]
    with open(analysis_path, "w") as f:
        json.dump(analysis, f, indent=2, default=str)
    log.info(f"\nSaved: {analysis_path}")

    # Save UMAP coords for visualization
    viz_df = df[["pair_slug", "source", "variant", "text_len", "cluster", "umap_x", "umap_y"]].copy()
    viz_path = OUT_DIR / "umap_coords.parquet"
    viz_df.to_parquet(viz_path, index=False)
    log.info(f"Saved: {viz_path}")

    # Save noise texts for manual review
    if len(noise_df) > 0:
        noise_path = OUT_DIR / "noise_texts.parquet"
        noise_df.to_parquet(noise_path, index=False)
        log.info(f"Saved noise for review: {noise_path} ({len(noise_df)} texts)")


if __name__ == "__main__":
    main()
