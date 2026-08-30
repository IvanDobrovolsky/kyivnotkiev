"""Soft clustering of one pair's corpus: which contexts each spelling lives in,
and which documents sit between them.

    python -m pipeline.stats.clusters --pair chornobyl

DESIGN
------
Input is the store pair parquet plus the dedup stage's output; only canonical
rows are clustered, so syndicated wire copy cannot manufacture a cluster.

A Gaussian mixture fitted by EM, not HDBSCAN. Hard labels have no notion of a
document between two registers; the mixture's posteriors define it directly:

    margin = p1 - p2  over the top two components
    borderline  <=>  margin < BORDERLINE_MARGIN

Those borderline documents are the contested-context evidence — a text the model
cannot commit to either register.

EM runs in the FULL embedding space (384-d, diagonal covariance), never in UMAP
space: UMAP distorts densities, so posteriors computed there are artifacts of
the projection. UMAP is used once, at the end, for the 2-D picture.

k is chosen by BIC over K_RANGE. BIC can fragment (pick many small components);
components under MIN_CLUSTER_SHARE of the sample are merged into their nearest
neighbour by component-mean distance rather than argued with.

MEMORY (this machine)
---------------------
UMAP has OOMed above 40K texts here. The sample is capped at SAMPLE_CAP with a
seeded, stratified draw (source x variant x year), embeddings are float32,
stages free their inputs before the next allocation, and thread pools are
capped. Keep it to one run at a time.

Outputs, per pair, under data/stats/<pair>/clusters/:
    assignments.parquet   record_id, cluster, p1, p2, margin, borderline, umap x/y
    summary.json          per cluster: size, top terms, variant share, year drift
"""

import argparse
import gc
import json
import os
import pathlib
import sys

os.environ.setdefault("OMP_NUM_THREADS", "4")
os.environ.setdefault("MKL_NUM_THREADS", "4")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

import numpy as np
import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[2]
STORE = ROOT / "data" / "store" / "pairs"
STATS = ROOT / "data" / "stats"

SEED = 20260829
SAMPLE_CAP = 15_000          # UMAP OOMs above ~40K on this machine; measured
# k's ceiling scales with corpus size: BIC fragmented 1,138 VTG texts into 17
# clusters, most of them one register shredded. One cluster per ~150 documents,
# floor 6, cap 24 (chornobyl's 15K keeps its 24).
def k_range_for(n: int) -> range:
    return range(4, max(6, min(24, n // 150)) + 1)
MIN_CLUSTER_SHARE = 0.02     # components smaller than this are merged, not narrated
BORDERLINE_MARGIN = 0.20
EMBED_MODEL = "all-MiniLM-L6-v2"
EMBED_BATCH = 256


def log(msg: str) -> None:
    print(msg, flush=True)


def load_canonical(pair: str) -> pd.DataFrame:
    df = pd.read_parquet(STORE / f"{pair}.parquet")
    # analyze_pair writes the dedup columns into records.parquet, not a separate
    # dedup file — the guard and this loader both guessed the wrong name once.
    dd = STATS / pair / "records.parquet"
    if dd.exists():
        d = pd.read_parquet(dd, columns=["record_id", "is_canonical"])
        df = df.merge(d, on="record_id", how="left")
        before = len(df)
        df = df[df.is_canonical.fillna(True)]
        log(f"  canonical rows: {len(df):,} of {before:,}")
    else:
        log("  WARNING: no dedup output — clustering all rows, duplicates included")
    # URLs are noise for both the embeddings and the term lists: the first run's
    # top terms were https/com/www/xml, and two clusters were essentially "posts
    # containing links". Strip them, plus bare domains and html entities.
    df = df[df.text.notna()].copy()
    df["text"] = (df.text
                  .str.replace(r"https?://\S+", " ", regex=True)
                  .str.replace(r"\bwww\.\S+", " ", regex=True)
                  .str.replace(r"&[a-z]+;", " ", regex=True)
                  .str.replace(r"\s+", " ", regex=True).str.strip())
    df = df[df.text.str.len() > 40]
    df["year"] = df.date.astype(str).str[:4]
    return df.reset_index(drop=True)


def stratified_sample(df: pd.DataFrame, cap: int) -> pd.DataFrame:
    """Seeded draw preserving the source x variant x year mix."""
    if len(df) <= cap:
        return df
    frac = cap / len(df)
    out = (df.groupby(["source", "variant", "year"], group_keys=False)
             .apply(lambda g: g.sample(max(1, int(round(len(g) * frac))),
                                       random_state=SEED), include_groups=True))
    log(f"  sampled {len(out):,} of {len(df):,} (stratified, seed {SEED})")
    return out.reset_index(drop=True)


def embed(texts: list[str]) -> np.ndarray:
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(EMBED_MODEL)
    v = model.encode(texts, batch_size=EMBED_BATCH, show_progress_bar=False,
                     convert_to_numpy=True, normalize_embeddings=True)
    del model
    gc.collect()
    return v.astype(np.float32)


PCA_DIM = 50


def fit_gmm(X: np.ndarray):
    """Diagonal-covariance GMM, k by BIC — in PCA space, not the raw 384-d.

    At 384-d the component likelihoods differ by hundreds of log units, so the
    posteriors saturate: the first run put p1 ~= 1 on 99.4% of documents and the
    borderline concept had nothing to bite on. PCA is linear, so unlike UMAP it
    does not distort the density structure the mixture is fitting; 50 components
    kept ~90% of variance here."""
    from sklearn.decomposition import PCA
    from sklearn.mixture import GaussianMixture
    pca = PCA(n_components=min(PCA_DIM, X.shape[1]), random_state=SEED)
    X = pca.fit_transform(X).astype(np.float32)
    log(f"  pca -> {X.shape[1]}d, variance kept {pca.explained_variance_ratio_.sum()*100:.0f}%")
    best, best_bic, bics = None, np.inf, {}
    fits = {}
    for k in k_range_for(len(X)):
        gm = GaussianMixture(n_components=k, covariance_type="diag",
                             random_state=SEED, n_init=2, max_iter=200)
        gm.fit(X)
        fits[k] = gm
        bics[k] = float(gm.bic(X))
    log(f"  BIC by k: { {k: round(v) for k, v in bics.items()} }")
    # BIC decreases monotonically at this scale, so its minimum is just the top of
    # the range. Take the knee instead: the last k whose improvement is still at
    # least 20% of the largest single-step improvement.
    ks = sorted(bics)
    drops = {ks[i + 1]: bics[ks[i]] - bics[ks[i + 1]] for i in range(len(ks) - 1)}
    biggest = max(drops.values())
    k_knee = ks[0]
    for k in ks[1:]:
        if drops[k] >= 0.2 * biggest:
            k_knee = k
    log(f"  chosen k = {k_knee} (BIC knee; min was at the range edge)")
    return fits[k_knee], X


def merge_small(post: np.ndarray, means: np.ndarray) -> np.ndarray:
    """Reassign components under MIN_CLUSTER_SHARE to their nearest neighbour
    (component-mean distance). Returns a relabelling map."""
    k = post.shape[1]
    share = post.argmax(1)
    sizes = np.bincount(share, minlength=k) / len(share)
    mapping = np.arange(k)
    for c in np.where(sizes < MIN_CLUSTER_SHARE)[0]:
        d = np.linalg.norm(means - means[c], axis=1)
        d[c] = np.inf
        for small in np.where(sizes < MIN_CLUSTER_SHARE)[0]:
            d[small] = np.inf          # never merge small into small
        if np.isfinite(d).any():
            mapping[c] = int(np.argmin(d))
    return mapping


def top_terms(texts: pd.Series, labels: np.ndarray, k: int, n: int = 10) -> dict:
    """c-TF-IDF: each cluster as one document, so terms are distinguishing
    rather than merely frequent."""
    from sklearn.feature_extraction.text import CountVectorizer, ENGLISH_STOP_WORDS
    # sklearn's english list keeps conversational fillers, so the biggest casual
    # cluster labelled itself "like - just". Filler and non-English function words
    # are stopped as well; a Spanish-language cluster should surface through terms
    # like "video" or stay generic, not advertise "que - por" as if it meant
    # something about naming.
    FILLERS = {"like", "just", "time", "people", "know", "think", "really", "going",
               "want", "got", "way", "thing", "things", "good", "make", "say", "said",
               "yeah", "don", "didn", "doesn", "ve", "ll", "im", "actually", "right",
               "new", "old", "year", "years", "day", "days", "watch", "video",
               "que", "por", "para", "una", "del", "las", "los", "con", "este",
               "und", "der", "die", "das", "les", "des", "dans", "pour"}
    stop = list(ENGLISH_STOP_WORDS | FILLERS)
    docs = [" ".join(texts[labels == c].head(2000)) for c in range(k)]
    cv = CountVectorizer(stop_words=stop, max_features=30_000,
                         token_pattern=r"[a-zA-Z][a-zA-Z'-]{2,}")
    tf = cv.fit_transform(docs).toarray().astype(np.float64)
    tf = tf / np.maximum(tf.sum(1, keepdims=True), 1)
    idf = np.log(1 + k / np.maximum((tf > 0).sum(0), 1))
    ct = tf * idf
    vocab = np.array(cv.get_feature_names_out())
    return {int(c): [str(w) for w in vocab[np.argsort(-ct[c])[:n]]] for c in range(k)}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    a = ap.parse_args()

    out_dir = STATS / a.pair / "clusters"
    out_dir.mkdir(parents=True, exist_ok=True)
    log(f"=== clusters: {a.pair} ===")

    df = load_canonical(a.pair)
    df = stratified_sample(df, SAMPLE_CAP)

    log(f"  embedding {len(df):,} texts ({EMBED_MODEL})")
    X = embed(df.text.tolist())

    gm, Xr = fit_gmm(X)
    post = gm.predict_proba(Xr)
    del Xr
    mapping = merge_small(post, gm.means_)
    if (mapping != np.arange(len(mapping))).any():
        merged_post = np.zeros_like(post)
        for src, dst in enumerate(mapping):
            merged_post[:, dst] += post[:, src]
        post = merged_post
        log(f"  merged {int((mapping != np.arange(len(mapping))).sum())} small component(s)")

    # ── Merge components that say the same thing ─────────────────────────────
    # BIC rewards likelihood, so it happily splits one register across many
    # near-identical components: the first k=24 run produced 29 cluster pairs
    # sharing at least half their top terms, eight of them at 75%. Statistical
    # structure and presentational structure are different things — components
    # whose top-8 c-TF-IDF terms overlap >= 0.5 are one context and are merged
    # (union-find), their posteriors summed. The merge is part of the pipeline,
    # deterministic, and reported.
    k0 = post.shape[1]
    lab0 = post.argmax(1)
    terms0 = top_terms(df.text, lab0, k0, n=8)
    parent2 = list(range(k0))
    def find2(x):
        while parent2[x] != x:
            parent2[x] = parent2[parent2[x]]
            x = parent2[x]
        return x
    # Never merge across the variant axis. The two STALKER games share their
    # vocabulary but sit at 0.2% and 92% Ukrainian — the first version fused them
    # into one 38%-UA cluster and erased the franchise-rename finding. Components
    # merge only when they agree on BOTH topic (term overlap) and spelling regime
    # (UA share within 20 points).
    _ua0 = np.array([
        (df.variant.values[lab0 == c] == "ukrainian").mean() if (lab0 == c).any() else 0.0
        for c in range(k0)])
    for i in range(k0):
        for j in range(i + 1, k0):
            ti, tj = set(terms0.get(i, [])), set(terms0.get(j, []))
            if not ti or not tj or len(ti & tj) / 8 < 0.5:
                continue
            if abs(_ua0[i] - _ua0[j]) > 0.20:
                continue
            ri, rj = find2(i), find2(j)
            if ri != rj:
                parent2[max(ri, rj)] = min(ri, rj)
    roots = sorted({find2(i) for i in range(k0)})
    if len(roots) < k0:
        remap = {r: n for n, r in enumerate(roots)}
        merged = np.zeros((post.shape[0], len(roots)), dtype=post.dtype)
        for src in range(k0):
            merged[:, remap[find2(src)]] += post[:, src]
        post = merged
        log(f"  merged {k0} components -> {len(roots)} distinct contexts (term overlap >= 0.5)")

    order = np.argsort(-post, axis=1)
    p1 = post[np.arange(len(post)), order[:, 0]]
    p2 = post[np.arange(len(post)), order[:, 1]]
    labels = order[:, 0]
    margin = p1 - p2
    borderline = margin < BORDERLINE_MARGIN

    # 2-D picture only; every quantity above was computed in the full space.
    log("  umap 2-d (viz only)")
    import umap
    coords = umap.UMAP(n_components=2, n_neighbors=15, min_dist=0.1,
                       random_state=SEED).fit_transform(X)
    del X
    gc.collect()

    keep = [c for c in ("record_id", "source", "variant", "date", "year") if c in df.columns]
    outdf = df[keep].copy()
    outdf["cluster"] = labels
    outdf["p1"] = p1.round(4)
    outdf["p2"] = p2.round(4)
    outdf["margin"] = margin.round(4)
    outdf["borderline"] = borderline
    outdf["umap_x"] = coords[:, 0].round(3)
    outdf["umap_y"] = coords[:, 1].round(3)
    outdf.to_parquet(out_dir / "assignments.parquet", index=False)

    k = post.shape[1]
    terms = top_terms(df.text, labels, k)
    summary = {"pair": a.pair, "seed": SEED, "n": len(df), "k_chosen": int(k),
               "borderline_margin": BORDERLINE_MARGIN,
               "borderline_share": round(float(borderline.mean()), 4),
               "clusters": []}
    for c in range(k):
        m = labels == c
        if not m.any():
            continue
        g = df[m]
        vs = g.variant.value_counts(normalize=True).round(3).to_dict()
        yr = g.groupby("year").size()
        # English stopword hit-rate flags non-English clusters: the corpus is meant
        # to be English-only, but reddit and youtube leak other languages, and one
        # Spanish cluster labelled itself from token fragments. Low ratio -> the
        # exporter names it plainly instead of pretending the fragments are terms.
        _tok = g.text.str.lower().str.findall(r"[a-z']+")
        _eng = {"the","and","of","to","in","is","it","that","for","was","on","with","as","this"}
        # Judge only texts long enough to have function words at all: hashtag-style
        # YouTube Shorts titles carry ~5 tokens and no stopwords in any language,
        # and the first version branded that cluster non-English. Under 30% judgeable
        # docs the ratio abstains (1.0 = assume English).
        _long = _tok[_tok.str.len() >= 20]
        if len(_long) >= 0.3 * len(_tok):
            _ratio = float(_long.apply(lambda ws: sum(w in _eng for w in ws[:60]) / min(len(ws), 60)).mean())
        else:
            _ratio = 1.0
        summary["clusters"].append({
            "cluster": int(c), "size": int(m.sum()),
            "english_ratio": round(_ratio, 3),
            "top_terms": terms.get(c, []),
            "variant_share": vs,
            "borderline_share": round(float(borderline[m].mean()), 4),
            "peak_year": str(yr.idxmax()) if len(yr) else None,
        })
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=1))
    log(f"  wrote {out_dir}/assignments.parquet and summary.json")
    log(f"  borderline documents: {int(borderline.sum()):,} ({borderline.mean()*100:.1f}%)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
