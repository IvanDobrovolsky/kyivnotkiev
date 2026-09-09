# Training Corpus Specification

One deterministic builder produces the corpus for all supervised work
(context classifier, later models). Re-running it after any upstream data
change is the ONLY way the corpus updates — nothing is edited in place.

## Inputs (verified layers only)

| Source   | File                                            | Text used            |
|----------|--------------------------------------------------|----------------------|
| news     | `data/cl/corpus/gdelt_verified/<pair>.parquet`   | article body         |
| youtube  | `data/cl/raw/youtube_census/<pair>_enriched.parquet` (verified rows) | title + description |
| reddit   | `data/store/reddit_processed.parquet`            | title + selftext     |
| openalex | `data/cl/raw/openalex/all_pairs.parquet`         | title + abstract     |

The legacy training corpus is void (see memory: corpus reset) and is never
read.

## Rules

1. **Exact matches only.** A record enters under a pair only if the pair's
   surface form matches word-bounded in the text used. Verified exhaustively
   across all pairs and both directions (2026-09-09: zero misclassifications).
2. **Labels**: `variant` ∈ {ukrainian, russian, both} from the standalone
   word-bounded recount, never carried from upstream metadata.
3. **Source is metadata, never a feature.** It is kept for stratification and
   audits; models must not receive it.
4. **The same cleaning as the published series**: config `homonym_filters` +
   `youtube_homonym_filters` (title+description+channel), the mirror-bot
   subreddit exclusion (already inside reddit_processed), wrong-referent
   guards (strain IDs, Kiev-Rus garble), and the referent filters
   (odesa evidence rule, borscht frozen compound).
5. **English screen**: documents failing the function-word + non-ASCII-word
   test are dropped — the corpus measures English usage. Counts of everything
   dropped go to the manifest.
6. **Determinism**: rows sorted by (source, doc_id); the manifest records
   input file SHA1s and per-pair×variant×source counts. Identical inputs ⇒
   byte-identical corpus.

## Taxonomy (decided — do not reinvent)

- **Layer 1 — CORE registers** (Biber & Egbert): the citable register set.
  Labels arrive in the annotation/weak-supervision phase.
- **Layer 2 — cluster-derived frames**: the per-pair GMM discourse clusters
  already computed; descriptive names only, grounded in cluster terms.

## Imbalance handling

The builder emits `_imbalance.json`: the pair × variant × source matrix and
inverse-frequency class weights (normalized). Policy: class weights over
oversampling; per-pair stratified k-fold CV; leave-one-pair-out as the
generalization check; Optuna for tuning (standing rule: never grid search,
never a single holdout).

## Stack

JAX/Flax; TPU training on the GCP agent platform.
