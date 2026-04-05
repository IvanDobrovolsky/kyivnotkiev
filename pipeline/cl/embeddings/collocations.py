"""Collocation extraction — top co-occurring words per variant.

Extracts significant collocates using PMI (pointwise mutual information)
for each toponym form. Reveals different discourse contexts:
e.g., "Kiev" + "chicken" vs "Kyiv" + "missile".

Usage:
    python -m pipeline.cl.embeddings.collocations
"""

import json
import logging
import re
from collections import Counter, defaultdict
from math import log2

import pandas as pd

from pipeline.cl.config import CL_BALANCED_DIR, CL_EMBEDDINGS_DIR, ensure_cl_dirs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Stopwords — minimal set, don't over-filter
STOPWORDS = {
    "the", "a", "an", "is", "was", "are", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "must", "need",
    "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them",
    "my", "your", "his", "its", "our", "their", "this", "that", "these", "those",
    "and", "or", "but", "nor", "not", "so", "yet", "both", "either", "neither",
    "in", "on", "at", "to", "for", "of", "with", "by", "from", "up", "about",
    "into", "through", "during", "before", "after", "above", "below", "between",
    "out", "off", "over", "under", "again", "further", "then", "once",
    "here", "there", "when", "where", "why", "how", "all", "each", "every",
    "some", "any", "few", "more", "most", "other", "no", "only", "own", "same",
    "than", "too", "very", "just", "also", "now", "new", "said", "says",
    "one", "two", "first", "also", "like", "even", "much", "many", "well",
    "back", "get", "got", "going", "go", "make", "made",
}

WINDOW_SIZE = 5  # Words before/after the target term
MIN_FREQ = 3
TOP_N = 30


def tokenize(text):
    """Simple word tokenization."""
    return re.findall(r'\b[a-zA-Z]{2,}\b', text.lower())


def extract_collocates(texts, target_terms, window=WINDOW_SIZE):
    """Extract collocates within a window around target terms."""
    target_set = {t.lower() for t in target_terms}
    collocate_counts = Counter()
    target_count = 0
    total_words = 0

    for text in texts:
        words = tokenize(text)
        total_words += len(words)

        for i, word in enumerate(words):
            if word in target_set:
                target_count += 1
                # Window
                start = max(0, i - window)
                end = min(len(words), i + window + 1)
                for j in range(start, end):
                    if j != i:
                        w = words[j]
                        if w not in STOPWORDS and w not in target_set and len(w) > 2:
                            collocate_counts[w] += 1

    return collocate_counts, target_count, total_words


def compute_pmi(collocate_counts, target_count, total_words, word_freqs):
    """Compute PMI for each collocate."""
    results = []
    for word, cooc_count in collocate_counts.items():
        if cooc_count < MIN_FREQ:
            continue

        word_freq = word_freqs.get(word, 1)
        # PMI = log2(P(word,target) / (P(word) * P(target)))
        p_cooc = cooc_count / total_words
        p_word = word_freq / total_words
        p_target = target_count / total_words

        if p_word > 0 and p_target > 0:
            pmi = log2(p_cooc / (p_word * p_target))
            results.append({
                "word": word,
                "count": cooc_count,
                "pmi": pmi,
                "frequency": word_freq,
            })

    results.sort(key=lambda x: x["pmi"], reverse=True)
    return results[:TOP_N]


def run_collocations():
    ensure_cl_dirs()

    corpus_path = CL_BALANCED_DIR / "corpus.parquet"
    if not corpus_path.exists():
        raise FileNotFoundError("Balanced corpus not found.")

    df = pd.read_parquet(corpus_path)
    log.info(f"Extracting collocations from {len(df)} texts")

    # Global word frequencies
    all_words = Counter()
    for text in df["text"].fillna(""):
        all_words.update(tokenize(text))

    results = {}

    for pair_id in sorted(df["pair_id"].unique()):
        pdf = df[df["pair_id"] == pair_id]

        pair_results = {}
        for variant in ["russian", "ukrainian"]:
            vdf = pdf[pdf["variant"] == variant]
            if len(vdf) < 10:
                continue

            texts = vdf["text"].fillna("").tolist()
            terms = vdf["matched_term"].unique().tolist()

            collocates, target_n, total_n = extract_collocates(texts, terms)
            pmi_ranked = compute_pmi(collocates, target_n, total_n, all_words)

            pair_results[variant] = {
                "n_texts": len(vdf),
                "target_terms": terms,
                "collocates": pmi_ranked,
            }

        if pair_results:
            results[int(pair_id)] = pair_results
            ru_top = [c["word"] for c in pair_results.get("russian", {}).get("collocates", [])[:5]]
            ua_top = [c["word"] for c in pair_results.get("ukrainian", {}).get("collocates", [])[:5]]
            log.info(f"  Pair {pair_id}: RU=[{', '.join(ru_top)}] UA=[{', '.join(ua_top)}]")

    out_path = CL_EMBEDDINGS_DIR / "collocations.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2)
    log.info(f"Saved collocations: {out_path} ({len(results)} pairs)")

    return results


if __name__ == "__main__":
    run_collocations()
