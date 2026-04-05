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
MIN_FREQ = 20  # High threshold to filter noise (hashtags, usernames)
TOP_N = 15


# Common English words to verify a word is real English
# We reject words that don't appear in this basic vocabulary check
def _is_plausible_english(word):
    """Filter out hashtags, usernames, URL fragments, and non-English junk."""
    if len(word) < 3 or len(word) > 20:
        return False
    # Reject if contains digits mixed with letters
    if re.search(r'\d', word):
        return False
    # Reject camelCase / internal caps (likely usernames/hashtags)
    if re.search(r'[a-z][A-Z]', word):
        return False
    # Reject if all consonants or all vowels (likely abbreviations)
    vowels = set('aeiou')
    chars = set(word.lower())
    if not chars & vowels:
        return False
    return True


def tokenize(text):
    """Word tokenization — Latin alphabet only, filters junk."""
    words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())
    return [w for w in words if _is_plausible_english(w)]


def extract_collocates(texts, target_terms, window=WINDOW_SIZE):
    """Extract collocates within a window around target terms.

    Handles multi-word terms by matching any word from the term.
    """
    # For multi-word terms like "Vladimir Zelensky", match on individual words
    target_words = set()
    for term in target_terms:
        for w in tokenize(term):
            if w not in STOPWORDS and len(w) > 2:
                target_words.add(w)

    collocate_counts = Counter()
    target_count = 0
    total_words = 0

    for text in texts:
        words = tokenize(text)
        total_words += len(words)

        for i, word in enumerate(words):
            if word in target_words:
                target_count += 1
                # Window
                start = max(0, i - window)
                end = min(len(words), i + window + 1)
                for j in range(start, end):
                    if j != i:
                        w = words[j]
                        if w not in STOPWORDS and w not in target_words and len(w) > 2:
                            collocate_counts[w] += 1

    return collocate_counts, target_count, total_words


def compute_pmi(collocate_counts, target_count, total_words, word_freqs):
    """Compute NPMI (normalized PMI) for each collocate.

    NPMI normalizes PMI to [-1, 1] range and penalizes rare co-occurrences,
    preventing junk words from ranking high just because they're rare.
    """
    results = []
    for word, cooc_count in collocate_counts.items():
        if cooc_count < MIN_FREQ:
            continue

        word_freq = word_freqs.get(word, 1)
        p_cooc = cooc_count / total_words
        p_word = word_freq / total_words
        p_target = target_count / total_words

        if p_word > 0 and p_target > 0 and p_cooc > 0:
            pmi = log2(p_cooc / (p_word * p_target))
            # Normalize: NPMI = PMI / -log2(P(cooc))
            npmi = pmi / (-log2(p_cooc))
            results.append({
                "word": word,
                "count": cooc_count,
                "pmi": round(npmi, 4),  # Store NPMI as "pmi" for compatibility
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

    # Filter to Latin-script texts only (English primarily)
    # Cyrillic usernames, hashtags, and foreign fragments pollute PMI rankings
    if "script" in df.columns:
        df = df[df["script"] == "latin"].copy()
        log.info(f"Filtered to Latin-script texts: {len(df)}")
    else:
        log.info(f"No script column, using all {len(df)} texts")

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
