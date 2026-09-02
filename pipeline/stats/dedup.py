"""Near-duplicate detection stage. Import `run(df)`; not a CLI.

Exact hashing on full text only catches byte-identical documents. It caught a channel
reusing one keyword-stuffed description across 44 videos, but missed a syndicated
column that ran on six sites with small differences in site chrome. Contrastive
analysis over a corpus holding 44 copies of one spam description partly measures the
spam, so this runs before anything else.

Character 5-gram shingles -> MinHash -> banded LSH -> exact Jaccard on candidates.
Near-linear; all-pairs cosine would not finish on chornobyl's 223k records. Character
shingles rather than word shingles because the duplicates differ by boilerplate and
punctuation, which word tokenisation hides.

Nothing is deleted. Rows get `dup_group` and `is_canonical` (longest text in the
group); downstream filters to canonical rows and the rest stay inspectable.
"""
from __future__ import annotations

import hashlib
import re
from collections import defaultdict

import numpy as np
import pandas as pd

SHINGLE = 5
NUM_PERM = 128
BANDS = 32
THRESHOLD = 0.80
_PRIME = np.uint64((1 << 61) - 1)


def shingles(text: str, k: int = SHINGLE) -> set:
    t = re.sub(r"\s+", " ", str(text or "").lower()).strip()
    if len(t) < k:
        return {t} if t else set()
    return {t[i:i + k] for i in range(len(t) - k + 1)}


def _minhash(sh: set, a: np.ndarray, b: np.ndarray) -> np.ndarray:
    if not sh:
        return np.full(NUM_PERM, np.iinfo(np.uint64).max, dtype=np.uint64)
    hv = np.array([int(hashlib.md5(s.encode()).hexdigest()[:16], 16) for s in sh],
                  dtype=np.uint64)
    return ((np.outer(a, hv) + b[:, None]) % _PRIME).min(axis=1)


def run(df: pd.DataFrame, threshold: float = THRESHOLD, quiet: bool = False) -> tuple:
    n = len(df)
    rng = np.random.default_rng(20260828)          # fixed seed: same input, same groups
    a = rng.integers(1, int(_PRIME), NUM_PERM, dtype=np.uint64)
    b = rng.integers(0, int(_PRIME), NUM_PERM, dtype=np.uint64)

    sigs = np.zeros((n, NUM_PERM), dtype=np.uint64)
    texts = df.text.values
    for i, t in enumerate(texts):
        sigs[i] = _minhash(shingles(t), a, b)
        if not quiet and i and i % 25000 == 0:
            print(f"    hashed {i:,}/{n:,}", flush=True)

    rows_per_band = NUM_PERM // BANDS

    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    # Verify candidates against the SIGNATURES, not the raw text. The previous
    # version recomputed shingles — regex normalisation and a set build over the
    # full text — for every candidate pair. Reddit boilerplate produces millions
    # of candidates, so verification ran for 50+ minutes of pure re.sub and had
    # no bound. The MinHash estimator (fraction of agreeing signature rows,
    # unbiased, sd ~= sqrt(t(1-t)/128) ~= 0.03 at 128 permutations) needs no
    # text access and vectorises. Same seed, same input -> same groups.
    # Band-by-band instead of one global candidate set. A syndication-heavy pair
    # (zelenskyy: 258k gdelt rows of wire copy) produced enough candidate tuples
    # to OOM-kill the process; per-band processing bounds memory at the largest
    # band. The final partition is the transitive closure of all verified pairs,
    # so processing order cannot change the result — same seed, same groups. A
    # pair surfacing in several bands is re-verified redundantly; that costs a
    # little compute and no correctness.
    confirmed = 0
    n_cand = 0
    for band in range(BANDS):
        buckets = defaultdict(list)
        blk = sigs[:, band * rows_per_band:(band + 1) * rows_per_band]
        for i, row in enumerate(blk):
            buckets[row.tobytes()].append(i)
        pairs_i, pairs_j = [], []
        for idxs in buckets.values():
            if len(idxs) < 2:
                continue
            if len(idxs) > 400:                     # boilerplate bucket: star, don't clique
                pairs_i.extend([idxs[0]] * (len(idxs) - 1))
                pairs_j.extend(idxs[1:])
            else:
                for x in range(len(idxs)):
                    for y in range(x + 1, len(idxs)):
                        pairs_i.append(idxs[x])
                        pairs_j.append(idxs[y])
        if not pairs_i:
            continue
        pi = np.asarray(pairs_i, dtype=np.int64)
        pj = np.asarray(pairs_j, dtype=np.int64)
        n_cand += len(pi)
        est = (sigs[pi] == sigs[pj]).mean(axis=1)
        for i, j, ok in zip(pi, pj, est >= threshold):
            if not ok:
                continue
            ri, rj = find(int(i)), find(int(j))
            if ri != rj:
                parent[max(ri, rj)] = min(ri, rj)
            confirmed += 1

    # Lead-fingerprint pass: wire syndications share their opening verbatim
    # while site chrome varies the tail, which can push full-text Jaccard
    # under the threshold (babyn-yar: 18 surviving copies of one RFE story).
    # The first 300 normalised characters are a deterministic story identity.
    lead = defaultdict(list)
    for i, t in enumerate(texts):
        k = re.sub(r"\s+", " ", str(t or "").lower()).strip()[:300]
        if len(k) >= 120:
            lead[k].append(i)
    for idxs in lead.values():
        for j in idxs[1:]:
            ri, rj = find(idxs[0]), find(j)
            if ri != rj:
                parent[max(ri, rj)] = min(ri, rj)

    groups = defaultdict(list)
    for i in range(n):
        groups[find(i)].append(i)
    dupes = {k: v for k, v in groups.items() if len(v) > 1}
    canonical = {max(m, key=lambda i: len(str(texts[i]))) for m in groups.values()}

    out = df.copy()
    gid = {i: k for k, m in groups.items() for i in m}
    out["dup_group"] = [gid[i] for i in range(n)]
    out["is_canonical"] = [i in canonical for i in range(n)]

    stats = {
        "records": n,
        "candidate_pairs": n_cand,
        "confirmed_pairs": confirmed,
        "duplicate_groups": len(dupes),
        "rows_in_duplicate_groups": sum(len(v) for v in dupes.values()),
        "canonical": len(canonical),
        "redundant": n - len(canonical),
        "redundant_pct": round((n - len(canonical)) / max(n, 1) * 100, 2),
        "exact_hash_would_catch": int(df.text_hash.duplicated().sum()),
        "jaccard_threshold": threshold,
        "largest_groups": [
            {"size": len(m), "sources": out.iloc[m].source.value_counts().to_dict(),
             "sample": str(out.iloc[m[0]].title or out.iloc[m[0]].text)[:90]}
            for m in sorted(dupes.values(), key=len, reverse=True)[:5]],
    }
    return out, stats
