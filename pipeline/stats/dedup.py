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
    cand = set()
    for band in range(BANDS):
        buckets = defaultdict(list)
        blk = sigs[:, band * rows_per_band:(band + 1) * rows_per_band]
        for i, row in enumerate(blk):
            buckets[row.tobytes()].append(i)
        for idxs in buckets.values():
            if len(idxs) < 2:
                continue
            if len(idxs) > 400:                     # boilerplate bucket: star, don't clique
                for j in idxs[1:]:
                    cand.add((idxs[0], j))
            else:
                for x in range(len(idxs)):
                    for y in range(x + 1, len(idxs)):
                        cand.add((idxs[x], idxs[y]))

    parent = list(range(n))

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    confirmed = 0
    for i, j in cand:
        si, sj = shingles(texts[i]), shingles(texts[j])
        if not si or not sj:
            continue
        if len(si & sj) / len(si | sj) >= threshold:
            ri, rj = find(i), find(j)
            if ri != rj:
                parent[max(ri, rj)] = min(ri, rj)
            confirmed += 1

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
        "candidate_pairs": len(cand),
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
