"""Semantic prosody stage. Import `run(df, terms)`; not a CLI.

Document-level sentiment is the wrong instrument for this corpus: nearly everything is
war or disaster coverage, so both spellings score negative and the comparison is empty.
Semantic prosody asks a narrower question that does discriminate -- of the words that
actually surround the toponym, what proportion carry evaluative load, and does that
differ by spelling?

Uses match_context, the window already extracted around each mention, rather than the
whole document, so the measure is about the immediate environment of the term.

The lexicon is small and explicit rather than a trained model: with no annotated data
for this domain, a transparent word list is auditable and a black-box score is not.
"""
from __future__ import annotations

import re
from collections import Counter

import pandas as pd

NEG = set("""war attack killed death dead destroy destroyed bomb bombing shelling
invasion occupied occupation strike struck missile drone casualties wounded siege
disaster catastrophe crisis threat danger radiation contamination evacuate evacuation
refugee massacre atrocity genocide terror victim victims damage destruction collapse
fire explosion blast conflict fighting battle assault regime propaganda""".split())
POS = set("""heritage culture cultural history historic tradition traditional festival
celebrate celebration monument memorial museum art artist beautiful famous popular
tourism tourist visit restore restoration rebuild revival independence freedom pride
recognition award honour honor achievement победа""".split())


def _net_per_1k(contexts) -> float:
    neg = pos = tot = 0
    for ctx in contexts:
        ws = re.findall(r"[a-z]{3,}", str(ctx).lower())
        tot += len(ws)
        neg += sum(1 for w in ws if w in NEG)
        pos += sum(1 for w in ws if w in POS)
    return (pos - neg) / max(tot, 1) * 1000


BOOT_N = 1000          # resamples
BOOT_CAP = 20_000      # documents per side actually resampled


def _doc_counts(contexts) -> tuple:
    """(pos, neg, tot) per document — tokenised once, not once per resample."""
    import numpy as np
    pos, neg, tot = [], [], []
    for ctx in contexts:
        ws = re.findall(r"[a-z]{3,}", str(ctx).lower())
        tot.append(len(ws))
        neg.append(sum(1 for w in ws if w in NEG))
        pos.append(sum(1 for w in ws if w in POS))
    return (np.array(pos, dtype=np.int64), np.array(neg, dtype=np.int64),
            np.array(tot, dtype=np.int64))


def _bootstrap_ci(df: pd.DataFrame, terms: list[str], seed: int = 0) -> tuple | None:
    """Percentile CI for the UA-minus-RU difference, resampling DOCUMENTS.

    Resampling documents rather than tokens is the point: sentiment words
    cluster inside a document, so token-level resampling would understate the
    spread and hand back a confident interval for a handful of texts.

    The counts are computed once per document and the resampling then works on
    integers. Re-tokenising inside the loop made a single large pair take longer
    than the entire rest of the analysis.
    """
    import numpy as np
    ua = df[df.variant == "ukrainian"].match_context.fillna("")
    ru = df[df.variant == "russian"].match_context.fillna("")
    if len(ua) < 25 or len(ru) < 25:
        return None
    rng = np.random.default_rng(seed)
    if len(ua) > BOOT_CAP:
        ua = ua.sample(BOOT_CAP, random_state=seed)
    if len(ru) > BOOT_CAP:
        ru = ru.sample(BOOT_CAP, random_state=seed)
    pa, na_, ta = _doc_counts(ua.tolist())
    pb, nb_, tb = _doc_counts(ru.tolist())
    diffs = np.empty(BOOT_N)
    for i in range(BOOT_N):
        ia = rng.integers(0, len(ta), len(ta))
        ib = rng.integers(0, len(tb), len(tb))
        da = (pa[ia].sum() - na_[ia].sum()) / max(ta[ia].sum(), 1) * 1000
        db = (pb[ib].sum() - nb_[ib].sum()) / max(tb[ib].sum(), 1) * 1000
        diffs[i] = da - db
    lo, hi = np.percentile(diffs, [2.5, 97.5])
    return (round(float(lo), 2), round(float(hi), 2))


def run(df: pd.DataFrame, terms: list[str]) -> dict:
    out = {}
    for variant in ("ukrainian", "russian"):
        sub = df[df.variant == variant]
        if len(sub) < 25:
            out[variant] = {"n": len(sub), "insufficient": True}
            continue
        neg = pos = tot = 0
        for ctx in sub.match_context.fillna(""):
            ws = re.findall(r"[a-z]{3,}", str(ctx).lower())
            tot += len(ws)
            neg += sum(1 for w in ws if w in NEG)
            pos += sum(1 for w in ws if w in POS)
        out[variant] = {"n": len(sub), "tokens": tot,
                        "neg_per_1k": round(neg / max(tot, 1) * 1000, 2),
                        "pos_per_1k": round(pos / max(tot, 1) * 1000, 2),
                        "net": round((pos - neg) / max(tot, 1) * 1000, 2)}
    a, b = out.get("ukrainian", {}), out.get("russian", {})
    ci = None
    if a.get("insufficient") or b.get("insufficient"):
        summary = "insufficient documents on one side; not comparable"
    else:
        d = a["net"] - b["net"]
        # Twenty-five documents can clear the guard and still produce a large
        # difference by chance: mykola-hohol reported +22.69/1k off 38 Ukrainian
        # -form documents, judged against a flat +/-0.5 cutoff. Resample the
        # documents to find out how much of that the sample supports, and only
        # name a direction when the interval excludes zero.
        ci = _bootstrap_ci(df, terms)
        if ci is None:
            direction = ("Ukrainian-form contexts are less negative" if d > 0.5 else
                         "Russian-form contexts are less negative" if d < -0.5 else
                         "no meaningful difference")
        elif ci[0] > 0:
            direction = "Ukrainian-form contexts are less negative"
        elif ci[1] < 0:
            direction = "Russian-form contexts are less negative"
        else:
            direction = "no difference the sample can support"
        summary = (f"UA net {a['net']:+.2f}/1k vs RU net {b['net']:+.2f}/1k "
                   f"(difference {d:+.2f}"
                   + (f", 95% CI {ci[0]:+.2f} to {ci[1]:+.2f}" if ci else "")
                   + f"); {direction}")
    return {"method": "explicit lexicon over match_context, per 1k tokens; "
                      "difference bootstrapped over documents (1,000 resamples)",
            "by_variant": out, "ci95": ci, "summary": summary}
