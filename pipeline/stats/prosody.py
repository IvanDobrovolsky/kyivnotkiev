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
    if a.get("insufficient") or b.get("insufficient"):
        summary = "insufficient documents on one side; not comparable"
    else:
        d = a["net"] - b["net"]
        summary = (f"UA net {a['net']:+.2f}/1k vs RU net {b['net']:+.2f}/1k "
                   f"(difference {d:+.2f}); "
                   + ("Ukrainian-form contexts are less negative" if d > 0.5 else
                      "Russian-form contexts are less negative" if d < -0.5 else
                      "no meaningful difference"))
    return {"method": "explicit lexicon over match_context, per 1k tokens",
            "by_variant": out, "summary": summary}
