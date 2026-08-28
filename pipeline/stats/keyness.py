"""Contrastive vocabulary stage. Import `run(df, terms)`; not a CLI.

Log-odds ratio with an informative Dirichlet prior (Monroe, Colaresi & Quinn 2008),
z-scored. PMI over-rewards rare words -- a term appearing twice on one side and never
on the other scores enormously on almost no evidence, which is how "download" and
"commons" reached an earlier top-10. The prior is the pooled corpus, so a word is
distinctive only if it exceeds the rate the pooled corpus predicts.

Computed WITHIN each source. Document length varies ~200x by source (gdelt ~3,600
chars, openalex ~134) and variants are unevenly spread across sources, so a pooled
contrast partly measures register -- news prose vs video descriptions -- rather than
spelling. A term is reported as robust only when it leans the same way in two or more
sources, which is what separates a discourse difference from a source artefact.

The toponym and its variants are masked: they are the label, not evidence.
"""
from __future__ import annotations

import math
import re
from collections import Counter

import pandas as pd

STOP = set("""a an the and or but if then than that this these those of in on at to for
with from by as is are was were be been being it its he she they them his her their we
you i not no nor so such own same too very can will just should now about into over
after before under above between out up down off again further once here there when
where why how all any both each few more most other some only have has had do does did
would could may might must one two also new like get got go going make made said say
says see seen come came take took know knew think thought want wanted use used first
last long good best time year years day days www com http https org html video watch
subscribe channel please thanks thank welcome what who whom which while because until
during against among within without upon per via etc""".split())
MIN_COUNT = 4
MIN_DOCS = 25
MIN_Z = 1.5
TOP_N = 25


def tokenise(text: str, mask) -> list[str]:
    t = str(text or "").lower()
    for rx in mask:
        t = rx.sub(" ", t)
    return [w for w in re.findall(r"[a-z][a-z'’-]{2,}", t) if w not in STOP]


def _log_odds(ca: Counter, cb: Counter) -> dict:
    prior = ca + cb
    na, nb, npr = sum(ca.values()), sum(cb.values()), sum(prior.values())
    out = {}
    if not npr:
        return out
    for w, c in prior.items():
        if c < MIN_COUNT:
            continue
        a0, b0 = c * (na / npr), c * (nb / npr)
        ya, yb = ca[w] + a0 + 0.01, cb[w] + b0 + 0.01
        d = math.log(ya / (na + npr - ya)) - math.log(yb / (nb + npr - yb))
        out[w] = (d / math.sqrt(1.0 / ya + 1.0 / yb), ca[w], cb[w])
    return out


def run(df: pd.DataFrame, terms: list[str], quiet: bool = False) -> dict:
    mask = [re.compile(r"\b" + r"[\s\-_,.]+".join(re.escape(w) for w in t.split()) + r"\b", re.I)
            for t in terms]
    per_source, skipped = {}, {}
    for src, g in df.groupby("source"):
        ua, ru = g[g.variant == "ukrainian"], g[g.variant == "russian"]
        if len(ua) < MIN_DOCS or len(ru) < MIN_DOCS:
            skipped[src] = {"ukrainian": len(ua), "russian": len(ru)}
            continue
        ca = Counter(w for t in ua.text for w in tokenise(t, mask))
        cb = Counter(w for t in ru.text for w in tokenise(t, mask))
        sc = _log_odds(ca, cb)
        ranked = sorted(sc.items(), key=lambda kv: -kv[1][0])
        per_source[src] = {
            "n_ukrainian": len(ua), "n_russian": len(ru), "terms_scored": len(sc),
            "ukrainian": [{"word": w, "z": round(z, 2), "n_ua": na, "n_ru": nb}
                          for w, (z, na, nb) in ranked if z >= MIN_Z][:TOP_N],
            "russian": [{"word": w, "z": round(z, 2), "n_ua": na, "n_ru": nb}
                        for w, (z, na, nb) in ranked[::-1] if z <= -MIN_Z][:TOP_N],
            "_scores": sc,
        }

    usable = sorted(per_source)
    robust = {}
    if len(usable) >= 2:
        allw = set().union(*[set(per_source[s]["_scores"]) for s in usable])
        for w in allw:
            zs = [per_source[s]["_scores"][w][0] for s in usable if w in per_source[s]["_scores"]]
            if len(zs) >= 2 and (all(z >= MIN_Z for z in zs) or all(z <= -MIN_Z for z in zs)):
                robust[w] = sum(zs) / len(zs)
    rr = sorted(robust.items(), key=lambda kv: -kv[1])
    for s in per_source:
        per_source[s].pop("_scores")

    return {
        "method": "log-odds ratio, informative Dirichlet prior (Monroe et al. 2008), within source",
        "min_docs_per_side": MIN_DOCS, "min_term_count": MIN_COUNT, "min_abs_z": MIN_Z,
        "sources_used": usable, "sources_skipped": skipped,
        "per_source": per_source,
        "robust_ukrainian": [{"word": w, "mean_z": round(z, 2)} for w, z in rr if z >= MIN_Z][:TOP_N],
        "robust_russian": [{"word": w, "mean_z": round(z, 2)} for w, z in rr[::-1] if z <= -MIN_Z][:TOP_N],
        "interpretable": len(usable) >= 2,
    }
