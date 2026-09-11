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
him never every always something often big near books million army home live local world
europe didn't didn’t doesn't wasn't weren't isn't aren't won't can't couldn't wouldn't
shouldn't anything everything nothing someone everyone
with from by as is are was were be been being it its he she they them his her their we
you i not no nor so such own same too very can will just should now about into over
after before under above between out up down off again further once here there when
where why how all any both each few more most other some only have has had do does did
would could may might must one two also new like get got go going make made said say
our ours us your yours my mine it's i'm don't that's through back still even really much many
reddit youtube twitter facebook instagram tiktok php www http https com html way life people thing things lot kind stuff
folha reuters rfe rferl afp bbc cnn cnbc tass interfax unian ap-photo getty epa
says see seen come came take took know knew think thought want wanted use used first
last long good best time year years day days www com http https org html video watch
subscribe channel please thanks thank welcome what who whom which while because until
during against among within without upon per via etc""".split())
MIN_COUNT = 4
MIN_DOCS = 25
# Second tier for small pairs. Below MIN_DOCS a single source cannot carry a
# contrast on its own, but three thin sources agreeing on the same direction
# still beat the solo profile, which contrasts against OTHER pairs' corpora and
# so surfaces whatever scraping debris is unique to this one. Same agreement
# rule as the strong tier; only the per-source floor moves.
MIN_DOCS_WEAK = 10
# A term must be spread across DOCUMENTS, not just frequent. MIN_COUNT counts
# pooled tokens, so four hits inside one post qualified a word: borscht shipped
# six of ten Russian chips resting on 2-7 documents, one of them a single
# Motley Fool transcript mentioning an analyst named Borsch.
MIN_DOC_FREQ = 5
# One document contributes at most this many tokens. Without a cap a long text
# that mentions the pair once donates its whole length as "context": 103 SEC
# and legal filings are 18.0% of luhansk's Ukrainian GDELT token mass while
# being 1.2% of its documents, each naming the place a median of ONCE in
# sanctions boilerplate, and the longest is a 79,603-token prospectus with a
# single mention. Fourteen of twenty-five terms on that side were filing
# vocabulary. Capping keeps the row — OFAC writing "Luhansk" is real
# institutional evidence — while stopping it from outvoting a thousand
# ordinary articles.
MAX_TOKENS_PER_DOC = 1_000
MIN_SIDE_DOCS = 3           # documents on the side a term leans to
# A source whose two sides differ in token mass beyond this ratio cannot
# produce a significant score for ANY term: the shared prior is then almost
# entirely the larger side, and the log-odds collapses toward zero. On
# oleksandr-usyk's reddit layer (163:1) the highest z over 4,619 terms is
# +0.33. Such a source carries no information, and counting it as "usable"
# only blocks the sources that do.
MAX_TOKEN_RATIO = 20
MIN_Z = 1.5
TOP_N = 25


# Top function words of the major Romance/Germanic languages: whole non-English
# documents leak into the Reddit/YouTube layers (Spanish coverage of "Lugansk"
# is heavy — the Romance-reservoir finding), and their function words otherwise
# surface as "collocations". Content words from those docs are neutralised by
# the fragment rule below plus these anchors.
NON_EN_STOP = {
    "una", "unos", "unas", "sus", "los", "las", "del", "por", "para", "con",
    "este", "esta", "estos", "estas", "pero", "más", "mas", "como", "qué",
    "que", "qui", "pas", "une", "contre", "russes", "russie", "ukrainien",
    "militaire", "l'ukraine", "sono", "ucrania", "ukrayna", "ancak", "izle",
    "canli", "canl", "een", "tegen", "niet", "voor", "wedstrijd", "ich",
    "cho", "hoy", "desde", "rusos", "rusas", "vov", "mai",
    "les", "des", "dans", "avec", "pour", "sur", "aux", "cette", "nous",
    "vous", "und", "der", "die", "das", "den", "dem", "ein", "eine", "nicht",
    "von", "mit", "für", "auf", "ist", "sich", "dei", "della", "delle",
    "degli", "nel", "nella", "che", "gli", "una", "uma", "dos", "das", "não",
    "por", "são", "fuerzas", "conflicto", "guerra", "contra", "entre",
}


# Reddit match-thread flair markup ([](#sprite1-p8), (#bar-2-green)) and inline
# CSS from badly-scraped HTML shipped as "collocations" (sprite, bar-, icon-,
# background-image) across five pairs. Strip structurally, before tokens exist.
MARKUP = re.compile(r"\[\]\(#[\w-]+\)|\(#[\w-]+\)|\b(?:background-(?:image|position|size|color)|linear-gradient|border-radius)\b[^;\n]*", re.I)


# URLs are not language. Left in, they shred into tokens that read like
# vocabulary: donbas's "isch" is the tbm=isch parameter of a Google image
# search pasted into 1,502 YouTube descriptions, and feodosiia's entire solo
# profile was host names (easybranches, youdao, webpagetranslate). The rows
# themselves are genuine mentions, so the text is cleaned rather than dropped.
URLS = re.compile(r"https?://\S+|\bwww\.\S+"
                  r"|\b[a-z0-9][a-z0-9-]*(?:\.[a-z0-9-]+)*"
                  r"\.(?:com|org|net|edu|gov|ru|ua|io|tv|me|info|"
                  r"co|de|fr|es|it|pl|uk|eu|cz|biz|xyz|online|site)\b\S*", re.I)


def tokenise(text: str, mask) -> list[str]:
    t = str(text or "").lower()
    t = URLS.sub(" ", t)
    t = MARKUP.sub(" ", t)
    for rx in mask:
        t = rx.sub(" ", t)
    # Accented characters are WORD characters, not separators: splitting on
    # them shredded Spanish words into English-looking fragments — "análisis"
    # became "anal", "república" became "blica". Words carrying any non-ASCII
    # letter are dropped whole (this study measures English usage).
    if re.search(r"[à-öø-ÿ]", t):
        t = re.sub(r"[a-zà-öø-ÿ'’-]*[à-öø-ÿ][a-zà-öø-ÿ'’-]*", " ", t)
    out = []
    for w in re.findall(r"[a-z][a-z'’-]{2,}", t):
        # "kiev", "kiev's" and the curly-quote "kiev’s" are one word: unify
        # apostrophes and strip the possessive before counting, or the same
        # term shows up three times in the contrast table.
        w = w.replace("’", "'")
        if w.endswith("'s"):
            w = w[:-2]
        if len(w) >= 3 and w not in STOP and w not in NON_EN_STOP:
            out.append(w)
    return out


def _log_odds(ca: Counter, cb: Counter, da: Counter | None = None,
              db: Counter | None = None) -> dict:
    prior = ca + cb
    na, nb, npr = sum(ca.values()), sum(cb.values()), sum(prior.values())
    out = {}
    if not npr:
        return out
    for w, c in prior.items():
        if c < MIN_COUNT:
            continue
        # Document frequency, when the caller measured it: the word has to be
        # used by several texts on the side it leans to, not repeated inside one.
        if da is not None and db is not None:
            if (da[w] + db[w]) < MIN_DOC_FREQ:
                continue
            # ...and the side it leans TO must itself be several documents.
            # Summing both sides let a term lean to a side that holds it in one
            # text: "myself" shipped for oleksandr-usyk on a single reddit post.
            if (ca[w] / max(na, 1)) >= (cb[w] / max(nb, 1)):
                if da[w] < MIN_SIDE_DOCS:
                    continue
            elif db[w] < MIN_SIDE_DOCS:
                continue
        # Monroe et al. add the SAME alpha_w to both sides. Splitting it in
        # proportion to each side's token mass looks symmetric — it preserves
        # each side's rate — but it is not: the smaller side's y sits far below
        # the shared npr term in the denominator, so the ratio tilts toward
        # whichever corpus has more tokens, for every word.
        #
        # Measured on borscht/gdelt, where the Ukrainian side holds 9.9x the
        # tokens: of 1,447 words whose true per-million rates match within 15%,
        # the proportional prior scored 100% of them as Ukrainian-leaning,
        # median z +4.00. With alpha_w shared they centre on zero, 51.2%
        # positive. The bias is a mis-centering, not a tuning choice — it holds
        # at every alpha_0 tried.
        ya, yb = ca[w] + c + 0.01, cb[w] + c + 0.01
        d = math.log(ya / (na + npr - ya)) - math.log(yb / (nb + npr - yb))
        out[w] = (d / math.sqrt(1.0 / ya + 1.0 / yb), ca[w], cb[w])
    return out


def run(df: pd.DataFrame, terms: list[str], quiet: bool = False) -> dict:
    # The full phrase AND each of its words: masking only "Igor Sikorsky"
    # let "sikorsky" alone ship as that pair's own top collocation (self-echo
    # in nine pairs). The label is not evidence, in any of its parts.
    mask = [re.compile(r"\b" + r"[\s\-_,.]+".join(re.escape(w) for w in t.split()) + r"\b", re.I)
            for t in terms]
    mask += [re.compile(r"\b" + re.escape(w) + r"\b", re.I)
             for t in terms for w in str(t).split() if len(w) >= 3]
    # Hashtag forms run the words together, so a word-bounded mask misses them:
    # "#oleksandrusyk" is in 19.0% of that pair's documents and shipped as its
    # #2 YouTube term at z=17.31 — the pair's own name scoring as its own
    # collocation.
    mask += [re.compile(r"\b" + re.escape("".join(str(t).split())) + r"\b", re.I)
             for t in terms if len(str(t).split()) > 1]
    def scan(floor: int) -> tuple[dict, dict]:
      per_source, skipped = {}, {}
      for src, g in df.groupby("source"):
        ua, ru = g[g.variant == "ukrainian"], g[g.variant == "russian"]
        if len(ua) < floor or len(ru) < floor:
            skipped[src] = {"ukrainian": len(ua), "russian": len(ru)}
            continue
        ca, da = Counter(), Counter()
        for t in ua.text:
            _tk = tokenise(t, mask)[:MAX_TOKENS_PER_DOC]
            ca.update(_tk); da.update(set(_tk))
        cb, db = Counter(), Counter()
        for t in ru.text:
            _tk = tokenise(t, mask)[:MAX_TOKENS_PER_DOC]
            cb.update(_tk); db.update(set(_tk))
        _na, _nb = sum(ca.values()), sum(cb.values())
        _ratio = max(_na, _nb) / max(min(_na, _nb), 1)
        if _ratio > MAX_TOKEN_RATIO:
            skipped[src] = {"ukrainian": len(ua), "russian": len(ru),
                            "token_ratio": round(_ratio, 1),
                            "reason": "token imbalance beyond MAX_TOKEN_RATIO"}
            continue
        sc = _log_odds(ca, cb, da, db)
        ranked = sorted(sc.items(), key=lambda kv: -kv[1][0])
        per_source[src] = {
            "n_ukrainian": len(ua), "n_russian": len(ru), "terms_scored": len(sc),
            "ukrainian": [{"word": w, "z": round(z, 2), "n_ua": na, "n_ru": nb}
                          for w, (z, na, nb) in ranked if z >= MIN_Z][:TOP_N],
            "russian": [{"word": w, "z": round(z, 2), "n_ua": na, "n_ru": nb}
                        for w, (z, na, nb) in ranked[::-1] if z <= -MIN_Z][:TOP_N],
            "_scores": sc,
        }
      return per_source, skipped

    per_source, skipped = scan(MIN_DOCS)
    tier, floor_used = "robust", MIN_DOCS
    if len(per_source) < 2:
        weak, weak_skipped = scan(MIN_DOCS_WEAK)
        if len(weak) >= 2:
            per_source, skipped = weak, weak_skipped
            tier, floor_used = "exploratory", MIN_DOCS_WEAK

    usable = sorted(per_source)
    robust, robust_src = {}, {}
    if len(usable) >= 2:
        allw = set().union(*[set(per_source[s]["_scores"]) for s in usable])
        for w in allw:
            hits = [(s, per_source[s]["_scores"][w][0]) for s in usable
                    if w in per_source[s]["_scores"]]
            zs = [z for _, z in hits]
            # Two sources must AGREE, and none may contradict — but a source
            # that merely fails to reach the threshold no longer vetoes.
            #
            # Requiring every scoring source to clear MIN_Z made one degenerate
            # source able to silence the rest. oleksandr-usyk's reddit layer is
            # 163:1 in tokens, so the shared prior is 99.4% one side and the
            # HIGHEST z it can produce across 4,619 terms is +0.33 — even for a
            # word with 1,962 tokens on one side and none on the other. That
            # vetoed all 1,174 terms its YouTube layer supports, and the one
            # chip that survived did so by appearing in a single document.
            pos = sum(1 for z in zs if z >= MIN_Z)
            neg = sum(1 for z in zs if z <= -MIN_Z)
            if (pos >= 2 and neg == 0) or (neg >= 2 and pos == 0):
                robust[w] = sum(zs) / len(zs)
                # Record WHICH sources carried the term. The exporter used to
                # recover this from each source's top-25 display list, but
                # robustness is decided over every scored term, so a term robust
                # in four sources could ship with an empty source list — and the
                # page then printed the solo-tier disclaimer ("measured against
                # the cross-pair background") on a four-source pair.
                robust_src[w] = [s for s, z in hits if abs(z) >= MIN_Z]
    rr = sorted(robust.items(), key=lambda kv: -kv[1])
    for s in per_source:
        per_source[s].pop("_scores")

    # One-sided pairs: when the Ukrainian side is too thin to contrast, the
    # dominant form's discourse is still the signal — profile it against a
    # background sampled from the other pairs' corpora ("what company keeps
    # the Russian form alive"). Same log-odds machinery, different baseline.
    solo = []
    solo_variant = None
    if len(usable) < 2:
        counts = df.variant.value_counts()
        solo_variant = ("russian" if counts.get("russian", 0) >= counts.get("ukrainian", 0)
                        else "ukrainian")
        own = df[df.variant == solo_variant]
        if len(own) >= MIN_DOCS:
            import glob as _glob
            import pandas as _pd
            bg_frames = []
            here = df.pair_slug.iloc[0] if "pair_slug" in df.columns and len(df) else ""
            for f in sorted(_glob.glob("data/stats/*/records.parquet")):
                if f"/{here}/" in f:
                    continue
                try:
                    bg_frames.append(_pd.read_parquet(f, columns=["text"]).sample(
                        n=1500, random_state=7, replace=False))
                except Exception:                      # noqa: BLE001
                    continue
                if len(bg_frames) >= 8:
                    break
            if bg_frames:
                bg = _pd.concat(bg_frames, ignore_index=True)
                ca = Counter(w for t in own.text for w in tokenise(t, mask))
                cb = Counter(w for t in bg.text.dropna() for w in tokenise(t, mask))
                sc = _log_odds(ca, cb)
                solo = [{"word": w, "mean_z": round(z, 2)}
                        for w, (z, *_) in sorted(sc.items(), key=lambda kv: -kv[1][0])
                        if z >= MIN_Z][:TOP_N]

    return {
        "method": "log-odds ratio, informative Dirichlet prior (Monroe et al. 2008), within source",
        "solo_variant": solo_variant,
        "solo_terms": solo,
        "tier": tier if len(usable) >= 2 else ("solo" if solo else "none"),
        "min_docs_per_side": floor_used, "min_term_count": MIN_COUNT, "min_abs_z": MIN_Z,
        "sources_used": usable, "sources_skipped": skipped,
        "per_source": per_source,
        "robust_ukrainian": [{"word": w, "mean_z": round(z, 2),
                              "sources": robust_src.get(w, [])}
                             for w, z in rr if z >= MIN_Z][:TOP_N],
        "robust_russian": [{"word": w, "mean_z": round(z, 2),
                            "sources": robust_src.get(w, [])}
                           for w, z in rr[::-1] if z <= -MIN_Z][:TOP_N],
        "interpretable": len(usable) >= 2,
    }
