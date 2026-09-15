"""Where do the two spellings sit in a model's representation space?

Three questions, measured on real sentences from our own corpus:

  1. SYNONYMY  — in matched English contexts, how far apart are the
     contextual vectors of the Ukrainian and Russian forms? Near-identical
     means the model treats them as pure orthography; separated means the
     spelling carries associations.
  2. REGISTER  — is the Ukrainian form in NON-ENGLISH text closer to its
     English usage than to the Russian form in the same language? That
     would mark the new spelling as an anglophone-media import rather than
     a naturalised word.
  3. ADOPTION  — does that separation track our corpus adoption curve?
     Early switchers vs never-switchers should differ.

Contextual vectors come from XLM-R (multilingual, so language is not a
confound). The token's own subword positions are pooled, so we measure the
name in context rather than the sentence.

    python -m pipeline.stats.embedding_geometry [--pairs kyiv,odesa,...]
"""

import argparse
import json
import pathlib
import re

import numpy as np
import pandas as pd
import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
OUT = ROOT / "data" / "audit"
MODEL = "xlm-roberta-base"
PER_CELL = 60          # sentences per (pair, form, language) cell
MAX_TOK = 96


def sentences_for(df: pd.DataFrame, rx: re.Pattern, n: int) -> list[str]:
    """One sentence per row, centred on the matched form."""
    out = []
    for t in df.text.astype(str):
        m = rx.search(t)
        if not m:
            continue
        s = re.sub(r"\s+", " ", t[max(0, m.start() - 180):m.end() + 180]).strip()
        if len(s) > 40:
            out.append(s)
        if len(out) >= n:
            break
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", default="kyiv,odesa,chornobyl,bakhmut,borscht,lviv")
    a = ap.parse_args()

    import torch
    from transformers import AutoModel, AutoTokenizer
    tok = AutoTokenizer.from_pretrained(MODEL)
    model = AutoModel.from_pretrained(MODEL).eval()

    cfg = {p["slug"]: p for p in yaml.safe_load(open(ROOT / "config" / "pairs.yaml"))["pairs"]}
    tags = None
    tp = OUT / "language_tags.parquet"
    if tp.exists():
        tags = pd.read_parquet(tp).set_index("record_id")["lang"]

    def embed(sents: list[str], term: str) -> np.ndarray | None:
        """Mean of the term's subword vectors, averaged over sentences."""
        vecs = []
        for s in sents:
            enc = tok(s, return_tensors="pt", truncation=True, max_length=MAX_TOK,
                      return_offsets_mapping=True)
            off = enc.pop("offset_mapping")[0].tolist()
            low = s.lower()
            start = low.find(term.lower())
            if start < 0:
                continue
            end = start + len(term)
            idx = [i for i, (a0, b0) in enumerate(off) if a0 < end and b0 > start]
            if not idx:
                continue
            with torch.no_grad():
                h = model(**enc).last_hidden_state[0]
            vecs.append(h[idx].mean(0).numpy())
        if not vecs:
            return None
        v = np.mean(vecs, axis=0)
        return v / (np.linalg.norm(v) + 1e-9)

    cos = lambda a, b: float(np.dot(a, b)) if a is not None and b is not None else None
    report = {}
    for slug in a.pairs.split(","):
        p = cfg.get(slug)
        f = ROOT / "data" / "store" / "pairs" / f"{slug}.parquet"
        if not p or not f.exists():
            continue
        d = pd.read_parquet(f, columns=["record_id", "text", "variant"])
        if tags is not None and d.record_id.map(tags).notna().mean() > 0.5:
            d["lang"] = d.record_id.map(tags).fillna("und")
        else:
            # tags not built yet: detect on this pair's own sample, so the
            # prototype does not depend on the corpus-wide tagging job
            from langdetect import DetectorFactory, detect
            DetectorFactory.seed = 0
            def _lang(t):
                t = str(t).strip()[:300]
                if len(t) < 25:
                    return "und"
                try:
                    return detect(t)
                except Exception:                      # noqa: BLE001
                    return "und"
            d = d.sample(min(len(d), 6000), random_state=0).copy()
            d["lang"] = d.text.map(_lang)
        ua_t, ru_t = str(p["ukrainian"]), str(p["russian"])
        mk = lambda t: re.compile(r"\b" + re.escape(t).replace(r"\ ", r"\s+") + r"\b", re.I)
        cells = {}
        for lang in ("en", "es", "de", "fr", "ru"):
            sub = d[d.lang == lang]
            if len(sub) < 20:
                continue
            for form, term in (("ua", ua_t), ("ru", ru_t)):
                s = sentences_for(sub[sub.variant == ("ukrainian" if form == "ua" else "russian")],
                                  mk(term), PER_CELL)
                if len(s) >= 10:
                    cells[(lang, form)] = embed(s, term)
        r = {"cells": {f"{l}/{fm}": (None if v is None else 1) for (l, fm), v in cells.items()}}
        if ("en", "ua") in cells and ("en", "ru") in cells:
            r["synonymy_en"] = round(cos(cells[("en", "ua")], cells[("en", "ru")]), 4)
        for lang in ("es", "de", "fr", "ru"):
            if (lang, "ua") in cells and ("en", "ua") in cells:
                r[f"{lang}_ua_vs_en_ua"] = round(cos(cells[(lang, "ua")], cells[("en", "ua")]), 4)
            if (lang, "ua") in cells and (lang, "ru") in cells:
                r[f"{lang}_ua_vs_{lang}_ru"] = round(cos(cells[(lang, "ua")], cells[(lang, "ru")]), 4)
        v = d.variant.value_counts()
        u, rr = int(v.get("ukrainian", 0)), int(v.get("russian", 0))
        r["corpus_ua_pct"] = round(100 * u / (u + rr), 1) if u + rr else None
        report[slug] = r
        print(f"{slug:14s} synonymy_en={r.get('synonymy_en')} "
              f"corpus_ua={r['corpus_ua_pct']}% cells={len(cells)}")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "embedding_geometry.json").write_text(json.dumps(report, indent=1))
    print("\nwrote data/audit/embedding_geometry.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
