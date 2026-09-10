"""Tag every corpus row with its detected language — for reporting, not filtering.

The study measures the LATIN-SCRIPT SIGNAL: the text that reaches model
training corpora and shapes tokenizer statistics and embeddings. Tokenizers
do not respect language boundaries, so neither does the measurement — a
Russian-language video writing Latin "vareniki" contributes to the same
weights as a BBC article writing "Kyiv".

Language is therefore recorded, never used to include or exclude:
  * composition — what share of each pair's signal is which language
  * robustness  — how much adoption would move if restricted to English

Writes data/audit/language_tags.parquet (record key + lang + confidence)
and data/audit/language_composition.json.

    python -m pipeline.audit.tag_language
"""

import json
import pathlib

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
OUT = ROOT / "data" / "audit"
MIN_CHARS = 25          # below this, detection is noise; row is tagged "und"


def main() -> int:
    from langdetect import DetectorFactory, detect_langs
    DetectorFactory.seed = 0

    rows, comp = [], {}
    for f in sorted((ROOT / "data" / "store" / "pairs").glob("*.parquet")):
        slug = f.stem
        d = pd.read_parquet(f, columns=["record_id", "source", "text", "variant"])
        langs, conf = [], []
        for t in d.text.fillna("").astype(str):
            t = t.strip()[:400]
            if len(t) < MIN_CHARS:
                langs.append("und"); conf.append(0.0); continue
            try:
                best = detect_langs(t)[0]
                langs.append(best.lang); conf.append(round(best.prob, 3))
            except Exception:                          # noqa: BLE001
                langs.append("und"); conf.append(0.0)
        d = d.assign(lang=langs, lang_conf=conf)
        rows.append(d[["record_id", "lang", "lang_conf"]].assign(pair_slug=slug))

        counts = d.lang.value_counts()
        def ua(sub):
            v = sub.variant.value_counts()
            u, r = v.get("ukrainian", 0), v.get("russian", 0)
            return round(100 * u / (u + r), 1) if u + r else None
        comp[slug] = {
            "rows": int(len(d)),
            "by_language": {L: {"rows": int(n), "ua_pct": ua(d[d.lang == L])}
                            for L, n in counts.head(12).items()},
            "ua_all": ua(d),
            "ua_english_only": ua(d[d.lang == "en"]),
            "non_english_share": round(float((d.lang.isin(["en", "und"]) == False).mean()), 4),
        }
        print(f"{slug:22s} {len(d):8,} rows · non-English "
              f"{100*comp[slug]['non_english_share']:5.1f}% · "
              f"ua {comp[slug]['ua_all']} (en-only {comp[slug]['ua_english_only']})")

    tags = pd.concat(rows, ignore_index=True)
    OUT.mkdir(parents=True, exist_ok=True)
    tags.to_parquet(OUT / "language_tags.parquet", compression="zstd", index=False)
    (OUT / "language_composition.json").write_text(json.dumps(comp, indent=1))
    print(f"\ntagged {len(tags):,} rows across {len(comp)} pairs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
