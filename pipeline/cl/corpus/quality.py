"""Per-pair data completeness and quality report, rendered on the pair page.

Answers three questions per source, from measured numbers only:
  COMPLETENESS  what could the source give, and how much do we actually hold?
  VALIDITY      does a counted "mention" correspond to an attested English string?
  LINKABILITY   can a reader click through and check the record themselves?

A source that cannot be checked is reported as such rather than shown as a bare
count, because a count nobody can verify is the thing this project keeps finding
to be wrong.

Usage:
    python -m pipeline.cl.corpus.quality --pair volodymyr-the-great
"""

import argparse
import json
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent.parent
PAIRS_DIR = ROOT / "data" / "corpus" / "pairs"
DATASET = ROOT / "dataset"
OUT = ROOT / "site" / "src" / "data" / "corpus_quality.json"

# sources that carry a metric but no text, so they can never enter a corpus
METRIC_ONLY = {
    "wikipedia": "pageviews only — no article text stored",
    "trends":    "search-interest index — no text exists",
    "ngrams":    "frequency counts — no text exists",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair", required=True)
    args = ap.parse_args()
    slug = args.pair

    src = pd.read_csv(PAIRS_DIR / f"{slug}_sources.csv")
    corpus = pd.read_parquet(PAIRS_DIR / f"{slug}.parquet")
    man = pd.read_parquet(PAIRS_DIR / f"{slug}_manifest.parquet")

    rows = []
    for _, r in src.iterrows():
        name = r["source"].replace("_full", "").replace("_articles", "").replace("_census", "")
        scanned, matched, kept = int(r.scanned), int(r.matched), int(r.kept)
        sub = man[man.source == name]
        in_corpus = len(sub)
        linked = int((sub.url.str.len() > 0).sum()) if len(sub) else 0
        rows.append({
            "source": name, "raw_source": r["source"],
            "scanned": scanned, "exact_match": matched, "kept": kept,
            "in_corpus": in_corpus, "linked": linked,
            "match_rate": round(100 * matched / scanned, 3) if scanned else None,
            "linkable": bool(linked and linked == in_corpus),
        })
    # collapse duplicated source names (gdelt appears twice: counts + articles)
    # A logical source can span several raw directories (gdelt counts + gdelt
    # articles), so sum them — but only for occurrences after the first, or the
    # seed row gets counted twice.
    agg = {}
    for r in rows:
        name = r["source"]
        if name not in agg:
            agg[name] = dict(r, raw_sources=[r["raw_source"]])
        else:
            for k in ("scanned", "exact_match", "kept"):
                agg[name][k] += r[k]
            agg[name]["raw_sources"].append(r["raw_source"])
    merged = []
    for name, a in agg.items():
        sub = man[man.source == name]
        a["in_corpus"] = len(sub)
        a["linked"] = int((sub.url.str.len() > 0).sum()) if len(sub) else 0
        a["linkable"] = bool(a["linked"] and a["linked"] == a["in_corpus"])
        a["match_rate"] = round(100 * a["exact_match"] / a["scanned"], 3) if a["scanned"] else None
        a.pop("raw_source", None)
        merged.append(a)
    merged.sort(key=lambda x: -x["in_corpus"])

    caveats = []
    # GDELT: counts without text, and translingual normalisation
    gpath = DATASET / "raw_gdelt.parquet"
    if gpath.exists():
        g = pd.read_parquet(gpath)
        gn = g[g.pair_slug == slug]
        if len(gn):
            total = int(gn["count"].sum())
            ua = int(gn[gn.source_domain.str.endswith(".ua", na=False)]["count"].sum())
            ru = int(gn[gn.source_domain.str.endswith(".ru", na=False)]["count"].sum())
            with_text = int(man[man.source == "gdelt"].shape[0])
            caveats.append({
                "source": "gdelt", "severity": "high",
                "text": (f"{total:,} counted mentions but only {with_text} have retrievable text "
                         f"({100*(1-with_text/max(total,1)):.1f}% unverifiable). "
                         f"{100*(ua+ru)/max(total,1):.1f}% sit on .ua/.ru domains "
                         f"({ua:,} / {ru:,}), which publish in Ukrainian and Russian — GDELT "
                         f"normalises entity names across languages, so those counts are not "
                         f"attested English strings.")})
    for name, why in METRIC_ONLY.items():
        p = DATASET / f"raw_{name}.parquet"
        if p.exists():
            d = pd.read_parquet(p)
            n = int((d.pair_slug == slug).sum())
            if n:
                caveats.append({"source": name, "severity": "info",
                                "text": f"{n:,} rows for this pair, but {why}. Contributes to the "
                                        f"adoption metric, never to the corpus."})
    dropped = int(src.kept.sum()) - len(corpus)
    payload = {
        "pair": slug,
        "corpus_records": len(corpus),
        "kept_before_dedup": int(src.kept.sum()),
        "dropped_duplicate_context": dropped,
        "variant": corpus.variant.value_counts().to_dict(),
        "sources": merged,
        "caveats": caveats,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    existing = json.loads(OUT.read_text()) if OUT.exists() else {}
    existing[slug] = payload
    OUT.write_text(json.dumps(existing))

    log.info(f"{slug}: {len(corpus):,} corpus records ({dropped:,} dropped as duplicate context)")
    for m_ in merged:
        lk = "linkable" if m_["linkable"] else ("no links" if m_["in_corpus"] else "—")
        log.info(f"  {m_['source']:<12} scanned={m_['scanned']:>9,} match={m_['exact_match']:>6,} "
                 f"corpus={m_['in_corpus']:>5,}  {lk}")
    for c in caveats:
        log.info(f"  [{c['severity']}] {c['source']}: {c['text'][:110]}…")


if __name__ == "__main__":
    main()
