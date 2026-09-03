"""Build the verified GDELT record for a pair: body text decides everything.

PRINCIPLE
---------
The article text is the only authority. Whatever the URL slug claimed, whatever
GDELT's AllNames said, the variant is whichever spelling actually appears in the
prose. A slug is written once and never revised; AllNames is canonicalised across
languages. Both are retrieval hints, not evidence.

Three consequences, applied here:

1. RECLASSIFY, don't reconcile. url_variant is carried through for auditing only.
   A row whose slug says "kiev" over a body saying Kyiv is recorded as ukrainian,
   with no flag and no apology -- the body was always the ground truth.

2. NO USAGE MEANS NO RECORD. An article can match GDELT's retrieval and never
   actually use either spelling: the entity came from a headline that extraction
   dropped, a nav menu, a related-links block. Those are low-quality retrievals,
   not observations of usage. They are removed from the texts AND from the series,
   because counting them as a mention would be counting GDELT's guess rather than
   an author's choice.

3. ONE RECORD PER ARTICLE. Wire copy runs under many mastheads and GDELT re-records
   the same url across dates, so rows are deduplicated on the body hash.

The series is then a *derived* view of the surviving records, not a separate
artifact -- so the chart and the corpus can never disagree.

USAGE
-----
    python -m pipeline.cl.corpus.gdelt_verified --pair volodymyr-the-great
"""
from __future__ import annotations

import argparse
import pathlib

import pandas as pd

TEXTS = pathlib.Path("data/raw/gdelt/texts/article_texts.parquet")
OUT = pathlib.Path("data/cl/corpus/gdelt_verified")


def build(pair: str) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    d = pd.read_parquet(TEXTS)
    d = d[d.pair_slug == pair].copy()
    audit = {"urls_attempted": len(d)}

    fetched = d[d.text.notna()].copy()
    audit["fetch_failed"] = int(len(d) - len(fetched))

    # 2. retrieval without usage is not an observation
    usage = fetched[fetched.body_variant.isin(["ukrainian", "russian", "both"])].copy()
    audit["dropped_no_usage"] = int(len(fetched) - len(usage))

    # 2b. homonym false positives: the match term names a different referent
    # entirely (Odessa TX / Odessa A'zion / The Odessa File for the odesa pair).
    # Patterns come from pairs.yaml homonym_filters and run over the BODY text —
    # the domain-level filter upstream cannot see a Texas story on a national
    # outlet. Measured 2026-08-31: 17% of odesa texts carried Texas markers.
    import re as _re
    import yaml as _yaml
    _cfg = _yaml.safe_load(open("config/pairs.yaml"))
    _pats = [_re.compile(f, _re.I) for p in _cfg["pairs"]
             if p.get("slug") == pair for f in p.get("homonym_filters", [])]
    if _pats:
        _hit = usage.text.astype(str).apply(lambda t: any(r.search(t) for r in _pats))
        audit["dropped_homonym"] = int(_hit.sum())
        usage = usage[~_hit].copy()

    # 3. one record per article
    verified = usage.drop_duplicates("text_hash").copy()
    audit["dropped_duplicate_body"] = int(len(usage) - len(verified))

    # 3b. one record per STORY. Syndication farms republish identical wire copy
    # under dozens of mastheads with only site chrome varying; the first 300
    # normalised characters are the story identity (same rule as stats dedup).
    # Measured 2026-09-03: 2-17% duplicate stories per pair; kyivan-rus adoption
    # was inflated 4.7pp by a farm on its Ukrainian side.
    _lead = (verified.text.astype(str).str.replace(r"\s+", " ", regex=True)
             .str.lower().str.slice(0, 300))
    before_story = len(verified)
    verified = verified.loc[_lead.drop_duplicates().index].copy()
    audit["dropped_duplicate_story"] = int(before_story - len(verified))
    audit["verified_records"] = len(verified)

    # 1. the body decides; the slug is kept only so the disagreement is auditable
    verified["variant"] = verified.body_variant
    verified["url_claimed"] = verified.url_variant
    # Two different things were being conflated. A body containing BOTH spellings does
    # not contradict the slug, it exceeds it -- on babyn-yar, 90 of 99 flagged rows were
    # this, which made the url metric look far less reliable than it is.
    both = verified.variant == "both"
    verified["enriched"] = verified.url_claimed.notna() & both
    verified["reclassified"] = (verified.url_claimed.notna() & ~both
                                & (verified.url_claimed != verified.variant))
    strict = verified[verified.url_claimed.notna() & ~both]
    audit["reclassified_from_url"] = int(verified.reclassified.sum())
    audit["enriched_to_both"] = int(verified.enriched.sum())
    audit["url_agreement_%"] = (round((strict.url_claimed == strict.variant).mean() * 100, 1)
                                if len(strict) else None)
    audit["had_no_url_claim"] = int(verified.url_claimed.isna().sum())

    verified["date"] = pd.to_datetime(verified.date)
    verified["month"] = verified.date.dt.to_period("M").astype(str)
    cols = ["url", "domain", "date", "month", "variant", "body_ua", "body_ru",
            "url_claimed", "reclassified", "enriched", "text", "text_len", "text_hash"]
    verified = verified[cols].sort_values("date").reset_index(drop=True)

    series = (verified.groupby(["month", "variant"]).size()
              .reset_index(name="articles").sort_values("month"))
    return verified, series, audit


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pair")
    g.add_argument("--all-pairs", action="store_true",
                   help="rebuild every pair that has fetched text on disk")
    a = ap.parse_args()

    OUT.mkdir(parents=True, exist_ok=True)
    if a.all_pairs:
        if not TEXTS.exists():
            print("no fetched text yet"); return 0
        pairs = sorted(pd.read_parquet(TEXTS, columns=["pair_slug"]).pair_slug.dropna().unique())
    else:
        pairs = [a.pair]

    rows = []
    for slug in pairs:
        verified, series, audit = build(slug)
        if not len(verified):
            if not a.all_pairs:
                print(f"{slug}: no verified records")
            continue
        verified.to_parquet(OUT / f"{slug}.parquet", compression="zstd", index=False)
        series.to_parquet(OUT / f"{slug}_series.parquet", compression="zstd", index=False)
        vc = verified.variant.value_counts()
        ua, ru = int(vc.get("ukrainian", 0)), int(vc.get("russian", 0))
        rows.append({"pair": slug, "urls": audit["urls_attempted"], "records": len(verified),
                     "ua": ua, "ru": ru, "both": int(vc.get("both", 0)),
                     "ua_%": round(ua / (ua + ru) * 100, 1) if ua + ru else None,
                     "domains": verified.domain.nunique(),
                     "url_agree_%": audit["url_agreement_%"],
                     "chartable": len(verified) >= 30})
        if not a.all_pairs:
            for k, v in audit.items():
                print(f"  {k:<26}{v!s:>8}")
    if rows:
        summary = pd.DataFrame(rows).sort_values("records", ascending=False)
        print(summary.to_string(index=False))
        summary.to_csv(OUT / "_summary.csv", index=False)
        print(f"{int(summary.records.sum()):,} verified records, {len(summary)} pair(s), "
              f"{int(summary.chartable.sum())} above the 30-record chart threshold")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
