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

    # 3. one record per article
    verified = usage.drop_duplicates("text_hash").copy()
    audit["dropped_duplicate_body"] = int(len(usage) - len(verified))
    audit["verified_records"] = len(verified)

    # 1. the body decides; the slug is kept only so the disagreement is auditable
    verified["variant"] = verified.body_variant
    verified["url_claimed"] = verified.url_variant
    verified["reclassified"] = (verified.url_claimed.notna()
                                & (verified.url_claimed != verified.variant))
    audit["reclassified_from_url"] = int(verified.reclassified.sum())
    audit["had_no_url_claim"] = int(verified.url_claimed.isna().sum())

    verified["date"] = pd.to_datetime(verified.date)
    verified["month"] = verified.date.dt.to_period("M").astype(str)
    cols = ["url", "domain", "date", "month", "variant", "body_ua", "body_ru",
            "url_claimed", "reclassified", "text", "text_len", "text_hash"]
    verified = verified[cols].sort_values("date").reset_index(drop=True)

    series = (verified.groupby(["month", "variant"]).size()
              .reset_index(name="articles").sort_values("month"))
    return verified, series, audit


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pair", required=True)
    a = ap.parse_args()

    verified, series, audit = build(a.pair)
    OUT.mkdir(parents=True, exist_ok=True)
    verified.to_parquet(OUT / f"{a.pair}.parquet", compression="zstd", index=False)
    series.to_parquet(OUT / f"{a.pair}_series.parquet", compression="zstd", index=False)

    print(f"=== {a.pair} ===")
    for k, v in audit.items():
        print(f"  {k:<26}{v:>6,}")
    if len(verified):
        print(f"\n  variant split: {verified.variant.value_counts().to_dict()}")
        print(f"  date span    : {verified.date.min().date()} -> {verified.date.max().date()}")
        print(f"  median chars : {int(verified.text_len.median()):,}")
        print(f"  domains      : {verified.domain.nunique()}")
    print(f"\nwrote {OUT / (a.pair + '.parquet')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
