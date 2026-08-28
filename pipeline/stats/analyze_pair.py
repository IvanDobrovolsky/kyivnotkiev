"""Statistical analysis pipeline for one pair. Input: a store pair parquet.

    python -m pipeline.stats.analyze_pair --pair volodymyr-the-great
    python -m pipeline.stats.analyze_pair --all

Stages run in order, each consuming the previous:

  1. dedup    near-duplicate groups; downstream uses canonical rows only
  2. keyness  contrastive vocabulary, computed within source
  3. prosody  which side sits in more evaluative context

Deduplication is first because it changes what every later stage sees: a channel
reusing one description across 44 videos would otherwise contribute 44 votes to the
vocabulary.

Everything is written to data/stats/<pair>/ — the annotated records, each stage's
result, and a run manifest with input checksum and row counts. Re-running with
unchanged input reproduces identical output; the MinHash seed is fixed for that reason.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import pathlib
import sys
from datetime import datetime, timezone

import pandas as pd
import yaml

from pipeline.stats import dedup, keyness, prosody

STORE = pathlib.Path("data/store/pairs")
OUT = pathlib.Path("data/stats")


def pair_terms(slug: str) -> list[str]:
    doc = yaml.safe_load(pathlib.Path("config/pairs.yaml").read_text())
    pairs = doc["pairs"] if isinstance(doc, dict) and "pairs" in doc else doc
    p = next((x for x in pairs if x["slug"] == slug), None)
    if p is None:
        raise SystemExit(f"{slug} is not in config/pairs.yaml")
    return [str(p["ukrainian"]), str(p["russian"])]


def analyse(slug: str, quiet: bool = False) -> dict:
    src = STORE / f"{slug}.parquet"
    if not src.exists():
        raise SystemExit(f"no store file: {src}")
    df = pd.read_parquet(src)
    terms = pair_terms(slug)
    outdir = OUT / slug
    outdir.mkdir(parents=True, exist_ok=True)

    print(f"\n=== {slug} ===")
    print(f"  input {src}  {len(df):,} records")
    print(f"  sources {df.source.value_counts().to_dict()}")
    print(f"  variants {df.variant.value_counts().to_dict()}")

    print("  [1/3] dedup")
    df, d_stats = dedup.run(df, quiet=quiet)
    print(f"        {d_stats['duplicate_groups']:,} groups, "
          f"{d_stats['redundant']:,} redundant ({d_stats['redundant_pct']}%), "
          f"exact hashing would have caught {d_stats['exact_hash_would_catch']:,}")
    df.to_parquet(outdir / "records.parquet", compression="zstd", index=False)

    canon = df[df.is_canonical]
    print(f"  [2/3] keyness on {len(canon):,} canonical records")
    k = keyness.run(canon, terms, quiet=quiet)
    if k["sources_skipped"]:
        print(f"        skipped (too few docs): {k['sources_skipped']}")
    if not k["interpretable"]:
        print(f"        NOT INTERPRETABLE — under 2 usable sources, so a contrast "
              f"cannot be separated from source register")
    else:
        print(f"        usable sources {k['sources_used']}; "
              f"{len(k['robust_ukrainian'])} robust UA terms, "
              f"{len(k['robust_russian'])} robust RU terms")

    print("  [3/3] prosody")
    pr = prosody.run(canon, terms)
    print(f"        {pr['summary']}")

    manifest = {
        "pair": slug,
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "input": str(src),
        "input_sha1": hashlib.sha1(src.read_bytes()).hexdigest()[:16],
        "input_rows": len(df),
        "terms": terms,
        "sources": df.source.value_counts().to_dict(),
        "variants": df.variant.value_counts().to_dict(),
        "dedup": d_stats,
        "keyness": k,
        "prosody": pr,
    }
    (outdir / "analysis.json").write_text(json.dumps(manifest, indent=2, default=str))
    print(f"  wrote {outdir}/records.parquet and analysis.json")
    return manifest


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pair")
    g.add_argument("--all", action="store_true")
    ap.add_argument("--quiet", action="store_true")
    a = ap.parse_args()

    slugs = ([pathlib.Path(f).stem for f in sorted(glob.glob(str(STORE / "*.parquet")))]
             if a.all else [a.pair])
    rows = []
    for s in slugs:
        m = analyse(s, quiet=a.quiet)
        rows.append({"pair": s, "records": m["input_rows"],
                     "redundant_pct": m["dedup"]["redundant_pct"],
                     "sources_usable": len(m["keyness"]["sources_used"]),
                     "interpretable": m["keyness"]["interpretable"]})
    if len(rows) > 1:
        print("\n" + pd.DataFrame(rows).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
