"""End-to-end build of the verified GDELT record for one pair, several, or all.

    python -m pipeline.build_gdelt_verified --pair babyn-yar
    python -m pipeline.build_gdelt_verified --pairs kyiv,odesa,chornobyl
    python -m pipeline.build_gdelt_verified --all
    python -m pipeline.build_gdelt_verified --all --dry-run     # show the work, fetch nothing

For each pair it runs the same three steps in order:

  1. fetch the ATTESTED urls        (spelling is in the url path)
  2. fetch the UNATTESTED urls      (matched via AllNames; spelling unknown until read)
  3. build the verified record      (body decides the variant; no-usage rows dropped)

Both pools are fetched because the series and the corpus must come from the same
records -- a pair built from one pool and charted from the other would disagree with
itself. Fetching is resumable via the url ledger, so re-running skips completed urls
and only picks up what is missing; interrupting is safe.

The site consumes the output automatically: `pipeline.export_site_data` prefers
`data/cl/corpus/gdelt_verified/<slug>.parquet` over the url-based series and over the
legacy holdout parquets, so `python -m pipeline.rebuild` is all that is needed after.

Ordering is smallest-pair-first so a long run produces complete pairs early rather
than leaving everything half-done.
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import yaml

PY = sys.executable
FINAL = Path("data/raw/gdelt/mentions_v2/gdelt_mentions_final.parquet")
MATCHED = Path("data/raw/gdelt/mentions_v2/gdelt_mentions_matched.parquet")
VERIFIED = Path("data/cl/corpus/gdelt_verified")


def enabled_pairs() -> list[str]:
    doc = yaml.safe_load(Path("config/pairs.yaml").read_text())
    pairs = doc["pairs"] if isinstance(doc, dict) and "pairs" in doc else doc
    return [p["slug"] for p in pairs if p.get("enabled")]


def workload() -> pd.DataFrame:
    """Urls per pair in each pool, so the run can be ordered and estimated."""
    att = pd.read_parquet(FINAL, columns=["url", "pair_slug"])
    m = pd.read_parquet(MATCHED, columns=["url", "pair_slug", "native_en", "url_match"])
    una = m[m.native_en & ~m.url_match].drop_duplicates("url")
    una = una[~una.url.isin(set(att.url)) & una.pair_slug.notna()]
    t = pd.DataFrame({"attested": att.groupby("pair_slug").size(),
                      "unattested": una.groupby("pair_slug").size()}).fillna(0).astype(int)
    t["total"] = t.attested + t.unattested
    return t


def run(cmd: list[str]) -> int:
    r = subprocess.run(cmd, capture_output=True, text=True)
    tail = [x for x in r.stdout.strip().split("\n") if x.strip()][-1:] or [""]
    if r.returncode:
        print(f"      FAILED: {(r.stderr or r.stdout).strip().split(chr(10))[-1][:160]}")
    else:
        print(f"      {tail[0][:150]}")
    return r.returncode


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--pair")
    g.add_argument("--pairs", help="comma-separated slugs")
    g.add_argument("--all", action="store_true")
    ap.add_argument("--concurrency", type=int, default=32)
    ap.add_argument("--dry-run", action="store_true", help="print the plan and stop")
    ap.add_argument("--skip-fetch", action="store_true",
                    help="rebuild verified records from text already on disk")
    a = ap.parse_args()

    enabled = enabled_pairs()
    if a.pair:
        targets = [a.pair]
    elif a.pairs:
        targets = [x.strip() for x in a.pairs.split(",") if x.strip()]
    else:
        targets = enabled
    unknown = [t for t in targets if t not in enabled]
    if unknown:
        print(f"not enabled in config/pairs.yaml: {', '.join(unknown)}", file=sys.stderr)
        return 1

    w = workload()
    plan = w.reindex(targets).fillna(0).astype(int).sort_values("total")   # smallest first
    plan["est_min"] = (plan.total / 11.8 / 60).round(1)
    print(plan.to_string())
    print(f"\n{len(plan)} pair(s), {int(plan.total.sum()):,} urls, "
          f"~{plan.est_min.sum():.0f} min at 11.8 url/s")
    if a.dry_run:
        return 0

    t0 = time.time()
    for i, slug in enumerate(plan.index, 1):
        n = int(plan.loc[slug, "total"])
        print(f"\n[{i}/{len(plan)}] {slug}  ({n:,} urls)", flush=True)
        if not a.skip_fetch:
            print("   attested…", flush=True)
            run([PY, "-W", "ignore", "-m", "pipeline.ingestion.gdelt_fetch_texts",
                 "--pair", slug, "--concurrency", str(a.concurrency)])
            print("   unattested…", flush=True)
            run([PY, "-W", "ignore", "-m", "pipeline.ingestion.gdelt_fetch_texts",
                 "--pair", slug, "--unattested", "--concurrency", str(a.concurrency)])
        print("   building verified record…", flush=True)
        run([PY, "-m", "pipeline.cl.corpus.gdelt_verified", "--pair", slug])

    print(f"\nfinished in {(time.time() - t0) / 60:.1f} min")
    rows = []
    for f in sorted(VERIFIED.glob("*.parquet")):
        if f.stem.endswith("_series"):
            continue
        d = pd.read_parquet(f)
        vc = d.variant.value_counts()
        ua, ru = int(vc.get("ukrainian", 0)), int(vc.get("russian", 0))
        rows.append({"pair": f.stem, "records": len(d), "ua": ua, "ru": ru,
                     "both": int(vc.get("both", 0)),
                     "ua_%": round(ua / (ua + ru) * 100, 1) if ua + ru else None,
                     "domains": d.domain.nunique(),
                     "chartable": len(d) >= 30})
    if rows:
        summary = pd.DataFrame(rows).sort_values("records", ascending=False)
        print("\n" + summary.to_string(index=False))
        VERIFIED.mkdir(parents=True, exist_ok=True)
        summary.to_csv(VERIFIED / "_summary.csv", index=False)
        print(f"\n{int(summary.records.sum()):,} verified records across {len(summary)} pair(s); "
              f"{int(summary.chartable.sum())} meet the 30-record chart threshold")
    print("\nnext: python -m pipeline.rebuild")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
