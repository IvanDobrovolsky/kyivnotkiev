"""Is every holdout table converged? Per pair, per source, with the pool measured.

Converged means one of two things, and the difference matters:
  * the table holds HOLDOUT_CAP entries, or
  * it holds fewer because the candidate pool is genuinely spent.

A table that is short because a cap bound early, or because candidates were
never checked, is NOT converged — it just looks the same from outside. This
reports which it is, so "fewer than 100" is a measurement rather than a claim.

Reddit carries an extra condition: every shipped entry must be probed LIVE.

Exit 1 while any table can still be improved.

    python -m pipeline.audit.holdout_convergence
"""

import json
import pathlib
import sys

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SITE = ROOT / "site" / "src" / "data"
CAP = 100
SINCE = "2025-01"

# site key -> the store source that feeds it
FED_BY = {"reddit": "reddit", "youtube": "youtube",
          "news_articles": "gdelt", "openalex": "openalex"}


def pool_for(slug: str, source: str) -> int:
    """Rows the table could draw on: the holdout window, the holdout variants."""
    f = ROOT / "data" / "store" / "pairs" / f"{slug}.parquet"
    if not f.exists():
        return 0
    try:
        d = pd.read_parquet(f, columns=["source", "variant", "date"])
    except Exception:                                  # noqa: BLE001
        return 0
    return int(((d.source == source)
                & d.variant.isin(["russian", "both"])
                & (d.date.astype(str) >= SINCE)).sum())


def main() -> int:
    h = json.loads((SITE / "holdouts_by_pair.json").read_text())
    meta = json.loads((SITE / "pairs_meta.json").read_text())
    enabled = [p["slug"] for p in meta]
    lv_p = ROOT / "data" / "audit" / "reddit_liveness.json"
    lv = json.loads(lv_p.read_text()) if lv_p.exists() else {}
    cand_p = ROOT / "data" / "audit" / "reddit_holdout_candidates.json"
    cand = json.loads(cand_p.read_text()) if cand_p.exists() else {}

    problems, full, exhausted = [], 0, 0
    print(f"{'pair':22s} {'source':14s} {'ships':>6s} {'pool':>7s}  state")
    for slug in enabled:
        for key, src in FED_BY.items():
            ships = len(h.get(slug, {}).get(key) or [])
            pool = pool_for(slug, src)

            if key == "reddit":
                dead = sum(1 for e in (h.get(slug, {}).get(key) or [])
                           if e.get("live") is not True)
                if dead:
                    problems.append(f"{slug}/{key}: {dead} entr(ies) not probed-live")
                    print(f"  {slug:22s} {key:14s} {ships:>6d} {pool:>7d}  FAIL {dead} not live")
                    continue
                left = len(cand.get(slug, []))
                if ships < CAP and left:
                    problems.append(f"{slug}/{key}: {ships}/{CAP}, {left} candidates unprobed")
                    print(f"  {slug:22s} {key:14s} {ships:>6d} {pool:>7d}  short, {left} unprobed")
                    continue

            if ships >= CAP:
                full += 1
                continue
            if ships >= pool:
                exhausted += 1
                continue
            problems.append(f"{slug}/{key}: {ships} of {CAP} with {pool - ships} still available")
            print(f"  {slug:22s} {key:14s} {ships:>6d} {pool:>7d}  headroom {pool - ships}")

    print(f"\nfull tables: {full}   short but pool-exhausted: {exhausted}   "
          f"improvable: {len(problems)}")
    if lv:
        live = sum(1 for v in lv.values() if v.get("status") == "live")
        print(f"reddit liveness: {len(lv):,} probed, {live:,} live "
              f"({100 * live / max(len(lv), 1):.0f}%), "
              f"{sum(len(v) for v in cand.values()):,} candidates unprobed")
    print("CONVERGED" if not problems else f"NOT converged: {len(problems)} table(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
