"""Has every holdout table converged? Report it per pair and per source.

Converged means: at most HOLDOUT_CAP entries, every one of them verified, and
for reddit every one probed LIVE — not merely unprobed. A table that is short
because the corpus holds fewer candidates is converged; a table that is short
because posts were dropped as dead is not, until the pool is exhausted.

Exit 1 when any table could still be improved, so a deploy can gate on it.

    python -m pipeline.audit.holdout_convergence
"""

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
CAP = 100


def main() -> int:
    h = json.loads((ROOT / "site" / "src" / "data" / "holdouts_by_pair.json").read_text())
    lv_p = ROOT / "data" / "audit" / "reddit_liveness.json"
    lv = json.loads(lv_p.read_text()) if lv_p.exists() else {}
    cand_p = ROOT / "data" / "audit" / "reddit_holdout_candidates.json"
    cand = json.loads(cand_p.read_text()) if cand_p.exists() else {}

    print(f"{'pair':22s} {'source':14s} {'shipped':>8s} {'unprobed left':>14s}  status")
    short = 0
    for slug in sorted(h):
        for src in sorted(h[slug]):
            entries = h[slug][src]
            if not isinstance(entries, list):
                continue
            n = len(entries)
            left = len(cand.get(slug, [])) if src == "reddit" else 0
            if src == "reddit":
                not_live = sum(1 for e in entries if e.get("live") is not True)
                if not_live:
                    print(f"  {slug:22s} {src:14s} {n:>8d} {left:>14d}  "
                          f"FAIL: {not_live} not probed-live")
                    short += 1
                    continue
            if n >= CAP:
                continue
            state = "short, pool exhausted" if left == 0 else f"short, {left} candidates unprobed"
            if left:
                short += 1
            print(f"  {slug:22s} {src:14s} {n:>8d} {left:>14d}  {state}")

    probed = len(lv)
    live = sum(1 for v in lv.values() if v.get("status") == "live")
    pool = sum(len(v) for v in cand.values())
    print(f"\nliveness cache: {probed:,} probed, {live:,} live "
          f"({100 * live / max(probed, 1):.0f}%), {pool:,} candidates still unprobed")
    print("converged" if not short else f"NOT converged: {short} table(s) can still be improved")
    return 1 if short else 0


if __name__ == "__main__":
    sys.exit(main())
