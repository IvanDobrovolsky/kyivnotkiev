"""Assert what a reader sees, per pair, before anything ships.

Every failure this checks was found by the user on the live site after I had
reported the work done. Each one is visible by opening a single pair, and each
one was already measurable at export time — so it belongs in a gate, not in a
report I write afterwards.

    python -m pipeline.audit.site_invariants

Exits 1 listing every violation.
"""

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SITE = ROOT / "site" / "src" / "data"
CAP = 100


def main() -> int:
    key = json.loads((SITE / "cl_keyness.json").read_text())
    clus = json.loads((SITE / "cl_clusters.json").read_text())
    hold = json.loads((SITE / "holdouts_by_pair.json").read_text())
    cand_p = ROOT / "data" / "audit" / "reddit_holdout_candidates.json"
    cand = json.loads(cand_p.read_text()) if cand_p.exists() else {}

    bad: list = []

    # 1. Every displayed chip carries a gloss. A bare token is not a finding.
    for slug, p in key.items():
        for side in ("ua", "ru", "solo"):
            for x in (p.get(side) or []):
                if not (x.get("g") or "").strip():
                    bad.append(f"{slug}: chip '{x.get('w')}' has no gloss")

    # 2. Every displayed chip quotes an example that contains it.
    for slug, p in key.items():
        for side in ("ua", "ru", "solo"):
            for x in (p.get(side) or []):
                ex = (x.get("ex") or {}).get("t") if isinstance(x.get("ex"), dict) else None
                if ex and str(x.get("w", "")).lower() not in str(ex).lower():
                    bad.append(f"{slug}: chip '{x.get('w')}' quotes a text not containing it")

    # 3. Every cluster has a description, and it is not just the label again.
    for slug, p in clus.items():
        cs = p.get("clusters") if isinstance(p, dict) else p
        if isinstance(cs, dict):
            cs = list(cs.values())
        for c in (cs or []):
            if not isinstance(c, dict):
                continue
            desc = (c.get("desc") or "").strip()
            label = str(c.get("gloss") or c.get("label") or "").strip()
            # `name` was unguarded, which is how an exact-key lookup once
            # orphaned 62 of 129 clusters unnoticed: a missing name renders as
            # the raw c-TF-IDF label, so it reads as poor labelling rather than
            # a lookup miss. Checking desc alone does not catch it — the two
            # live in separate curated books and can go missing independently.
            if not (c.get("name") or "").strip():
                bad.append(f"{slug}: cluster '{label}' has no legend name")
            if not desc:
                bad.append(f"{slug}: cluster '{label}' has no description")
            elif desc.lower() == label.lower():
                bad.append(f"{slug}: cluster '{label}' description repeats the label")

    # 4. Every reddit holdout was probed and found live. No padding.
    for slug, srcs in hold.items():
        for e in (srcs.get("reddit") or []):
            if e.get("live") is not True:
                bad.append(f"{slug}: reddit holdout {e.get('url')} is not probed-live")

    # 5. A short table is only acceptable when its pool is exhausted.
    for slug, srcs in hold.items():
        n = len(srcs.get("reddit") or [])
        left = len(cand.get(slug, []))
        if n < CAP and left:
            bad.append(f"{slug}: reddit table has {n} of {CAP} with {left} candidates unprobed")

    # 6. A pair that shows nothing must really have nothing — if a solo profile
    #    could be computed, "no distinctive vocabulary" is wrong.
    for slug, p in key.items():
        if not (p.get("ua") or p.get("ru") or p.get("solo")):
            bad.append(f"{slug}: ships no vocabulary at all and no solo profile")

    if not bad:
        print("site invariants: all pass")
        return 0
    print(f"site invariants: {len(bad)} violation(s)\n")
    for b in bad[:60]:
        print(f"  {b}")
    if len(bad) > 60:
        print(f"  ... and {len(bad) - 60} more")
    return 1


if __name__ == "__main__":
    sys.exit(main())
