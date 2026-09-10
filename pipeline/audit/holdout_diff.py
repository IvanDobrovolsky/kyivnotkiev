"""Which holdout entries have never been verified?

Exhibits refill to 100 after every cleanup round, so the replacements are
unaudited by construction. This diffs the current tables against the
verification record and writes the unseen entries per pair — the input for
the next verification round. The loop ends when a round returns few enough
new drops that another pass cannot move the numbers.

    python -m pipeline.audit.holdout_diff
"""

import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
AUDIT = ROOT / "data" / "audit"


def main() -> int:
    h = json.loads((ROOT / "site" / "src" / "data" / "holdouts_by_pair.json").read_text())
    seen = set()
    vf = AUDIT / "holdout_ai_verification.json"
    if vf.exists():
        rep = json.loads(vf.read_text())
        for d in rep.get("drops", []):
            seen.add((d.get("pair"), str(d.get("url"))))
        # kept entries are recorded in the round files, if present
        for rf in sorted(AUDIT.glob("holdout_round*_seen.json")):
            for pair, urls in json.loads(rf.read_text()).items():
                seen |= {(pair, u) for u in urls}

    new, cur = {}, {}
    for pair, srcs in h.items():
        for src, entries in srcs.items():
            if not isinstance(entries, list):
                continue
            for e in entries:
                u = str(e.get("url", ""))
                if not u:
                    continue
                cur.setdefault(pair, set()).add(u)
                if (pair, u) not in seen:
                    new.setdefault(pair, []).append({"source": src, "url": u,
                                                     "name": e.get("name", "")})

    (AUDIT / "holdout_unverified.json").write_text(
        json.dumps(new, indent=1, ensure_ascii=False))
    # record everything currently displayed, so the next diff knows it was seen
    (AUDIT / "holdout_round_current_seen.json").write_text(
        json.dumps({k: sorted(v) for k, v in cur.items()}, indent=1))

    tot_cur = sum(len(v) for v in cur.values())
    tot_new = sum(len(v) for v in new.values())
    print(f"displayed entries: {tot_cur:,}")
    print(f"never verified:    {tot_new:,} ({100*tot_new/max(tot_cur,1):.0f}%)")
    for pair, items in sorted(new.items(), key=lambda x: -len(x[1]))[:10]:
        print(f"  {pair:22s} {len(items):4d} new")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
