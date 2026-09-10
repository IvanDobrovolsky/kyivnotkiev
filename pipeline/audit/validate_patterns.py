"""Validate agent-proposed drop patterns before they touch the corpus.

The verification agents propose regexes from a 100-entry exhibit sample. A
pattern that looks safe there can be catastrophic corpus-wide ("lvov" would
delete the pair). Every pattern is measured against the FULL corpus first:

  matches      how many rows it would remove, per source
  share        that as a fraction of the pair's rows
  samples      three matched texts, so over-broad rules are visible
  verdict      auto-safe (<2% and not matching the pair's own form),
               review (2-10%), reject (>10% or matches the variant term)

Nothing is applied here — this writes data/audit/pattern_validation.md and
a machine-readable json for the apply step.

    python -m pipeline.audit.validate_patterns
"""

import json
import pathlib
import re

import pandas as pd
import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
AUDIT = ROOT / "data" / "audit"
SAFE, REVIEW = 0.02, 0.10


def main() -> int:
    rep = json.loads((AUDIT / "holdout_ai_verification.json").read_text())
    cfg = {p["slug"]: p for p in yaml.safe_load(open(ROOT / "config" / "pairs.yaml"))["pairs"]}
    out, rows = {}, []

    for slug, pats in rep.get("patterns", {}).items():
        pf = ROOT / "data" / "store" / "pairs" / f"{slug}.parquet"
        if not pf.exists():
            continue
        d = pd.read_parquet(pf, columns=["source", "title", "text"])
        blob = (d.get("title", pd.Series("", index=d.index)).fillna("").astype(str)
                + " | " + d["text"].fillna("").astype(str))
        pc = cfg.get(slug, {})
        terms = [str(pc.get("ukrainian", "")), str(pc.get("russian", ""))]
        for pat in pats:
            try:
                rx = re.compile(pat, re.I)
            except re.error as e:
                rows.append((slug, pat, 0, 0.0, "reject", f"invalid regex: {e}", []))
                continue
            hit = blob.str.contains(rx)
            n, share = int(hit.sum()), float(hit.mean())
            # a pattern that matches a bare variant term would delete the pair
            bare = any(rx.fullmatch(t.lower()) or rx.pattern.strip() == t.lower()
                       for t in terms if t)
            if bare or share > REVIEW:
                verdict = "reject"
            elif share > SAFE:
                verdict = "review"
            else:
                verdict = "auto-safe"
            samples = [re.sub(r"\s+", " ", s)[:110] for s in d[hit].text.head(3)]
            per_src = d[hit].source.value_counts().to_dict()
            rows.append((slug, pat, n, share, verdict, json.dumps(per_src), samples))
            out.setdefault(verdict, []).append({"pair": slug, "pattern": pat,
                                                "matches": n, "share": round(share, 4)})

    (AUDIT / "pattern_validation.json").write_text(json.dumps(out, indent=1))
    lines = ["# Drop-pattern validation", "",
             f"auto-safe {len(out.get('auto-safe', []))} · "
             f"review {len(out.get('review', []))} · reject {len(out.get('reject', []))}", ""]
    for verdict in ("reject", "review", "auto-safe"):
        lines.append(f"## {verdict}")
        for slug, pat, n, share, v, src, samples in rows:
            if v != verdict:
                continue
            lines.append(f"- **{slug}** `{pat}` — {n:,} rows ({share*100:.2f}%) {src}")
            for s in samples:
                lines.append(f"    - {s}")
        lines.append("")
    (AUDIT / "pattern_validation.md").write_text("\n".join(lines))
    for v in ("auto-safe", "review", "reject"):
        print(f"{v}: {len(out.get(v, []))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
