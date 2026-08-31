"""Export the LLM spelling audit (v2 raw) to the site and the HF store.

Reads data/raw/llm_spelling/v2/llm_spelling_v2_results.json — the only source
of truth: 72 models x 66 pairs x 3 trials, with every prompt and verbatim
response — and writes:

    data/store/llm_spelling_raw.parquet   flat trials, ALL pairs (preservation:
                                          rides pipeline.store.publish to HF)
    site/src/data/llm_per_pair.json       enabled pairs only, absolute counts,
                                          the actual questions, per-model answers
    site/src/data/llm_trajectory.json     per-model absolute totals across pairs

No TAS anywhere: the composite score was display-only, never used in analysis
(verified 2026-08-30), and hides the one honest finding — free recall lags
forced choice because the training data is older than the alignment layer.

    python -m pipeline.analysis.export_llm_site
"""
from __future__ import annotations

import json
import pathlib

import pandas as pd
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw" / "llm_spelling" / "v2" / "llm_spelling_v2_results.json"
OLD_SITE = ROOT / "site" / "src" / "data" / "llm_per_pair.json"

FAMILY_COLORS = {}  # taken from the previous site file so colors stay stable


def main() -> int:
    raw = json.loads(RAW.read_text())
    old = json.loads(OLD_SITE.read_text())
    FAMILY_COLORS.update({k: v["color"] for k, v in old["families"].items()})

    cfg = yaml.safe_load(open(ROOT / "config" / "pairs.yaml"))
    by_terms = {}
    for p in cfg["pairs"]:
        by_terms[(p["russian"], p["ukrainian"])] = p

    # ── flat trials table (all pairs, preservation) ──
    rows = []
    for m in raw:
        for p in m["pairs"]:
            meta = by_terms.get((p["russian"], p["ukrainian"]), {})
            for t in p["trials"]:
                rows.append({
                    "model": m["model"], "full_model": m.get("full_model"),
                    "family": m["family"], "tier": m.get("tier"),
                    "provider": m.get("provider"),
                    "release_date": m.get("release_date"),
                    "pair_slug": meta.get("slug"),
                    "russian": p["russian"], "ukrainian": p["ukrainian"],
                    "category": p.get("category"),
                    "test": t["test"], "prompt": t["prompt"],
                    "response": t.get("response"), "x": t.get("x"),
                })
    flat = pd.DataFrame(rows)
    store = ROOT / "data" / "store"
    store.mkdir(parents=True, exist_ok=True)
    flat.to_parquet(store / "llm_spelling_raw.parquet",
                    compression="zstd", index=False)
    print(f"store: {len(flat):,} trials -> llm_spelling_raw.parquet")

    # release_date may be missing on some models — trajectory needs it
    models_meta = {m["model"]: m for m in raw}

    # ── per-pair site JSON (enabled pairs only) ──
    enabled = {p["slug"]: p for p in cfg["pairs"] if p.get("enabled", True)}
    pairs_out = {}
    for (ru, uk), meta in by_terms.items():
        slug = meta.get("slug")
        if slug not in enabled:
            continue
        sub = flat[(flat.russian == ru) & (flat.ukrainian == uk)]
        if sub.empty:
            continue
        qs = {t: sub[sub.test == t].prompt.iloc[0]
              for t in ("open", "forced_ru_first", "forced_ua_first")
              if (sub.test == t).any()}
        models = []
        for key, g in sub.groupby("model", sort=False):
            mm = models_meta[key]
            e = {"key": key, "family": mm["family"], "tier": mm.get("tier"),
                 "release_date": mm.get("release_date")}
            other = {}
            for t in ("open", "forced_ru_first", "forced_ua_first"):
                r = g[g.test == t]
                x = None if r.empty or pd.isna(r.x.iloc[0]) else int(r.x.iloc[0])
                e["x_" + t] = x
                if x is None and not r.empty:
                    other[t] = str(r.response.iloc[0] or "")[:80]
            if other:
                e["other"] = other
            models.append(e)
        fam_counts: dict = {}
        opn = ops = fcn = fcs = 0
        for e in models:
            f = fam_counts.setdefault(e["family"], {"n": 0, "open_ua": 0,
                                                    "open_n": 0, "forced_ua": 0,
                                                    "forced_n": 0})
            f["n"] += 1
            if e["x_open"] is not None:
                f["open_n"] += 1; opn += 1
                if e["x_open"] == 1:
                    f["open_ua"] += 1; ops += 1
            for t in ("x_forced_ru_first", "x_forced_ua_first"):
                if e[t] is not None:
                    f["forced_n"] += 1; fcn += 1
                    if e[t] == 1:
                        f["forced_ua"] += 1; fcs += 1
        pairs_out[slug] = {
            "russian": ru, "ukrainian": uk,
            "category": meta.get("category"),
            "significance": meta.get("significance"),
            "questions": qs,
            "models": models,
            "summary": {"n_models": len(models),
                        "open_ua": ops, "open_n": opn,
                        "forced_ua": fcs, "forced_n": fcn,
                        "by_family": fam_counts},
        }

    fam_meta = {f: {"color": FAMILY_COLORS.get(f, "#666"),
                    "n_models": int((flat.drop_duplicates("model").family == f).sum())}
                for f in sorted(flat.family.unique())}
    site = ROOT / "site" / "src" / "data"
    out1 = {"n_pairs": len(pairs_out), "n_models": flat.model.nunique(),
            "families": fam_meta, "pairs": pairs_out}
    (site / "llm_per_pair.json").write_text(json.dumps(out1, allow_nan=False))
    print(f"site: llm_per_pair.json — {len(pairs_out)} pairs, "
          f"{out1['n_models']} models")

    # ── trajectory: per-model absolute totals across ALL audited pairs ──
    traj = []
    for key, g in flat.groupby("model", sort=False):
        mm = models_meta[key]
        o = g[g.test == "open"]; fc = g[g.test != "open"]
        traj.append({
            "key": key, "family": mm["family"], "tier": mm.get("tier"),
            "release_date": mm.get("release_date"),
            "open_ua": int((o.x == 1).sum()), "open_n": int(o.x.notna().sum()),
            "forced_ua": int((fc.x == 1).sum()), "forced_n": int(fc.x.notna().sum()),
            "other_n": int(g.x.isna().sum()),
        })
    traj.sort(key=lambda e: (e["release_date"] or "9999"))
    out2 = {"n_models": len(traj), "n_pairs": int(flat.pair_slug.nunique()),
            "n_pairs_total": int(flat.groupby(["russian", "ukrainian"]).ngroups),
            "families": fam_meta, "models": traj}
    (site / "llm_trajectory.json").write_text(json.dumps(out2, allow_nan=False))
    print(f"site: llm_trajectory.json — {len(traj)} models")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
