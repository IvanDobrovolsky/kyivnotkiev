"""Does model behaviour track the web signal? Per-pair, measured.

The study's causal claim: Latin-script usage in public text becomes model
training data, so a name written the old way on the web is written the old
way by models. This tests it directly — per-pair adoption in our corpus
against per-pair adoption in 72 models' answers — instead of asserting it.

    python -m pipeline.stats.web_vs_llm
"""

import json
import pathlib

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
OUT = ROOT / "data" / "audit"


def main() -> int:
    # web signal: per pair, over the whole corpus (all sources, all years)
    web = {}
    for f in sorted((ROOT / "data" / "store" / "pairs").glob("*.parquet")):
        d = pd.read_parquet(f, columns=["variant"])
        v = d.variant.value_counts()
        u, r = int(v.get("ukrainian", 0)), int(v.get("russian", 0))
        if u + r >= 100:
            web[f.stem] = {"ua_pct": round(100 * u / (u + r), 1), "rows": u + r}

    # model signal: share of model answers using the Ukrainian form, per pair
    res = json.loads((ROOT / "data" / "raw" / "llm_spelling"
                      / "llm_spelling_results.json").read_text())
    cfg = __import__("yaml").safe_load(open(ROOT / "config" / "pairs.yaml"))
    by_terms = {(str(p["ukrainian"]).lower(), str(p["russian"]).lower()): p["slug"]
                for p in cfg["pairs"]}
    llm = {}
    for model in res:
        for p in model.get("pairs", []):
            key = (str(p.get("ukrainian", "")).lower(), str(p.get("russian", "")).lower())
            slug = by_terms.get(key)
            if not slug or p.get("classified") not in ("ukrainian", "russian"):
                continue
            e = llm.setdefault(slug, {"ua": 0, "n": 0})
            e["n"] += 1
            e["ua"] += p["classified"] == "ukrainian"

    rows = []
    for slug, w in web.items():
        m = llm.get(slug)
        if not m or m["n"] < 5:
            continue
        rows.append({"pair": slug, "web_ua": w["ua_pct"], "rows": w["rows"],
                     "llm_ua": round(100 * m["ua"] / m["n"], 1), "models": m["n"],
                     "gap": round(100 * m["ua"] / m["n"] - w["ua_pct"], 1)})
    df = pd.DataFrame(rows).sort_values("web_ua", ascending=False)
    if len(df) >= 3:
        pear = df.web_ua.corr(df.llm_ua)
        spear = df.web_ua.corr(df.llm_ua, method="spearman")
    else:
        pear = spear = float("nan")

    print(f"{'pair':22s} {'web ua%':>8s} {'llm ua%':>8s} {'gap':>7s} {'models':>7s}")
    for _, r in df.iterrows():
        print(f"{r['pair']:22s} {r.web_ua:8.1f} {r.llm_ua:8.1f} {r.gap:+7.1f} {int(r.models):7d}")
    print(f"\npairs compared: {len(df)}")
    print(f"Pearson r  = {pear:.3f}")
    print(f"Spearman r = {spear:.3f}")
    print(f"mean gap (llm - web) = {df.gap.mean():+.1f} pp")

    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "web_vs_llm.json").write_text(json.dumps(
        {"pearson": None if pd.isna(pear) else round(float(pear), 4),
         "spearman": None if pd.isna(spear) else round(float(spear), 4),
         "mean_gap": round(float(df.gap.mean()), 2),
         "pairs": df.to_dict("records")}, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
