"""Recompute all statistical tests from local parquet data.

Outputs results as JSON for the paper and site manifest.
No BigQuery required — reads from dataset/*.parquet.

Usage:
    python -m pipeline.analysis.recompute_stats
"""

import json
import logging
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
DATASET_DIR = ROOT / "dataset"
OUT = ROOT / "site" / "src" / "data"


def _load(name: str) -> pd.DataFrame:
    path = DATASET_DIR / f"raw_{name}.parquet"
    if not path.exists():
        path = DATASET_DIR / f"{name}.parquet"
    if not path.exists():
        log.warning(f"  Parquet not found: {path}")
        return pd.DataFrame()
    import pyarrow.parquet as pq
    import pyarrow as pa
    table = pq.read_table(path)
    for i, field in enumerate(table.schema):
        if "date" in str(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
    table = table.replace_schema_metadata({})
    return table.to_pandas()


def load_pairs_config():
    import yaml
    p = ROOT / "config" / "pairs.yaml"
    with open(p) as f:
        cfg = yaml.safe_load(f)
    return {p["id"]: p for p in cfg["pairs"]
            if p.get("enabled", True) and not p.get("is_control", False)}


def compute_adoption_by_pair_source():
    """Get recent adoption ratio per pair per source."""
    log.info("Computing adoption by pair and source...")
    cutoff = "2024-01-01"
    frames = []

    # Trends
    df = _load("trends")
    if len(df):
        t = df[(df["geo"] == "") | (df["geo"].isna())].copy()
        t["date"] = pd.to_datetime(t["date"])
        t = t[t["date"] >= cutoff]
        g = t.groupby(["pair_id", "variant"])["interest"].sum().reset_index()
        p = g.pivot_table(index="pair_id", columns="variant", values="interest", fill_value=0).reset_index()
        p["source"] = "trends"
        p["ukr"] = p.get("ukrainian", 0).astype(float)
        p["rus"] = p.get("russian", 0).astype(float)
        p["total"] = p["ukr"] + p["rus"]
        p["adoption_ratio"] = p["ukr"] / p["total"]
        frames.append(p[["pair_id", "source", "ukr", "rus", "total", "adoption_ratio"]])

    # GDELT
    df = _load("gdelt")
    if len(df):
        d = df[pd.to_datetime(df["date"]) >= cutoff].copy()
        g = d.groupby(["pair_id", "variant"])["count"].sum().reset_index()
        p = g.pivot_table(index="pair_id", columns="variant", values="count", fill_value=0).reset_index()
        p["source"] = "gdelt"
        p["ukr"] = p.get("ukrainian", 0).astype(float)
        p["rus"] = p.get("russian", 0).astype(float)
        p["total"] = p["ukr"] + p["rus"]
        p["adoption_ratio"] = p["ukr"] / p["total"]
        frames.append(p[["pair_id", "source", "ukr", "rus", "total", "adoption_ratio"]])

    # Wikipedia
    df = _load("wikipedia")
    if len(df):
        d = df[pd.to_datetime(df["date"]) >= cutoff].copy()
        g = d.groupby(["pair_id", "variant"])["pageviews"].sum().reset_index()
        p = g.pivot_table(index="pair_id", columns="variant", values="pageviews", fill_value=0).reset_index()
        p["source"] = "wikipedia"
        p["ukr"] = p.get("ukrainian", 0).astype(float)
        p["rus"] = p.get("russian", 0).astype(float)
        p["total"] = p["ukr"] + p["rus"]
        p["adoption_ratio"] = p["ukr"] / p["total"]
        frames.append(p[["pair_id", "source", "ukr", "rus", "total", "adoption_ratio"]])

    # Reddit
    df = _load("reddit")
    if len(df):
        d = df[pd.to_datetime(df["date"]) >= cutoff].copy()
        g = d.groupby(["pair_id", "variant"]).size().reset_index(name="cnt")
        p = g.pivot_table(index="pair_id", columns="variant", values="cnt", fill_value=0).reset_index()
        p["source"] = "reddit"
        p["ukr"] = p.get("ukrainian", 0).astype(float)
        p["rus"] = p.get("russian", 0).astype(float)
        p["total"] = p["ukr"] + p["rus"]
        p["adoption_ratio"] = p["ukr"] / p["total"]
        frames.append(p[["pair_id", "source", "ukr", "rus", "total", "adoption_ratio"]])

    if not frames:
        return pd.DataFrame(columns=["pair_id", "source", "ukr", "rus", "total", "adoption_ratio"])
    result = pd.concat(frames, ignore_index=True)
    result = result[result["total"] > 0]
    return result


def compute_pre_post_invasion():
    """Compute pre vs post Feb 2022 adoption for Wilcoxon test."""
    log.info("Computing pre/post invasion adoption...")
    invasion_date = "2022-02-24"
    frames = []

    for source_name, value_col, agg_mode in [
        ("trends", "interest", "sum"),
        ("gdelt", "count", "sum"),
        ("wikipedia", "pageviews", "sum"),
        ("reddit", None, "count"),
    ]:
        df = _load(source_name)
        if not len(df):
            continue
        d = df.copy()
        if source_name == "trends":
            d = d[(d["geo"] == "") | (d["geo"].isna())]
        d["dt"] = pd.to_datetime(d["date"])
        d["period"] = np.where(d["dt"] < invasion_date, "pre", "post")

        if agg_mode == "count":
            g = d.groupby(["pair_id", "period", "variant"]).size().reset_index(name="val")
        else:
            g = d.groupby(["pair_id", "period", "variant"])[value_col].sum().reset_index(name="val")

        p = g.pivot_table(index=["pair_id", "period"], columns="variant", values="val", fill_value=0).reset_index()
        p["ukr"] = p.get("ukrainian", 0).astype(float)
        p["rus"] = p.get("russian", 0).astype(float)
        p["total"] = p["ukr"] + p["rus"]
        p = p[p["total"] > 10]
        p["adoption_ratio"] = p["ukr"] / p["total"]
        p["source"] = source_name
        frames.append(p[["pair_id", "source", "period", "adoption_ratio"]])

    if not frames:
        return pd.DataFrame(columns=["pair_id", "source", "period", "adoption_ratio"])
    return pd.concat(frames, ignore_index=True)


def main():
    pairs = load_pairs_config()
    pair_cats = {pid: p["category"] for pid, p in pairs.items()}

    # ── 1. Category-level Kruskal-Wallis ──
    log.info("=" * 60)
    log.info("1. KRUSKAL-WALLIS: category differences in adoption")
    log.info("=" * 60)

    adoption_df = compute_adoption_by_pair_source()
    trends_adopt = adoption_df[adoption_df["source"] == "trends"].copy()
    trends_adopt["category"] = trends_adopt["pair_id"].map(pair_cats)
    trends_adopt = trends_adopt.dropna(subset=["category", "adoption_ratio"])

    cat_groups = {cat: grp["adoption_ratio"].values
                  for cat, grp in trends_adopt.groupby("category")
                  if len(grp) >= 2}

    kw_results = {}
    if len(cat_groups) >= 2:
        groups = list(cat_groups.values())
        h_stat, p_value = scipy_stats.kruskal(*groups)
        kw_results = {"H": round(h_stat, 2), "p": round(p_value, 4),
                      "significant": p_value < 0.05, "n_categories": len(cat_groups)}
        log.info(f"  H = {h_stat:.2f}, p = {p_value:.4f}")

    cat_means = {}
    for cat, vals in cat_groups.items():
        cat_means[cat] = {
            "mean": round(float(np.mean(vals)) * 100, 1),
            "median": round(float(np.median(vals)) * 100, 1),
            "n": len(vals),
        }
        log.info(f"  {cat}: mean={cat_means[cat]['mean']}%, n={cat_means[cat]['n']}")

    # Pairwise Mann-Whitney with Holm-Bonferroni correction
    pairwise = []
    cats = list(cat_groups.keys())
    for i, c1 in enumerate(cats):
        for c2 in cats[i + 1:]:
            u, p = scipy_stats.mannwhitneyu(cat_groups[c1], cat_groups[c2], alternative="two-sided")
            pairwise.append({"cat1": c1, "cat2": c2, "U": round(float(u), 1),
                             "p_raw": float(p)})

    # Holm-Bonferroni step-down: sort by p ascending, enforce monotonicity
    pairwise.sort(key=lambda x: x["p_raw"])
    m = len(pairwise)
    prev_adj = 0.0
    for i, pw in enumerate(pairwise):
        adj = min(pw["p_raw"] * (m - i), 1.0)
        adj = max(adj, prev_adj)  # enforce monotonicity
        prev_adj = adj
        pw["p"] = round(pw["p_raw"], 4)
        pw["p_adjusted"] = round(adj, 4)
        pw["sig"] = adj < 0.05
    for pw in pairwise:
        del pw["p_raw"]
    sig_pairs = [pw for pw in pairwise if pw["sig"]]
    log.info(f"  Pairwise (Holm-Bonferroni): {len(sig_pairs)}/{len(pairwise)} significant")

    # ── 2. Pre/Post Invasion: Wilcoxon signed-rank ──
    log.info("=" * 60)
    log.info("2. WILCOXON: pre vs post Feb 2022 invasion effect")
    log.info("=" * 60)

    invasion_df = compute_pre_post_invasion()
    invasion_results = {}

    for source in ["trends", "gdelt", "wikipedia", "reddit"]:
        src_df = invasion_df[invasion_df["source"] == source]
        pre = src_df[src_df["period"] == "pre"].set_index("pair_id")["adoption_ratio"]
        post = src_df[src_df["period"] == "post"].set_index("pair_id")["adoption_ratio"]
        common = pre.index.intersection(post.index)

        if len(common) < 5:
            log.info(f"  {source}: not enough paired data ({len(common)} pairs)")
            continue

        pre_vals = pre.loc[common].values
        post_vals = post.loc[common].values
        diff = post_vals - pre_vals

        stat, p = scipy_stats.wilcoxon(diff, alternative="greater")
        d = float(np.mean(diff) / np.std(diff)) if np.std(diff) > 0 else 0

        invasion_results[source] = {
            "n_pairs": len(common),
            "pre_mean": round(float(np.mean(pre_vals)) * 100, 1),
            "post_mean": round(float(np.mean(post_vals)) * 100, 1),
            "delta_pp": round(float(np.mean(diff)) * 100, 1),
            "wilcoxon_stat": round(float(stat), 1),
            "p": round(float(p), 6),
            "cohens_d": round(d, 2),
            "significant": p < 0.05,
        }
        log.info(f"  {source}: pre={invasion_results[source]['pre_mean']}% → "
                 f"post={invasion_results[source]['post_mean']}%, d={d:.2f}, p={p:.6f}")

    # ── 3. Cross-source correlation (Spearman) ──
    log.info("=" * 60)
    log.info("3. SPEARMAN: cross-source correlation")
    log.info("=" * 60)

    pivot = adoption_df.pivot_table(index="pair_id", columns="source", values="adoption_ratio")
    correlations = {}
    sources = [s for s in ["trends", "gdelt", "wikipedia", "reddit"] if s in pivot.columns]
    for i, s1 in enumerate(sources):
        for s2 in sources[i + 1:]:
            valid = pivot[[s1, s2]].dropna()
            if len(valid) >= 5:
                rho, p = scipy_stats.spearmanr(valid[s1], valid[s2])
                correlations[f"{s1}_vs_{s2}"] = {"rho": round(float(rho), 3), "p": round(float(p), 4)}
                log.info(f"  {s1} vs {s2}: rho={rho:.3f}, p={p:.4f}")

    # ── 4. OLS regression: what predicts adoption? ──
    log.info("=" * 60)
    log.info("4. OLS REGRESSION: predictors of adoption")
    log.info("=" * 60)

    regression_results = {}
    trends_df = _load("trends")
    if len(trends_df):
        t = trends_df[(trends_df["geo"] == "") | (trends_df["geo"].isna())].copy()
        t["dt"] = pd.to_datetime(t["date"])

        pre = t[t["dt"] < "2022-02-24"].groupby(["pair_id", "variant"])["interest"].sum().reset_index()
        pre_p = pre.pivot_table(index="pair_id", columns="variant", values="interest", fill_value=0).reset_index()
        pre_p["baseline_adoption"] = pre_p.get("ukrainian", 0) / (pre_p.get("ukrainian", 0) + pre_p.get("russian", 0))

        post = t[t["dt"] >= "2024-01-01"].groupby(["pair_id", "variant"])["interest"].sum().reset_index()
        post_p = post.pivot_table(index="pair_id", columns="variant", values="interest", fill_value=0).reset_index()
        post_p["current_adoption"] = post_p.get("ukrainian", 0) / (post_p.get("ukrainian", 0) + post_p.get("russian", 0))

        merged = pre_p[["pair_id", "baseline_adoption"]].merge(post_p[["pair_id", "current_adoption"]], on="pair_id")
        merged["category"] = merged["pair_id"].map(pair_cats)
        merged = merged.dropna(subset=["category", "baseline_adoption", "current_adoption"])
        # Filter out pairs where both pre and post sums are 0
        merged = merged[(merged["baseline_adoption"].notna()) & (merged["current_adoption"].notna())]

        if len(merged) >= 10:
            from sklearn.linear_model import LinearRegression
            X = merged[["baseline_adoption"]].values
            y = merged["current_adoption"].values
            reg = LinearRegression().fit(X, y)
            y_pred = reg.predict(X)
            ss_res = np.sum((y - y_pred) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r2 = 1 - ss_res / ss_tot

            n = len(y)
            se = np.sqrt(ss_res / (n - 2) / np.sum((X[:, 0] - np.mean(X[:, 0])) ** 2))
            t_stat = reg.coef_[0] / se
            p_val = 2 * (1 - scipy_stats.t.cdf(abs(t_stat), n - 2))

            regression_results = {
                "beta": round(float(reg.coef_[0]), 3),
                "intercept": round(float(reg.intercept_), 3),
                "r_squared": round(float(r2), 3),
                "t_stat": round(float(t_stat), 2),
                "p": round(float(p_val), 6),
                "n": n,
            }
            log.info(f"  beta={regression_results['beta']}, R²={regression_results['r_squared']}, p={regression_results['p']}")

    # ── Compile and save ──
    results = {
        "kruskal_wallis": kw_results,
        "category_means_trends": cat_means,
        "pairwise_mannwhitney": pairwise,
        "invasion_effect": invasion_results,
        "cross_source_correlations": correlations,
        "regression": regression_results,
    }

    out_path = OUT / "analysis.json"
    with open(out_path, "w") as f:
        json.dump(results, f, indent=2, default=str)
    log.info(f"\nSaved: {out_path}")

    log.info("\n" + "=" * 60)
    log.info("SUMMARY FOR PAPER")
    log.info("=" * 60)
    log.info(f"Kruskal-Wallis: H={kw_results.get('H')}, p={kw_results.get('p')}")
    log.info(f"OLS Regression: R²={regression_results.get('r_squared')}, beta={regression_results.get('beta')}, p={regression_results.get('p')}")
    for src, res in invasion_results.items():
        log.info(f"Invasion ({src}): d={res['cohens_d']}, p={res['p']}")


if __name__ == "__main__":
    main()
