"""Recompute all statistical tests from fresh BigQuery data.

Outputs results as JSON for the paper and site manifest.

Usage:
    python -m pipeline.analysis.recompute_stats
"""

import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from scipy import stats as scipy_stats
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT = "kyivnotkiev-research"
DATASET = "kyivnotkiev"
client = bigquery.Client(project=PROJECT)
OUT = Path(__file__).resolve().parent.parent.parent / "site" / "src" / "data"


def query_df(sql: str) -> pd.DataFrame:
    return client.query(sql).to_dataframe()


def load_pairs_config():
    import yaml
    p = Path(__file__).resolve().parent.parent.parent / "config" / "pairs.yaml"
    with open(p) as f:
        cfg = yaml.safe_load(f)
    return {p["id"]: p for p in cfg["pairs"]
            if p.get("enabled", True) and not p.get("is_control", False)}


def compute_adoption_by_pair_source():
    """Get recent adoption ratio per pair per source."""
    log.info("Computing adoption by pair and source...")
    df = query_df(f"""
        WITH src AS (
            SELECT pair_id, 'trends' as source,
                SUM(IF(variant='ukrainian', interest, 0)) as ukr,
                SUM(IF(variant='russian', interest, 0)) as rus
            FROM `{DATASET}.raw_trends`
            WHERE (geo='' OR geo IS NULL) AND date >= '2024-01-01'
            GROUP BY pair_id
            UNION ALL
            SELECT pair_id, 'gdelt',
                COUNTIF(variant='ukrainian'), COUNTIF(variant='russian')
            FROM `{DATASET}.raw_gdelt`
            WHERE date >= '2024-01-01'
            GROUP BY pair_id
            UNION ALL
            SELECT pair_id, 'wikipedia',
                SUM(IF(variant='ukrainian', pageviews, 0)),
                SUM(IF(variant='russian', pageviews, 0))
            FROM `{DATASET}.raw_wikipedia`
            WHERE date >= '2024-01-01'
            GROUP BY pair_id
            UNION ALL
            SELECT pair_id, 'reddit',
                COUNTIF(variant='ukrainian'), COUNTIF(variant='russian')
            FROM `{DATASET}.raw_reddit`
            WHERE DATE(created_utc) >= '2024-01-01'
            GROUP BY pair_id
        )
        SELECT pair_id, source, ukr, rus, (ukr + rus) as total,
            SAFE_DIVIDE(ukr, ukr + rus) as adoption_ratio
        FROM src
        WHERE (ukr + rus) > 0
    """)
    return df


def compute_pre_post_invasion():
    """Compute pre vs post Feb 2022 adoption for Wilcoxon test."""
    log.info("Computing pre/post invasion adoption...")
    df = query_df(f"""
        WITH periods AS (
            SELECT pair_id, 'trends' as source,
                IF(date < '2022-02-24', 'pre', 'post') as pd,
                SUM(IF(variant='ukrainian', interest, 0)) as ukr,
                SUM(IF(variant='russian', interest, 0)) as rus
            FROM `{DATASET}.raw_trends`
            WHERE (geo='' OR geo IS NULL)
            GROUP BY pair_id, pd
            UNION ALL
            SELECT pair_id, 'gdelt',
                IF(date < '2022-02-24', 'pre', 'post') as pd,
                COUNTIF(variant='ukrainian'), COUNTIF(variant='russian')
            FROM `{DATASET}.raw_gdelt`
            GROUP BY pair_id, pd
            UNION ALL
            SELECT pair_id, 'wikipedia',
                IF(date < '2022-02-24', 'pre', 'post') as pd,
                SUM(IF(variant='ukrainian', pageviews, 0)),
                SUM(IF(variant='russian', pageviews, 0))
            FROM `{DATASET}.raw_wikipedia`
            GROUP BY pair_id, pd
            UNION ALL
            SELECT pair_id, 'reddit',
                IF(DATE(created_utc) < '2022-02-24', 'pre', 'post') as pd,
                COUNTIF(variant='ukrainian'), COUNTIF(variant='russian')
            FROM `{DATASET}.raw_reddit`
            GROUP BY pair_id, pd
        )
        SELECT pair_id, source, pd as period,
            SAFE_DIVIDE(ukr, ukr + rus) as adoption_ratio
        FROM periods
        WHERE (ukr + rus) > 10
    """)
    return df


def main():
    pairs = load_pairs_config()
    pair_cats = {pid: p["category"] for pid, p in pairs.items()}

    # ── 1. Category-level Kruskal-Wallis ──
    log.info("=" * 60)
    log.info("1. KRUSKAL-WALLIS: category differences in adoption")
    log.info("=" * 60)

    adoption_df = compute_adoption_by_pair_source()
    # Use trends as the primary source for category comparison
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
    else:
        log.warning("  Not enough categories")

    # Category means
    cat_means = {}
    for cat, vals in cat_groups.items():
        cat_means[cat] = {
            "mean": round(float(np.mean(vals)) * 100, 1),
            "median": round(float(np.median(vals)) * 100, 1),
            "n": len(vals),
        }
        log.info(f"  {cat}: mean={cat_means[cat]['mean']}%, median={cat_means[cat]['median']}%, n={cat_means[cat]['n']}")

    # Pairwise Mann-Whitney
    pairwise = []
    cats = list(cat_groups.keys())
    for i, c1 in enumerate(cats):
        for c2 in cats[i + 1:]:
            u, p = scipy_stats.mannwhitneyu(cat_groups[c1], cat_groups[c2], alternative="two-sided")
            pairwise.append({"cat1": c1, "cat2": c2, "U": round(float(u), 1),
                             "p": round(float(p), 4), "sig": p < 0.05})
    sig_pairs = [pw for pw in pairwise if pw["sig"]]
    log.info(f"  Pairwise: {len(sig_pairs)}/{len(pairwise)} significant")

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
        # Cohen's d
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
                 f"post={invasion_results[source]['post_mean']}%, "
                 f"d={d:.2f}, p={p:.6f}")

    # ── 3. Cross-source correlation (Spearman) ──
    log.info("=" * 60)
    log.info("3. SPEARMAN: cross-source correlation")
    log.info("=" * 60)

    # Pivot adoption by pair × source
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

    # Get pre-2022 baseline adoption per pair (trends)
    baseline_df = query_df(f"""
        SELECT pair_id,
            SAFE_DIVIDE(
                SUM(IF(variant='ukrainian' AND date < '2022-02-24', interest, 0)),
                SUM(IF(date < '2022-02-24', interest, 0))
            ) as baseline_adoption,
            SAFE_DIVIDE(
                SUM(IF(variant='ukrainian' AND date >= '2024-01-01', interest, 0)),
                SUM(IF(date >= '2024-01-01', interest, 0))
            ) as current_adoption
        FROM `{DATASET}.raw_trends`
        WHERE (geo='' OR geo IS NULL)
        GROUP BY pair_id
        HAVING SUM(IF(date < '2022-02-24', interest, 0)) > 0
            AND SUM(IF(date >= '2024-01-01', interest, 0)) > 0
    """)
    baseline_df["category"] = baseline_df["pair_id"].map(pair_cats)
    baseline_df = baseline_df.dropna(subset=["category", "baseline_adoption", "current_adoption"])

    regression_results = {}
    if len(baseline_df) >= 10:
        from sklearn.linear_model import LinearRegression
        X = baseline_df[["baseline_adoption"]].values
        y = baseline_df["current_adoption"].values
        reg = LinearRegression().fit(X, y)
        y_pred = reg.predict(X)
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2 = 1 - ss_res / ss_tot

        # t-test for beta significance
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
        log.info(f"  beta={regression_results['beta']}, R²={regression_results['r_squared']}, "
                 f"p={regression_results['p']}")

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

    # Print summary for paper
    log.info("\n" + "=" * 60)
    log.info("SUMMARY FOR PAPER")
    log.info("=" * 60)
    log.info(f"Kruskal-Wallis: H={kw_results.get('H')}, p={kw_results.get('p')}")
    log.info(f"OLS Regression: R²={regression_results.get('r_squared')}, beta={regression_results.get('beta')}, p={regression_results.get('p')}")
    for src, res in invasion_results.items():
        log.info(f"Invasion ({src}): d={res['cohens_d']}, p={res['p']}")


if __name__ == "__main__":
    main()
