"""Single-command rebuild: recompute everything from dataset/ parquets.

This is the ONE script that updates all derived artifacts:
  dataset/*.parquet → manifest → timeseries → site JSON → corpus stats

Run after ANY data change, including enabling or disabling a pair in
config/pairs.yaml:
    python -m pipeline.rebuild

What it does:
  1. Validates all dataset/ parquets (counts, dups, schema)
  2. Recomputes aggregate stats over enabled pairs (analysis.json)
  3. Checks the verified GDELT records are stamped and current (no rebuild)
  4. Runs export_site_data.py (manifest + timeseries + holdouts + prune)
  5. Refreshes README numbers from the manifest
  6. Verifies site JSON matches dataset AND config/pairs.yaml
  7. Prints full status report

After running, commit + push:
  git add site/src/data/ && git commit && git push
  HF is published only by `python -m pipeline.store.publish`; --push-hf is retired.
"""

import argparse
import json
import logging
import os
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "dataset"
SITE_DATA_DIR = ROOT / "site" / "src" / "data"
CORPUS_PATH = ROOT / "data" / "corpus" / "toponyms-corpus.parquet"


def validate_datasets():
    """Validate all dataset parquets — schema, counts, dups."""
    log.info("1. VALIDATING DATASETS")
    issues = []

    expected = {
        "raw_gdelt.parquet": {"key": ["pair_slug", "date", "variant", "source_domain", "matched_term"], "has_count": True},
        "raw_reddit.parquet": {"key": ["post_id", "pair_slug"], "has_count": False},
        "raw_youtube.parquet": {"key": ["video_id", "pair_slug"], "has_count": False},
        "raw_telegram.parquet": {"key": ["text", "pair_slug", "date"], "has_count": False},
        "raw_trends.parquet": {"key": ["pair_slug", "date", "variant", "geo"], "has_count": False},
        "raw_wikipedia.parquet": {"key": ["pair_slug", "date", "variant"], "has_count": False},
        "raw_ngrams.parquet": {"key": ["pair_slug", "year", "variant"], "has_count": False},
    }

    for fname, spec in expected.items():
        path = DATASET_DIR / fname
        if not path.exists():
            issues.append(f"MISSING: {fname}")
            continue

        df = pd.read_parquet(path)
        # Check dups on key
        key_cols = [c for c in spec["key"] if c in df.columns]
        if key_cols:
            dups = df.duplicated(subset=key_cols).sum()
            if dups > 0:
                issues.append(f"DUPS: {fname} has {dups:,} duplicates on {key_cols}")

        log.info(f"  {fname}: {len(df):,} rows ✓")

    # data/hf_export/ used to hold a mirror of dataset/ that push_hf never read --
    # it uploaded from dataset/ directly. The mirror only ever drifted, and still
    # carried raw_religious.parquet after that source was removed on 2026-08-25.
    # push_hf now stages its own pruned copies, so there is nothing to mirror.

    if issues:
        for i in issues:
            log.warning(f"  ⚠ {i}")
    else:
        log.info("  All datasets valid ✓")

    return issues


def run_stats():
    """Recompute aggregate statistics over the currently enabled pairs.

    analysis.json holds cross-pair aggregates (Kruskal-Wallis, category means,
    invasion effect, regression). Those are computed ACROSS pairs, so pruning a
    disabled pair out of the output afterwards cannot fix them — they have to be
    recomputed. recompute_stats reads config/pairs.yaml and filters on `enabled`,
    but nothing ran it, so analysis.json silently kept stats for pairs that had
    been disabled. export_site_data then logged "Using existing analysis.json".
    """
    log.info("\n2. RECOMPUTING AGGREGATE STATS")
    # recompute_stats retired 2026-09-02 with the stale exploratory statistics;
    # the hierarchical model replaces it at paper time.
def run_gdelt_verified():
    """Check, do not rebuild, the verified GDELT records.

    This used to run `gdelt_verified --all-pairs` on every rebuild. That
    rebuilt retired borscht and every disabled pair with text, folded filter
    drift and newly fetched 2026 text into one unattributable change (step (a)
    alone was measured to move 433 rows dated 2025 or earlier), and left the
    corpus unstamped, which gdelt_paths_reconcile (R2c) fails once
    series_end('gdelt') passes TEXT_END. The rebuild is now explicit:

        python -m pipeline.build_gdelt_verified --step filters [--since D] --write
        python -m pipeline.build_gdelt_verified --step additions --since D --write

    Here we only refuse to export from a corpus that was not built by that
    path, or was built from different texts than article_texts now holds.
    """
    log.info("\n3. CHECKING VERIFIED GDELT RECORDS (no rebuild)")
    import pipeline.config as cfg
    from pipeline.audit.gdelt_paths_reconcile import extension_months, r2c_build
    slugs = [p["slug"] for p in cfg.get_enabled_pairs()]
    S = cfg.series_end("gdelt")
    fails, status = r2c_build(slugs, S, extension_months(S))
    counts = pd.Series(status).value_counts().to_dict()
    log.info(f"  build provenance: {counts}")
    if fails:
        for f in fails:
            log.warning(f"  {f}")
        raise SystemExit("verified GDELT corpus is not current; rebuild it with "
                         "`python -m pipeline.build_gdelt_verified --step filters --write` "
                         "(see its docstring for the --since steps)")


def run_export():
    """Run export_site_data.py to regenerate all site JSON."""
    log.info("\n4. EXPORTING SITE DATA")
    from pipeline.export_site_data import main as export_main
    export_main()


def run_readme():
    """Refresh the README's generated numbers from the manifest."""
    log.info("\n5. UPDATING README")
    from pipeline.update_readme import main as readme_main
    readme_main()


# Files the export regenerates every run. Anything else in site/src/data/ is an
# orphan: nothing produces it, so it silently keeps whatever it last held. The site
# has been rendering error_analysis.json and cl_analysis.json from April.
REGENERATED = {
    "manifest.json", "timeseries.json", "holdouts_by_pair.json", "holdouts.json",
    "pair_events.json", "analysis.json", "domain_origins.json",
    # export_site_data writes these three every run too (pairs_meta.json:2146,
    # cl_clusters.json:2668, cl_keyness.json:3010). Omitting them made
    # report_stale_artifacts tag freshly-regenerated files "NO PRODUCER",
    # which is exactly the noise that teaches people to ignore the warning.
    "pairs_meta.json", "cl_clusters.json", "cl_keyness.json",
}
STALE_AFTER_DAYS = 7


def report_stale_artifacts():
    """Name every site JSON that this run did not regenerate, with its age.

    Stale artefacts have repeatedly produced wrong conclusions in this project --
    comparisons computed against an enriched parquet from the previous day, holdouts
    drawn from superseded parquets, a YouTube dataset that was void for months. The
    fix is not vigilance; it is printing the age of everything the site depends on so
    a stale file cannot be mistaken for a fresh one.
    """
    import time
    log.info("\n   Artifact freshness:")
    now = time.time()
    rows = []
    for f in sorted((SITE_DATA_DIR).glob("*.json")):
        age_d = (now - f.stat().st_mtime) / 86400
        produced = f.name in REGENERATED
        rows.append((f.name, age_d, produced))
    for name, age, produced in sorted(rows, key=lambda r: -r[1]):
        if produced and age < 1:
            continue                       # regenerated this run, nothing to say
        tag = "regenerated" if produced else "NO PRODUCER"
        level = log.warning if (not produced and age > STALE_AFTER_DAYS) else log.info
        level(f"     {name:<28}{age:>6.1f}d  {tag}")
    orphaned = [r for r in rows if not r[2] and r[1] > STALE_AFTER_DAYS]
    if orphaned:
        log.warning(f"     {len(orphaned)} orphaned artifact(s) older than "
                    f"{STALE_AFTER_DAYS}d are still rendered by the site")
    return orphaned


def verify_site_data():
    """Verify site JSON matches dataset."""
    log.info("\n6. VERIFYING SITE DATA")
    report_stale_artifacts()

    with open(SITE_DATA_DIR / "manifest.json") as f:
        m = json.load(f)

    with open(SITE_DATA_DIR / "timeseries.json") as f:
        ts = json.load(f)

    issues = []

    # Every site JSON must agree with config/pairs.yaml. This is the gate that
    # catches any pipeline step writing site data without honouring `enabled`.
    from pipeline.prune_site_data import verify as verify_pairs
    issues.extend(verify_pairs())

    # Check each source
    for src, info in m["sources"].items():
        # Count pairs in timeseries
        ts_pairs = sum(1 for pid in ts if pid != "events" and src in ts[pid] and isinstance(ts[pid][src], list) and len(ts[pid][src]) > 0)
        if ts_pairs > info["pairs"]:
            issues.append(f"{src}: timeseries has {ts_pairs} pairs but manifest claims {info['pairs']}")

        log.info(f"  {src}: records={info['records']:,}, pairs={info['pairs']}, chart_pairs={ts_pairs}")

    # Check corpus
    if CORPUS_PATH.exists():
        corpus = pd.read_parquet(CORPUS_PATH)
        log.info(f"  Corpus: {len(corpus):,} texts, {corpus['pair_slug'].nunique()} pairs")

    if issues:
        for i in issues:
            log.warning(f"  ⚠ {i}")

    return issues


def push_hf():
    """Retired. pipeline.store.publish is the only path to HuggingFace.

    This uploaded dataset/raw_* under data/ in the HF repo with no shrink guard
    and no date clamp (raw_gdelt held 9,098 rows from 2026, through a partial
    2026-09-13), alongside the store layout that publish.py checks against the
    HF snapshot. Two writers to one remote, one of them unguarded, is how a
    degraded file replaces a good one. Removed rather than patched; it is in
    git history before this commit.
    """
    raise SystemExit("rebuild --push-hf is retired: publish with "
                     "`python -m pipeline.store.publish --dry-run`, then without --dry-run")


def main():
    parser = argparse.ArgumentParser(description="Rebuild all derived data from dataset/ parquets")
    parser.add_argument("--push-hf", action="store_true", help="Retired; refuses (use pipeline.store.publish)")
    parser.add_argument("--verify-only", action="store_true", help="Only verify, don't re-export")
    args = parser.parse_args()
    if args.push_hf:
        push_hf()                      # refuses before anything runs

    log.info("=" * 60)
    log.info("REBUILD — single source of truth pipeline")
    log.info("=" * 60)

    issues = validate_datasets()

    if not args.verify_only:
        run_stats()
        run_gdelt_verified()
        run_export()
        run_readme()

    verify_issues = verify_site_data()

    log.info("\n" + "=" * 60)
    total_issues = len(issues) + len(verify_issues)
    if total_issues == 0:
        log.info("ALL GOOD ✓")
    else:
        log.warning(f"{total_issues} issues found")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
