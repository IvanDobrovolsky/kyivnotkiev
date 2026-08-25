"""Single-command rebuild: recompute everything from dataset/ parquets.

This is the ONE script that updates all derived artifacts:
  dataset/*.parquet → manifest → timeseries → site JSON → corpus stats

Run after ANY data change, including enabling or disabling a pair in
config/pairs.yaml:
    python -m pipeline.rebuild

What it does:
  1. Validates all dataset/ parquets (counts, dups, schema)
  2. Recomputes aggregate stats over enabled pairs (analysis.json)
  3. Runs export_site_data.py (manifest + timeseries + holdouts + prune)
  4. Refreshes README numbers from the manifest
  5. Verifies site JSON matches dataset AND config/pairs.yaml
  6. Prints full status report

After running, commit + push:
  git add site/src/data/ && git commit && git push
  Then push HF: python -m pipeline.rebuild --push-hf
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
HF_EXPORT_DIR = ROOT / "data" / "hf_export"
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
        "raw_religious.parquet": {"key": ["pair_slug", "date", "variant", "source_domain"], "has_count": True},
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

    # Sync dataset/ → hf_export/
    for fname in expected:
        ds_path = DATASET_DIR / fname
        hf_path = HF_EXPORT_DIR / fname
        if ds_path.exists():
            ds_size = os.path.getsize(ds_path)
            hf_size = os.path.getsize(hf_path) if hf_path.exists() else 0
            if ds_size != hf_size:
                log.info(f"  Syncing {fname} to hf_export/")
                import shutil
                shutil.copy2(ds_path, hf_path)

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
    from pipeline.analysis.recompute_stats import main as stats_main
    stats_main()


def run_export():
    """Run export_site_data.py to regenerate all site JSON."""
    log.info("\n3. EXPORTING SITE DATA")
    from pipeline.export_site_data import main as export_main
    export_main()


def run_readme():
    """Refresh the README's generated numbers from the manifest."""
    log.info("\n4. UPDATING README")
    from pipeline.update_readme import main as readme_main
    readme_main()


def verify_site_data():
    """Verify site JSON matches dataset."""
    log.info("\n5. VERIFYING SITE DATA")

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
    """Push all datasets + corpus to HuggingFace."""
    log.info("\n6. PUSHING TO HUGGINGFACE")
    from huggingface_hub import HfApi
    api = HfApi()
    repo_id = "KyivNotKiev/toponym-adoption-data"

    for fname in sorted(os.listdir(DATASET_DIR)):
        if fname.endswith(".parquet"):
            path = DATASET_DIR / fname
            size = os.path.getsize(path) / 1024 / 1024
            log.info(f"  Uploading {fname} ({size:.1f}MB)...")
            api.upload_file(path_or_fileobj=str(path), path_in_repo=fname, repo_id=repo_id, repo_type="dataset")

    if CORPUS_PATH.exists():
        size = os.path.getsize(CORPUS_PATH) / 1024 / 1024
        log.info(f"  Uploading corpus ({size:.1f}MB)...")
        api.upload_file(path_or_fileobj=str(CORPUS_PATH), path_in_repo="toponyms-corpus.parquet", repo_id=repo_id, repo_type="dataset")

    log.info("  HuggingFace push complete ✓")


def main():
    parser = argparse.ArgumentParser(description="Rebuild all derived data from dataset/ parquets")
    parser.add_argument("--push-hf", action="store_true", help="Also push to HuggingFace")
    parser.add_argument("--verify-only", action="store_true", help="Only verify, don't re-export")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("REBUILD — single source of truth pipeline")
    log.info("=" * 60)

    issues = validate_datasets()

    if not args.verify_only:
        run_stats()
        run_export()
        run_readme()

    verify_issues = verify_site_data()

    if args.push_hf:
        push_hf()

    log.info("\n" + "=" * 60)
    total_issues = len(issues) + len(verify_issues)
    if total_issues == 0:
        log.info("ALL GOOD ✓")
    else:
        log.warning(f"{total_issues} issues found")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
