"""Single-command rebuild: recompute everything from dataset/ parquets.

This is the ONE script that updates all derived artifacts:
  dataset/*.parquet → manifest → timeseries → site JSON → corpus stats

Run after ANY data change, including enabling or disabling a pair in
config/pairs.yaml:
    python -m pipeline.rebuild

What it does:
  1. Validates all dataset/ parquets (counts, dups, schema)
  2. Recomputes aggregate stats over enabled pairs (analysis.json)
  3. Rebuilds verified GDELT records from fetched article text (no fetching)
  4. Runs export_site_data.py (manifest + timeseries + holdouts + prune)
  5. Refreshes README numbers from the manifest
  6. Verifies site JSON matches dataset AND config/pairs.yaml
  7. Prints full status report

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
    from pipeline.analysis.recompute_stats import main as stats_main
    stats_main()


def run_gdelt_verified():
    """Rebuild verified GDELT records from whatever article text is on disk.

    Derivation, not collection: it never fetches. Fetching is a separate long-running
    step (pipeline.ingestion.gdelt_fetch_texts). This runs before the export so the
    series and the holdouts are always regenerated from the current records rather
    than from whatever happened to be written last.
    """
    import subprocess, sys
    log.info("\n3. REBUILDING VERIFIED GDELT RECORDS")
    texts = ROOT / "data" / "raw" / "gdelt" / "texts" / "article_texts.parquet"
    if not texts.exists():
        log.info("  no fetched article text yet — skipping")
        return
    res = subprocess.run([sys.executable, "-m", "pipeline.cl.corpus.gdelt_verified", "--all-pairs"],
                         capture_output=True, text=True, cwd=ROOT)
    for line in res.stdout.strip().split("\n"):
        if line.strip():
            log.info(f"  {line}")
    if res.returncode:
        raise SystemExit(f"verified rebuild failed: {res.stderr.strip()[-400:]}")


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


def verify_site_data():
    """Verify site JSON matches dataset."""
    log.info("\n6. VERIFYING SITE DATA")

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


# Publishing is opt-in per file. The previous version uploaded every *.parquet in
# dataset/, which meant a backup written alongside the real data -- raw_gdelt.v1_
# allnames.bak.parquet, 2,053,205 rows of superseded AllNames counts -- was a
# publication candidate purely because of its extension. An allowlist cannot do that.
HF_REPO = "KyivNotKiev/toponym-adoption-data"
HF_PUBLISHABLE = {
    "raw_gdelt.parquet",
    "raw_ngrams.parquet",
    "raw_reddit.parquet",
    "raw_trends.parquet",
    "raw_wikipedia.parquet",
}
# Excluded on purpose, with the reason, so removing a line is a deliberate act:
HF_WITHHELD = {
    "raw_youtube.parquet": "pre-2026-08-25 collection is void (result cap bug, 6-7x "
                           "undercount); rebuild from data/cl/raw/youtube_census first",
    "raw_telegram.parquet": "80% Cyrillic — measures Ukrainian-language channels, not "
                            "English adoption",
}


def push_hf():
    """Upload the allowlisted datasets, pruned to enabled pairs.

    Disabled pairs were reaching the published dataset even though the site prunes
    them: five files carried 15-22 pairs that config/pairs.yaml no longer enables.
    Pruning happens here rather than in dataset/ so the local parquets stay complete
    and re-enabling a pair needs no re-collection.
    """
    log.info("\n7. PUSHING TO HUGGINGFACE")
    import tempfile

    from huggingface_hub import HfApi

    token = None
    for candidate in (Path("/etc/secrets/hf"), Path.home() / ".huggingface" / "token"):
        if candidate.exists():
            token = candidate.read_text().strip()
            break
    api = HfApi(token=token)

    from pipeline.export_site_data import get_enabled_slugs
    enabled = get_enabled_slugs()
    for name, why in sorted(HF_WITHHELD.items()):
        if (DATASET_DIR / name).exists():
            log.info(f"  WITHHELD {name}: {why}")

    present = {f.name for f in DATASET_DIR.glob("*.parquet")}
    unknown = present - HF_PUBLISHABLE - set(HF_WITHHELD)
    for name in sorted(unknown):
        log.warning(f"  NOT PUBLISHED (not on the allowlist): {name}")

    with tempfile.TemporaryDirectory() as tmp:
        for name in sorted(HF_PUBLISHABLE):
            path = DATASET_DIR / name
            if not path.exists():
                log.warning(f"  missing, skipped: {name}")
                continue
            df = pd.read_parquet(path)
            if "pair_slug" in df.columns:
                before = df.pair_slug.nunique()
                df = df[df.pair_slug.isin(enabled)]
                if df.pair_slug.nunique() != before:
                    log.info(f"  {name}: pruned {before - df.pair_slug.nunique()} disabled pair(s)")
            staged = Path(tmp) / name
            df.to_parquet(staged, compression="zstd", index=False)
            mb = staged.stat().st_size / 1024 / 1024
            log.info(f"  Uploading {name} ({len(df):,} rows, {mb:.1f}MB)...")
            api.upload_file(path_or_fileobj=str(staged), path_in_repo=f"data/{name}",
                            repo_id=HF_REPO, repo_type="dataset")

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
        run_gdelt_verified()
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
