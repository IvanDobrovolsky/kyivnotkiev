"""Extract GDELT article texts for holdouts and corpus.

Full pipeline: BQ URLs → filter to verified English domains → fetch HTML →
extract text with trafilatura → verify (English, has toponym, >100ch) →
save holdouts + corpus segments.

Domain list: data/audit/english_news_domains_curated.csv (818 verified outlets)
Source: palewire/news-homepages + manually curated Ukrainian English-language media

Usage:
    python -m pipeline.ingestion.gdelt_extract_articles --pair chornobyl
    python -m pipeline.ingestion.gdelt_extract_articles --all
"""

import argparse
import logging
import re
import time
from pathlib import Path

import pandas as pd
import requests
import trafilatura
import yaml
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
DOMAINS_PATH = ROOT / "data" / "audit" / "english_news_domains_curated.csv"
OUT_DIR = ROOT / "data" / "corpus" / "gdelt_holdouts"
GCP_PROJECT = "kyivnotkiev-research"

MAX_TEXT_LEN = 2000
FETCH_DELAY = 0.5
MAX_URLS_PER_PAIR = 200  # fetch up to 200, expect ~50-70% success


def load_pairs():
    with open(CONFIG_PATH) as f:
        data = yaml.safe_load(f)
    return [p for p in data["pairs"] if p.get("enabled", True)]


def load_english_domains():
    df = pd.read_csv(DOMAINS_PATH)
    return set(df["domain"].str.lower())


def get_urls_from_bq(pair: dict, english_domains: set) -> pd.DataFrame:
    """Query GDELT BQ for article URLs mentioning this pair's terms."""
    client = bigquery.Client(project=GCP_PROJECT)
    ru = pair["russian"].replace("'", "\\'").lower()
    ua = pair["ukrainian"].replace("'", "\\'").lower()

    # Build URL/AllNames match conditions
    conditions = [
        f"LOWER(DocumentIdentifier) LIKE '%{ru}%'",
        f"LOWER(DocumentIdentifier) LIKE '%{ua}%'",
        f"LOWER(IFNULL(AllNames,'')) LIKE '%{ru}%'",
        f"LOWER(IFNULL(AllNames,'')) LIKE '%{ua}%'",
    ]
    # Also match hyphenated forms
    if " " in pair["russian"]:
        conditions.append(f"LOWER(DocumentIdentifier) LIKE '%{ru.replace(' ', '-')}%'")
    if " " in pair["ukrainian"]:
        conditions.append(f"LOWER(DocumentIdentifier) LIKE '%{ua.replace(' ', '-')}%'")

    where = " OR ".join(conditions)

    q = f"""
    SELECT DISTINCT
        DocumentIdentifier as url,
        SourceCommonName as domain,
        FORMAT_DATE('%Y-%m', CAST(_PARTITIONTIME AS DATE)) as month
    FROM `gdelt-bq.gdeltv2.gkg_partitioned`
    WHERE _PARTITIONTIME >= '2015-02-18' AND _PARTITIONTIME < '2026-01-01'
    AND ({where})
    """

    log.info(f"  Querying BQ for URLs...")
    df = client.query(q).to_dataframe()
    log.info(f"  Raw: {len(df):,} URLs from {df['domain'].nunique():,} domains")

    # Filter to English domains
    df["domain_lower"] = df["domain"].str.lower()
    df = df[df["domain_lower"].isin(english_domains)]
    df = df.drop_duplicates(subset=["url"])
    log.info(f"  English-domain: {len(df):,} URLs from {df['domain'].nunique():,} domains")

    return df


def fetch_and_extract(url: str) -> str | None:
    """Fetch URL and extract article text with trafilatura."""
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return None
        text = trafilatura.extract(downloaded, include_comments=False, include_tables=False)
        return text if text and len(text) > 100 else None
    except Exception:
        return None


def verify_text(text: str, pair: dict) -> dict | None:
    """Verify extracted text: English, contains toponym, classify variant."""
    # English check
    latin = len(re.findall(r"[a-zA-Z]", text))
    cyrillic = len(re.findall(r"[а-яА-ЯіїєґІЇЄҐ]", text))
    if cyrillic > latin:
        return None

    # Toponym check
    has_ru = bool(re.search(re.escape(pair["russian"]), text, re.IGNORECASE))
    has_ua = bool(re.search(re.escape(pair["ukrainian"]), text, re.IGNORECASE))
    if not has_ru and not has_ua:
        return None

    variant = "both" if (has_ru and has_ua) else ("ukrainian" if has_ua else "russian")
    return {"variant": variant}


def extract_pair(pair: dict, english_domains: set) -> pd.DataFrame:
    """Full pipeline for one pair."""
    slug = pair["slug"]
    out_path = OUT_DIR / f"{slug}.parquet"

    if out_path.exists():
        existing = pd.read_parquet(out_path)
        log.info(f"  Already exists: {len(existing)} articles — skipping")
        return existing

    # Step 1: Get URLs from BQ
    urls_df = get_urls_from_bq(pair, english_domains)
    if urls_df.empty:
        log.info(f"  No English-domain URLs found")
        return pd.DataFrame()

    # Step 2: Sample URLs (don't fetch thousands)
    sample = urls_df.sample(min(MAX_URLS_PER_PAIR, len(urls_df)), random_state=42)
    log.info(f"  Fetching {len(sample)} URLs...")

    # Step 3: Fetch + extract + verify
    results = []
    failed = 0
    for i, (_, row) in enumerate(sample.iterrows()):
        text = fetch_and_extract(row["url"])
        if text:
            verification = verify_text(text, pair)
            if verification:
                results.append({
                    "url": row["url"],
                    "domain": row["domain"],
                    "month": row["month"],
                    "text": text[:MAX_TEXT_LEN],
                    "text_len": min(len(text), MAX_TEXT_LEN),
                    "full_text_len": len(text),
                    "variant": verification["variant"],
                    "pair_slug": slug,
                    "source": "gdelt",
                })
            else:
                failed += 1
        else:
            failed += 1

        if (i + 1) % 50 == 0:
            log.info(f"    {i+1}/{len(sample)}: {len(results)} extracted, {failed} failed")
        time.sleep(FETCH_DELAY)

    if not results:
        log.info(f"  No articles extracted")
        return pd.DataFrame()

    df = pd.DataFrame(results)
    df.to_parquet(out_path, index=False)
    log.info(f"  Saved: {len(df)} articles ({len(df)/len(sample)*100:.0f}% success)")
    log.info(f"  Variants: {df['variant'].value_counts().to_dict()}")
    log.info(f"  Top domains: {df['domain'].value_counts().head(5).to_dict()}")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", type=str, default=None)
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pairs = load_pairs()
    english_domains = load_english_domains()
    log.info(f"English domains loaded: {len(english_domains)}")

    if args.pair:
        pairs = [p for p in pairs if p["slug"] == args.pair]
        if not pairs:
            log.error(f"Pair not found")
            return

    if not args.all and not args.pair:
        log.error("Specify --pair <slug> or --all")
        return

    total = 0
    for i, pair in enumerate(pairs):
        log.info(f"\n[{i+1}/{len(pairs)}] {pair['slug']}")
        df = extract_pair(pair, english_domains)
        total += len(df)

    log.info(f"\nDONE: {total} articles extracted")


if __name__ == "__main__":
    main()
