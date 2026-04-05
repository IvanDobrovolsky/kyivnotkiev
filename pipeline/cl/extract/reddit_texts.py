"""Extract Reddit texts (titles + bodies) from BigQuery for all pairs.

Pulls all Reddit posts/comments with text content, writes per-pair
parquet files to data/cl/raw/reddit/.

Usage:
    python -m pipeline.cl.extract.reddit_texts
"""

import logging

import pandas as pd
from google.cloud import bigquery

from pipeline.cl.config import (
    BQ_DATASET, BQ_PROJECT, CL_RAW_DIR, ensure_cl_dirs,
)
from pipeline.config import get_enabled_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = CL_RAW_DIR / "reddit"


def extract_reddit():
    ensure_cl_dirs()
    client = bigquery.Client(project=BQ_PROJECT)
    pairs = get_enabled_pairs()
    pair_ids = [p["id"] for p in pairs if not p.get("is_control", False)]

    log.info(f"Extracting Reddit texts for {len(pair_ids)} pairs")

    query = f"""
    SELECT
        pair_id,
        post_id,
        subreddit,
        title,
        body,
        variant,
        matched_term,
        score,
        EXTRACT(YEAR FROM created_utc) AS year,
        created_utc
    FROM `{BQ_PROJECT}.{BQ_DATASET}.raw_reddit`
    WHERE pair_id IN UNNEST(@pair_ids)
      AND title IS NOT NULL
      AND title != ''
    ORDER BY pair_id, created_utc
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("pair_ids", "INT64", pair_ids),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()
    log.info(f"Total Reddit rows: {len(df)}")

    # Combine title + body into single text field
    df["text"] = df.apply(
        lambda r: (r["title"] or "") + ("\n\n" + r["body"] if pd.notna(r["body"]) and r["body"] else ""),
        axis=1,
    )
    df["text"] = df["text"].str.strip()
    df["source"] = "reddit"
    df["word_count"] = df["text"].str.split().str.len()

    # Save per pair
    total = 0
    for pair_id in sorted(df["pair_id"].unique()):
        pdf = df[df["pair_id"] == pair_id]
        out_path = OUT_DIR / f"pair_{pair_id}.parquet"
        pdf.to_parquet(out_path, index=False)
        ru = len(pdf[pdf["variant"] == "russian"])
        ua = len(pdf[pdf["variant"] == "ukrainian"])
        log.info(f"  Pair {pair_id:3d}: {len(pdf):5d} texts (RU={ru}, UA={ua})")
        total += len(pdf)

    # Also save combined
    combined_path = OUT_DIR / "all_pairs.parquet"
    df.to_parquet(combined_path, index=False)
    log.info(f"Saved {total} Reddit texts to {OUT_DIR}")
    return df


if __name__ == "__main__":
    extract_reddit()
