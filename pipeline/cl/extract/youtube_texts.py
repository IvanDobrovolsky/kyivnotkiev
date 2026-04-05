"""Extract YouTube texts (titles + descriptions) from BigQuery for all pairs.

Usage:
    python -m pipeline.cl.extract.youtube_texts
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

OUT_DIR = CL_RAW_DIR / "youtube"


def extract_youtube():
    ensure_cl_dirs()
    client = bigquery.Client(project=BQ_PROJECT)
    pairs = get_enabled_pairs()
    pair_ids = [p["id"] for p in pairs if not p.get("is_control", False)]

    log.info(f"Extracting YouTube texts for {len(pair_ids)} pairs")

    query = f"""
    SELECT
        pair_id,
        video_id,
        channel_title,
        title,
        description,
        variant,
        matched_term,
        view_count,
        EXTRACT(YEAR FROM published_at) AS year,
        published_at
    FROM `{BQ_PROJECT}.{BQ_DATASET}.raw_youtube`
    WHERE pair_id IN UNNEST(@pair_ids)
      AND title IS NOT NULL
      AND title != ''
    ORDER BY pair_id, published_at
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("pair_ids", "INT64", pair_ids),
        ]
    )

    df = client.query(query, job_config=job_config).to_dataframe()
    log.info(f"Total YouTube rows: {len(df)}")

    # Combine title + description
    df["text"] = df.apply(
        lambda r: (r["title"] or "") + (
            "\n\n" + r["description"][:500] if pd.notna(r["description"]) and r["description"] else ""
        ),
        axis=1,
    )
    df["text"] = df["text"].str.strip()
    df["source"] = "youtube"
    df["word_count"] = df["text"].str.split().str.len()

    total = 0
    for pair_id in sorted(df["pair_id"].unique()):
        pdf = df[df["pair_id"] == pair_id]
        out_path = OUT_DIR / f"pair_{pair_id}.parquet"
        pdf.to_parquet(out_path, index=False)
        ru = len(pdf[pdf["variant"] == "russian"])
        ua = len(pdf[pdf["variant"] == "ukrainian"])
        log.info(f"  Pair {pair_id:3d}: {len(pdf):5d} texts (RU={ru}, UA={ua})")
        total += len(pdf)

    combined_path = OUT_DIR / "all_pairs.parquet"
    df.to_parquet(combined_path, index=False)
    log.info(f"Saved {total} YouTube texts to {OUT_DIR}")
    return df


if __name__ == "__main__":
    extract_youtube()
