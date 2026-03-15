"""Process raw GDELT BigQuery exports into unified long-format dataset.

Transforms the wide-format parquet files (russian_1, ukrainian_1, etc.)
into a normalized long-format dataset suitable for analysis.

Usage:
    python scripts/process_gdelt_raw.py
"""

import json
import logging

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from src.config import GDELT_RAW_DIR, PROCESSED_DIR, ensure_dirs


def read_parquet_safe(path) -> pd.DataFrame:
    """Read parquet with date32 column handling."""
    table = pq.read_table(path)
    # Cast date32 columns to timestamps so pandas can handle them
    import pyarrow as pa
    new_columns = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_date(field.type):
            col = col.cast(pa.timestamp("ns"))
        new_columns.append(col)
    table = pa.table(
        {field.name: col for field, col in zip(table.schema, new_columns)},
    )
    return table.to_pandas()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_toponym_config() -> dict:
    with open("data/toponym_pairs.json") as f:
        return json.load(f)


def process_wide_to_long(df: pd.DataFrame, source_label: str) -> pd.DataFrame:
    """Convert wide-format (russian_1, ukrainian_1, ...) to long-format rows."""
    config = load_toponym_config()
    pairs_lookup = {p["id"]: p for p in config["pairs"]}

    # Find all pair IDs present in columns
    pair_ids = set()
    for col in df.columns:
        if col.startswith("russian_") or col.startswith("ukrainian_"):
            try:
                pid = int(col.split("_", 1)[1])
                pair_ids.add(pid)
            except ValueError:
                continue

    rows = []
    for _, row in df.iterrows():
        week = row["week"]
        for pid in sorted(pair_ids):
            r_col = f"russian_{pid}"
            u_col = f"ukrainian_{pid}"

            r_count = int(row.get(r_col, 0))
            u_count = int(row.get(u_col, 0))

            if r_count == 0 and u_count == 0:
                continue

            pair = pairs_lookup.get(pid, {})
            total = r_count + u_count
            ratio = u_count / total if total > 0 else np.nan

            rows.append({
                "week": week,
                "pair_id": pid,
                "russian_term": pair.get("russian", ""),
                "ukrainian_term": pair.get("ukrainian", ""),
                "category": pair.get("category", ""),
                "russian_count": r_count,
                "ukrainian_count": u_count,
                "total_count": total,
                "adoption_ratio": ratio,
                "source": source_label,
            })

    return pd.DataFrame(rows)


def main():
    ensure_dirs()

    frames = []

    # Process geographical events data
    geo_path = GDELT_RAW_DIR / "geographical_events.parquet"
    if geo_path.exists():
        log.info(f"Processing {geo_path}")
        df = read_parquet_safe(geo_path)
        long = process_wide_to_long(df, "gdelt_events")
        log.info(f"  {len(long)} rows from geographical events")
        frames.append(long)

    # Process non-geographical GKG data
    nongeo_path = GDELT_RAW_DIR / "non_geographical_gkg.parquet"
    if nongeo_path.exists():
        log.info(f"Processing {nongeo_path}")
        df = read_parquet_safe(nongeo_path)
        long = process_wide_to_long(df, "gdelt_gkg")
        log.info(f"  {len(long)} rows from non-geographical GKG")
        frames.append(long)

    if not frames:
        log.error("No raw GDELT data found")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined["week"] = pd.to_datetime(combined["week"])
    combined = combined.sort_values(["pair_id", "week"])

    out_path = PROCESSED_DIR / "gdelt_merged.parquet"
    combined.to_parquet(out_path, index=False)
    log.info(f"Saved: {out_path} ({len(combined)} rows, {combined['pair_id'].nunique()} pairs)")

    # Print summary
    log.info("\nPer-pair summary:")
    for pid in sorted(combined["pair_id"].unique()):
        pair_data = combined[combined["pair_id"] == pid]
        r = pair_data["russian_term"].iloc[0]
        u = pair_data["ukrainian_term"].iloc[0]
        cat = pair_data["category"].iloc[0]
        mean_ratio = pair_data["adoption_ratio"].mean()
        latest_ratio = pair_data.tail(4)["adoption_ratio"].mean()
        log.info(f"  {pid:2d}. {r:25s} -> {u:20s} [{cat:15s}] mean={mean_ratio:.3f} latest={latest_ratio:.3f}")


if __name__ == "__main__":
    main()
