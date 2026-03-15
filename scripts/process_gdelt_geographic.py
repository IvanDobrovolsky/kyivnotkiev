"""Process raw GDELT geographic (by-country) data for diffusion analysis.

Usage:
    python scripts/process_gdelt_geographic.py
"""

import logging
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from src.config import GDELT_RAW_DIR, PROCESSED_DIR, ensure_dirs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PAIR_MAP = {
    "kiev": (1, "Kiev", "Kyiv"),
    "kharkov": (2, "Kharkov", "Kharkiv"),
    "odessa": (3, "Odessa", "Odesa"),
    "lvov": (4, "Lvov", "Lviv"),
}


def main():
    ensure_dirs()

    path = GDELT_RAW_DIR / "geographic_by_country.parquet"
    if not path.exists():
        log.error(f"Not found: {path}")
        return

    table = pq.read_table(path)
    # Fix date columns
    new_columns = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_date(field.type):
            col = col.cast(pa.timestamp("ns"))
        new_columns.append(col)
    table = pa.table({field.name: col for field, col in zip(table.schema, new_columns)})
    df = table.to_pandas()

    log.info(f"Loaded: {len(df)} rows, {df['country'].nunique()} countries")

    # Melt into long format with one row per (month, country, pair_id)
    rows = []
    for _, row in df.iterrows():
        month = row["month"]
        country = row["country"]

        # Kiev/Kyiv
        r, u = row["kiev_count"], row["kyiv_count"]
        if r + u > 0:
            rows.append({"week": month, "source_country": country, "pair_id": 1,
                          "russian_count": r, "ukrainian_count": u,
                          "adoption_ratio": u / (r + u)})

        # Kharkov/Kharkiv
        r, u = row["kharkov_count"], row["kharkiv_count"]
        if r + u > 0:
            rows.append({"week": month, "source_country": country, "pair_id": 2,
                          "russian_count": r, "ukrainian_count": u,
                          "adoption_ratio": u / (r + u)})

        # Odessa/Odesa
        r, u = row["odessa_count"], row["odesa_count"]
        if r + u > 0:
            rows.append({"week": month, "source_country": country, "pair_id": 3,
                          "russian_count": r, "ukrainian_count": u,
                          "adoption_ratio": u / (r + u)})

        # Lvov/Lviv
        r, u = row["lvov_count"], row["lviv_count"]
        if r + u > 0:
            rows.append({"week": month, "source_country": country, "pair_id": 4,
                          "russian_count": r, "ukrainian_count": u,
                          "adoption_ratio": u / (r + u)})

    result = pd.DataFrame(rows)
    result["week"] = pd.to_datetime(result["week"])

    out_path = PROCESSED_DIR / "gdelt_geographic.parquet"
    result.to_parquet(out_path, index=False)
    log.info(f"Saved: {out_path} ({len(result)} rows)")


if __name__ == "__main__":
    main()
