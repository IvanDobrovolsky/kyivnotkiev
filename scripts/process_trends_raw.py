"""Process raw Google Trends CSV files into unified long-format dataset.

Usage:
    python scripts/process_trends_raw.py
"""

import json
import logging

import numpy as np
import pandas as pd

from src.config import PROCESSED_DIR, TRENDS_RAW_DIR, ensure_dirs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_toponym_config() -> dict:
    with open("data/toponym_pairs.json") as f:
        return json.load(f)


def main():
    ensure_dirs()
    config = load_toponym_config()
    pairs_lookup = {p["id"]: p for p in config["pairs"]}

    csv_files = sorted(TRENDS_RAW_DIR.glob("pair_*_worldwide.csv"))
    log.info(f"Processing {len(csv_files)} Trends files")

    frames = []
    for f in csv_files:
        try:
            pid = int(f.stem.split("_")[1])
        except (IndexError, ValueError):
            continue

        pair = pairs_lookup.get(pid)
        if pair is None:
            continue

        df = pd.read_csv(f)

        russian = pair["russian"]
        ukrainian = pair["ukrainian"]

        # The CSV has a 'date' column and columns named after the search terms
        if "date" not in df.columns:
            log.warning(f"No 'date' column in {f}")
            continue

        r_col = russian if russian in df.columns else None
        u_col = ukrainian if ukrainian in df.columns else None

        if r_col is None and u_col is None:
            log.warning(f"Neither term found in columns for pair {pid}: {df.columns.tolist()}")
            continue

        rows = []
        for _, row in df.iterrows():
            r_val = int(row.get(r_col, 0)) if r_col else 0
            u_val = int(row.get(u_col, 0)) if u_col else 0

            # Google Trends returns 0-100 scale
            total = r_val + u_val
            ratio = u_val / total if total > 0 else np.nan

            rows.append({
                "week": row["date"],
                "pair_id": pid,
                "russian_term": russian,
                "ukrainian_term": ukrainian,
                "category": pair["category"],
                "russian_interest": r_val,
                "ukrainian_interest": u_val,
                "adoption_ratio": ratio,
                "source": "trends",
            })

        pair_df = pd.DataFrame(rows)
        frames.append(pair_df)

        latest_ratio = pair_df.tail(4)["adoption_ratio"].mean()
        log.info(f"  {pid:2d}. {russian:25s} -> {ukrainian:20s} latest_ratio={latest_ratio:.3f} ({len(pair_df)} weeks)")

    if not frames:
        log.error("No valid Trends data")
        return

    combined = pd.concat(frames, ignore_index=True)
    combined["week"] = pd.to_datetime(combined["week"])
    combined = combined.sort_values(["pair_id", "week"])

    out_path = PROCESSED_DIR / "trends_merged.parquet"
    combined.to_parquet(out_path, index=False)
    log.info(f"\nSaved: {out_path} ({len(combined)} rows, {combined['pair_id'].nunique()} pairs)")


if __name__ == "__main__":
    main()
