"""Process ALL raw data from all sources into unified datasets.

Handles GDELT (events + GKG + extras + expanded), Trends, and Ngrams.
Produces gdelt_merged.parquet and trends_merged.parquet with all pairs.

Usage:
    python scripts/process_all.py
"""

import json
import logging

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.config import GDELT_RAW_DIR, NGRAMS_RAW_DIR, PROCESSED_DIR, TRENDS_RAW_DIR, ensure_dirs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_config():
    with open("data/toponym_pairs.json") as f:
        return json.load(f)


def read_parquet_safe(path):
    table = pq.read_table(path)
    new_columns = []
    for i, field in enumerate(table.schema):
        col = table.column(i)
        if pa.types.is_date(field.type):
            col = col.cast(pa.timestamp("ns"))
        new_columns.append(col)
    return pa.table({field.name: col for field, col in zip(table.schema, new_columns)}).to_pandas()


def process_wide_gdelt(df, pair_map):
    """Convert wide-format (russian_N, ukrainian_N) to long-format."""
    rows = []
    for _, row in df.iterrows():
        week = row["week"]
        for col in df.columns:
            if col.startswith("russian_"):
                pid = int(col.split("_", 1)[1])
                u_col = f"ukrainian_{pid}"
                r_count = int(row.get(col, 0))
                u_count = int(row.get(u_col, 0))
                if r_count == 0 and u_count == 0:
                    continue
                pair = pair_map.get(pid, {})
                total = r_count + u_count
                rows.append({
                    "week": week, "pair_id": pid,
                    "russian_term": pair.get("russian", ""),
                    "ukrainian_term": pair.get("ukrainian", ""),
                    "category": pair.get("category", ""),
                    "russian_count": r_count, "ukrainian_count": u_count,
                    "total_count": total,
                    "adoption_ratio": u_count / total if total > 0 else np.nan,
                    "source": "gdelt",
                })
    return pd.DataFrame(rows)


def process_expanded_cities(df, pair_map):
    """Process the expanded_cities.parquet with custom column mapping."""
    # Map column names to pair IDs
    city_map = {
        ("chernigov_count", "chernihiv_count"): 38,
        ("chernovtsy_count", "chernivtsi_count"): 39,
        ("zhitomir_count", "zhytomyr_count"): 40,
        ("cherkassy_count", "cherkasy_count"): 41,
        ("uzhgorod_count", "uzhhorod_count"): 42,
        ("kremenchug_count", "kremenchuk_count"): 43,
        ("kirovograd_count", "ivanofrankivsk_count"): None,  # skip, different pairing
        ("tarnopol_count", "ternopil_count"): 45,
    }

    # Kirovograd → Kropyvnytskyi (pair 44)
    # We need kirovograd_count and kropyvnytskyi_count
    rows = []
    for _, row in df.iterrows():
        week = row["week"]

        for (r_col, u_col), pid in city_map.items():
            if pid is None:
                continue
            if r_col not in df.columns or u_col not in df.columns:
                continue
            r_count = int(row.get(r_col, 0))
            u_count = int(row.get(u_col, 0))
            if r_count == 0 and u_count == 0:
                continue
            pair = pair_map.get(pid, {})
            total = r_count + u_count
            rows.append({
                "week": week, "pair_id": pid,
                "russian_term": pair.get("russian", ""),
                "ukrainian_term": pair.get("ukrainian", ""),
                "category": pair.get("category", ""),
                "russian_count": r_count, "ukrainian_count": u_count,
                "total_count": total,
                "adoption_ratio": u_count / total if total > 0 else np.nan,
                "source": "gdelt",
            })

        # Kirovograd/Kropyvnytskyi (pair 44)
        if "kirovograd_count" in df.columns:
            r44 = int(row.get("kirovograd_count", 0))
            u44 = int(row.get("kropyvnytskyi_count", 0)) if "kropyvnytskyi_count" in df.columns else 0
            if r44 + u44 > 0:
                pair = pair_map.get(44, {})
                total = r44 + u44
                rows.append({
                    "week": week, "pair_id": 44,
                    "russian_term": pair.get("russian", "Kirovograd"),
                    "ukrainian_term": pair.get("ukrainian", "Kropyvnytskyi"),
                    "category": "geographical",
                    "russian_count": r44, "ukrainian_count": u44,
                    "total_count": total,
                    "adoption_ratio": u44 / total if total > 0 else np.nan,
                    "source": "gdelt",
                })

    return pd.DataFrame(rows)


def main():
    ensure_dirs()
    config = load_config()
    pair_map = {p["id"]: p for p in config["pairs"]}

    # ── GDELT ──────────────────────────────────────────────────────────────────
    gdelt_frames = []

    # 1. Geographical events
    path = GDELT_RAW_DIR / "geographical_events.parquet"
    if path.exists():
        df = read_parquet_safe(path)
        long = process_wide_gdelt(df, pair_map)
        log.info(f"geographical_events: {len(long)} rows")
        gdelt_frames.append(long)

    # 2. Non-geographical GKG (DocumentIdentifier search)
    path = GDELT_RAW_DIR / "non_geographical_gkg.parquet"
    if path.exists():
        df = read_parquet_safe(path)
        long = process_wide_gdelt(df, pair_map)
        log.info(f"non_geographical_gkg: {len(long)} rows")
        gdelt_frames.append(long)

    # 3. Extras search (full-text GKG)
    path = GDELT_RAW_DIR / "extras_search.parquet"
    if path.exists():
        df = read_parquet_safe(path)
        long = process_wide_gdelt(df, pair_map)
        log.info(f"extras_search: {len(long)} rows")
        gdelt_frames.append(long)

    # 4. Expanded cities
    path = GDELT_RAW_DIR / "expanded_cities.parquet"
    if path.exists():
        df = read_parquet_safe(path)
        long = process_expanded_cities(df, pair_map)
        log.info(f"expanded_cities: {len(long)} rows")
        gdelt_frames.append(long)

    if gdelt_frames:
        gdelt = pd.concat(gdelt_frames, ignore_index=True)
        gdelt["week"] = pd.to_datetime(gdelt["week"])

        # Deduplicate: if same pair_id+week appears from multiple sources, sum counts
        gdelt_agg = gdelt.groupby(["week", "pair_id"]).agg({
            "russian_term": "first",
            "ukrainian_term": "first",
            "category": "first",
            "russian_count": "sum",
            "ukrainian_count": "sum",
            "total_count": "sum",
            "source": "first",
        }).reset_index()

        gdelt_agg["adoption_ratio"] = gdelt_agg["ukrainian_count"] / gdelt_agg["total_count"]
        gdelt_agg["adoption_ratio"] = gdelt_agg["adoption_ratio"].replace([np.inf, -np.inf], np.nan)
        gdelt_agg = gdelt_agg.sort_values(["pair_id", "week"])

        out = PROCESSED_DIR / "gdelt_merged.parquet"
        gdelt_agg.to_parquet(out, index=False)
        log.info(f"GDELT merged: {len(gdelt_agg)} rows, {gdelt_agg['pair_id'].nunique()} pairs -> {out}")

        # Summary
        for pid in sorted(gdelt_agg["pair_id"].unique()):
            p = gdelt_agg[gdelt_agg["pair_id"] == pid]
            latest = p.tail(8)["adoption_ratio"].mean()
            log.info(f"  {pid:2d}. {p['russian_term'].iloc[0]:25s} -> {p['ukrainian_term'].iloc[0]:20s} latest={latest:.3f}")

    # ── Trends ─────────────────────────────────────────────────────────────────
    csv_files = sorted(TRENDS_RAW_DIR.glob("pair_*_worldwide.csv"))
    trends_frames = []

    for f in csv_files:
        try:
            pid = int(f.stem.split("_")[1])
        except (IndexError, ValueError):
            continue

        pair = pair_map.get(pid)
        if pair is None:
            # New pair not yet in config — try to infer from CSV
            df = pd.read_csv(f)
            if "date" not in df.columns:
                continue
            # Use column names as term names
            cols = [c for c in df.columns if c not in ["date", "pair_id", "geo", "isPartial"]]
            if len(cols) < 2:
                continue
            russian, ukrainian = cols[0], cols[1]
            category = "geographical"
        else:
            russian = pair["russian"]
            ukrainian = pair["ukrainian"]
            category = pair["category"]
            df = pd.read_csv(f)

        if "date" not in df.columns:
            continue

        r_col = russian if russian in df.columns else None
        u_col = ukrainian if ukrainian in df.columns else None

        if r_col is None and u_col is None:
            continue

        rows = []
        for _, row in df.iterrows():
            r_val = int(row.get(r_col, 0)) if r_col else 0
            u_val = int(row.get(u_col, 0)) if u_col else 0
            total = r_val + u_val
            rows.append({
                "week": row["date"], "pair_id": pid,
                "russian_term": russian, "ukrainian_term": ukrainian,
                "category": category,
                "russian_interest": r_val, "ukrainian_interest": u_val,
                "adoption_ratio": u_val / total if total > 0 else np.nan,
                "source": "trends",
            })

        pair_df = pd.DataFrame(rows)
        trends_frames.append(pair_df)

    if trends_frames:
        trends = pd.concat(trends_frames, ignore_index=True)
        trends["week"] = pd.to_datetime(trends["week"])
        trends = trends.sort_values(["pair_id", "week"])

        out = PROCESSED_DIR / "trends_merged.parquet"
        trends.to_parquet(out, index=False)
        log.info(f"Trends merged: {len(trends)} rows, {trends['pair_id'].nunique()} pairs -> {out}")

    # ── Ngrams ─────────────────────────────────────────────────────────────────
    csv_files = sorted(NGRAMS_RAW_DIR.glob("pair_*.csv"))
    ngrams_frames = []

    for f in csv_files:
        df = pd.read_csv(f)
        if "pair_id" not in df.columns:
            continue
        pid = df["pair_id"].iloc[0]
        pair = pair_map.get(pid)
        if pair is None:
            continue

        if "variant" in df.columns:
            r_data = df[df["variant"] == "russian"][["year", "frequency"]].rename(columns={"frequency": "russian_freq"})
            u_data = df[df["variant"] == "ukrainian"][["year", "frequency"]].rename(columns={"frequency": "ukrainian_freq"})

            if not r_data.empty and not u_data.empty:
                merged = r_data.merge(u_data, on="year", how="outer").fillna(0)
            elif not r_data.empty:
                merged = r_data.copy()
                merged["ukrainian_freq"] = 0
            else:
                merged = u_data.copy()
                merged["russian_freq"] = 0

            total = merged["russian_freq"] + merged["ukrainian_freq"]
            merged["adoption_ratio"] = merged["ukrainian_freq"] / total.replace(0, np.nan)
            merged["pair_id"] = pid
            merged["category"] = pair["category"]
            merged["russian_term"] = pair["russian"]
            merged["ukrainian_term"] = pair["ukrainian"]
            ngrams_frames.append(merged)

    if ngrams_frames:
        ngrams = pd.concat(ngrams_frames, ignore_index=True)
        ngrams["source"] = "ngrams"
        out = PROCESSED_DIR / "ngrams_merged.parquet"
        ngrams.to_parquet(out, index=False)
        log.info(f"Ngrams merged: {len(ngrams)} rows, {ngrams['pair_id'].nunique()} pairs -> {out}")

    log.info("=== All processing complete ===")


if __name__ == "__main__":
    main()
