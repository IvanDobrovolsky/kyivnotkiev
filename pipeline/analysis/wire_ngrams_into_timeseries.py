"""Wire fresh Google Books Ngrams data (corpus 37, English All 2022) into
the site's timeseries.json.

The ngrams ingestion saves one CSV per pair under data/raw/ngrams/. Each
CSV has columns year, frequency, term, variant, pair_id. The timeseries
shape the site expects is per-pair per-year:
  {"date": "YYYY-01", "adoption": pct, "ukr": int_count, "rus": int_count}

Ngrams gives us *frequency* (relative freq, not raw count). To convert
to the int counts the timeseries chart expects, we scale by 1e9 — the
site only cares about ratios so the absolute scale just needs to be
recognisable to the chart's tooltip rendering.

Usage:
    python -m pipeline.analysis.wire_ngrams_into_timeseries
"""

import csv
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
NGRAMS_DIR = ROOT / "data" / "raw" / "ngrams"
TIMESERIES_PATH = ROOT / "site" / "src" / "data" / "timeseries.json"

# Multiply ngrams relative frequency by this so we can store as int.
# Doesn't affect adoption ratio; only the absolute counts in tooltips.
SCALE = 1e9

# Only emit data points within this range. Pre-1900 noise dropped.
MIN_YEAR = 1900
MAX_YEAR = 2022


def load_pair_csv(path: Path) -> dict[int, dict]:
    """Returns {year: {ukr: float, rus: float}} for one pair CSV."""
    by_year: dict[int, dict] = {}
    with open(path) as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                year = int(row["year"])
                freq = float(row["frequency"])
            except (ValueError, KeyError):
                continue
            if year < MIN_YEAR or year > MAX_YEAR:
                continue
            slot = by_year.setdefault(year, {"ukr": 0.0, "rus": 0.0})
            if row["variant"] == "ukrainian":
                slot["ukr"] += freq
            elif row["variant"] == "russian":
                slot["rus"] += freq
    return by_year


def main():
    if not NGRAMS_DIR.exists():
        print(f"no ngrams dir: {NGRAMS_DIR}")
        return

    ts = json.load(open(TIMESERIES_PATH))
    print(f"timeseries pairs before: {len(ts)}")

    pairs_updated = 0
    total_points = 0

    for csv_path in sorted(NGRAMS_DIR.glob("pair_*.csv")):
        # pair_01.csv → 1
        try:
            pid = int(csv_path.stem.replace("pair_", ""))
        except ValueError:
            continue

        by_year = load_pair_csv(csv_path)
        if not by_year:
            continue

        series = []
        for year in sorted(by_year.keys()):
            ukr_freq = by_year[year]["ukr"]
            rus_freq = by_year[year]["rus"]
            total = ukr_freq + rus_freq
            if total == 0:
                continue
            adoption = round(ukr_freq / total * 100, 1)
            series.append({
                "date": f"{year}-01",
                "adoption": adoption,
                "ukr": int(round(ukr_freq * SCALE)),
                "rus": int(round(rus_freq * SCALE)),
            })

        if not series:
            continue

        pid_str = str(pid)
        if pid_str not in ts:
            ts[pid_str] = {}
        ts[pid_str]["ngrams"] = series
        pairs_updated += 1
        total_points += len(series)

    with open(TIMESERIES_PATH, "w") as f:
        json.dump(ts, f, indent=2)

    print(f"ngrams refreshed for {pairs_updated} pairs, {total_points} total data points")
    print(f"wrote {TIMESERIES_PATH}")


if __name__ == "__main__":
    main()
