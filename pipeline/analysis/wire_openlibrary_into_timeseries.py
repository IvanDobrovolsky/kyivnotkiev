"""Wire Open Library data into the site's timeseries.json.

The Open Library scrape ran (data/raw/openlibrary/openlibrary_results.json
contains 1728 rows of {pair_id, year, term, variant, book_count}) and
the source is registered in site/src/data/manifest.json — but the
per-pair-per-year data was never written into timeseries.json, which is
the file that drives all the per-pair source charts on the site.

This script pivots the raw rows into the per-source per-pair format
the site expects and writes it back into timeseries.json under the new
"openlibrary" key.

Output shape per pair:
  "openlibrary": [
    {"date": "2010-01", "adoption": 12.5, "ukr": 100, "rus": 700},
    {"date": "2011-01", ...},
    ...
  ]

Usage:
    python -m pipeline.analysis.wire_openlibrary_into_timeseries
"""

import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
RAW_PATH = ROOT / "data" / "raw" / "openlibrary" / "openlibrary_results.json"
TIMESERIES_PATH = ROOT / "site" / "src" / "data" / "timeseries.json"


def main():
    raw = json.load(open(RAW_PATH))
    print(f"raw rows: {len(raw)}")

    # Pivot: for each (pair_id, year) combine ru + ua book_counts
    by_pair_year = defaultdict(lambda: {"ukr": 0, "rus": 0})
    for row in raw:
        key = (row["pair_id"], row["year"])
        if row["variant"] == "ukrainian":
            by_pair_year[key]["ukr"] += row["book_count"]
        elif row["variant"] == "russian":
            by_pair_year[key]["rus"] += row["book_count"]

    # Group by pair, sort by year, build timeseries entries
    by_pair = defaultdict(list)
    for (pair_id, year), counts in by_pair_year.items():
        total = counts["ukr"] + counts["rus"]
        if total == 0:
            continue
        adoption = round(counts["ukr"] / total * 100, 1)
        by_pair[str(pair_id)].append({
            "date": f"{year}-01",
            "adoption": adoption,
            "ukr": counts["ukr"],
            "rus": counts["rus"],
        })
    for pid in by_pair:
        by_pair[pid].sort(key=lambda r: r["date"])

    # Merge into timeseries.json
    ts = json.load(open(TIMESERIES_PATH))
    print(f"timeseries pairs before: {len(ts)}")
    pairs_with_openlibrary = 0
    total_points = 0
    for pid, series in by_pair.items():
        if pid not in ts:
            ts[pid] = {}
        ts[pid]["openlibrary"] = series
        pairs_with_openlibrary += 1
        total_points += len(series)

    with open(TIMESERIES_PATH, "w") as f:
        json.dump(ts, f, indent=2)

    print(f"openlibrary added to {pairs_with_openlibrary} pairs, {total_points} total data points")
    print(f"wrote {TIMESERIES_PATH}")


if __name__ == "__main__":
    main()
