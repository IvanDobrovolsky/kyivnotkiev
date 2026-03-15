"""Geographic diffusion analysis of toponym adoption.

Identifies crossover dates per country and models the spatial diffusion
pattern of Ukrainian spelling adoption.

Usage:
    python -m src.analysis.geographic [--source gdelt|trends] [--pair-ids 1,2,3]
"""

import argparse
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd

from src.config import (
    PROCESSED_DIR,
    TARGET_COUNTRIES,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@dataclass
class CountryCrossover:
    """Crossover result for a single country + toponym pair."""
    pair_id: int
    country: str
    crossover_date: pd.Timestamp | None
    adoption_ratio_current: float
    has_crossed: bool
    days_from_first: int | None  # days after first country to cross


@dataclass
class DiffusionResult:
    """Full diffusion analysis for a toponym pair."""
    pair_id: int
    russian_term: str
    ukrainian_term: str
    first_adopter: str | None
    first_crossover_date: pd.Timestamp | None
    last_adopter: str | None
    last_crossover_date: pd.Timestamp | None
    n_countries_crossed: int
    n_countries_total: int
    median_crossover_date: pd.Timestamp | None
    country_crossovers: list[CountryCrossover]


def find_country_crossover(
    df: pd.DataFrame,
    window: int = 4,
) -> pd.Timestamp | None:
    """Find date when adoption ratio sustainably crosses 0.5 for a country."""
    if df.empty or "adoption_ratio" not in df.columns:
        return None

    ratio = df["adoption_ratio"].fillna(0)
    dates = pd.to_datetime(df["week"] if "week" in df.columns else df.index)

    smoothed = ratio.rolling(window=window, min_periods=1).mean()
    above = smoothed >= 0.5

    for i in range(len(above)):
        remaining = above.iloc[i:]
        if len(remaining) >= window and remaining.iloc[:window].all():
            return dates.iloc[i]

    return None


def analyze_pair_diffusion(
    df: pd.DataFrame,
    pair: dict,
) -> DiffusionResult:
    """Analyze geographic diffusion for a single toponym pair."""
    pair_id = pair["id"]
    log.info(f"Analyzing diffusion for pair {pair_id}: '{pair['russian']}' vs '{pair['ukrainian']}'")

    country_col = "source_country" if "source_country" in df.columns else "geo"
    countries = df[country_col].unique()

    crossovers = []
    for country in countries:
        country_data = df[df[country_col] == country].sort_values(
            "week" if "week" in df.columns else df.columns[0]
        )

        crossover_date = find_country_crossover(country_data)
        current_ratio = country_data["adoption_ratio"].iloc[-10:].mean() if len(country_data) >= 10 else 0

        crossovers.append(CountryCrossover(
            pair_id=pair_id,
            country=country,
            crossover_date=crossover_date,
            adoption_ratio_current=float(current_ratio),
            has_crossed=crossover_date is not None,
            days_from_first=None,  # computed below
        ))

    # Sort by crossover date
    crossed = [c for c in crossovers if c.has_crossed]
    crossed.sort(key=lambda c: c.crossover_date)

    # Compute days from first
    if crossed:
        first_date = crossed[0].crossover_date
        for c in crossovers:
            if c.has_crossed:
                c.days_from_first = (c.crossover_date - first_date).days

    # Median crossover
    median_date = None
    if crossed:
        dates = [c.crossover_date for c in crossed]
        median_idx = len(dates) // 2
        median_date = dates[median_idx]

    result = DiffusionResult(
        pair_id=pair_id,
        russian_term=pair["russian"],
        ukrainian_term=pair["ukrainian"],
        first_adopter=crossed[0].country if crossed else None,
        first_crossover_date=crossed[0].crossover_date if crossed else None,
        last_adopter=crossed[-1].country if crossed else None,
        last_crossover_date=crossed[-1].crossover_date if crossed else None,
        n_countries_crossed=len(crossed),
        n_countries_total=len(crossovers),
        median_crossover_date=median_date,
        country_crossovers=crossovers,
    )

    log.info(
        f"  {len(crossed)}/{len(crossovers)} countries crossed over. "
        f"First: {result.first_adopter} ({result.first_crossover_date})"
    )

    return result


def analyze_all(
    source: str = "gdelt",
    pair_ids: list[int] | None = None,
) -> list[DiffusionResult]:
    """Run geographic diffusion analysis for all pairs."""
    ensure_dirs()

    # Try geographic-specific file first, fall back to merged
    geo_data_path = PROCESSED_DIR / f"{source}_geographic.parquet"
    data_path = geo_data_path if geo_data_path.exists() else PROCESSED_DIR / f"{source}_merged.parquet"
    if not data_path.exists():
        log.error(f"Processed data not found: {data_path}")
        return []

    df = pd.read_parquet(data_path)
    pairs = get_all_pairs()

    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]
    else:
        # Only analyze pairs present in the geographic data
        available_ids = set(df["pair_id"].unique())
        pairs = [p for p in pairs if p["id"] in available_ids]

    geo_pairs = [p for p in pairs if not p["is_control"] or p["russian"] != p["ukrainian"]]

    results = []
    for pair in geo_pairs:
        pair_data = df[df["pair_id"] == pair["id"]]
        if pair_data.empty:
            continue

        result = analyze_pair_diffusion(pair_data, pair)
        results.append(result)

    # Save summary
    summary_rows = []
    for r in results:
        for cc in r.country_crossovers:
            summary_rows.append({
                "pair_id": r.pair_id,
                "russian_term": r.russian_term,
                "ukrainian_term": r.ukrainian_term,
                "country": cc.country,
                "crossover_date": cc.crossover_date,
                "has_crossed": cc.has_crossed,
                "days_from_first": cc.days_from_first,
                "adoption_ratio_current": cc.adoption_ratio_current,
            })

    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        out_path = PROCESSED_DIR / f"geographic_{source}.parquet"
        summary_df.to_parquet(out_path, index=False)
        log.info(f"Geographic results saved: {out_path}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Run geographic diffusion analysis")
    parser.add_argument("--source", type=str, default="gdelt",
                        choices=["gdelt", "trends"])
    parser.add_argument("--pair-ids", type=str, default=None)
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    analyze_all(source=args.source, pair_ids=pair_ids)


if __name__ == "__main__":
    main()
