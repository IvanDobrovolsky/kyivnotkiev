"""Event correlation analysis using Granger causality and lagged correlation.

Tests whether geopolitical events (invasion, campaigns, media changes)
caused measurable shifts in toponym adoption.

Usage:
    python -m pipeline.analysis.events [--source gdelt|trends] [--pair-ids 1,2,3]
"""

import argparse
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats as sp_stats

from pipeline.config import (
    EVENTS_TIMELINE,
    PROCESSED_DIR,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@dataclass
class EventImpact:
    """Measured impact of a geopolitical event on a toponym pair."""
    pair_id: int
    event_name: str
    event_date: str
    ratio_before: float  # mean adoption ratio in window before event
    ratio_after: float   # mean adoption ratio in window after event
    delta: float         # after - before
    pct_change: float    # relative change
    t_statistic: float
    p_value: float
    significant: bool
    effect_size: float   # Cohen's d


def compute_event_impact(
    df: pd.DataFrame,
    pair: dict,
    event: dict,
    window_weeks: int = 8,
) -> EventImpact | None:
    """Compute the impact of a single event on a toponym pair's adoption ratio."""
    event_date = pd.Timestamp(event["date"])
    time_col = "week" if "week" in df.columns else df.columns[0]

    df = df.sort_values(time_col)
    dates = pd.to_datetime(df[time_col])

    # Windows before and after the event
    before_mask = (dates >= event_date - pd.Timedelta(weeks=window_weeks)) & (dates < event_date)
    after_mask = (dates >= event_date) & (dates < event_date + pd.Timedelta(weeks=window_weeks))

    before = df.loc[before_mask, "adoption_ratio"].dropna()
    after = df.loc[after_mask, "adoption_ratio"].dropna()

    if len(before) < 3 or len(after) < 3:
        return None

    before_mean = before.mean()
    after_mean = after.mean()
    delta = after_mean - before_mean
    pct_change = delta / before_mean if before_mean > 0 else float("inf")

    # Welch's t-test
    t_stat, p_val = sp_stats.ttest_ind(before.values, after.values, equal_var=False)

    # Cohen's d effect size
    pooled_std = np.sqrt((before.std() ** 2 + after.std() ** 2) / 2)
    cohens_d = delta / pooled_std if pooled_std > 0 else 0

    return EventImpact(
        pair_id=pair["id"],
        event_name=event["name"],
        event_date=event["date"],
        ratio_before=float(before_mean),
        ratio_after=float(after_mean),
        delta=float(delta),
        pct_change=float(pct_change),
        t_statistic=float(t_stat),
        p_value=float(p_val),
        significant=p_val < 0.05,
        effect_size=float(cohens_d),
    )


def granger_causality_test(
    adoption_ratio: np.ndarray,
    event_indicator: np.ndarray,
    max_lag: int = 4,
) -> dict:
    """Simplified Granger causality test.

    Tests whether the event indicator series helps predict adoption ratio
    beyond what adoption ratio alone can predict.
    """
    n = len(adoption_ratio)
    if n < max_lag * 3:
        return {"significant": False, "p_value": 1.0, "best_lag": 0}

    best_p = 1.0
    best_lag = 0

    for lag in range(1, max_lag + 1):
        # Restricted model: AR(lag) on adoption ratio only
        y = adoption_ratio[lag:]
        X_restricted = np.column_stack([adoption_ratio[lag - i - 1:n - i - 1] for i in range(lag)])

        # Unrestricted model: AR(lag) + lagged event indicator
        X_unrestricted = np.column_stack([
            X_restricted,
            *[event_indicator[lag - i - 1:n - i - 1].reshape(-1, 1) for i in range(lag)],
        ])

        # OLS for both models
        try:
            beta_r = np.linalg.lstsq(X_restricted, y, rcond=None)[0]
            resid_r = y - X_restricted @ beta_r
            ssr_r = (resid_r ** 2).sum()

            beta_u = np.linalg.lstsq(X_unrestricted, y, rcond=None)[0]
            resid_u = y - X_unrestricted @ beta_u
            ssr_u = (resid_u ** 2).sum()

            # F-test
            df1 = lag
            df2 = len(y) - 2 * lag
            if df2 > 0 and ssr_u > 0:
                f_stat = ((ssr_r - ssr_u) / df1) / (ssr_u / df2)
                p_val = 1 - sp_stats.f.cdf(f_stat, df1, df2)

                if p_val < best_p:
                    best_p = p_val
                    best_lag = lag
        except np.linalg.LinAlgError:
            continue

    return {
        "significant": best_p < 0.05,
        "p_value": float(best_p),
        "best_lag": best_lag,
    }


def create_event_indicator(dates: pd.Series, event_date: str, decay_weeks: int = 4) -> np.ndarray:
    """Create a binary/decaying indicator variable for an event."""
    event_ts = pd.Timestamp(event_date)
    indicator = np.zeros(len(dates))

    for i, d in enumerate(dates):
        d = pd.Timestamp(d)
        weeks_after = (d - event_ts).days / 7
        if 0 <= weeks_after <= decay_weeks:
            indicator[i] = 1.0 - (weeks_after / decay_weeks)  # linear decay

    return indicator


def analyze_pair_events(
    df: pd.DataFrame,
    pair: dict,
) -> list[EventImpact]:
    """Analyze impact of all events on a single toponym pair."""
    results = []

    for event in EVENTS_TIMELINE:
        impact = compute_event_impact(df, pair, event)
        if impact is not None:
            results.append(impact)
            if impact.significant:
                log.info(
                    f"  {event['name']}: delta={impact.delta:+.3f}, "
                    f"p={impact.p_value:.4f}, d={impact.effect_size:.2f}"
                )

    return results


def analyze_all(
    source: str = "gdelt",
    pair_ids: list[int] | None = None,
) -> pd.DataFrame:
    """Run event correlation analysis for all pairs."""
    ensure_dirs()

    data_path = PROCESSED_DIR / f"{source}_merged.parquet"
    if not data_path.exists():
        log.error(f"Processed data not found: {data_path}")
        return pd.DataFrame()

    df = pd.read_parquet(data_path)
    pairs = get_all_pairs()

    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    all_impacts = []
    for pair in pairs:
        if pair["is_control"] and pair["russian"] == pair["ukrainian"]:
            continue

        pair_data = df[df["pair_id"] == pair["id"]]
        if pair_data.empty:
            continue

        log.info(f"Pair {pair['id']}: '{pair['russian']}' vs '{pair['ukrainian']}'")
        impacts = analyze_pair_events(pair_data, pair)
        all_impacts.extend(impacts)

    if all_impacts:
        results_df = pd.DataFrame([vars(i) for i in all_impacts])
        out_path = PROCESSED_DIR / f"events_{source}.parquet"
        results_df.to_parquet(out_path, index=False)
        log.info(f"Event analysis saved: {out_path} ({len(results_df)} impact measurements)")
        return results_df

    return pd.DataFrame()


def main():
    parser = argparse.ArgumentParser(description="Event correlation analysis")
    parser.add_argument("--source", type=str, default="gdelt", choices=["gdelt", "trends"])
    parser.add_argument("--pair-ids", type=str, default=None)
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    analyze_all(source=args.source, pair_ids=pair_ids)


if __name__ == "__main__":
    main()
