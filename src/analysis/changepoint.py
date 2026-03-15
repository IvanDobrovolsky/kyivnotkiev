"""Change-point detection for toponym adoption timeseries.

Applies PELT, BOCPD, and CUSUM algorithms to identify when Ukrainian
spellings overtook Russian-derived spellings in each data source.

Usage:
    python -m src.analysis.changepoint [--source gdelt|trends] [--pair-ids 1,2,3]
"""

import argparse
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
import ruptures as rpt
from scipy.stats import norm

from src.config import (
    PROCESSED_DIR,
    CHANGEPOINT_MIN_SIZE,
    CHANGEPOINT_PENALTY,
    CHANGEPOINT_MODELS,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


@dataclass
class ChangePointResult:
    """Result of change-point detection for a single toponym pair."""
    pair_id: int
    russian_term: str
    ukrainian_term: str
    category: str
    source: str
    method: str
    crossover_date: pd.Timestamp | None
    change_points: list[pd.Timestamp]
    change_type: str  # "step", "ramp", "none"
    confidence: float  # 0-1
    adoption_ratio_before: float
    adoption_ratio_after: float


def detect_pelt(signal: np.ndarray, min_size: int = CHANGEPOINT_MIN_SIZE) -> list[int]:
    """Apply PELT algorithm with multiple cost models, return consensus change points."""
    all_bkps = []
    for model in CHANGEPOINT_MODELS:
        algo = rpt.Pelt(model=model, min_size=min_size).fit(signal)
        bkps = algo.predict(pen=np.log(len(signal)) * signal.var())
        # Remove the last breakpoint (always == len(signal))
        all_bkps.append(set(bkps[:-1]) if bkps else set())

    # Consensus: keep change points found by at least one model
    # but prefer points found by multiple models
    if not all_bkps:
        return []

    consensus = set.union(*all_bkps)
    return sorted(consensus)


def detect_cusum(signal: np.ndarray, threshold: float | None = None) -> list[int]:
    """Apply CUSUM (Cumulative Sum) algorithm for change-point detection."""
    std = signal.std()
    if std == 0:
        return []

    if threshold is None:
        threshold = 0.5 * std

    mean = signal.mean()
    cusum_pos = np.zeros(len(signal))
    cusum_neg = np.zeros(len(signal))
    change_points = []

    detect_threshold = 3 * std

    for i in range(1, len(signal)):
        cusum_pos[i] = max(0, cusum_pos[i - 1] + signal[i] - mean - threshold)
        cusum_neg[i] = min(0, cusum_neg[i - 1] + signal[i] - mean + threshold)

        if cusum_pos[i] > detect_threshold or cusum_neg[i] < -detect_threshold:
            change_points.append(i)
            cusum_pos[i] = 0
            cusum_neg[i] = 0

    return change_points


def detect_bocpd(signal: np.ndarray, hazard_rate: float = 1 / 250) -> list[int]:
    """Bayesian Online Change Point Detection (simplified).

    Uses a sliding-window approach: compares the distribution of values
    in a window before vs after each point using a two-sample test.
    Points where the distributions differ significantly are change points.
    """
    n = len(signal)
    if n < 10:
        return []

    window = max(5, n // 20)
    change_points = []

    for t in range(window, n - window):
        before = signal[t - window:t]
        after = signal[t:t + window]

        # Welch's t-test between before and after windows
        t_stat, p_val = sp_ttest(before, after)

        if p_val < 0.01:  # significant difference
            # Only keep if not too close to existing change point
            if not change_points or t - change_points[-1] > window // 2:
                change_points.append(t)

    return change_points


def sp_ttest(a: np.ndarray, b: np.ndarray) -> tuple[float, float]:
    """Welch's t-test between two samples."""
    n1, n2 = len(a), len(b)
    m1, m2 = a.mean(), b.mean()
    v1, v2 = a.var(ddof=1), b.var(ddof=1)

    se = np.sqrt(v1 / n1 + v2 / n2) if (v1 / n1 + v2 / n2) > 0 else 1e-10
    t_stat = (m1 - m2) / se

    # Approximate p-value using normal distribution for simplicity
    p_val = 2 * (1 - norm.cdf(abs(t_stat)))
    return t_stat, p_val


def classify_change_type(signal: np.ndarray, cp_idx: int, window: int = 8) -> str:
    """Classify whether a change point represents a step or ramp transition.

    Compares the ratio of change achieved in a narrow zone (3 samples around
    the change point) vs a wide zone (2*window samples). Steps concentrate
    most change in the narrow zone; ramps spread it evenly.
    """
    wide = max(window, 10)
    if cp_idx < wide or cp_idx >= len(signal) - wide:
        return "step"

    # Wide zone: full extent of the transition
    wide_before = signal[max(0, cp_idx - wide):cp_idx - 1].mean()
    wide_after = signal[cp_idx + 1:min(len(signal), cp_idx + wide)].mean()
    total_jump = abs(wide_after - wide_before)

    if total_jump < 1e-9:
        return "none"

    # Narrow zone: just 1 sample before and after the change point
    narrow_jump = abs(signal[min(cp_idx + 1, len(signal) - 1)] - signal[max(cp_idx - 1, 0)])

    # In a step, the narrow zone captures most of the total change
    # In a ramp, the narrow zone captures only a small fraction
    if narrow_jump / total_jump > 0.5:
        return "step"
    else:
        return "ramp"


def find_crossover_date(
    dates: pd.Series,
    adoption_ratio: np.ndarray,
    window: int = 4,
) -> pd.Timestamp | None:
    """Find the date when adoption ratio crosses 0.5 (Ukrainian > Russian)."""
    # Smooth with rolling mean
    smoothed = pd.Series(adoption_ratio).rolling(window=window, min_periods=1).mean()

    # Find first sustained crossing above 0.5
    above = smoothed >= 0.5
    for i in range(len(above)):
        if i + window <= len(above) and above.iloc[i:i + window].all():
            return dates[i]

    return None


def analyze_pair(
    df: pd.DataFrame,
    pair: dict,
    source: str,
) -> ChangePointResult:
    """Run full change-point analysis on a single pair's timeseries."""
    pair_id = pair["id"]
    log.info(f"Analyzing pair {pair_id}: '{pair['russian']}' vs '{pair['ukrainian']}' ({source})")

    signal = df["adoption_ratio"].fillna(0).values
    dates = df["week"] if "week" in df.columns else df["year"]
    dates = pd.to_datetime(dates)

    if len(signal) < CHANGEPOINT_MIN_SIZE * 2:
        log.warning(f"  Pair {pair_id}: insufficient data ({len(signal)} points)")
        return ChangePointResult(
            pair_id=pair_id, russian_term=pair["russian"],
            ukrainian_term=pair["ukrainian"], category=pair["category"],
            source=source, method="none", crossover_date=None,
            change_points=[], change_type="none", confidence=0.0,
            adoption_ratio_before=0.0, adoption_ratio_after=0.0,
        )

    # Run all three detectors
    pelt_cps = detect_pelt(signal)
    cusum_cps = detect_cusum(signal)
    bocpd_cps = detect_bocpd(signal)

    # Merge nearby change points (within 4 weeks)
    all_cps = sorted(set(pelt_cps) | set(cusum_cps) | set(bocpd_cps))
    merged_cps = []
    for cp in all_cps:
        if not merged_cps or cp - merged_cps[-1] > 4:
            merged_cps.append(cp)

    # Convert indices to dates
    cp_dates = [dates.iloc[min(i, len(dates) - 1)] for i in merged_cps]

    # Find crossover
    crossover = find_crossover_date(dates, signal)

    # Classify change type at the most significant change point
    if merged_cps:
        # Most significant = largest jump
        jumps = []
        for cp in merged_cps:
            before = signal[max(0, cp - 4):cp].mean() if cp > 0 else 0
            after = signal[cp:min(len(signal), cp + 4)].mean()
            jumps.append(abs(after - before))
        main_cp = merged_cps[np.argmax(jumps)]
        change_type = classify_change_type(signal, main_cp)
    else:
        change_type = "none"

    # Confidence: how many methods agree?
    n_methods = sum([
        len(pelt_cps) > 0,
        len(cusum_cps) > 0,
        len(bocpd_cps) > 0,
    ])
    confidence = n_methods / 3.0

    # Before/after adoption ratios
    if merged_cps:
        main_idx = merged_cps[np.argmax(jumps)]
        ratio_before = signal[:main_idx].mean() if main_idx > 0 else 0
        ratio_after = signal[main_idx:].mean()
    else:
        ratio_before = signal[:len(signal) // 2].mean()
        ratio_after = signal[len(signal) // 2:].mean()

    result = ChangePointResult(
        pair_id=pair_id,
        russian_term=pair["russian"],
        ukrainian_term=pair["ukrainian"],
        category=pair["category"],
        source=source,
        method="ensemble(pelt+cusum+bocpd)",
        crossover_date=crossover,
        change_points=cp_dates,
        change_type=change_type,
        confidence=confidence,
        adoption_ratio_before=float(ratio_before),
        adoption_ratio_after=float(ratio_after),
    )

    log.info(f"  Crossover: {crossover}, type: {change_type}, confidence: {confidence:.2f}")
    return result


def analyze_all(
    source: str = "gdelt",
    pair_ids: list[int] | None = None,
) -> list[ChangePointResult]:
    """Run change-point analysis on all pairs for a given data source."""
    ensure_dirs()

    data_path = PROCESSED_DIR / f"{source}_merged.parquet"
    if not data_path.exists():
        log.error(f"Processed data not found: {data_path}")
        return []

    df = pd.read_parquet(data_path)
    pairs = get_all_pairs()

    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    results = []
    for pair in pairs:
        if pair["is_control"] and pair["russian"] == pair["ukrainian"]:
            continue

        pair_data = df[df["pair_id"] == pair["id"]]
        if pair_data.empty:
            log.warning(f"No data for pair {pair['id']}")
            continue

        # Aggregate to weekly level if needed
        if "week" in pair_data.columns:
            pair_data = pair_data.groupby("week").agg({
                "adoption_ratio": "mean",
            }).reset_index().sort_values("week")

        result = analyze_pair(pair_data, pair, source)
        results.append(result)

    # Save results
    results_df = pd.DataFrame([vars(r) for r in results])
    # Convert lists to strings for parquet compatibility
    results_df["change_points"] = results_df["change_points"].apply(str)
    out_path = PROCESSED_DIR / f"changepoints_{source}.parquet"
    results_df.to_parquet(out_path, index=False)
    log.info(f"Results saved: {out_path}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Run change-point detection on toponym data")
    parser.add_argument("--source", type=str, default="gdelt",
                        choices=["gdelt", "trends"],
                        help="Data source to analyze (default: gdelt)")
    parser.add_argument("--pair-ids", type=str, default=None,
                        help="Comma-separated pair IDs (default: all)")
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    analyze_all(source=args.source, pair_ids=pair_ids)


if __name__ == "__main__":
    main()
