"""Tests for change-point detection algorithms."""

import numpy as np
import pandas as pd

from pipeline.analysis.changepoint import (
    classify_change_type,
    detect_bocpd,
    detect_cusum,
    detect_pelt,
    find_crossover_date,
)


def _make_step_signal(n=200, cp=100, before=0.2, after=0.8, noise=0.05):
    """Create a synthetic step-change signal."""
    signal = np.concatenate([
        np.random.normal(before, noise, cp),
        np.random.normal(after, noise, n - cp),
    ])
    return signal


def _make_ramp_signal(n=200, start=50, end=150, low=0.1, high=0.9, noise=0.03):
    """Create a synthetic gradual-ramp signal."""
    signal = np.full(n, low)
    ramp = np.linspace(low, high, end - start)
    signal[start:end] = ramp
    signal[end:] = high
    return signal + np.random.normal(0, noise, n)


def test_pelt_detects_step():
    signal = _make_step_signal()
    cps = detect_pelt(signal)
    assert len(cps) >= 1
    # Change point should be near index 100
    assert any(abs(cp - 100) < 15 for cp in cps)


def test_cusum_detects_step():
    signal = _make_step_signal()
    cps = detect_cusum(signal)
    assert len(cps) >= 1


def test_bocpd_detects_step():
    signal = _make_step_signal(n=200, before=0.0, after=1.0, noise=0.05)
    cps = detect_bocpd(signal, hazard_rate=1 / 100)
    assert len(cps) >= 1


def test_classify_step():
    signal = _make_step_signal()
    change_type = classify_change_type(signal, 100)
    assert change_type == "step"


def test_classify_ramp():
    np.random.seed(42)
    signal = _make_ramp_signal(noise=0.01)
    change_type = classify_change_type(signal, 100)
    assert change_type == "ramp"


def test_find_crossover_date():
    dates = pd.date_range("2020-01-01", periods=100, freq="W")
    # Ratio crosses 0.5 at index 50
    ratio = np.concatenate([
        np.full(50, 0.3),
        np.full(50, 0.7),
    ])
    crossover = find_crossover_date(dates, ratio)
    assert crossover is not None
    # Rolling mean causes slight delay; crossover should be near index 50
    crossover_idx = list(dates).index(crossover)
    assert 49 <= crossover_idx <= 53


def test_find_crossover_no_cross():
    dates = pd.date_range("2020-01-01", periods=100, freq="W")
    ratio = np.full(100, 0.3)  # never crosses
    crossover = find_crossover_date(dates, ratio)
    assert crossover is None
