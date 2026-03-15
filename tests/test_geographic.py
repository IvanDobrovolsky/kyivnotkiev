"""Tests for geographic diffusion analysis."""

import numpy as np
import pandas as pd

from src.analysis.geographic import find_country_crossover


def test_crossover_found():
    dates = pd.date_range("2020-01-01", periods=100, freq="W")
    df = pd.DataFrame({
        "week": dates,
        "adoption_ratio": np.concatenate([np.full(40, 0.3), np.full(60, 0.7)]),
    })
    result = find_country_crossover(df)
    assert result is not None
    assert result >= dates[40]


def test_crossover_not_found():
    dates = pd.date_range("2020-01-01", periods=100, freq="W")
    df = pd.DataFrame({
        "week": dates,
        "adoption_ratio": np.full(100, 0.2),
    })
    result = find_country_crossover(df)
    assert result is None


def test_crossover_requires_sustained():
    """A brief spike above 0.5 should not count as crossover."""
    dates = pd.date_range("2020-01-01", periods=100, freq="W")
    ratio = np.full(100, 0.3)
    ratio[50] = 0.6  # single spike
    ratio[51] = 0.3  # back down
    df = pd.DataFrame({"week": dates, "adoption_ratio": ratio})
    result = find_country_crossover(df, window=4)
    assert result is None
