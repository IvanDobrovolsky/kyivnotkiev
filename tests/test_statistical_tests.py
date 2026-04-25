"""Tests for statistical_tests — bootstrap CIs, Pettitt, Wilson."""

import numpy as np
import pytest

from pipeline.analysis.statistical_tests import (
    bootstrap_pct_ci,
    pettitt_changepoint,
    wilson_ci,
)


class TestBootstrapCI:
    def test_large_n_normal_approximation(self):
        """For n > 50,000, uses normal approximation."""
        result = bootstrap_pct_ci(ru=30000, ua=30000)
        assert result["method"] == "normal"
        assert abs(result["point"] - 50.0) < 0.5
        assert result["lo"] < result["point"]
        assert result["hi"] > result["point"]
        assert result["lo"] >= 0
        assert result["hi"] <= 100

    def test_small_n_bootstrap(self):
        """For n <= 50,000, uses parametric bootstrap."""
        result = bootstrap_pct_ci(ru=50, ua=50)
        assert result["method"] == "bootstrap"
        assert abs(result["point"] - 50.0) < 10.0
        assert result["lo"] < result["hi"]

    def test_zero_adoption(self):
        result = bootstrap_pct_ci(ru=1000, ua=0)
        assert result["point"] == 0.0

    def test_full_adoption(self):
        result = bootstrap_pct_ci(ru=0, ua=1000)
        assert result["point"] == 100.0

    def test_ci_brackets_point(self):
        result = bootstrap_pct_ci(ru=300, ua=700)
        assert result["lo"] <= result["point"] <= result["hi"]

    def test_narrow_ci_for_large_n(self):
        result = bootstrap_pct_ci(ru=500000, ua=500000)
        width = result["hi"] - result["lo"]
        assert width < 1.0, f"CI too wide for n=1M: {width}pp"


class TestPettittTest:
    def test_clear_changepoint(self):
        """Series with obvious shift should detect changepoint."""
        series = [10] * 20 + [90] * 20
        result = pettitt_changepoint(series)
        assert 15 <= result["changepoint_index"] <= 25
        assert result["p_value"] < 0.05

    def test_no_changepoint(self):
        """Flat series should not detect changepoint."""
        series = [50] * 40
        result = pettitt_changepoint(series)
        assert result["p_value"] > 0.05 or result["K_stat"] < 100

    def test_gradual_change(self):
        series = list(range(0, 100, 5))
        result = pettitt_changepoint(series)
        assert isinstance(result["changepoint_index"], int)


class TestWilsonCI:
    def test_basic_wilson(self):
        result = wilson_ci(k=50, n=100)
        assert result is not None
        lo, hi = result
        assert lo < 0.5 < hi
        assert lo > 0
        assert hi < 1

    def test_zero_successes(self):
        result = wilson_ci(k=0, n=100)
        assert result is not None
        lo, hi = result
        assert lo < 0.05
        assert hi > 0

    def test_all_successes(self):
        result = wilson_ci(k=100, n=100)
        assert result is not None
        lo, hi = result
        assert lo < 1
        assert hi > 0.95

    def test_zero_total(self):
        result = wilson_ci(k=0, n=0)
        assert result is None
