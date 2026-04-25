"""Tests for export_site_data — parquet → JSON export pipeline."""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.export_site_data import (
    get_enabled_pair_ids,
    get_control_pair_ids,
    get_analyzable_pair_ids,
    smooth_series,
    _safe_div,
    GEO_TO_NUMERIC,
    GEO_NAMES,
)


class TestPairFiltering:
    def test_enabled_pairs_count(self):
        enabled = get_enabled_pair_ids()
        assert len(enabled) == 59

    def test_control_pairs_count(self):
        controls = get_control_pair_ids()
        assert len(controls) == 6

    def test_analyzable_pairs(self):
        analyzable = get_analyzable_pair_ids()
        enabled = get_enabled_pair_ids()
        controls = get_control_pair_ids()
        # Analyzable = enabled minus controls that are also enabled
        # (controls are disabled, so analyzable should equal enabled)
        assert len(analyzable) == 59

    def test_disabled_pair_27_not_enabled(self):
        enabled = get_enabled_pair_ids()
        assert 27 not in enabled, "Pair 27 (the Ukraine) should be disabled"

    def test_control_pair_12_not_analyzable(self):
        analyzable = get_analyzable_pair_ids()
        assert 12 not in analyzable, "Pair 12 (Donetsk control) should not be analyzable"

    def test_pair_1_is_enabled(self):
        enabled = get_enabled_pair_ids()
        assert 1 in enabled, "Pair 1 (Kiev/Kyiv) must be enabled"


class TestSmoothSeries:
    def test_empty_series(self):
        assert smooth_series([]) == []

    def test_no_smoothing_needed(self):
        series = [
            {"date": "2024-01", "adoption": 50.0},
            {"date": "2024-02", "adoption": 51.0},
            {"date": "2024-03", "adoption": 52.0},
        ]
        result = smooth_series(series)
        assert len(result) == 3
        assert all(d["adoption"] is not None for d in result)

    def test_null_values_filtered_when_not_noisy(self):
        series = [
            {"date": "2024-01", "adoption": 50.0},
            {"date": "2024-02", "adoption": None},
            {"date": "2024-03", "adoption": 52.0},
        ]
        result = smooth_series(series)
        # With < 10% nulls and no jumps, nulls are just filtered
        assert all(d["adoption"] is not None for d in result)

    def test_smoothing_with_high_noise(self):
        series = [
            {"date": f"2024-{i:02d}", "adoption": 90.0 if i % 2 == 0 else 10.0}
            for i in range(1, 13)
        ]
        result = smooth_series(series)
        # Smoothed values should be less extreme
        assert all(d["adoption"] is not None for d in result)


class TestSafeDiv:
    def test_normal_division(self):
        assert _safe_div(10, 2) == 5.0

    def test_zero_denominator(self):
        assert _safe_div(10, 0) == 0.0

    def test_zero_numerator(self):
        assert _safe_div(0, 10) == 0.0


class TestGeoMappings:
    def test_us_mapping(self):
        assert GEO_TO_NUMERIC["US"] == "840"
        assert GEO_NAMES["840"] == "United States"

    def test_ua_mapping(self):
        assert GEO_TO_NUMERIC["UA"] == "804"
        assert GEO_NAMES["804"] == "Ukraine"

    def test_ru_mapping(self):
        assert GEO_TO_NUMERIC["RU"] == "643"
        assert GEO_NAMES["643"] == "Russia"

    def test_all_numeric_codes_have_names(self):
        for geo, numeric in GEO_TO_NUMERIC.items():
            assert numeric in GEO_NAMES, f"No name for {geo} ({numeric})"
