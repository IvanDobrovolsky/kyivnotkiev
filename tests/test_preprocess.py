"""Tests for preprocessing validation."""

import pandas as pd

from pipeline.transform.preprocess import validate


def test_validate_valid_data():
    df = pd.DataFrame({
        "pair_id": [1, 1, 2, 2],
        "adoption_ratio": [0.3, 0.7, 0.5, 0.8],
    })
    assert validate(df, "test") is True


def test_validate_empty():
    df = pd.DataFrame()
    assert validate(df, "test") is False


def test_validate_out_of_range():
    df = pd.DataFrame({
        "pair_id": [1],
        "adoption_ratio": [1.5],  # invalid
    })
    assert validate(df, "test") is False
