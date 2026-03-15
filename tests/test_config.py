"""Tests for config module."""

from src.config import (
    load_toponym_pairs,
    get_all_pairs,
    get_pairs_by_category,
    get_non_control_pairs,
    get_categories,
    ROOT_DIR,
    TOPONYM_PAIRS_PATH,
)


def test_toponym_pairs_loads():
    data = load_toponym_pairs()
    assert "pairs" in data
    assert "categories" in data
    assert "metadata" in data


def test_pair_count():
    pairs = get_all_pairs()
    assert len(pairs) == 37


def test_category_count():
    categories = get_categories()
    assert len(categories) == 7


def test_non_control_pairs():
    non_control = get_non_control_pairs()
    all_pairs = get_all_pairs()
    controls = [p for p in all_pairs if p["is_control"]]
    assert len(non_control) == len(all_pairs) - len(controls)


def test_geographical_is_largest():
    geo = get_pairs_by_category("geographical")
    for cat_id in ["food", "landmarks", "country", "institutional", "sports", "historical"]:
        other = get_pairs_by_category(cat_id)
        assert len(geo) > len(other), f"geographical ({len(geo)}) should be larger than {cat_id} ({len(other)})"


def test_pair_schema():
    pairs = get_all_pairs()
    required_fields = {"id", "russian", "ukrainian", "category", "is_control"}
    for pair in pairs:
        assert required_fields.issubset(pair.keys()), f"Pair {pair.get('id')} missing fields"


def test_control_cases():
    pairs = get_all_pairs()
    controls = [p for p in pairs if p["is_control"]]
    assert len(controls) >= 3  # Donetsk, Mariupol, Kherson, Shakhtar


def test_paths_exist():
    assert ROOT_DIR.exists()
    assert TOPONYM_PAIRS_PATH.exists()
