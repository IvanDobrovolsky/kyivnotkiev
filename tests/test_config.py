"""Tests for config module."""

from pipeline.config import (
    ROOT_DIR,
    CONFIG_DIR,
    get_all_pairs,
    get_categories,
    get_enabled_pairs,
    get_non_control_pairs,
    get_pairs_by_category,
    load_pairs,
)


def test_pairs_loads():
    data = load_pairs()
    assert "pairs" in data
    assert "categories" in data
    assert "metadata" in data


def test_enabled_pair_count():
    pairs = get_enabled_pairs()
    assert len(pairs) >= 50  # at least 50 enabled pairs


def test_category_count():
    categories = get_categories()
    assert len(categories) == 8


def test_non_control_pairs():
    non_control = get_non_control_pairs()
    all_pairs = get_all_pairs()
    controls = [p for p in all_pairs if p.get("is_control", False)]
    assert len(non_control) == len(all_pairs) - len(controls)


def test_geographical_is_largest():
    geo = get_pairs_by_category("geographical")
    for cat_id in ["food", "landmarks", "country", "institutional", "sports", "historical"]:
        other = get_pairs_by_category(cat_id)
        assert len(geo) >= len(other), f"geographical ({len(geo)}) should be >= {cat_id} ({len(other)})"


def test_pair_schema():
    pairs = get_all_pairs()
    required_fields = {"id", "russian", "ukrainian", "category"}
    for pair in pairs:
        assert required_fields.issubset(pair.keys()), f"Pair {pair.get('id')} missing fields"


def test_starred_pairs():
    pairs = get_all_pairs()
    starred = [p for p in pairs if p.get("starred", False)]
    assert len(starred) == 9, f"Expected 9 starred pairs, got {len(starred)}"
    starred_ids = {p["id"] for p in starred}
    assert starred_ids == {1, 3, 10, 17, 23, 61, 70, 72, 85}


def test_paths_exist():
    assert ROOT_DIR.exists()
    assert CONFIG_DIR.exists()
    assert (CONFIG_DIR / "pairs.yaml").exists()
