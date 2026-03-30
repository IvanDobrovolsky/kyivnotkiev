"""Load and validate pipeline configuration from YAML files."""

from pathlib import Path
from typing import Optional
import yaml

CONFIG_DIR = Path(__file__).parent.parent / "config"


def load_pairs(config_dir: Path = CONFIG_DIR) -> dict:
    """Load pairs.yaml and return parsed config."""
    with open(config_dir / "pairs.yaml") as f:
        return yaml.safe_load(f)


def load_sources(config_dir: Path = CONFIG_DIR) -> dict:
    """Load sources.yaml and return parsed config."""
    with open(config_dir / "sources.yaml") as f:
        return yaml.safe_load(f)


def load_pipeline(config_dir: Path = CONFIG_DIR) -> dict:
    """Load pipeline.yaml and return parsed config."""
    with open(config_dir / "pipeline.yaml") as f:
        return yaml.safe_load(f)


def get_enabled_pairs(config_dir: Path = CONFIG_DIR) -> list[dict]:
    """Return only enabled (non-disabled) pairs."""
    cfg = load_pairs(config_dir)
    return [p for p in cfg["pairs"] if p.get("enabled", True)]


def get_pair_by_id(pair_id: int, config_dir: Path = CONFIG_DIR) -> Optional[dict]:
    """Look up a single pair by its ID."""
    cfg = load_pairs(config_dir)
    for p in cfg["pairs"]:
        if p["id"] == pair_id:
            return p
    return None


def get_pairs_by_category(category: str, config_dir: Path = CONFIG_DIR) -> list[dict]:
    """Return all enabled pairs in a given category."""
    return [p for p in get_enabled_pairs(config_dir) if p["category"] == category]


def get_gcp_config(config_dir: Path = CONFIG_DIR) -> dict:
    """Return GCP project/region/dataset config."""
    return load_pipeline(config_dir)["gcp"]
