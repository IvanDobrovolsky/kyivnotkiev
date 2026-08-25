"""Load and validate pipeline configuration from YAML files.

Single source of truth: config/pairs.yaml
All pair identification uses slugs, never numeric IDs.
"""

from pathlib import Path
from typing import Optional
import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "config"
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

START_DATE = "2010-01-01"
END_DATE = "2025-12-31"


def load_pairs(config_dir: Path = CONFIG_DIR) -> dict:
    """Load pairs.yaml and return parsed config."""
    with open(config_dir / "pairs.yaml") as f:
        return yaml.safe_load(f)


def get_enabled_pairs(config_dir: Path = CONFIG_DIR) -> list[dict]:
    """Return all enabled pairs, ordered by slug."""
    cfg = load_pairs(config_dir)
    return [p for p in cfg["pairs"] if p.get("enabled", True)]


def get_pair_by_slug(slug: str, config_dir: Path = CONFIG_DIR) -> Optional[dict]:
    """Look up a single pair by its slug."""
    for p in get_enabled_pairs(config_dir):
        if p["slug"] == slug:
            return p
    return None


def get_slug_map(config_dir: Path = CONFIG_DIR) -> dict[str, dict]:
    """Return {slug: pair_config} for all enabled pairs."""
    return {p["slug"]: p for p in get_enabled_pairs(config_dir)}
