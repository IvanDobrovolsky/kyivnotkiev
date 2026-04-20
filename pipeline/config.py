"""Load and validate pipeline configuration from YAML files."""

from pathlib import Path
from typing import Optional
import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "config"
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
GDELT_RAW_DIR = RAW_DIR / "gdelt"
TRENDS_RAW_DIR = RAW_DIR / "trends"
NGRAMS_RAW_DIR = RAW_DIR / "ngrams"
PROCESSED_DIR = DATA_DIR / "processed"
FIGURES_DIR = ROOT_DIR / "figures"

# Time range
START_DATE = "2010-01-01"
END_DATE = "2026-12-31"

# Google Trends settings
TRENDS_TIMEFRAME = "2015-01-01 2026-03-14"
TRENDS_GEO = ""
TRENDS_LANGUAGE = "en"
TRENDS_REQUEST_DELAY = 10
TRENDS_MAX_RETRIES = 5
TRENDS_BACKOFF_FACTOR = 2

# Change-point detection settings
CHANGEPOINT_MIN_SIZE = 4
CHANGEPOINT_MODELS = ["l2", "rbf"]

# Visualization constants
COLOR_RUSSIAN = "#E74C3C"
COLOR_UKRAINIAN = "#0057B8"
COLOR_EVENT = "#7F8C8D"
VIZ_DPI = 300
VIZ_FIGSIZE = (14, 7)
VIZ_STYLE = "seaborn-v0_8-whitegrid"

# Geopolitical events timeline
EVENTS_TIMELINE = [
    {"date": "2012-06-08", "name": "Euro 2012 (Kyiv)", "color": "#87CEEB"},
    {"date": "2013-11-21", "name": "Revolution of Dignity begins", "color": "#FFD700"},
    {"date": "2014-02-22", "name": "Euromaidan revolution", "color": "#FFD700"},
    {"date": "2014-03-18", "name": "Crimea annexation", "color": "#FF4500"},
    {"date": "2018-10-02", "name": "#KyivNotKiev campaign", "color": "#0057B8"},
    {"date": "2019-08-14", "name": "AP adopts Kyiv", "color": "#87CEEB"},
    {"date": "2019-09-01", "name": "Wikipedia switches to Kyiv", "color": "#87CEEB"},
    {"date": "2019-10-14", "name": "BBC adopts Kyiv", "color": "#87CEEB"},
    {"date": "2022-02-24", "name": "Full-scale invasion", "color": "#DC143C"},
    {"date": "2022-09-06", "name": "Kharkiv counteroffensive", "color": "#228B22"},
    {"date": "2022-11-11", "name": "Kherson liberation", "color": "#228B22"},
]

# Countries for geographic analysis
TARGET_COUNTRIES = [
    "US", "GB", "CA", "AU", "IE",
    "DE", "FR", "IT", "ES", "NL",
    "PL", "CZ", "SK", "HU", "RO",
    "LT", "LV", "EE", "FI", "SE",
    "NO", "DK",
    "UA", "BY", "RU",
    "TR", "IL", "IN", "JP", "BR",
    "ZA", "NG", "KR", "MX",
]


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


def get_all_pairs(config_dir: Path = CONFIG_DIR) -> list[dict]:
    """Return all pairs (enabled and disabled)."""
    cfg = load_pairs(config_dir)
    return cfg["pairs"]


def get_non_control_pairs(config_dir: Path = CONFIG_DIR) -> list[dict]:
    """Return all pairs that are not control cases."""
    return [p for p in get_all_pairs(config_dir) if not p.get("is_control", False)]


def get_categories(config_dir: Path = CONFIG_DIR) -> dict:
    """Return category definitions."""
    cfg = load_pairs(config_dir)
    return cfg.get("categories", {})


def ensure_dirs():
    """Create all output directories if they don't exist."""
    for d in [GDELT_RAW_DIR, TRENDS_RAW_DIR, NGRAMS_RAW_DIR, PROCESSED_DIR, FIGURES_DIR]:
        d.mkdir(parents=True, exist_ok=True)
