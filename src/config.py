"""Shared configuration, paths, and constants for the KyivNotKiev pipeline."""

import json
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────────

ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
GDELT_RAW_DIR = RAW_DIR / "gdelt"
TRENDS_RAW_DIR = RAW_DIR / "trends"
NGRAMS_RAW_DIR = RAW_DIR / "ngrams"
PROCESSED_DIR = DATA_DIR / "processed"
FIGURES_DIR = ROOT_DIR / "paper" / "figures"
TOPONYM_PAIRS_PATH = DATA_DIR / "toponym_pairs.json"

# ── Time range ─────────────────────────────────────────────────────────────────

START_DATE = "2015-01-01"
END_DATE = "2026-03-14"

# ── Google Trends settings ─────────────────────────────────────────────────────

TRENDS_TIMEFRAME = "2015-01-01 2026-03-14"
TRENDS_GEO = ""  # worldwide
TRENDS_LANGUAGE = "en"
TRENDS_REQUEST_DELAY = 10  # seconds between requests
TRENDS_MAX_RETRIES = 5
TRENDS_BACKOFF_FACTOR = 2

# ── GDELT BigQuery settings ───────────────────────────────────────────────────

GDELT_PROJECT = "gdelt-bq"
GDELT_GKG_TABLE = "gdelt-bq.gdeltv2.gkg_partitioned"
GDELT_EVENTS_TABLE = "gdelt-bq.gdeltv2.events_partitioned"
BQ_COST_LIMIT_TB = 0.9  # stay under 1TB free tier

# ── Countries for geographic analysis ──────────────────────────────────────────

TARGET_COUNTRIES = [
    "US", "GB", "CA", "AU", "IE",   # Anglosphere
    "DE", "FR", "IT", "ES", "NL",   # Western Europe
    "PL", "CZ", "SK", "HU", "RO",   # Central Europe
    "LT", "LV", "EE", "FI", "SE",   # Nordics + Baltics
    "NO", "DK",                       # Nordics
    "UA", "BY", "RU",                 # Post-Soviet
    "TR", "IL", "IN", "JP", "BR",   # Global
    "ZA", "NG", "KR", "MX",         # Global
]

# ── Geopolitical events timeline ───────────────────────────────────────────────

EVENTS_TIMELINE = [
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

# ── Change-point detection settings ────────────────────────────────────────────

CHANGEPOINT_MIN_SIZE = 4   # minimum segment length (weeks)
CHANGEPOINT_PENALTY = "bic"  # BIC penalty for PELT
CHANGEPOINT_MODELS = ["l2", "rbf"]  # cost models to try

# ── Visualization settings ─────────────────────────────────────────────────────

VIZ_DPI = 300
VIZ_FIGSIZE = (14, 7)
VIZ_STYLE = "seaborn-v0_8-whitegrid"
COLOR_RUSSIAN = "#E74C3C"   # red
COLOR_UKRAINIAN = "#0057B8"  # Ukrainian blue
COLOR_EVENT = "#7F8C8D"     # gray for event lines


def load_toponym_pairs() -> dict:
    """Load the toponym pairs configuration."""
    with open(TOPONYM_PAIRS_PATH) as f:
        return json.load(f)


def get_pairs_by_category(category_id: str) -> list[dict]:
    """Get all toponym pairs for a given category."""
    data = load_toponym_pairs()
    return [p for p in data["pairs"] if p["category"] == category_id]


def get_non_control_pairs() -> list[dict]:
    """Get all pairs that are not control cases."""
    data = load_toponym_pairs()
    return [p for p in data["pairs"] if not p["is_control"]]


def get_all_pairs() -> list[dict]:
    """Get all toponym pairs."""
    return load_toponym_pairs()["pairs"]


def get_categories() -> list[dict]:
    """Get all category definitions."""
    return load_toponym_pairs()["categories"]


def ensure_dirs():
    """Create all output directories if they don't exist."""
    for d in [GDELT_RAW_DIR, TRENDS_RAW_DIR, NGRAMS_RAW_DIR, PROCESSED_DIR, FIGURES_DIR]:
        d.mkdir(parents=True, exist_ok=True)
