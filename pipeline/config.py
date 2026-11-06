"""Load and validate pipeline configuration from YAML files.

Single source of truth: config/pairs.yaml
All pair identification uses slugs, never numeric IDs.
"""

import calendar
from datetime import date
from pathlib import Path
from typing import Optional
import yaml

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT_DIR / "config"
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# ── Study window ─────────────────────────────────────────────────────────────
# The only place the window is defined; every clamp reads it. Three windows,
# deliberately (decided 2026-10-15):
#
#   SERIES   what the charts plot. Advances one CLOSED month at a time, by hand.
#   TEXT     what keyness, collocations and clusters read. Frozen at 2010-2025
#            until 2026 closes: 27,499 GDELT rows from 2026 (2.25% of the
#            corpus) leaked into keyness and moved 12 of 372 chips, and every
#            move costs a full rebuild plus hand-written glosses and names.
#   HEADLINE the complete calendar year that headline numbers and holdout
#            tables describe. Never a partial year: an unbounded '>= 2025-01'
#            filter would have averaged 21 months under a "2025" label.
#
# SOURCE_SERIES_END is each source's last month that is collected, closed AND
# settled. A source plots through min(SERIES_END, its entry); later months take
# the chart's no-data hatch. Bump an entry only after that month is ingested and
# checked. Recent months are not comparable on arrival: a YouTube month queried
# 28 days after it closed read 8.6x the same month a year earlier, against 1.1x
# on GDELT and Wikipedia (chornobyl, July 2026).
STUDY_START = "2010-01"
SERIES_END = "2026-10"
TEXT_END = "2025-12"
HEADLINE_YEAR = 2025
SOURCE_SERIES_END = {
    "gdelt": "2025-12",
    "trends": "2026-10",
    "wikipedia": "2025-12",
    "ngrams": "2022-12",     # Google Books corpus 37 ends at 2022; none is newer
    "openalex": "2025-12",
    "reddit": "2025-12",
    "youtube": "2026-08",
}


def last_day(ym: str) -> str:
    """'2026-09' -> '2026-09-30'."""
    y, m = int(ym[:4]), int(ym[5:7])
    return f"{ym[:7]}-{calendar.monthrange(y, m)[1]:02d}"


def series_end(source: str) -> str:
    """Last month ('YYYY-MM') a source may plot."""
    return min(SERIES_END, SOURCE_SERIES_END.get(source, SERIES_END))


# The narrowest YouTube census window a year must reach, raised from a year on.
# A pair's youtube_depth (config/pairs.yaml) describes the floor its history was
# collected at; from each year here the floor is at least this level. 2026 at the
# week floor was under-sampled: re-run at day floor (2026-11-05) March and June
# returned 47->82 and 8->83 videos for kazymyr-malevych UA, 125->280 and 157->306
# for mykola-hohol UA, 224->336 in June for kyiv UA, losing 0-49 of the week set.
# The same test on kyiv UA 2025 read 1.15x and 0.94x, so 2025 at week floor was
# already near saturation and is left as collected.
YOUTUBE_FLOOR_FROM_YEAR = {2026: "day"}


def _check_window() -> None:
    open_month = date.today().strftime("%Y-%m")
    if SERIES_END >= open_month:
        raise ValueError(f"SERIES_END {SERIES_END} is not a closed month (today is in {open_month})")
    if not (STUDY_START <= TEXT_END <= SERIES_END):
        raise ValueError(f"window out of order: {STUDY_START} <= {TEXT_END} <= {SERIES_END}")
    if f"{HEADLINE_YEAR}-12" > TEXT_END:
        raise ValueError(f"HEADLINE_YEAR {HEADLINE_YEAR} must be a complete year inside TEXT_END {TEXT_END}")
    for src, ym in SOURCE_SERIES_END.items():
        if ym > SERIES_END:
            raise ValueError(f"SOURCE_SERIES_END[{src}] {ym} is past SERIES_END {SERIES_END}")


_check_window()


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
