"""Export dataset to site JSON files.

Single source of truth: ALL site data is generated from this script.
The site reads only from site/src/data/*.json — nothing is computed at build time.

Reads from HuggingFace-format parquet files in dataset/ — no BigQuery required.

Usage:
    python -m pipeline.export_site_data
"""

import csv
import json
import logging
import os
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from pipeline.config import load_pairs

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "dataset"
STUDY_END_DATE = "2025-12-31"   # the study period ends here; 2026 is partial
MAX_ZERO_FILL_RUN = 6          # longer silent runs read as non-collection, not zeros
DATA_DIR = ROOT / "data"
SITE_DATA_DIR = ROOT / "site" / "src" / "data"

# ── Minimum data thresholds ──────────────────────────────────────────────────
# Below these, a source is excluded from a pair's chart (too noisy to display).
# See data/audit/data_quality_findings.json for methodology.
MIN_COUNT_THRESHOLD = 30          # min total mentions for count-based sources
MIN_NGRAMS_FREQ = 1e-9            # min max-frequency for ngrams (~500 book occurrences)


# ── Lazy parquet loading ─────────────────────────────────────────────────────

_cache = {}


def _load(name: str) -> pd.DataFrame:
    if name not in _cache:
        path = DATASET_DIR / f"raw_{name}.parquet"
        if not path.exists():
            path = DATASET_DIR / f"{name}.parquet"
        if not path.exists():
            log.warning(f"  Parquet not found: {path}")
            _cache[name] = pd.DataFrame()
        else:
            log.info(f"  Loading {path.name}...")
            import pyarrow.parquet as pq
            import pyarrow as pa
            table = pq.read_table(path)
            for i, field in enumerate(table.schema):
                if "date" in str(field.type):
                    table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
            table = table.replace_schema_metadata({})
            df = table.to_pandas()
            # Clamp every source to the study period in one place. 2026 is a
            # partial year: a trailing fragment beside sixteen complete ones reads
            # as a collapse in volume rather than an incomplete window. GDELT ran
            # to 2026-08 and was drawing points past the axis end.
            if "date" in df.columns:
                # utc=True then drop the zone: telegram's dates are tz-aware, and
                # comparing them against a naive Timestamp raised — which killed the
                # whole export, not just the telegram block.
                _d = pd.to_datetime(df["date"], errors="coerce", utc=True).dt.tz_localize(None)
                _keep = _d.isna() | (_d <= pd.Timestamp(STUDY_END_DATE))
                if int((~_keep).sum()):
                    log.info(f"  {name}: dropped {int((~_keep).sum()):,} rows after {STUDY_END_DATE}")
                    df = df[_keep].reset_index(drop=True)
            elif "year" in df.columns:
                _y = pd.to_numeric(df["year"], errors="coerce")
                _keep = _y.isna() | (_y <= int(STUDY_END_DATE[:4]))
                if int((~_keep).sum()):
                    log.info(f"  {name}: dropped {int((~_keep).sum()):,} rows after {STUDY_END_DATE[:4]}")
                    df = df[_keep].reset_index(drop=True)

            # Apply homonym filters from pairs.yaml for GDELT
            if name == "gdelt" and "source_domain" in df.columns:
                df = _apply_homonym_filters(df)
            # Filter YouTube: English titles + verified term presence
            if name == "youtube" and "title" in df.columns:
                df = _filter_youtube(df)
            _cache[name] = df
    return _cache[name]


def _stale_youtube_years() -> set:
    """(pair, year) that must not be plotted. A year is excluded when it is:

      * past STUDY_END_YEAR — a partial year is not comparable with full ones
      * missing a variant entirely — the other side then reads as 0% adoption
      * incomplete — fewer than 12 resolved months on either side, so the year's
        total is a fraction of the truth and its dip reads as history
      * collected at a depth other than the one declared in config/pairs.yaml

    Depth is declared, not inferred: a sparse pair peaks at 58 results in a month
    window and needs no day-level descent, while chornobyl peaks at 521 and loses most
    of a month. Pairs marked `legacy` were collected with cap-triggered descent, where
    depth varies month to month by construction; they are never filtered on depth
    because no single depth describes them.

    A gap is more honest than a value the collection cannot support.
    """
    import json as _json
    cfg = load_pairs()
    want = {p["slug"]: p.get("youtube_depth") for p in cfg["pairs"]
            if p.get("youtube_depth") and p.get("youtube_depth") != "legacy"}
    ck = ROOT / "data" / "cl" / "raw" / "youtube_census" / ".checkpoints"
    if not ck.exists():
        return set()

    seen: dict = {}
    for f in ck.glob("*.json"):
        parts = f.stem.rsplit("_", 2)
        if len(parts) != 3:
            continue
        pair, variant, year = parts
        try:
            d = _json.loads(f.read_text())
        except Exception:                              # noqa: BLE001
            continue
        months = d.get("months", {})
        w = len(d.get("done_windows", []))
        seen.setdefault((pair, year), {})[variant] = {
            "resolved": sum(1 for m in months.values() if m.get("resolved")),
            "depth": d.get("min_depth") or ("day" if w >= 300 else "week" if w >= 60 else "month"),
        }

    stale = set()
    for (pair, year), v in seen.items():
        if int(year) > STUDY_END_YEAR:
            stale.add((pair, year)); continue
        ru, uk = v.get("russian"), v.get("ukrainian")
        if ru is None or uk is None:
            stale.add((pair, year)); continue
        if ru["resolved"] < 12 or uk["resolved"] < 12:
            stale.add((pair, year)); continue
        if pair in want and (ru["depth"] != want[pair] or uk["depth"] != want[pair]):
            stale.add((pair, year))
    return stale


def _load_youtube_census() -> pd.DataFrame:
    """YouTube comes from the census, never from dataset/raw_youtube.parquet.

    Everything collected before 2026-08-25 is void: those fetches treated a
    window as complete unless it hit a 500-result ceiling, but the real ceiling
    is one full page (50), so any window returning 50 was silently truncated.
    The undercount measured 6-7x on volodymyr-the-great 2010.

    Rows are kept only where the spelling is actually present in the title or
    description, and `form` is used rather than `variant` — `variant` records
    which query surfaced the video, `form` records which spelling the author
    actually wrote, which is the thing being measured. Coincidental spans
    ("Vladimir the Great Dane") are excluded via span_artifact.

    Language detection is deliberately NOT applied: the presence of the
    Latin-script English form IS the signal, and langdetect on short titles is
    unreliable. A mixed-language title that writes "Saint Vladimir the Great"
    made the orthographic choice being measured.

    Pairs with no census file simply have no YouTube data — that is correct,
    not a gap to be filled from the void dataset.
    """
    census_dir = ROOT / "data" / "cl" / "raw" / "youtube_census"
    files = sorted(census_dir.glob("*_enriched.parquet"))
    if not files:
        log.warning("  YouTube: no census files — source will be absent")
        return pd.DataFrame()

    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    total = len(df)
    df = df[df["verified"] & ~df.get("span_artifact", False)]
    df = df[df["form"].isin(["russian", "ukrainian", "both"])].copy()
    df["variant"] = df["form"]
    df["date"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
    df = df.dropna(subset=["date"])

    # Drop anything past the last complete calendar year: a partial year is not
    # comparable with full ones and, on chornobyl 2026, its ukrainian half was never
    # collected at all, so every month read as 0% adoption.
    before_year = len(df)
    df = df[df.date.str[:4].astype(int) <= STUDY_END_YEAR].copy()
    if before_year - len(df):
        log.info(f"  YouTube: dropped {before_year-len(df):,} rows after "
                 f"{STUDY_END_YEAR} (incomplete year)")

    stale = _stale_youtube_years()
    if stale:
        before = len(df)
        df = df[[(p, d[:4]) not in stale for p, d in zip(df.pair_slug, df.date)]].copy()
        if before - len(df):
            years = sorted({f"{p} {y}" for p, y in stale})
            log.info(f"  YouTube: dropped {before-len(df):,} rows whose collection depth "
                     f"differs from the declared youtube_depth ({', '.join(years[:6])}"
                     f"{'...' if len(years) > 6 else ''}) — shown as gaps, not wrong values")

    log.info(f"  YouTube census: {len(files)} pair file(s), "
             f"{total:,} collected -> {len(df):,} verified "
             f"({100*len(df)/max(total,1):.1f}%), "
             f"{df.pair_slug.nunique()} pair(s)")
    return df[["pair_slug", "date", "variant", "title", "channel_title", "video_id"]]


def _filter_youtube(df: pd.DataFrame) -> pd.DataFrame:
    """Filter YouTube data: English titles only + verified term presence."""
    import re
    import langdetect
    langdetect.DetectorFactory.seed = 42

    cfg = load_pairs()
    pair_terms = {p["slug"]: (p["russian"], p["ukrainian"]) for p in cfg["pairs"]}
    before = len(df)

    # 1. Language filter — keep only English titles
    def is_english(title):
        try:
            return langdetect.detect(str(title)) == "en"
        except:
            return False

    df = df[df["title"].apply(is_english)].copy()
    after_lang = len(df)

    # 2. Term presence — verify title contains the search term (word boundary)
    def has_term(row):
        title = str(row.get("title", "")).lower()
        slug = row.get("pair_slug", "")
        if slug not in pair_terms:
            return False
        ru, ua = pair_terms[slug]
        return ru.lower() in title or ua.lower() in title

    df = df[df.apply(has_term, axis=1)].copy()
    after_term = len(df)

    # 3. Re-classify variant from title (not search variant)
    def classify_variant(row):
        title = str(row.get("title", ""))
        slug = row.get("pair_slug", "")
        if slug not in pair_terms:
            return row.get("variant", "russian")
        ru, ua = pair_terms[slug]
        has_ru = bool(re.search(re.escape(ru), title, re.IGNORECASE))
        has_ua = bool(re.search(re.escape(ua), title, re.IGNORECASE))
        if has_ru and has_ua:
            return "both"
        if has_ua:
            return "ukrainian"
        return "russian"

    df["variant"] = df.apply(classify_variant, axis=1)

    log.info(f"  YouTube filter: {before:,} → {after_lang:,} (English) → {after_term:,} (term verified)")
    return df


# Holdouts are evidence of *current* usage. Outlets that switched in 2019 are not
# holdouts today, so the window starts at the 2022 invasion.
STUDY_END_YEAR = 2025      # last complete calendar year; partial years are not comparable
HOLDOUT_SINCE = "2022-01-01"
HOLDOUT_CAP = 100
# One outlet can otherwise own the table -- sputniknews.com was 77 of 100 rows for
# donbas and 66 for kyiv. The table is meant to show WHO still uses the old spelling,
# so breadth of outlets matters more than depth on any one of them. State-affiliated
# outlets publish at volume and would crowd out everyone else on raw recency.
HOLDOUT_PER_DOMAIN = 3
# Russian and mixed usage are the holdouts worth reading; Ukrainian-only rows are
# not holdouts at all. Applied identically to every source.
HOLDOUT_VARIANTS = ("russian",)   # a holdout is a Russian spelling; "both" uses each form


def _apply_homonym_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Remove false positives using homonym_filters from pairs.yaml."""
    import re
    cfg = load_pairs()
    before = len(df)
    for p in cfg["pairs"]:
        filters = p.get("homonym_filters", [])
        if not filters:
            continue
        slug = p.get("slug")
        regexes = [re.compile(f, re.IGNORECASE) for f in filters]
        mask = df["pair_slug"] == slug
        if mask.sum() == 0:
            continue
        fp_mask = df.loc[mask, "source_domain"].apply(
            lambda d: any(r.search(str(d)) for r in regexes)
        )
        n_fp = fp_mask.sum()
        if n_fp > 0:
            df = df.drop(df.loc[mask][fp_mask].index)
            log.info(f"    Homonym filter: removed {n_fp} FP rows for {slug}")
    if len(df) < before:
        log.info(f"    Total FP removed: {before - len(df)}")
    return df


def get_enabled_slugs() -> set[str]:
    cfg = load_pairs()
    return {p["slug"] for p in cfg["pairs"] if p.get("enabled", True)}


def get_control_slugs() -> set[str]:
    cfg = load_pairs()
    return {p["slug"] for p in cfg["pairs"] if p.get("is_control", False)}


def get_analyzable_slugs() -> set[str]:
    cfg = load_pairs()
    return {p["slug"] for p in cfg["pairs"]
            if p.get("enabled", True) and not p.get("is_control", False)}


# A monthly ratio built from a handful of documents is not a measurement, it is a
# coin flip: one video in a month reads 0% or 100% and nothing between. For
# volodymyr-the-great's YouTube series the median month holds 4 videos and 50 of 134
# months hold 2 or fewer, which is why that line oscillates full-scale.
#
# Averaging the percentages (what smooth_series does) is the wrong repair: it weights
# a 1-document month equally with a 50-document month. This instead recomputes the
# ratio over a window of SUMMED counts, and widens that window only where the
# denominator is too small. Dense months keep their true monthly resolution.
SMOOTH_MIN_DENOM = 20      # documents needed before a month is trusted on its own
SMOOTH_MAX_HALF = 6        # widest half-window, i.e. +/- 6 months
SMOOTH_MIN_PLOT = 5        # below this even at full width, the point is not plotted

# Sources whose ukr/rus really are document counts. Google Trends is NOT one: its
# values are a 0-100 normalised index, so "20 documents" is meaningless there, and
# calibrating the Ukrainian variant onto the Russian scale puts it below 1.0 for an
# asymmetric pair -- which int() then truncated to zero, flatlining the adoption line.
COUNT_BASED_SOURCES = {"gdelt", "youtube", "reddit", "openalex", "wikipedia", "ngrams"}


def smooth_ratio_series(series: list[dict]) -> list[dict]:
    """Adaptive ratio-of-sums. Requires ukr/rus counts; returns the series unchanged without them."""
    if not series or not all("ukr" in d and "rus" in d for d in series):
        return series
    ukr = [float(d.get("ukr") or 0) for d in series]
    rus = [float(d.get("rus") or 0) for d in series]
    out = []
    for i, d in enumerate(series):
        half = 0
        while True:
            lo, hi = max(0, i - half), min(len(series), i + half + 1)
            u, r = sum(ukr[lo:hi]), sum(rus[lo:hi])
            if u + r >= SMOOTH_MIN_DENOM or half >= SMOOTH_MAX_HALF:
                break
            half += 1
        total = u + r
        e = dict(d)
        # Widening exhausted and still almost nothing to divide: refuse to draw a
        # number rather than draw one the data cannot support.
        e["adoption"] = round(u / total * 100, 1) if total >= SMOOTH_MIN_PLOT else None
        e["n"] = round(ukr[i] + rus[i], 2)     # the month's own volume, for the tooltip
        e["window"] = half                      # 0 means the month stood on its own
        out.append(e)
    # Measured zeros survive with a null ratio: the month was observed and held
    # nothing, which is not the same as the month being unsupported.
    return [e for e in out if e["adoption"] is not None or e.get("measured_zero")]


def smooth_series(series: list[dict], window: int = 3) -> list[dict]:
    if not series:
        return series
    values = [d["adoption"] for d in series]
    non_null = [v for v in values if v is not None]
    if not non_null:
        return series
    null_pct = (len(values) - len(non_null)) / len(values)
    jumps = sum(
        1 for i in range(1, len(values))
        if values[i] is not None and values[i - 1] is not None
        and abs(values[i] - values[i - 1]) > 25
    )
    jump_rate = jumps / max(len(values) - 1, 1)
    needs_smoothing = null_pct > 0.1 or jump_rate > 0.05
    if not needs_smoothing:
        return [d for d in series if d["adoption"] is not None]
    if jump_rate > 0.1 or null_pct > 0.3:
        window = max(window, 7)
    elif jump_rate > 0.05 or null_pct > 0.15:
        window = max(window, 5)
    filled = []
    last_val = non_null[0] if non_null else 0
    for v in values:
        if v is not None:
            last_val = v
        filled.append(last_val)
    smoothed = []
    half = window // 2
    for i in range(len(filled)):
        start = max(0, i - half)
        end = min(len(filled), i + half + 1)
        avg = sum(filled[start:end]) / (end - start)
        smoothed.append(round(avg, 1))
    return [{**{k: v for k, v in series[i].items() if k != "adoption"}, "adoption": smoothed[i]}
            for i in range(len(series))]


# ── Country codes ─────────────────────────────────────────────────────────────

GEO_TO_NUMERIC = {
    "AF": "004", "AL": "008", "DZ": "012", "AR": "032", "AM": "051",
    "AU": "036", "AT": "040", "AZ": "031", "BD": "050", "BY": "112",
    "BE": "056", "BA": "070", "BR": "076", "BG": "100", "KH": "116",
    "CA": "124", "CL": "152", "CN": "156", "CO": "170", "HR": "191",
    "CU": "192", "CY": "196", "CZ": "203", "DK": "208", "DO": "214",
    "EC": "218", "EG": "818", "EE": "233", "ET": "231", "FI": "246",
    "FR": "250", "GE": "268", "DE": "276", "GH": "288", "GR": "300",
    "GT": "320", "HN": "340", "HK": "344", "HU": "348", "IS": "352",
    "IN": "356", "ID": "360", "IR": "364", "IQ": "368", "IE": "372",
    "IL": "376", "IT": "380", "JM": "388", "JP": "392", "JO": "400",
    "KZ": "398", "KE": "404", "KR": "410", "KW": "414", "KG": "417",
    "LV": "428", "LB": "422", "LY": "434", "LT": "440", "LU": "442",
    "MY": "458", "MX": "484", "MD": "498", "MN": "496", "ME": "499",
    "MA": "504", "MZ": "508", "MM": "104", "NP": "524", "NL": "528",
    "NZ": "554", "NI": "558", "NG": "566", "MK": "807", "NO": "578",
    "OM": "512", "PK": "586", "PS": "275", "PA": "591", "PY": "600",
    "PE": "604", "PH": "608", "PL": "616", "PT": "620", "PR": "630",
    "QA": "634", "RO": "642", "RU": "643", "SA": "682", "RS": "688",
    "SG": "702", "SK": "703", "SI": "705", "ZA": "710", "ES": "724",
    "LK": "144", "SE": "752", "CH": "756", "TW": "158", "TZ": "834",
    "TH": "764", "TN": "788", "TR": "792", "UA": "804", "AE": "784",
    "GB": "826", "US": "840", "UY": "858", "UZ": "860", "VE": "862",
    "VN": "704", "YE": "887", "ZM": "894", "ZW": "716",
    "SN": "686", "CI": "384", "CM": "120", "UG": "800",
}



GEO_NAMES = {
    "004": "Afghanistan", "008": "Albania", "012": "Algeria", "032": "Argentina",
    "036": "Australia", "040": "Austria", "051": "Armenia", "031": "Azerbaijan",
    "050": "Bangladesh", "056": "Belgium", "070": "Bosnia", "076": "Brazil",
    "100": "Bulgaria", "104": "Myanmar", "112": "Belarus", "116": "Cambodia",
    "120": "Cameroon", "124": "Canada", "144": "Sri Lanka", "152": "Chile",
    "156": "China", "158": "Taiwan", "170": "Colombia", "191": "Croatia",
    "192": "Cuba", "196": "Cyprus", "203": "Czechia", "208": "Denmark",
    "214": "Dominican Republic", "218": "Ecuador", "231": "Ethiopia",
    "233": "Estonia", "246": "Finland", "250": "France", "268": "Georgia",
    "276": "Germany", "288": "Ghana", "300": "Greece", "320": "Guatemala",
    "340": "Honduras", "344": "Hong Kong", "348": "Hungary", "352": "Iceland",
    "356": "India", "360": "Indonesia", "364": "Iran", "368": "Iraq",
    "372": "Ireland", "376": "Israel", "380": "Italy", "384": "Ivory Coast",
    "388": "Jamaica", "392": "Japan", "398": "Kazakhstan", "400": "Jordan",
    "404": "Kenya", "410": "South Korea", "414": "Kuwait", "417": "Kyrgyzstan",
    "422": "Lebanon", "428": "Latvia", "434": "Libya", "440": "Lithuania",
    "442": "Luxembourg", "458": "Malaysia", "484": "Mexico", "496": "Mongolia",
    "498": "Moldova", "499": "Montenegro", "504": "Morocco", "508": "Mozambique",
    "512": "Oman", "524": "Nepal", "528": "Netherlands", "554": "New Zealand",
    "558": "Nicaragua", "566": "Nigeria", "578": "Norway", "586": "Pakistan",
    "591": "Panama", "600": "Paraguay", "604": "Peru", "608": "Philippines",
    "616": "Poland", "620": "Portugal", "630": "Puerto Rico", "634": "Qatar",
    "642": "Romania", "643": "Russia", "682": "Saudi Arabia", "686": "Senegal",
    "688": "Serbia", "702": "Singapore", "703": "Slovakia",
    "705": "Slovenia", "710": "South Africa", "716": "Zimbabwe", "724": "Spain",
    "752": "Sweden", "756": "Switzerland", "764": "Thailand", "788": "Tunisia",
    "792": "Turkey", "800": "Uganda", "804": "Ukraine", "818": "Egypt",
    "826": "United Kingdom", "834": "Tanzania", "840": "United States",
    "858": "Uruguay", "860": "Uzbekistan", "862": "Venezuela", "704": "Vietnam",
    "784": "UAE", "807": "North Macedonia", "275": "Palestine",
    "887": "Yemen", "894": "Zambia",
}

def _get_cl_corpus_size():
    corpus_path = DATA_DIR / "corpus" / "toponyms-corpus.parquet"
    if corpus_path.exists():
        return len(pd.read_parquet(corpus_path, columns=["pair_slug"]))
    # Fallback: old location
    old_path = DATA_DIR / "cl" / "balanced" / "corpus.parquet"
    if old_path.exists():
        return len(pd.read_parquet(old_path, columns=["pair_slug"]))
    return 0


def _safe_div(a, b):
    return a / b if b > 0 else 0.0


# ── Exports ───────────────────────────────────────────────────────────────────

def export_timeseries(enabled_slugs: set[str]) -> dict:
    log.info("Exporting timeseries...")
    result = {"events": [
        {"date": "2014-02", "label": "Euromaidan", "color": "#d97706"},
        {"date": "2022-02", "label": "Full-scale war", "color": "#dc2626"},
    ]}

    # Trends (monthly, smoothed)
    log.info("  Trends...")
    df = _load("trends")
    if len(df):
        t = df[(df["geo"] == "") | (df["geo"].isna())].copy()
        t["month"] = pd.to_datetime(t["date"]).dt.strftime("%Y-%m")
        # Two scales, each used where it is correct. `interest` is the solo series
        # with each variant normalised to its own peak -- plotted, because both
        # variants then reach 100 at their own high points and each shape stays
        # legible. `interest_calibrated` puts UA on RU's scale and is used only for
        # the adoption share, where the cross-variant ratio actually matters; it is
        # useless for display since a 111x gap draws as a flat line on the axis.
        if "interest_calibrated" not in t.columns:
            t["interest_calibrated"] = t["interest"]
        g = t.groupby(["pair_slug", "month", "variant"])[["interest", "interest_calibrated"]].sum().reset_index()
        p = g.pivot_table(index=["pair_slug", "month"], columns="variant",
                          values="interest", fill_value=0).reset_index()
        pc = g.pivot_table(index=["pair_slug", "month"], columns="variant",
                           values="interest_calibrated", fill_value=0).reset_index()
        pc = pc.set_index(["pair_slug", "month"])
        ukr_col = "ukrainian" if "ukrainian" in p.columns else 0
        rus_col = "russian" if "russian" in p.columns else 0
        for pid, grp in p.groupby("pair_slug"):
            if pid not in enabled_slugs:
                continue
            raw = []
            for _, r in grp.sort_values("month").iterrows():
                # Trends interest is a normalised index, not a count. Calibrating the
                # Ukrainian variant onto the Russian variant's scale divides it by ~111
                # for an asymmetric pair like chornobyl, so int() truncated every value
                # to 0 and the adoption line flatlined. Keep the float.
                ukr = float(r.get(ukr_col, 0) or 0)
                rus = float(r.get(rus_col, 0) or 0)
                try:
                    cr = pc.loc[(pid, r["month"])]
                    cu = float(cr.get(ukr_col, 0) or 0)
                    cru = float(cr.get(rus_col, 0) or 0)
                except KeyError:
                    cu, cru = ukr, rus
                ctotal = cu + cru
                adoption = round(cu / ctotal * 100, 2) if ctotal > 0 else None
                raw.append({"date": r["month"], "adoption": adoption,
                            "ukr": round(ukr, 4), "rus": round(rus, 4)})
            result.setdefault(pid, {})
            result[pid]["trends"] = smooth_series(raw, window=3)

    # GDELT (monthly)
    log.info("  GDELT...")
    # Verified-text series only. There is no legacy fallback: a pair without a
    # verified build shows nothing rather than a series derived from URL spellings,
    # which is a different measurement and was silently standing in for the same one.
    _vdir = ROOT / "data" / "cl" / "corpus" / "gdelt_verified"
    if _vdir.exists():
        _swapped = []
        for _f in sorted(_vdir.glob("*_series.parquet")):
            _slug = _f.stem[:-len("_series")]
            if _slug not in enabled_slugs:
                continue
            _sr = pd.read_parquet(_f)
            _pv = _sr.pivot_table(index="month", columns="variant", values="articles",
                                  fill_value=0).reset_index()
            _pv = _pv[_pv["month"].astype(str) <= STUDY_END_DATE[:7]]
            _rows = []
            for _, r in _pv.sort_values("month").iterrows():
                ukr, rus = int(r.get("ukrainian", 0)), int(r.get("russian", 0))
                both = int(r.get("both", 0))
                if ukr + rus == 0:
                    continue
                _rows.append({"date": r["month"], "adoption": round(ukr / (ukr + rus) * 100, 1),
                              "ukr": ukr, "rus": rus, "both": both, "verified": True})
            if _rows:
                result.setdefault(_slug, {})["gdelt"] = _rows
                _swapped.append(_slug)
        _missing = sorted(enabled_slugs - set(_swapped))
        log.info(f"  GDELT series from verified text for {len(_swapped)} pair(s)"
                 + (f"; NO series for {_missing} (no verified build)" if _missing else ""))

    # Wikipedia (monthly) + rename annotations
    log.info("  Wikipedia...")
    df = _load("wikipedia")
    wiki_renames_path = ROOT / "data" / "audit" / "wikipedia_renames.json"
    wiki_renames = {}
    if wiki_renames_path.exists():
        wiki_renames = json.loads(wiki_renames_path.read_text())
    if len(df):
        df["month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
        g = df.groupby(["pair_slug", "month", "variant"])["pageviews"].sum().reset_index()
        p = g.pivot_table(index=["pair_slug", "month"], columns="variant", values="pageviews", fill_value=0).reset_index()
        for pid, grp in p.groupby("pair_slug"):
            if pid not in enabled_slugs:
                continue
            spid = pid
            result.setdefault(spid, {}).setdefault("wikipedia", [])
            for _, r in grp.sort_values("month").iterrows():
                ukr = int(r.get("ukrainian", 0))
                rus = int(r.get("russian", 0))
                total = ukr + rus
                if total > 0:
                    result[spid]["wikipedia"].append({"date": r["month"], "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})
            # Add rename annotation if detected
            rename_info = wiki_renames.get(pid, {})
            if rename_info.get("rename_month"):
                result[spid].setdefault("wikipedia_rename", rename_info["rename_month"])

    # Reddit (monthly)
    log.info("  Reddit...")
    df = _load("reddit")
    if len(df):
        df["month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
        g = df.groupby(["pair_slug", "month", "variant"]).size().reset_index(name="cnt")
        p = g.pivot_table(index=["pair_slug", "month"], columns="variant", values="cnt", fill_value=0).reset_index()
        for pid, grp in p.groupby("pair_slug"):
            if pid not in enabled_slugs:
                continue
            spid = pid
            result.setdefault(spid, {}).setdefault("reddit", [])
            for _, r in grp.sort_values("month").iterrows():
                ukr = int(r.get("ukrainian", 0))
                rus = int(r.get("russian", 0))
                total = ukr + rus
                if total >= 2:
                    result[spid]["reddit"].append({"date": r["month"], "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})

    # YouTube (monthly from parquet)
    log.info("  YouTube...")
    df = _load_youtube_census()
    if len(df):
        df["month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
        g = df.groupby(["pair_slug", "month", "variant"]).size().reset_index(name="cnt")
        p = g.pivot_table(index=["pair_slug", "month"], columns="variant", values="cnt", fill_value=0).reset_index()
        for pid, grp in p.groupby("pair_slug"):
            if pid not in enabled_slugs:
                continue
            spid = pid
            result.setdefault(spid, {}).setdefault("youtube", [])
            for _, r in grp.sort_values("month").iterrows():
                ukr = int(r.get("ukrainian", 0))
                rus = int(r.get("russian", 0))
                total = ukr + rus
                if total > 0:
                    result[spid]["youtube"].append({"date": r["month"], "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})

        # Coverage comes from the COLLECTION state, not from where the first verified
        # video happens to fall. volodymyr-the-great collected 5,160 videos for 2010
        # and verified none — that is a measured zero, and rendering it as a gap
        # claims the source was never observed. Every month of a complete,
        # study-period year (both variants, 12 resolved months, not stale) is
        # emitted; those without a verified video carry ukr=0, rus=0 and a null
        # adoption, since a share of nothing is undefined. Months outside collected
        # years stay absent, which is what a real gap looks like.
        import json as _cj, re as _cre
        _ck = ROOT / "data" / "cl" / "raw" / "youtube_census" / ".checkpoints"
        _stale2 = _stale_youtube_years()
        _complete: dict = {}
        if _ck.exists():
            for _f in _ck.glob("*.json"):
                _m = _cre.match(r"(.+)_(russian|ukrainian)_(\d{4})$", _f.stem)
                if not _m:
                    continue
                _slug2, _, _yr = _m.group(1), _m.group(2), int(_m.group(3))
                if _yr > int(STUDY_END_DATE[:4]) or (_slug2, _yr) in _stale2:
                    continue
                try:
                    _mo = _cj.load(open(_f)).get("months") or {}
                except Exception:
                    continue
                if sum(1 for v in _mo.values() if v.get("resolved")) >= 12:
                    _complete.setdefault(_slug2, {}).setdefault(_yr, 0)
                    _complete[_slug2][_yr] += 1
        _zeroed = 0
        for _slug2, _yrs in _complete.items():
            if _slug2 not in enabled_slugs:
                continue
            _ser = result.setdefault(_slug2, {}).setdefault("youtube", [])
            _have = {x["date"] for x in _ser}
            for _yr, _nvar in sorted(_yrs.items()):
                if _nvar < 2:
                    continue                    # one variant missing: not observed
                for _mm in range(1, 13):
                    _key = f"{_yr}-{_mm:02d}"
                    if _key not in _have:
                        _ser.append({"date": _key, "adoption": None,
                                     "ukr": 0, "rus": 0, "measured_zero": True})
                        _zeroed += 1
            _ser.sort(key=lambda x: x["date"])
        if _zeroed:
            log.info(f"  YouTube: {_zeroed} observed months with zero verified videos "
                     f"emitted as explicit zeros (collected but nothing named the term)")

    # Ngrams (yearly)
    log.info("  Ngrams...")
    df = _load("ngrams")
    if len(df):
        g = df[df["year"] >= 1900].groupby(["pair_slug", "year", "variant"])["frequency"].sum().reset_index()
        p = g.pivot_table(index=["pair_slug", "year"], columns="variant", values="frequency", fill_value=0).reset_index()
        for pid, grp in p.groupby("pair_slug"):
            if pid not in enabled_slugs:
                continue
            spid = pid
            result.setdefault(spid, {}).setdefault("ngrams", [])
            for _, r in grp.sort_values("year").iterrows():
                ukr = float(r.get("ukrainian", 0))
                rus = float(r.get("russian", 0))
                total = ukr + rus
                if total > 0:
                    result[spid]["ngrams"].append({"date": f"{int(r['year'])}-01", "adoption": round(ukr / total * 100, 1), "ukr": int(ukr * 1e9), "rus": int(rus * 1e9)})

    # OpenAlex (from local JSON)
    log.info("  OpenAlex...")
    openalex_path = DATA_DIR / "raw" / "openalex" / "openalex_all_pairs.json"
    if openalex_path.exists():
        with open(openalex_path) as f:
            openalex_data = json.load(f)
        for pair_data in openalex_data:
            pid = pair_data["pair_slug"]
            if pid not in enabled_slugs:
                continue
            spid = pid
            raw_series = []
            for yr in pair_data["yearly"]:
                total = yr["total"]
                adoption = round(yr["ukrainian_count"] / total * 100, 1) if total > 0 else None
                raw_series.append({"date": f"{yr['year']}-01", "adoption": adoption, "ukr": yr["ukrainian_count"], "rus": yr["russian_count"]})
            result.setdefault(spid, {})
            result[spid]["openalex"] = smooth_series(raw_series, window=3)

    # Telegram (monthly)
    log.info("  Telegram...")
    telegram_path = DATA_DIR / "cl" / "raw" / "telegram" / "all_channels.parquet"
    if telegram_path.exists():
        tg = pd.read_parquet(telegram_path)
        if len(tg) and "date" in tg.columns:
            tg["month"] = pd.to_datetime(tg["date"]).dt.strftime("%Y-%m")
            g = tg.groupby(["pair_slug", "month", "variant"]).size().reset_index(name="count")
            p = g.pivot_table(index=["pair_slug", "month"], columns="variant", values="count", fill_value=0).reset_index()
            for pid, grp in p.groupby("pair_slug"):
                if pid not in enabled_slugs:
                    continue
                spid = pid
                result.setdefault(spid, {}).setdefault("telegram", [])
                for _, r in grp.sort_values("month").iterrows():
                    ukr = int(r.get("ukrainian", 0))
                    rus = int(r.get("russian", 0))
                    total = ukr + rus
                    if total > 0:
                        result[spid]["telegram"].append({"date": r["month"], "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})

    # A month with zero mentions is a measurement, not a hole. Every source above
    # skips months whose total is 0, which erases the difference between "we counted
    # and found none" and "we never observed this month" — and the chart then had no
    # way to tell them apart either, hatching 258 measured zeros as missing data.
    # Fill interior months inside each series' own span with explicit zeros, so
    # absence from the series means only one thing: outside the coverage window.
    # Yearly sources (ngrams, openalex) are left alone; their gaps are cadence.
    _filled = _filled_series = _unfilled = 0
    _unfilled_detail: list[str] = []
    for _slug in list(result.keys()):
        if _slug == "events":
            continue
        for _src in list(result[_slug].keys()):
            _ser = result[_slug][_src]
            if not isinstance(_ser, list) or len(_ser) < 3:
                continue
            _months = sorted(d["date"] for d in _ser if d.get("date"))
            try:
                _span = pd.period_range(_months[0], _months[-1], freq="M").astype(str)
            except Exception:
                continue
            if len(_span) < 2 or len(_months) / len(_span) < 0.15:
                continue          # not a monthly series (yearly cadence) — leave it
            _have = {d["date"]: d for d in _ser}
            _missing = [m for m in _span if m not in _have]
            if not _missing:
                continue
            # Only short runs are filled. A handful of silent months in a sparse pair
            # is a real zero; half a year of unbroken silence in an otherwise monthly
            # source looks like non-collection (Reddit has a known PullPush outage),
            # and claiming "counted, found none" there is the same error as claiming
            # "never looked" for a genuine zero. We cannot separate them from the data
            # alone, so long runs keep the weaker claim and stay absent.
            _runs, _cur = [], []
            for m in _span:
                if m in _have:
                    if _cur:
                        _runs.append(_cur)
                        _cur = []
                else:
                    _cur.append(m)
            if _cur:
                _runs.append(_cur)
            _long = [r for r in _runs if len(r) > MAX_ZERO_FILL_RUN]
            if _long:
                _unfilled += sum(len(r) for r in _long)
                _unfilled_detail.append(f"{_slug}/{_src}:{max(len(r) for r in _long)}mo")
            _missing = [m for r in _runs if len(r) <= MAX_ZERO_FILL_RUN for m in r]
            if not _missing:
                continue
            for m in _missing:
                # adoption is None, not 0: a share of nothing is undefined, and the
                # smoother below recomputes it over a window where that is meaningful.
                _have[m] = {"date": m, "adoption": None, "ukr": 0, "rus": 0,
                            "measured_zero": True}
            result[_slug][_src] = [_have[m] for m in _span if m in _have]
            _filled += len(_missing)
            _filled_series += 1
    if _filled:
        log.info(f"  Filled {_filled:,} measured-zero months across {_filled_series} "
                 f"pair×source series (0 is a count, not a gap)")
    if _unfilled:
        log.info(f"  Left {_unfilled:,} month(s) unfilled in runs over "
                 f"{MAX_ZERO_FILL_RUN} months — likely non-collection, shown as gaps: "
                 f"{', '.join(sorted(_unfilled_detail)[:8])}")

    # Stabilise every count-based series before thresholding.
    _smoothed = 0
    for _slug in list(result.keys()):
        if _slug == "events":
            continue
        for _src in list(result[_slug].keys()):
            _ser = result[_slug][_src]
            if not isinstance(_ser, list) or not _ser:
                continue
            if _src not in COUNT_BASED_SOURCES:
                continue
            _new = smooth_ratio_series(_ser)
            if _new is not _ser:
                result[_slug][_src] = _new
                _smoothed += 1
    if _smoothed:
        log.info(f"  Adaptive ratio smoothing applied to {_smoothed} pair×source series "
                 f"(min denominator {SMOOTH_MIN_DENOM}, max ±{SMOOTH_MAX_HALF} months)")

    # ── Apply minimum data thresholds ──────────────────────────────────────
    # Remove pair×source combos that are too sparse to display meaningfully.
    removed = 0
    for slug in list(result.keys()):
        if slug == "events":
            continue
        pair_sources = result[slug]
        for src in list(pair_sources.keys()):
            series = pair_sources[src]
            if not isinstance(series, list):
                continue  # skip metadata fields like wikipedia_rename
            if not series:
                del pair_sources[src]
                removed += 1
                continue
            if src == "ngrams":
                # Ngrams ukr/rus are stored as freq * 1e9 (see line ~351)
                # Convert back: max_freq = max_stored / 1e9
                max_stored = max(max(d.get("ukr", 0), d.get("rus", 0)) for d in series)
                if max_stored / 1e9 < MIN_NGRAMS_FREQ:
                    del pair_sources[src]
                    removed += 1
            else:
                # For count-based sources, check total volume
                total = sum(d.get("ukr", 0) + d.get("rus", 0) for d in series)
                if total < MIN_COUNT_THRESHOLD:
                    del pair_sources[src]
                    removed += 1
    if removed:
        log.info(f"  Threshold filter: removed {removed} weak pair×source combos (min_count={MIN_COUNT_THRESHOLD}, min_ngrams_freq={MIN_NGRAMS_FREQ})")

    pair_count = len([k for k in result if k != "events"])
    log.info(f"  Timeseries: {pair_count} pairs")
    return result


def export_manifest(enabled_slugs: set[str], analyzable_slugs: set[str], control_slugs: set[str]) -> dict:
    log.info("Exporting manifest (single source of truth)...")

    pairs_cfg = load_pairs()

    # ── Per-source stats ──
    log.info("  Computing per-source stats...")
    source_stats = {}

    trends = _load("trends")
    if len(trends):
        source_stats["trends"] = {"records": len(trends), "pairs": int(trends["pair_slug"].nunique()), "unit": "datapoints"}

    gdelt = _load("gdelt")
    if len(gdelt):
        source_stats["gdelt"] = {"records": int(gdelt["count"].sum()), "pairs": int(gdelt["pair_slug"].nunique()), "unit": "articles"}

    wiki = _load("wikipedia")
    if len(wiki):
        source_stats["wikipedia"] = {"records": int(wiki["pageviews"].sum()), "pairs": int(wiki["pair_slug"].nunique()), "unit": "pageviews"}

    reddit = _load("reddit")
    if len(reddit):
        source_stats["reddit"] = {"records": len(reddit), "pairs": int(reddit["pair_slug"].nunique()), "unit": "posts"}

    youtube = _load_youtube_census()
    if len(youtube):
        source_stats["youtube"] = {"records": len(youtube), "pairs": int(youtube["pair_slug"].nunique()), "unit": "videos"}

    ngrams = _load("ngrams")
    if len(ngrams):
        source_stats["ngrams"] = {"records": len(ngrams), "pairs": int(ngrams["pair_slug"].nunique()), "unit": "records"}

    # Telegram
    telegram_path = DATA_DIR / "cl" / "raw" / "telegram" / "all_channels.parquet"
    telegram = pd.DataFrame()
    if telegram_path.exists():
        telegram = pd.read_parquet(telegram_path)
        source_stats["telegram"] = {"records": len(telegram), "pairs": int(telegram["pair_slug"].nunique()), "unit": "messages"}

    # Extra stats
    extra_map = {}
    if len(gdelt):
        extra_map["gdelt_domains"] = str(gdelt["source_domain"].nunique())
    if len(reddit):
        extra_map["reddit_subreddits"] = str(reddit["subreddit"].nunique())
    if len(youtube):
        extra_map["youtube_channels"] = str(youtube["channel_title"].nunique())
    if len(telegram):
        extra_map["telegram_channels"] = str(telegram["channel"].nunique())

    # Religious
    if len(trends):
        geo = trends[(trends["geo"] != "") & (trends["geo"].notna())]
        extra_map["trends_countries"] = str(geo["geo"].nunique())

    # OpenAlex — prefer parquet (has per-paper counts), fall back to JSON
    openalex_parquet = DATASET_DIR / "openalex.parquet"
    openalex_path = DATA_DIR / "raw" / "openalex" / "openalex_all_pairs.json"
    openalex_total_papers = 0
    openalex_total_pairs = 0
    if openalex_parquet.exists():
        oa_df = pd.read_parquet(openalex_parquet)
        openalex_total_papers = int(oa_df["count"].sum()) if "count" in oa_df.columns else len(oa_df)
        openalex_total_pairs = int(oa_df["pair_slug"].nunique())
    elif openalex_path.exists():
        with open(openalex_path) as f:
            oa_data = json.load(f)
        openalex_total_pairs = len(oa_data)
        openalex_total_papers = sum(sum(yr["total"] for yr in p["yearly"]) for p in oa_data)


    # ── Per-pair adoption (mean across sources, last 12 months / 5 years) ──
    log.info("  Computing per-pair adoption...")
    today = date.today()
    cutoff_12m = today - timedelta(days=365)
    cutoff_5y = today - timedelta(days=5 * 365)

    def _source_adoption(df, value_col, date_col, cutoff, agg_mode="sum", min_total=5):
        """Compute adoption ratio per pair for a single source."""
        if isinstance(cutoff, int):
            # Year-based cutoff (ngrams)
            d = df[df[date_col].astype(int) >= cutoff].copy()
        else:
            d = df[pd.to_datetime(df[date_col]).dt.date >= cutoff].copy()
        if not len(d):
            return {}
        if agg_mode == "count":
            g = d.groupby(["pair_slug", "variant"]).size().reset_index(name="val")
        else:
            g = d.groupby(["pair_slug", "variant"])[value_col].sum().reset_index(name="val")
        p = g.pivot_table(index="pair_slug", columns="variant", values="val", fill_value=0).reset_index()
        out = {}
        for _, r in p.iterrows():
            ukr = float(r.get("ukrainian", 0))
            rus = float(r.get("russian", 0))
            total = ukr + rus
            if total >= min_total:
                out[r["pair_slug"]] = ukr / total
        return out

    per_source = {}
    if len(trends):
        t = trends[(trends["geo"] == "") | (trends["geo"].isna())]
        per_source["trends"] = _source_adoption(t, "interest", "date", cutoff_12m)
    if len(gdelt):
        per_source["gdelt"] = _source_adoption(gdelt, "count", "date", cutoff_12m, min_total=5)
    if len(wiki):
        per_source["wikipedia"] = _source_adoption(wiki, "pageviews", "date", cutoff_12m, min_total=10)
    if len(reddit):
        per_source["reddit"] = _source_adoption(reddit, None, "date", cutoff_12m, agg_mode="count", min_total=3)
    if len(youtube):
        per_source["youtube"] = _source_adoption(youtube, None, "date", cutoff_12m, agg_mode="count", min_total=3)
    if len(ngrams):
        per_source["ngrams"] = _source_adoption(ngrams, "frequency", "year", cutoff_5y.year, min_total=0)
    # OpenAlex
    if openalex_path.exists():
        with open(openalex_path) as f:
            oa_data = json.load(f)
        oa_adopt = {}
        for p in oa_data:
            recent = [yr for yr in p["yearly"] if yr["year"] >= cutoff_5y.year]
            if recent:
                ukr = sum(yr["ukrainian_count"] for yr in recent)
                rus = sum(yr["russian_count"] for yr in recent)
                total = ukr + rus
                if total >= 3:
                    oa_adopt[p["pair_slug"]] = ukr / total
        per_source["openalex"] = oa_adopt
    # Telegram
    if len(telegram):
        per_source["telegram"] = _source_adoption(telegram, None, "date", cutoff_12m, agg_mode="count", min_total=3)

    # Mean adoption across sources per pair
    recent_map = {}
    all_pids = set()
    for src_ratios in per_source.values():
        all_pids |= set(src_ratios.keys())
    for pid in all_pids:
        ratios = [per_source[s][pid] for s in per_source if pid in per_source[s]]
        if ratios:
            recent_map[pid] = {"adoption": round(sum(ratios) / len(ratios) * 100, 1), "n_sources": len(ratios)}

    # Total mentions per pair
    log.info("  Computing total mentions...")
    total_map = {}
    if len(gdelt):
        for pid, cnt in gdelt.groupby("pair_slug")["count"].sum().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    if len(trends):
        t = trends[(trends["geo"] == "") | (trends["geo"].isna())]
        for pid, cnt in t.groupby("pair_slug")["interest"].sum().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    if len(wiki):
        for pid, cnt in wiki.groupby("pair_slug")["pageviews"].sum().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    if len(reddit):
        for pid, cnt in reddit.groupby("pair_slug").size().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    if len(youtube):
        for pid, cnt in youtube.groupby("pair_slug").size().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    if len(ngrams):
        for pid, cnt in ngrams.groupby("pair_slug")["frequency"].sum().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    # OpenAlex
    openalex_path2 = DATA_DIR / "raw" / "openalex" / "openalex_all_pairs.json"
    if openalex_path2.exists():
        with open(openalex_path2) as f:
            oa = json.load(f)
        for pair_data in oa:
            pid = pair_data.get("pair_slug")
            if pid in enabled_slugs:
                oa_total = sum(yr["total"] for yr in pair_data.get("yearly", []))
                total_map[pid] = total_map.get(pid, 0) + oa_total
    # Telegram
    if len(telegram):
        for pid, cnt in telegram.groupby("pair_slug").size().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)

    # Build pairs
    pairs_out = []
    for p in pairs_cfg["pairs"]:
        slug = p.get("slug")
        if not slug or slug not in enabled_slugs:
            continue
        recent = recent_map.get(slug, {})
        adoption_pct = 0.0 if slug in control_slugs else recent.get("adoption", 0.0)
        pairs_out.append({
            "slug": slug,
            "russian": p["russian"], "ukrainian": p["ukrainian"],
            "adoption": adoption_pct, "total": total_map.get(slug, 0),
            "is_control": slug in control_slugs,
        })

    # Only document-level matches count. Summing every source's `records` added
    # Wikipedia PAGEVIEWS (298.7M) and Trends index datapoints to article and post
    # counts, so the total read 300.3M when 99.5% of it was neither a document nor a
    # match. These four sources are the ones where a record is a text in which a
    # variant actually appears.
    TEXT_SOURCES_FOR_TOTAL = ("gdelt", "reddit", "youtube")
    toponym_matches = (sum(source_stats.get(s, {}).get("records", 0)
                           for s in TEXT_SOURCES_FOR_TOTAL) + openalex_total_papers)

    manifest = {
        "total_pairs": len(enabled_slugs),
        "analyzable_pairs": len(analyzable_slugs),
        "toponym_matches": toponym_matches,
        "cl_corpus": _get_cl_corpus_size(),
        "time_span": "2010-2025",
        "num_sources": 8,  # 7 standard + telegram (religious removed 2026-08-25)
        "num_countries": int(extra_map.get("trends_countries", "0")),
        "sources": {
            "trends": {"records": source_stats.get("trends", {}).get("records", 0), "pairs": source_stats.get("trends", {}).get("pairs", 0), "label": "Trends", "unit": "datapoints", "extra": f"Google · {extra_map.get('trends_countries', '55')} countries", "color": "#4285F4"},
            "gdelt": {"records": source_stats.get("gdelt", {}).get("records", 0), "pairs": source_stats.get("gdelt", {}).get("pairs", 0), "label": "News", "unit": "articles", "extra": f"GDELT · {extra_map.get('gdelt_domains', '0')} domains", "color": "#1e3a5f"},
            "wikipedia": {"records": source_stats.get("wikipedia", {}).get("records", 0), "pairs": source_stats.get("wikipedia", {}).get("pairs", 0), "label": "Wiki", "unit": "pageviews", "extra": "Wikipedia · monthly", "color": "#636466"},
            "reddit": {"records": source_stats.get("reddit", {}).get("records", 0), "pairs": source_stats.get("reddit", {}).get("pairs", 0), "label": "Reddit", "unit": "posts", "extra": f"{extra_map.get('reddit_subreddits', '0')} subreddits", "color": "#FF4500"},
            "youtube": {"records": source_stats.get("youtube", {}).get("records", 0), "pairs": source_stats.get("youtube", {}).get("pairs", 0), "label": "YouTube", "unit": "videos", "extra": f"{extra_map.get('youtube_channels', '0')} channels", "color": "#FF0000"},
            "ngrams": {"records": source_stats.get("ngrams", {}).get("records", 0), "pairs": source_stats.get("ngrams", {}).get("pairs", 0), "label": "Books", "unit": "records", "extra": "Google Books · 8M+ volumes", "color": "#7c3aed"},
            "openalex": {"records": openalex_total_papers, "pairs": openalex_total_pairs, "label": "Academic", "unit": "papers", "extra": "OpenAlex · 250M+ works", "color": "#06b6d4"},
            "telegram": {"records": source_stats.get("telegram", {}).get("records", 0), "pairs": source_stats.get("telegram", {}).get("pairs", 0), "label": "Telegram", "unit": "messages", "extra": f"{extra_map.get('telegram_channels', '0')} channels", "color": "#26A5E4"},
        },
        # Which source the chart opens on. It lives in config/pairs.yaml so it is set
        # in one place rather than duplicated across the pair view, the Ukrainian
        # landing page and their separate button markup, which had already drifted
        # apart once — the default said one source while the highlighted button said
        # another.
        "default_source": (load_pairs().get("metadata", {}) or {}).get("default_source", "trends"),
        "pairs": sorted(pairs_out, key=lambda x: x["slug"]),
    }

    log.info(f"  Manifest: {manifest['analyzable_pairs']} analyzable pairs, "
             f"{manifest['toponym_matches']:,} toponym matches, "
             f"default source '{manifest['default_source']}'")
    return manifest


def export_trends_countries(enabled_slugs: set[str]) -> dict:
    log.info("Exporting trends countries...")
    df = _load("trends")
    if not len(df):
        return {}
    t = df[(df["geo"] != "") & (df["geo"].notna())].copy()
    g = t.groupby(["pair_slug", "geo", "variant"])["interest"].sum().reset_index()
    p = g.pivot_table(index=["pair_slug", "geo"], columns="variant", values="interest", fill_value=0).reset_index()
    result = {}
    for _, r in p.iterrows():
        pid = r["pair_slug"]
        if pid not in enabled_slugs:
            continue
        ukr = float(r.get("ukrainian", 0))
        rus = float(r.get("russian", 0))
        total = ukr + rus
        if total < 100 or ukr == 0 or rus == 0:
            continue
        numeric = GEO_TO_NUMERIC.get(r["geo"])
        if not numeric:
            continue
        spid = pid
        result.setdefault(spid, {})
        result[spid][numeric] = {"name": GEO_NAMES.get(numeric, r["geo"]), "adoption": round(ukr / total * 100, 1)}
    log.info(f"  Trends countries: {len(result)} pairs")
    return result


# OpenAlex terms that collide with something other than the toponym. Excluded from
# academic holdouts because a word-boundary match cannot separate them:
#   borscht  -- "Borsch" is the plant taxonomist T. Borsch; 2,228 of 3,484 Russian-variant
#               papers since 2022 are botanical taxonomy and none contain "borscht".
#   ihor-sikorsky -- "Igor Sikorsky Kyiv Polytechnic Institute" is a university's official
#               English name, so matches are affiliation strings, not the person.
OPENALEX_COLLISIONS = {"borscht", "ihor-sikorsky"}
OPENALEX_SINCE_YEAR = int(HOLDOUT_SINCE[:4])   # same window as every other source


def _preview_around_match(text: str, variant: str, slug: str, width: int = 220) -> str:
    """Window the preview on the spelling being alleged.

    A first-200-chars preview failed to show the claimed spelling in 28.4% of rows,
    which defeats the point of an evidence table.
    """
    import re as _re
    cfg = load_pairs()
    pair = next((p for p in cfg["pairs"] if p.get("slug") == slug), None)
    if not pair:
        return str(text)[:width]
    terms = ([str(pair["russian"]), str(pair["ukrainian"])] if variant == "both"
             else [str(pair["russian"] if variant == "russian" else pair["ukrainian"])])
    for t in terms:
        rx = _re.compile(r"\b" + r"[-_\s]+".join(_re.escape(w) for w in t.split()) + r"\b", _re.I)
        m = rx.search(text or "")
        if m:
            lo = max(0, m.start() - width // 2)
            snippet = str(text)[lo:lo + width].strip()
            return ("…" if lo else "") + _re.sub(r"\s+", " ", snippet) + "…"
    return _re.sub(r"\s+", " ", str(text)[:width])


def export_openalex_holdouts(enabled_slugs: set[str]) -> dict:
    """Most-cited papers still using the Russian spelling, with links to the work.

    Mapped through `matched_term` rather than the parquet's `pair_id`: that column
    refers to a numeric scheme dropped when pairs.yaml moved to slugs. The config
    mapping agrees with the parquet's own `variant` on 99.98% of rows.
    """
    path = DATA_DIR / "cl" / "raw" / "openalex" / "all_pairs.parquet"
    if not path.exists():
        log.warning("  OpenAlex per-paper parquet missing; no academic holdouts")
        return {}
    cfg = load_pairs()
    lut = {}
    for p in cfg["pairs"]:
        if p.get("slug") not in enabled_slugs:
            continue
        for v in ("ukrainian", "russian"):
            lut[str(p[v]).strip().lower()] = (p["slug"], v)
    df = pd.read_parquet(path)
    df["mt"] = df["matched_term"].astype(str).str.strip().str.lower()
    mapped = df["mt"].map(lambda t: lut.get(t, (None, None)))
    df["slug"] = [m[0] for m in mapped]
    df["var"] = [m[1] for m in mapped]
    df = df[df["slug"].notna() & df["var"].isin(HOLDOUT_VARIANTS) & (df["year"] >= OPENALEX_SINCE_YEAR)]
    df = df[~df["slug"].isin(OPENALEX_COLLISIONS)]
    df = df[df["openalex_id"].notna() & df["title"].notna()]

    out, skipped = {}, sorted(OPENALEX_COLLISIONS)
    for slug, g in df.groupby("slug"):
        g = (g.drop_duplicates("openalex_id")
               .sort_values("cited_by_count", ascending=False)
               .groupby("year", sort=False, group_keys=False).head(HOLDOUT_PER_DOMAIN * 10)
               .nlargest(HOLDOUT_CAP, "cited_by_count"))
        out[slug] = [{
            "name": str(r["title"])[:160],
            "url": str(r["openalex_id"]),
            "cited": int(r["cited_by_count"] or 0),
            "year": int(r["year"]),
        } for _, r in g.iterrows()]
    log.info(f"  OpenAlex holdouts: {len(out)} pairs, {sum(len(v) for v in out.values()):,} papers "
             f"(since {OPENALEX_SINCE_YEAR}, cap {HOLDOUT_CAP}; excluded collisions: {', '.join(skipped)})")
    return out


def _youtube_view_counts(video_ids: list[str], key: str) -> dict:
    """viewCount per video id, fetched at export time and never written to disk.

    The census does not carry view counts and cannot: YouTube's terms do not allow
    retaining non-ID API data, so ranking by popularity has to be re-fetched each time
    the site is built and discarded afterwards. videos.list costs 1 unit per 50 ids
    from the daily units pool, which is separate from the search quota that actually
    binds, so a full site build is a few dozen units.
    """
    import requests
    out = {}
    for i in range(0, len(video_ids), 50):
        batch = video_ids[i:i + 50]
        try:
            r = requests.get("https://www.googleapis.com/youtube/v3/videos",
                             params={"part": "statistics", "id": ",".join(batch), "key": key},
                             timeout=30)
            if r.status_code != 200:
                log.warning(f"    view fetch: HTTP {r.status_code} on batch {i // 50} — skipped")
                continue
            for item in r.json().get("items", []):
                v = item.get("statistics", {}).get("viewCount")
                if v is not None:
                    out[item["id"]] = int(v)
        except Exception as e:                       # noqa: BLE001
            log.warning(f"    view fetch failed on batch {i // 50}: {type(e).__name__}")
    return out


def export_holdouts(enabled_slugs: set[str]) -> tuple[dict, list]:
    """Per-source holdouts for 2025: who still uses Russian spellings."""
    log.info("Exporting holdouts (2025, all sources)...")
    by_pair = {}

    # News (GDELT): domains using Russian spelling
    gdelt = _load("gdelt")
    if len(gdelt):
        g25 = gdelt[gdelt["date"] >= "2025-01"]
        ga = g25.groupby(["pair_slug", "source_domain", "variant"])["count"].sum().reset_index()
        gp = ga.pivot_table(index=["pair_slug", "source_domain"], columns="variant", values="count", fill_value=0).reset_index()
        gp["total"] = gp.get("russian", 0) + gp.get("ukrainian", 0)
        gp["rus_pct"] = (gp.get("russian", 0) / gp["total"] * 100).round(1)
        gp = gp[(gp["total"] >= 5) & (gp["rus_pct"] > 50)]
        for slug in enabled_slugs:
            h = gp[gp["pair_slug"] == slug].nlargest(50, "total")
            if len(h):
                by_pair.setdefault(slug, {})["news"] = [
                    {"name": r["source_domain"], "russian_pct": float(r["rus_pct"]), "total": int(r["total"])}
                    for _, r in h.iterrows()
                ]

    # Every source below uses the SAME rules as the news holdouts: the window starts
    # at HOLDOUT_SINCE, Russian and mixed usage both count, and the cap is HOLDOUT_CAP.
    # They previously used 2025-only, russian-only and a hard-coded 20, which is why
    # YouTube looked empty while its chart showed data.
    since = HOLDOUT_SINCE[:7]          # these sources carry "YYYY-MM" strings

    # Wikipedia: actual page URLs with the Russian spelling
    wiki = _load("wikipedia")
    if len(wiki) and "page_title" in wiki.columns:
        w = wiki[(wiki["date"] >= since) & (wiki["variant"].isin(HOLDOUT_VARIANTS))]
        for slug in enabled_slugs:
            pages = w[w["pair_slug"] == slug]
            if not len(pages):
                continue
            top = pages.groupby("page_title")["pageviews"].sum().nlargest(HOLDOUT_CAP)
            if len(top):
                by_pair.setdefault(slug, {})["wikipedia"] = [
                    {"name": t, "url": f"https://en.wikipedia.org/wiki/{t.replace(' ', '_')}", "views": int(v)}
                    for t, v in top.items()
                ]

    # Reddit: actual post URLs, best-scoring first
    reddit = _load("reddit")
    if len(reddit) and "post_id" in reddit.columns:
        r = reddit[(reddit["date"] >= since) & (reddit["variant"].isin(HOLDOUT_VARIANTS))]
        for slug in enabled_slugs:
            posts = r[r["pair_slug"] == slug]
            if not len(posts):
                continue
            posts = posts.nlargest(HOLDOUT_CAP, "score") if "score" in posts.columns else posts.head(HOLDOUT_CAP)
            by_pair.setdefault(slug, {})["reddit"] = [
                {"name": f"r/{x['subreddit']}: {str(x.get('title',''))[:80]}",
                 "url": f"https://reddit.com/r/{x['subreddit']}/comments/{x['post_id']}",
                 "score": int(x.get("score", 0) or 0)}
                for _, x in posts.iterrows()
            ]

    # YouTube: actual video URLs. One video per channel, so a single prolific channel
    # cannot own the table -- the same reason the news holdouts cap per domain.
    youtube = _load_youtube_census()
    # Fall back to the project key so the ordering is deterministic rather than
    # depending on whether someone exported a variable. Without it the table
    # silently falls back to recency, which is a different table.
    _yt_key = os.environ.get("YOUTUBE_API_KEY", "").strip()
    if not _yt_key:
        try:
            import subprocess as _sp
            _yt_key = _sp.run(
                ["gcloud", "services", "api-keys", "get-key-string",
                 "029b0141-1889-4665-a569-36d75c0f6191",
                 "--project=kyivnotkiev-yt", "--format=value(keyString)"],
                capture_output=True, text=True, timeout=30).stdout.strip()
        except Exception:
            _yt_key = ""
    _yt_ranked: list[str] = []
    if len(youtube) and "video_id" in youtube.columns:
        y = youtube[(youtube["date"] >= since) & (youtube["variant"].isin(HOLDOUT_VARIANTS))]
        for slug in enabled_slugs:
            vids = y[y["pair_slug"] == slug]
            if not len(vids):
                continue
            # Rank by views when a key is available, most-recent otherwise. Which one
            # ran is logged, so the table is never silently ordered by the fallback.
            vids = vids.sort_values("date", ascending=False)
            vids = vids.groupby("channel_title", sort=False, group_keys=False).head(HOLDOUT_PER_DOMAIN)
            if _yt_key:
                cand = vids.head(HOLDOUT_CAP * 3)
                views = _youtube_view_counts(cand.video_id.tolist(), _yt_key)
                if views:
                    cand = cand.assign(_v=cand.video_id.map(views).fillna(-1))
                    vids = cand.sort_values("_v", ascending=False).head(HOLDOUT_CAP)
                    _yt_ranked.append(slug)
                else:
                    vids = vids.head(HOLDOUT_CAP)
            else:
                vids = vids.head(HOLDOUT_CAP)
            by_pair.setdefault(slug, {})["youtube"] = [
                {"name": f"{x['channel_title']}: {str(x.get('title',''))[:80]}",
                 "url": f"https://youtube.com/watch?v={x['video_id']}"}
                for _, x in vids.iterrows()
            ]
        if _yt_ranked:
            log.info(f"  YouTube holdouts ranked by views for {len(_yt_ranked)} pair(s)")
        else:
            log.info("  YouTube holdouts ordered by recency — set YOUTUBE_API_KEY to rank by views")

    # Telegram: Latin-script Russian forms inside (mostly Cyrillic) public-channel
    # messages. One guard is mandatory: the match must survive URL-stripping —
    # dynamo.kiev.ua's own channel matched "Kiev" 311 times through the link
    # footer in every post, which is boilerplate, not usage.
    try:
        _tg = pd.read_parquet(ROOT / "data" / "store" / "telegram_raw.parquet")
    except Exception:
        _tg = pd.DataFrame()
    if len(_tg):
        import re as _re2
        _dstr = _tg.date.astype(str).str[:10]
        _t = _tg[(_tg.variant == "russian")
                 & (_dstr >= HOLDOUT_SINCE)
                 & (_dstr <= STUDY_END_DATE)
                 & _tg.pair_slug.isin(enabled_slugs)].copy()
        _t["_clean"] = (_t.text.astype(str)
                        .str.replace(r"https?://\S+", " ", regex=True)
                        .str.replace(r"\b[\w.-]+\.(?:ua|com|org|net|info)/\S*", " ", regex=True))
        _t = _t[[bool(_re2.search(r"\b" + _re2.escape(str(r.matched_term)) + r"\b",
                                  r._clean, _re2.I))
                 for r in _t.itertuples()]]
        _n_tg = 0
        for _slug, _g in _t.groupby("pair_slug"):
            _g = (_g.sort_values("views", ascending=False)
                    .groupby("channel_title", sort=False, group_keys=False).head(HOLDOUT_PER_DOMAIN)
                    .sort_values("views", ascending=False).head(HOLDOUT_CAP))
            if not len(_g):
                continue
            by_pair.setdefault(_slug, {})["telegram"] = [{
                "name": str(r.channel_title)[:60],
                "url": f"https://t.me/{r.channel}",
                "snippet": _re2.sub(r"\s+", " ", str(r.text))[:160],
                "term": str(r.matched_term),
                "views": int(r.views) if pd.notna(r.views) else 0,
                "month": str(r.date)[:7],
            } for r in _g.itertuples()]
            _n_tg += len(_g)
        log.info(f"  Telegram holdouts: {_n_tg} messages (russian form outside URLs, "
                 f"since {HOLDOUT_SINCE}, view-ranked)")

    log.info(f"  Holdouts: {len(by_pair)} pairs across news/wiki/reddit/youtube/telegram")

    # Global holdouts (top 100 news domains)
    global_list = []
    if len(gdelt):
        g25 = gdelt[gdelt["date"] >= "2025-01"]
        g2 = g25.groupby(["source_domain", "variant"])["count"].sum().reset_index()
        p2 = g2.pivot_table(index="source_domain", columns="variant", values="count", fill_value=0).reset_index()
        p2["total"] = p2.get("russian", 0) + p2.get("ukrainian", 0)
        p2["rus_pct"] = (p2.get("russian", 0) / p2["total"] * 100).round(1)
        p2 = p2[(p2["total"] >= 50) & (p2["rus_pct"] > 50)]
        p2["is_ru"] = p2["source_domain"].str.endswith(".ru")
        top = p2.nlargest(100, "total")
        global_list = [{"domain": r["source_domain"], "russian_pct": float(r["rus_pct"]),
                        "total": int(r["total"]), "is_ru": bool(r["is_ru"])} for _, r in top.iterrows()]

    return by_pair, global_list


def export_pair_events(enabled_slugs: set[str]) -> dict:
    log.info("Exporting pair events...")
    cfg = load_pairs()
    result = {}
    for p in cfg["pairs"]:
        slug = p.get("slug")
        if not slug or slug not in enabled_slugs:
            continue
        events = p.get("events", [])
        if events:
            result[slug] = [
                {"date": e["date"], "label": e["label"], "color": e.get("color", "#0057B8")}
                for e in events
            ]
    return result


def export_domain_origins(enabled_slugs: set[str]) -> dict:
    log.info("Exporting domain origins...")
    df = _load("gdelt")
    if not len(df):
        return {}
    cutoff = date.today() - timedelta(days=24 * 30)
    recent = df[pd.to_datetime(df["date"]).dt.date >= cutoff].copy()

    def _origin(domain):
        if domain.endswith(".ru"):
            return "ru"
        elif domain.endswith(".ua"):
            return "ua"
        return "intl"

    recent["origin"] = recent["source_domain"].apply(_origin)
    g = recent.groupby(["pair_slug", "origin", "variant"])["count"].sum().reset_index()
    p = g.pivot_table(index=["pair_slug", "origin"], columns="variant", values="count", fill_value=0).reset_index()
    p["total"] = p.get("russian", 0) + p.get("ukrainian", 0)

    result = {}
    for _, r in p.iterrows():
        pid = r["pair_slug"]
        if pid not in enabled_slugs:
            continue
        total = int(r["total"])
        result.setdefault(pid, {})[r["origin"]] = {
            "ukr": int(r.get("ukrainian", 0)), "rus": int(r.get("russian", 0)),
            "total": total, "adoption": round(int(r.get("ukrainian", 0)) / total * 100, 1) if total > 0 else 0,
        }
    log.info(f"  Domain origins: {len(result)} pairs")
    return result


def export_analysis() -> dict:
    """Export analysis data. Prefers recompute_stats output, falls back to dataset."""
    log.info("Exporting analysis...")
    # recompute_stats.py writes directly to site/src/data/analysis.json — use it if available
    site_path = SITE_DATA_DIR / "analysis.json"
    if site_path.exists():
        with open(site_path) as f:
            data = json.load(f)
        if data.get("kruskal_wallis") or data.get("regression"):
            log.info("  Using existing analysis.json from recompute_stats")
            return data
    # Fallback to dataset export
    analysis_path = DATASET_DIR / "analysis.json"
    if analysis_path.exists():
        with open(analysis_path) as f:
            return json.load(f)
    return {"changepoint_detection": [], "metadata": {"generated": "auto", "source": "parquet"}}


def write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"), default=str)
    log.info(f"  Wrote {path.name} ({path.stat().st_size / 1024:.0f} KB)")


def main():
    log.info("=" * 60)
    log.info("Exporting dataset to site JSON")
    log.info("=" * 60)

    enabled_slugs = get_enabled_slugs()
    analyzable_slugs = get_analyzable_slugs()
    control_slugs = get_control_slugs()
    log.info(f"Pairs: {len(enabled_slugs)} enabled, {len(analyzable_slugs)} analyzable, {len(control_slugs)} control")

    manifest = export_manifest(enabled_slugs, analyzable_slugs, control_slugs)
    timeseries = export_timeseries(enabled_slugs)

    # Per-source figures for the pair-view cards, derived from the SAME series the
    # chart draws, so the two cannot disagree. They were previously summed in the
    # browser over the plotted points, which printed the sum of Trends' 0-100 index
    # as a mention count; computing them from the raw frames instead would have
    # disagreed with the chart, since GDELT plots the verified-text series and
    # several sources are smoothed before display.
    INDEX_UNITS = {"trends": "index", "ngrams": "frequency"}
    UNITS = {"gdelt": "articles", "wikipedia": "pageviews", "reddit": "posts",
             "youtube": "videos", "openalex": "papers", "telegram": "messages"}
    pss: dict = {}
    for _slug, _srcs in timeseries.items():
        if _slug == "events" or not isinstance(_srcs, dict):
            continue
        for _src, _ser in _srcs.items():
            if not isinstance(_ser, list) or not _ser:
                continue
            u = sum(float(d.get("ukr") or 0) for d in _ser)
            r = sum(float(d.get("rus") or 0) for d in _ser)
            if u + r <= 0:
                continue
            # Prefer the series' own adoption where it exists: for Trends that is the
            # calibrated ratio, the only scale on which the two variants compare.
            adopts = [d["adoption"] for d in _ser
                      if d.get("adoption") is not None and (float(d.get("ukr") or 0) + float(d.get("rus") or 0)) > 0]
            wts = [float(d.get("ukr") or 0) + float(d.get("rus") or 0) for d in _ser
                   if d.get("adoption") is not None and (float(d.get("ukr") or 0) + float(d.get("rus") or 0)) > 0]
            adoption = (sum(a * w for a, w in zip(adopts, wts)) / sum(wts)) if wts else (u / (u + r) * 100)
            countable = _src not in INDEX_UNITS
            e = {"adoption": round(adoption, 1),
                 "unit": INDEX_UNITS.get(_src, UNITS.get(_src, "records")),
                 "countable": countable}
            if countable:
                e["ukr"], e["rus"] = int(round(u)), int(round(r))
            pss.setdefault(_slug, {})[_src] = e
    manifest["pair_source_stats"] = pss
    log.info(f"  Per-pair source stats: {len(pss)} pairs, "
             f"{sum(len(v) for v in pss.values())} pair×source entries "
             f"(derived from the plotted series)")

    # Update manifest source pairs to match post-threshold timeseries
    for src in manifest.get("sources", {}):
        chart_pairs = sum(1 for pid in timeseries if pid != "events" and src in timeseries[pid] and isinstance(timeseries[pid][src], list) and len(timeseries[pid][src]) > 0)
        manifest["sources"][src]["pairs"] = chart_pairs
    # trends_countries removed — country distribution from GDELT only
    # Holdouts: preserve existing file if it has URLs (built by BQ CSV scan)
    # Only regenerate if file doesn't exist
    holdouts_by_pair, _ = export_holdouts(enabled_slugs)
    _, holdouts_global = export_holdouts(enabled_slugs)

    # GDELT article holdouts, validated against the rebuilt attested mention set.
    #
    # The parquet files under data/corpus/gdelt_holdouts predate the v2 rebuild and
    # were derived from AllNames matching, where 78.9% of rows had no URL attestation
    # and 88.7% were machine translations. Rather than trust them, each row is kept
    # only when its URL also appears in gdelt_mentions_final.parquet -- 53.8% survive.
    # The variant is taken from the rebuilt data (attested in the URL path), never
    # from the old file. Article text is carried over because the rebuilt pull holds
    # URLs only; a text pipeline is a separate piece of work.
    # Verified records supersede the legacy holdout parquets wherever they exist.
    # Those parquets were written in May by the AllNames pipeline and their SELECTION
    # is whatever that pipeline happened to fetch; the verified set is fetched from the
    # rebuilt attested+unattested pools and classified from the article body. Pairs
    # without a verified build fall through to the legacy path below.
    _verified_dir = ROOT / "data" / "cl" / "corpus" / "gdelt_verified"
    _verified_pairs: set[str] = set()
    if _verified_dir.exists():
        for _vf in sorted(_verified_dir.glob("*.parquet")):
            if _vf.stem.endswith("_series"):
                continue
            _slug = _vf.stem
            if _slug not in enabled_slugs:
                continue
            _vdf = pd.read_parquet(_vf)
            _vdf = _vdf[_vdf.variant.isin(HOLDOUT_VARIANTS)
                        & (_vdf.date >= HOLDOUT_SINCE)
                        & (_vdf.date <= STUDY_END_DATE)]
            if not len(_vdf):
                continue
            _vdf = (_vdf.sort_values("date", ascending=False)
                        .groupby("domain", sort=False, group_keys=False).head(HOLDOUT_PER_DOMAIN)
                        .head(HOLDOUT_CAP))
            holdouts_by_pair.setdefault(_slug, {})["news_articles"] = [{
                "domain": r.domain,
                "url": r.url,
                "variant": r.variant,
                "text_preview": _preview_around_match(r.text, r.variant, _slug),
                "month": r.month,
            } for r in _vdf.itertuples()]
            _verified_pairs.add(_slug)
        if _verified_pairs:
            log.info(f"  Verified GDELT holdouts: {len(_verified_pairs)} pairs "
                     f"({', '.join(sorted(_verified_pairs))})")

    # No legacy holdout fallback. It derived the variant from the URL rather than
    # the article body, and padded the cap with Ukrainian rows when Russian ones ran
    # short — 99 of them reached the site under a table of Russian holdouts. A pair
    # without a verified build now shows no table at all.

    for _slug, _papers in export_openalex_holdouts(enabled_slugs).items():
        holdouts_by_pair.setdefault(_slug, {})["openalex"] = _papers
    pair_events = export_pair_events(enabled_slugs)
    analysis = export_analysis()
    domain_origins = export_domain_origins(enabled_slugs)

    write_json(SITE_DATA_DIR / "manifest.json", manifest)
    write_json(SITE_DATA_DIR / "timeseries.json", timeseries)
    write_json(SITE_DATA_DIR / "domain_origins.json", domain_origins)
    write_json(SITE_DATA_DIR / "holdouts_by_pair.json", holdouts_by_pair)
    write_json(SITE_DATA_DIR / "holdouts.json", holdouts_global)
    write_json(SITE_DATA_DIR / "pair_events.json", pair_events)

    # Pair metadata for the About table, straight from config/pairs.yaml so the forms
    # and rationales on the page cannot drift from what the study measures. Disabled
    # pairs are carried with enabled=false and filtered at render, not dropped here,
    # so the file stays a faithful view of the config.
    _meta = [{
        "slug": p["slug"],
        "enabled": bool(p.get("enabled", True)),
        "russian": p.get("russian", ""),
        "ukrainian": p.get("ukrainian", ""),
        "ukrainian_cyrillic": p.get("ukrainian_cyrillic", ""),
        "significance": p.get("significance", ""),
    } for p in load_pairs().get("pairs", [])]
    write_json(SITE_DATA_DIR / "pairs_meta.json", _meta)

    # Plain-language glosses for cluster registers. Deterministic keyword rules,
    # not per-pair hand edits: the first rule whose keywords intersect a cluster's
    # top terms names it. Editable here, reproducible everywhere.
    GLOSS_RULES = [
        ({"kievan", "kyivan", "rus"}, "Kyivan Rus history"),
        ({"church", "orthodox", "baptism", "christianity"}, "church & baptism"),
        ({"monument", "park", "statue", "arch"}, "Kyiv monuments & places"),
        ({"putin", "trump"}, "Putin nicknamed 'Vladimir the Great'"),
        ({"evony", "duke", "king", "bce"}, "games & other 'Greats'"),
        ({"coin", "coins", "hryvnia"}, "commemorative coins"),
        ({"heart"}, "the 2024 STALKER game"),
        ({"stalker", "shadow"}, "the 2007 STALKER game"),
        ({"stalker"}, "STALKER game fans"),
        ({"hbo", "series", "mazin"}, "the HBO miniseries"),
        ({"vudu", "itunes", "steam", "keys"}, "storefront listings"),
        ({"beat"}, "'type beat' music titles"),
        ({"fukushima"}, "nuclear-energy debate"),
        ({"thyroid", "exposure"}, "radiation-health research"),
        ({"pripyat", "exclusion", "tour", "tourism"}, "exclusion-zone visits"),
        ({"reactor", "disaster", "radiation", "accident", "nuclear"}, "the 1986 disaster"),
        ({"ukraine", "russian", "war", "invasion"}, "war & news coverage"),
        ({"recipe", "soup", "cook", "food"}, "cooking & recipes"),
        ({"boxing", "fight", "fury", "joshua"}, "boxing coverage"),
        ({"football", "league", "match", "goal"}, "football coverage"),
    ]

    def _gloss_for(terms: list, label: str) -> str:
        # Best keyword overlap wins; ties go to the earlier rule. First-match on a
        # single keyword mislabelled clusters — the 2007 game's terms contain
        # "heart", so it grabbed the 2024 gloss, and the storefront cluster carries
        # "stalker" deep in its list and became a game.
        low = {t.lower() for t in terms}
        best, best_n = None, 0
        for kw, g in GLOSS_RULES:
            n = len(kw & low)
            # multi-keyword rules need two hits: the war rule fired on every
            # cluster of a pair whose whole corpus is about Russia and Ukraine
            if n < min(2, len(kw)):
                continue
            if n > best_n:
                best, best_n = g, n
        return best if best_n else label

    # Cluster scatter for the pair pages, regenerated from the stats pipeline
    # (pipeline/stats/clusters.py). Only pairs whose clustering has been run on the
    # CURRENT corpus appear; there is no fallback to the old artifact, which was
    # computed before the GDELT and trends rebuilds and deleted for it.
    _cl_out = {}
    _cl_root = ROOT / "data" / "stats"
    for _cdir in sorted(_cl_root.glob("*/clusters")):
        _slug = _cdir.parent.name
        if _slug not in enabled_slugs:
            continue
        try:
            _summ = json.loads((_cdir / "summary.json").read_text())
            _asg = pd.read_parquet(_cdir / "assignments.parquet")
        except Exception as _e:
            log.warning(f"  clusters: skipping {_slug}: {_e}")
            continue
        # Cap the payload: 4,000 seeded points draw the same picture as 15,000 at
        # a fraction of the page weight.
        if len(_asg) > 4000:
            _asg_s = _asg.sample(4000, random_state=20260829)
        else:
            _asg_s = _asg
        _vmap = {"russian": "r", "ukrainian": "u", "both": "b"}
        _points = [{"x": float(r.umap_x), "y": float(r.umap_y),
                    "v": _vmap.get(r.variant, "b")}
                   for r in _asg_s.itertuples()]
        _clusters = {}
        # The pair's own spellings head almost every term list; excluding them makes
        # the labels describe the CONTEXT. Derived from config, not hardcoded.
        _pc = next((q for q in load_pairs().get("pairs", []) if q.get("slug") == _slug), {})
        _pairwords = {w.lower() for t in (_pc.get("russian", ""), _pc.get("ukrainian", ""))
                      for w in str(t).split()}
        for _c in _summ.get("clusters", []):
            _cid = _c["cluster"]
            _m = _asg[_asg.cluster == _cid]
            if not len(_m):
                continue
            # Label: the first two distinguishing terms that are not the pair's own
            # spellings, which head almost every list.
            _terms = [t for t in _c.get("top_terms", [])
                      if t.lower() not in _pairwords and len(t) > 2]
            # Labels must be unique on the chart: two stalker clusters both showed
            # "shadow · stalker". Extend with further terms until distinct.
            if _c.get("english_ratio", 1.0) < 0.08:
                # essentially no English function words: name the fact, not fragments
                _terms = []
                _label = "non-English"
            else:
                _label = " · ".join(_terms[:2]) if _terms else f"cluster {_cid}"
            _used = {v["label"] for v in _clusters.values()}
            _i = 2
            while _label in _used and _i < len(_terms):
                _label = " · ".join(_terms[:2] + [_terms[_i]])
                _i += 1
            # Anchor the label at the cluster's densest cell, not its median: for a
            # crescent or two-lobed cluster the median can sit outside the visible
            # mass entirely, which is how a large cluster appeared unlabelled.
            # A cluster can be multi-lobed. Each point is assigned to its nearest
            # candidate anchor; an anchor survives only if it owns >=15% of the
            # cluster AND sits at least 2.5 units from every stronger anchor —
            # three near-identical badges for one register were candidate cells
            # that barely cleared a bare count threshold. Each surviving lobe
            # carries a radius (the 80th-percentile distance of its points), so the
            # chart can draw extent instead of a bare dot.
            _cg = pd.concat([(_m.umap_x / 3).round() * 3,
                             (_m.umap_y / 3).round() * 3], axis=1)
            _cells = _cg.value_counts()
            _cand = []
            for (_cx0, _cy0), _cnt in _cells.head(6).items():
                _in = _m[(_cg.iloc[:, 0] == _cx0) & (_cg.iloc[:, 1] == _cy0)]
                _cand.append((float(_in.umap_x.median()), float(_in.umap_y.median())))
            _keep = []
            for _ax, _ay in _cand:
                if all(((_ax - kx) ** 2 + (_ay - ky) ** 2) ** 0.5 >= 2.5 for kx, ky, *_ in _keep):
                    _keep.append((_ax, _ay))
                if len(_keep) >= 3:
                    break
            import numpy as _np
            _pts = _m[["umap_x", "umap_y"]].to_numpy()
            _d = _np.stack([((_pts[:, 0] - kx) ** 2 + (_pts[:, 1] - ky) ** 2) ** 0.5
                            for kx, ky in _keep])
            _own = _d.argmin(0)
            _anchors = []
            for _ai, (_ax, _ay) in enumerate(_keep):
                _mine = _d[_ai][_own == _ai]
                _share = float((_own == _ai).mean())
                if _share < 0.15 and _anchors:
                    continue
                _anchors.append([round(_ax, 3), round(_ay, 3),
                                 round(float(_np.percentile(_mine, 80)) if len(_mine) else 1.0, 3),
                                 round(_share, 3)])
            _ua_pct = round(_c.get("variant_share", {}).get("ukrainian", 0) * 100, 1)
            if _ua_pct < 3:
                _phrase = "almost entirely Russian form"
            elif _ua_pct < 25:
                _phrase = "mostly Russian form"
            elif _ua_pct <= 75:
                _phrase = "genuinely mixed"
            else:
                _phrase = "mostly Ukrainian form"
            _peak = _c.get("peak_year")
            _clusters[str(_cid)] = {
                "label": _label,
                "anchors": _anchors,
                "cx": _anchors[0][0], "cy": _anchors[0][1],
                "ua_pct": _ua_pct,
                "size": _c.get("size", int(len(_m))),
                "gloss": ("non-English coverage" if _label == "non-English"
                          else _gloss_for(_c.get("top_terms", []), _label)),
                # filled below once all clusters exist; placeholder keeps key order
                "peak": _peak,
            }
        # Identical glosses on different clusters read as duplicates — the exact
        # complaint the glosses were meant to fix. Append each cluster's first
        # distinguishing term to break the tie.
        # Two clusters sharing a gloss AND a spelling regime are one PRESENTED
        # group — "the 1986 disaster (radiation)" vs "(disaster)" told a reader
        # nothing. Merge them for display: sizes sum, UA share weights, anchors
        # concatenate. Same gloss with genuinely different UA shares stays split
        # and gets the exact shares as the distinguisher.
        _by_gloss = {}
        for _k2, _v2 in list(_clusters.items()):
            _g = _v2["gloss"]
            if _g in _by_gloss and abs(_clusters[_by_gloss[_g]]["ua_pct"] - _v2["ua_pct"]) <= 3.0:
                _a = _clusters[_by_gloss[_g]]
                _tot = _a["size"] + _v2["size"]
                _a["ua_pct"] = round((_a["ua_pct"] * _a["size"] + _v2["ua_pct"] * _v2["size"]) / _tot, 1)
                _a["size"] = _tot
                _a["anchors"] = (_a.get("anchors", []) + _v2.get("anchors", []))[:4]
                _a["label"] = _a["label"] if len(_a["label"]) <= len(_v2["label"]) else _v2["label"]
                del _clusters[_k2]
            elif _g in _by_gloss:
                _clusters[_k2]["gloss"] = f"{_g} ({_v2['ua_pct']}% UA)"
            else:
                _by_gloss[_g] = _k2

        _cl_out[_slug] = {"points": _points, "clusters": _clusters,
                          "total": int(_summ.get("n", len(_asg))),
                          "n_clusters": int(_summ.get("k_chosen", len(_clusters))),
                          "borderline_share": _summ.get("borderline_share")}
    write_json(SITE_DATA_DIR / "cl_clusters.json", _cl_out)
    log.info(f"  Wrote cl_clusters.json ({len(_cl_out)} pair(s) with current clustering)")
    log.info(f"  Wrote pairs_meta.json ({sum(1 for x in _meta if x['enabled'])} enabled "
             f"of {len(_meta)} pairs)")
    write_json(SITE_DATA_DIR / "analysis.json", analysis)

    # Files written above already respect `enabled`, but several site JSONs come
    # from other pipeline steps that do not. Prune them here so config/pairs.yaml
    # is the single source of truth and a plain re-export is enough after
    # enabling or disabling a pair.
    from pipeline.prune_site_data import main as prune_main
    prune_main()

    log.info("=" * 60)
    log.info("Export complete!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
