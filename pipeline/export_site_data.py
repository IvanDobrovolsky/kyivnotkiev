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
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from pipeline.config import load_pairs

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

ROOT = Path(__file__).resolve().parent.parent
DATASET_DIR = ROOT / "dataset"
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
            # Apply homonym filters from pairs.yaml for GDELT
            if name == "gdelt" and "source_domain" in df.columns:
                df = _apply_homonym_filters(df)
            # Filter YouTube: English titles + verified term presence
            if name == "youtube" and "title" in df.columns:
                df = _filter_youtube(df)
            _cache[name] = df
    return _cache[name]


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
        g = t.groupby(["pair_slug", "month", "variant"])["interest"].sum().reset_index()
        p = g.pivot_table(index=["pair_slug", "month"], columns="variant", values="interest", fill_value=0).reset_index()
        ukr_col = "ukrainian" if "ukrainian" in p.columns else 0
        rus_col = "russian" if "russian" in p.columns else 0
        for pid, grp in p.groupby("pair_slug"):
            if pid not in enabled_slugs:
                continue
            raw = []
            for _, r in grp.sort_values("month").iterrows():
                ukr = int(r.get(ukr_col, 0))
                rus = int(r.get(rus_col, 0))
                total = ukr + rus
                adoption = round(ukr / total * 100, 1) if total > 0 else None
                raw.append({"date": r["month"], "adoption": adoption, "ukr": ukr, "rus": rus})
            result.setdefault(pid, {})
            result[pid]["trends"] = smooth_series(raw, window=3)

    # GDELT (monthly)
    log.info("  GDELT...")
    df = _load("gdelt")
    if len(df):
        df["month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
        g = df.groupby(["pair_slug", "month", "variant"])["count"].sum().reset_index()
        p = g.pivot_table(index=["pair_slug", "month"], columns="variant", values="count", fill_value=0).reset_index()
        for pid, grp in p.groupby("pair_slug"):
            if pid not in enabled_slugs:
                continue
            spid = pid
            result.setdefault(spid, {}).setdefault("gdelt", [])
            for _, r in grp.sort_values("month").iterrows():
                ukr = int(r.get("ukrainian", 0))
                rus = int(r.get("russian", 0))
                total = ukr + rus
                if total > 0:
                    result[spid]["gdelt"].append({"date": r["month"], "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})

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

    # Religious (yearly from scraped institutional sites)
    log.info("  Religious...")
    religious_path = DATASET_DIR / "raw_religious.parquet"
    if religious_path.exists():
        rel = pd.read_parquet(religious_path)
        if len(rel) and "date" in rel.columns:
            rel["month"] = pd.to_datetime(rel["date"]).dt.strftime("%Y-%m")
            g = rel.groupby(["pair_slug", "month", "variant"])["count"].sum().reset_index()
            p = g.pivot_table(index=["pair_slug", "month"], columns="variant", values="count", fill_value=0).reset_index()
            for pid, grp in p.groupby("pair_slug"):
                if pid not in enabled_slugs:
                    continue
                spid = pid
                result.setdefault(spid, {}).setdefault("religious", [])
                for _, r in grp.sort_values("month").iterrows():
                    ukr = int(r.get("ukrainian", 0))
                    rus = int(r.get("russian", 0))
                    total = ukr + rus
                    if total > 0:
                        result[spid]["religious"].append({"date": r["month"], "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})

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
    religious = pd.DataFrame()
    religious_path = DATASET_DIR / "raw_religious.parquet"
    if religious_path.exists():
        religious = pd.read_parquet(religious_path)
        source_stats["religious"] = {"records": int(religious["count"].sum()), "pairs": int(religious["pair_slug"].nunique()), "unit": "articles"}
        extra_map["religious_sites"] = str(religious["source_domain"].nunique())
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
    # Religious
    if len(religious):
        per_source["religious"] = _source_adoption(religious, "count", "date", cutoff_12m, min_total=3)

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
    # Religious
    if len(religious):
        for pid, cnt in religious.groupby("pair_slug")["count"].sum().items():
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

    toponym_matches = sum(s["records"] for s in source_stats.values()) + openalex_total_papers

    manifest = {
        "total_pairs": len(enabled_slugs),
        "analyzable_pairs": len(analyzable_slugs),
        "records_scanned": "90B+",
        "toponym_matches": toponym_matches,
        "cl_corpus": _get_cl_corpus_size(),
        "time_span": "2010-2025",
        "num_sources": 9,  # 7 standard + telegram + religious
        "num_countries": int(extra_map.get("trends_countries", "0")),
        "sources": {
            "gdelt": {"records": source_stats.get("gdelt", {}).get("records", 0), "pairs": source_stats.get("gdelt", {}).get("pairs", 0), "label": "News", "unit": "articles", "extra": f"GDELT · {extra_map.get('gdelt_domains', '0')} domains", "color": "#1e3a5f"},
            "trends": {"records": source_stats.get("trends", {}).get("records", 0), "pairs": source_stats.get("trends", {}).get("pairs", 0), "label": "Trends", "unit": "datapoints", "extra": f"Google · {extra_map.get('trends_countries', '55')} countries", "color": "#4285F4"},
            "wikipedia": {"records": source_stats.get("wikipedia", {}).get("records", 0), "pairs": source_stats.get("wikipedia", {}).get("pairs", 0), "label": "Wiki", "unit": "pageviews", "extra": "Wikipedia · monthly", "color": "#636466"},
            "reddit": {"records": source_stats.get("reddit", {}).get("records", 0), "pairs": source_stats.get("reddit", {}).get("pairs", 0), "label": "Reddit", "unit": "posts", "extra": f"{extra_map.get('reddit_subreddits', '0')} subreddits", "color": "#FF4500"},
            "youtube": {"records": source_stats.get("youtube", {}).get("records", 0), "pairs": source_stats.get("youtube", {}).get("pairs", 0), "label": "YouTube", "unit": "videos", "extra": f"{extra_map.get('youtube_channels', '0')} channels", "color": "#FF0000"},
            "ngrams": {"records": source_stats.get("ngrams", {}).get("records", 0), "pairs": source_stats.get("ngrams", {}).get("pairs", 0), "label": "Books", "unit": "records", "extra": "Google Books · 8M+ volumes", "color": "#7c3aed"},
            "openalex": {"records": openalex_total_papers, "pairs": openalex_total_pairs, "label": "Academic", "unit": "papers", "extra": "OpenAlex · 250M+ works", "color": "#06b6d4"},
            "telegram": {"records": source_stats.get("telegram", {}).get("records", 0), "pairs": source_stats.get("telegram", {}).get("pairs", 0), "label": "Telegram", "unit": "messages", "extra": f"{extra_map.get('telegram_channels', '0')} channels", "color": "#26A5E4"},
            "religious": {"records": source_stats.get("religious", {}).get("records", 0), "pairs": source_stats.get("religious", {}).get("pairs", 0), "label": "Religious", "unit": "articles", "extra": f"{extra_map.get('religious_sites', '0')} institutions", "color": "#8B0000"},
        },
        "pairs": sorted(pairs_out, key=lambda x: x["slug"]),
    }

    log.info(f"  Manifest: {manifest['analyzable_pairs']} analyzable pairs, {manifest['toponym_matches']:,} toponym matches")
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

    # Wikipedia: actual page URLs with Russian spelling
    wiki = _load("wikipedia")
    if len(wiki) and "page_title" in wiki.columns:
        w25 = wiki[wiki["date"] >= "2025-01"]
        for slug in enabled_slugs:
            pages = w25[(w25["pair_slug"] == slug) & (w25["variant"] == "russian")]
            if len(pages):
                top = pages.groupby("page_title")["pageviews"].sum().nlargest(20)
                if len(top):
                    by_pair.setdefault(slug, {})["wikipedia"] = [
                        {"name": t, "url": f"https://en.wikipedia.org/wiki/{t.replace(' ', '_')}", "views": int(v)}
                        for t, v in top.items()
                    ]

    # Reddit: actual post URLs
    reddit = _load("reddit")
    if len(reddit) and "post_id" in reddit.columns:
        r25 = reddit[(reddit["date"] >= "2025-01") & (reddit["variant"] == "russian")]
        for slug in enabled_slugs:
            posts = r25[r25["pair_slug"] == slug].nlargest(20, "score") if "score" in r25.columns else r25[r25["pair_slug"] == slug].head(20)
            if len(posts):
                by_pair.setdefault(slug, {})["reddit"] = [
                    {"name": f"r/{r['subreddit']}: {str(r.get('title',''))[:60]}",
                     "url": f"https://reddit.com/r/{r['subreddit']}/comments/{r['post_id']}",
                     "score": int(r.get("score", 0) or 0)}
                    for _, r in posts.iterrows()
                ]

    # YouTube: actual video URLs
    youtube = _load_youtube_census()
    if len(youtube) and "video_id" in youtube.columns:
        y25 = youtube[(youtube["date"] >= "2025-01") & (youtube["variant"] == "russian")]
        for slug in enabled_slugs:
            vids = y25[y25["pair_slug"] == slug].head(20)
            if len(vids):
                by_pair.setdefault(slug, {})["youtube"] = [
                    {"name": f"{r['channel_title']}: {str(r.get('title',''))[:60]}",
                     "url": f"https://youtube.com/watch?v={r['video_id']}"}
                    for _, r in vids.iterrows()
                ]

    log.info(f"  Holdouts: {len(by_pair)} pairs across news/wiki/reddit/youtube")

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

    # Update manifest source pairs to match post-threshold timeseries
    for src in manifest.get("sources", {}):
        chart_pairs = sum(1 for pid in timeseries if pid != "events" and src in timeseries[pid] and isinstance(timeseries[pid][src], list) and len(timeseries[pid][src]) > 0)
        manifest["sources"][src]["pairs"] = chart_pairs
    # trends_countries removed — country distribution from GDELT only
    # Holdouts: preserve existing file if it has URLs (built by BQ CSV scan)
    # Only regenerate if file doesn't exist
    holdouts_by_pair, _ = export_holdouts(enabled_slugs)
    _, holdouts_global = export_holdouts(enabled_slugs)

    # Inject GDELT article holdouts from cleaned parquet files
    _holdout_dir = ROOT / "data" / "corpus" / "gdelt_holdouts"
    if _holdout_dir.exists():
        import glob as _glob
        _hfiles = _glob.glob(str(_holdout_dir / "*.parquet"))
        _injected = 0
        for _hf in _hfiles:
            _slug = Path(_hf).stem
            if _slug not in enabled_slugs:
                continue
            _hdf = pd.read_parquet(_hf)
            if len(_hdf) == 0:
                continue
            _articles = []
            for _, _r in _hdf.iterrows():
                _articles.append({
                    "domain": _r.get("domain", ""),
                    "url": _r.get("url", ""),
                    "variant": _r.get("variant", ""),
                    "text_preview": str(_r.get("text", ""))[:200],
                    "month": str(_r.get("month", "")),
                })
            holdouts_by_pair.setdefault(_slug, {})["news_articles"] = _articles
            _injected += 1
        log.info(f"  Injected GDELT article holdouts: {_injected} pairs, {sum(len(holdouts_by_pair.get(s,{}).get('news_articles',[])) for s in enabled_slugs):,} articles")
    pair_events = export_pair_events(enabled_slugs)
    analysis = export_analysis()
    domain_origins = export_domain_origins(enabled_slugs)

    # GDELT per-country adoption (ccTLD + known outlets mapping)
    log.info("Exporting GDELT country distribution...")
    from pipeline.ingestion.gdelt_athena_countries import domain_to_country, ISO_NUM_TO_ALPHA
    gdelt = _load("gdelt")
    countries_by_pair = {}
    if len(gdelt):
        cutoff = (date.today() - timedelta(days=24 * 30)).isoformat()[:10]
        recent = gdelt[gdelt["date"] >= cutoff].copy()
        recent["country"] = recent["source_domain"].apply(domain_to_country)
        mapped = recent[recent["country"] != ""]
        alpha_to_num = {v: k for k, v in ISO_NUM_TO_ALPHA.items()}
        agg = mapped.groupby(["pair_slug", "country", "variant"])["count"].sum().reset_index()
        for slug in agg["pair_slug"].unique():
            if slug not in enabled_slugs:
                continue
            pair_agg = agg[agg["pair_slug"] == slug]
            cdata = {}
            for ca in pair_agg["country"].unique():
                cd = pair_agg[pair_agg["country"] == ca]
                ukr = int(cd[cd["variant"] == "ukrainian"]["count"].sum())
                rus = int(cd[cd["variant"] == "russian"]["count"].sum())
                total = ukr + rus
                if total < 10:
                    continue
                num = alpha_to_num.get(ca, "")
                if num:
                    cdata[num] = {"name": GEO_NAMES.get(num, ca), "adoption": round(ukr / total * 100, 1), "total": total, "ukr": ukr, "rus": rus}
            if cdata:
                countries_by_pair[slug] = cdata
        log.info(f"  GDELT countries: {len(countries_by_pair)} pairs")

    write_json(SITE_DATA_DIR / "manifest.json", manifest)
    write_json(SITE_DATA_DIR / "timeseries.json", timeseries)
    write_json(SITE_DATA_DIR / "countries_by_pair.json", countries_by_pair)
    write_json(SITE_DATA_DIR / "domain_origins.json", domain_origins)
    write_json(SITE_DATA_DIR / "holdouts_by_pair.json", holdouts_by_pair)
    write_json(SITE_DATA_DIR / "holdouts.json", holdouts_global)
    write_json(SITE_DATA_DIR / "pair_events.json", pair_events)
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
