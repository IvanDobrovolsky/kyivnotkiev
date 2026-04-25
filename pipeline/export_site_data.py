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
            _cache[name] = table.to_pandas()
    return _cache[name]


def get_enabled_pair_ids() -> set[int]:
    cfg = load_pairs()
    return {p["id"] for p in cfg["pairs"] if p.get("enabled", True)}


def get_control_pair_ids() -> set[int]:
    cfg = load_pairs()
    return {p["id"] for p in cfg["pairs"] if p.get("is_control", False)}


def get_analyzable_pair_ids() -> set[int]:
    cfg = load_pairs()
    return {p["id"] for p in cfg["pairs"]
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
    corpus_path = DATA_DIR / "cl" / "balanced" / "corpus.parquet"
    if corpus_path.exists():
        return len(pd.read_parquet(corpus_path, columns=["pair_id"]))
    raw_dir = DATA_DIR / "cl" / "raw"
    total = 0
    if raw_dir.exists():
        for src_dir in raw_dir.iterdir():
            if src_dir.is_dir():
                for f in src_dir.glob("*.parquet"):
                    total += len(pd.read_parquet(f, columns=["pair_id"]))
    return total if total > 0 else 80141


def _safe_div(a, b):
    return a / b if b > 0 else 0.0


# ── Exports ───────────────────────────────────────────────────────────────────

def export_timeseries(enabled_ids: set[int]) -> dict:
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
        g = t.groupby(["pair_id", "month", "variant"])["interest"].sum().reset_index()
        p = g.pivot_table(index=["pair_id", "month"], columns="variant", values="interest", fill_value=0).reset_index()
        ukr_col = "ukrainian" if "ukrainian" in p.columns else 0
        rus_col = "russian" if "russian" in p.columns else 0
        for pid, grp in p.groupby("pair_id"):
            if pid not in enabled_ids:
                continue
            raw = []
            for _, r in grp.sort_values("month").iterrows():
                ukr = int(r.get(ukr_col, 0))
                rus = int(r.get(rus_col, 0))
                total = ukr + rus
                adoption = round(ukr / total * 100, 1) if total > 0 else None
                raw.append({"date": r["month"], "adoption": adoption, "ukr": ukr, "rus": rus})
            result.setdefault(str(pid), {})
            result[str(pid)]["trends"] = smooth_series(raw, window=3)

    # GDELT (monthly)
    log.info("  GDELT...")
    df = _load("gdelt")
    if len(df):
        df["month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
        g = df.groupby(["pair_id", "month", "variant"])["count"].sum().reset_index()
        p = g.pivot_table(index=["pair_id", "month"], columns="variant", values="count", fill_value=0).reset_index()
        for pid, grp in p.groupby("pair_id"):
            if pid not in enabled_ids:
                continue
            spid = str(pid)
            result.setdefault(spid, {}).setdefault("gdelt", [])
            for _, r in grp.sort_values("month").iterrows():
                ukr = int(r.get("ukrainian", 0))
                rus = int(r.get("russian", 0))
                total = ukr + rus
                if total > 0:
                    result[spid]["gdelt"].append({"date": r["month"], "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})

    # Wikipedia (monthly)
    log.info("  Wikipedia...")
    df = _load("wikipedia")
    if len(df):
        df["month"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")
        g = df.groupby(["pair_id", "month", "variant"])["pageviews"].sum().reset_index()
        p = g.pivot_table(index=["pair_id", "month"], columns="variant", values="pageviews", fill_value=0).reset_index()
        for pid, grp in p.groupby("pair_id"):
            if pid not in enabled_ids:
                continue
            spid = str(pid)
            result.setdefault(spid, {}).setdefault("wikipedia", [])
            for _, r in grp.sort_values("month").iterrows():
                ukr = int(r.get("ukrainian", 0))
                rus = int(r.get("russian", 0))
                total = ukr + rus
                if total > 0:
                    result[spid]["wikipedia"].append({"date": r["month"], "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})

    # Reddit (annual)
    log.info("  Reddit...")
    df = _load("reddit")
    if len(df):
        df["yr"] = pd.to_datetime(df["date"]).dt.year.astype(str)
        g = df.groupby(["pair_id", "yr", "variant"]).size().reset_index(name="cnt")
        p = g.pivot_table(index=["pair_id", "yr"], columns="variant", values="cnt", fill_value=0).reset_index()
        for pid, grp in p.groupby("pair_id"):
            if pid not in enabled_ids:
                continue
            spid = str(pid)
            result.setdefault(spid, {}).setdefault("reddit", [])
            for _, r in grp.sort_values("yr").iterrows():
                ukr = int(r.get("ukrainian", 0))
                rus = int(r.get("russian", 0))
                total = ukr + rus
                if total >= 2:
                    result[spid]["reddit"].append({"date": f"{r['yr']}-01", "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})

    # YouTube (annual, merge BQ parquet + local CSVs)
    log.info("  YouTube...")
    yt_data = defaultdict(dict)
    df = _load("youtube")
    if len(df):
        df["yr"] = pd.to_datetime(df["date"]).dt.year.astype(str)
        g = df.groupby(["pair_id", "yr", "variant"]).size().reset_index(name="cnt")
        p = g.pivot_table(index=["pair_id", "yr"], columns="variant", values="cnt", fill_value=0).reset_index()
        for _, r in p.iterrows():
            pid = int(r["pair_id"])
            if pid not in enabled_ids:
                continue
            yt_data[pid][r["yr"]] = (int(r.get("ukrainian", 0)), int(r.get("russian", 0)))

    yt_local_dir = DATA_DIR / "raw" / "youtube"
    if yt_local_dir.exists():
        for csv_file in sorted(yt_local_dir.glob("pair_*.csv")):
            with open(csv_file) as f:
                for row in csv.DictReader(f):
                    pid = int(row["pair_id"])
                    if pid not in enabled_ids:
                        continue
                    yr = str(row["year"])
                    matches = int(row.get("title_matches", 0))
                    variant = row["variant"]
                    existing = yt_data[pid].get(yr, (0, 0))
                    if variant == "ukrainian":
                        yt_data[pid][yr] = (existing[0] + matches, existing[1])
                    else:
                        yt_data[pid][yr] = (existing[0], existing[1] + matches)

    for pid in sorted(yt_data.keys()):
        spid = str(pid)
        result.setdefault(spid, {}).setdefault("youtube", [])
        for yr in sorted(yt_data[pid].keys()):
            ukr, rus = yt_data[pid][yr]
            total = ukr + rus
            if total > 0:
                result[spid]["youtube"].append({"date": f"{yr}-01", "adoption": round(ukr / total * 100, 1), "ukr": ukr, "rus": rus})

    # Ngrams (yearly)
    log.info("  Ngrams...")
    df = _load("ngrams")
    if len(df):
        g = df[df["year"] >= 1900].groupby(["pair_id", "year", "variant"])["frequency"].sum().reset_index()
        p = g.pivot_table(index=["pair_id", "year"], columns="variant", values="frequency", fill_value=0).reset_index()
        for pid, grp in p.groupby("pair_id"):
            if pid not in enabled_ids:
                continue
            spid = str(pid)
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
            pid = pair_data["pair_id"]
            if pid not in enabled_ids:
                continue
            spid = str(pid)
            raw_series = []
            for yr in pair_data["yearly"]:
                total = yr["total"]
                adoption = round(yr["ukrainian_count"] / total * 100, 1) if total > 0 else None
                raw_series.append({"date": f"{yr['year']}-01", "adoption": adoption, "ukr": yr["ukrainian_count"], "rus": yr["russian_count"]})
            result.setdefault(spid, {})
            result[spid]["openalex"] = smooth_series(raw_series, window=3)

    pair_count = len([k for k in result if k != "events"])
    log.info(f"  Timeseries: {pair_count} pairs")
    return result


def export_manifest(enabled_ids: set[int], analyzable_ids: set[int], control_ids: set[int]) -> dict:
    log.info("Exporting manifest (single source of truth)...")

    pairs_cfg = load_pairs()
    categories_raw = pairs_cfg.get("categories", {})

    cat_colors = {
        "geographical": "#0057B8", "food": "#e6b800", "landmarks": "#8B4513",
        "country": "#228B22", "institutional": "#4B0082", "sports": "#DC143C",
        "historical": "#708090", "people": "#FF6347",
    }
    categories = [
        {"id": cid, "name": info["name"], "color": cat_colors.get(cid, "#888")}
        for cid, info in categories_raw.items()
    ]

    # ── Per-source stats ──
    log.info("  Computing per-source stats...")
    source_stats = {}

    trends = _load("trends")
    if len(trends):
        source_stats["trends"] = {"records": len(trends), "pairs": int(trends["pair_id"].nunique()), "unit": "datapoints"}

    gdelt = _load("gdelt")
    if len(gdelt):
        source_stats["gdelt"] = {"records": int(gdelt["count"].sum()), "pairs": int(gdelt["pair_id"].nunique()), "unit": "articles"}

    wiki = _load("wikipedia")
    if len(wiki):
        source_stats["wikipedia"] = {"records": int(wiki["pageviews"].sum()), "pairs": int(wiki["pair_id"].nunique()), "unit": "pageviews"}

    reddit = _load("reddit")
    if len(reddit):
        source_stats["reddit"] = {"records": len(reddit), "pairs": int(reddit["pair_id"].nunique()), "unit": "posts"}

    youtube = _load("youtube")
    if len(youtube):
        source_stats["youtube"] = {"records": len(youtube), "pairs": int(youtube["pair_id"].nunique()), "unit": "videos"}

    ngrams = _load("ngrams")
    if len(ngrams):
        source_stats["ngrams"] = {"records": len(ngrams), "pairs": int(ngrams["pair_id"].nunique()), "unit": "records"}

    # Extra stats
    extra_map = {}
    if len(gdelt):
        extra_map["gdelt_domains"] = str(gdelt["source_domain"].nunique())
    if len(reddit):
        extra_map["reddit_subreddits"] = str(reddit["subreddit"].nunique())
    if len(youtube):
        extra_map["youtube_channels"] = str(youtube["channel_title"].nunique())
    if len(trends):
        geo = trends[(trends["geo"] != "") & (trends["geo"].notna())]
        extra_map["trends_countries"] = str(geo["geo"].nunique())

    # OpenAlex from local data
    openalex_path = DATA_DIR / "raw" / "openalex" / "openalex_all_pairs.json"
    openalex_total_papers = 0
    openalex_total_pairs = 0
    if openalex_path.exists():
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
            g = d.groupby(["pair_id", "variant"]).size().reset_index(name="val")
        else:
            g = d.groupby(["pair_id", "variant"])[value_col].sum().reset_index(name="val")
        p = g.pivot_table(index="pair_id", columns="variant", values="val", fill_value=0).reset_index()
        out = {}
        for _, r in p.iterrows():
            ukr = float(r.get("ukrainian", 0))
            rus = float(r.get("russian", 0))
            total = ukr + rus
            if total >= min_total:
                out[int(r["pair_id"])] = ukr / total
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
                    oa_adopt[p["pair_id"]] = ukr / total
        per_source["openalex"] = oa_adopt

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
        for pid, cnt in gdelt.groupby("pair_id")["count"].sum().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    if len(trends):
        t = trends[(trends["geo"] == "") | (trends["geo"].isna())]
        for pid, cnt in t.groupby("pair_id")["interest"].sum().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    if len(wiki):
        for pid, cnt in wiki.groupby("pair_id")["pageviews"].sum().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    if len(reddit):
        for pid, cnt in reddit.groupby("pair_id").size().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)
    if len(youtube):
        for pid, cnt in youtube.groupby("pair_id").size().items():
            total_map[pid] = total_map.get(pid, 0) + int(cnt)

    # Build pairs
    pairs_out = []
    for p in pairs_cfg["pairs"]:
        if p["id"] not in enabled_ids:
            continue
        pid = p["id"]
        recent = recent_map.get(pid, {})
        adoption_pct = 0.0 if pid in control_ids else recent.get("adoption", 0.0)
        pairs_out.append({
            "id": pid, "category": p["category"],
            "russian": p["russian"], "ukrainian": p["ukrainian"],
            "adoption": adoption_pct, "total": total_map.get(pid, 0),
            "is_control": pid in control_ids,
            "starred": p.get("starred", False), "starred_label": p.get("starred_label", ""),
        })

    # Category stats
    cat_stats = {}
    for p in pairs_out:
        if p["is_control"]:
            continue
        cat_stats.setdefault(p["category"], []).append(p["adoption"])
    category_list = []
    for c in categories:
        vals = cat_stats.get(c["id"], [])
        category_list.append({
            **c, "count": len(vals),
            "avg_adoption": round(sum(vals) / len(vals), 1) if vals else 0,
        })

    toponym_matches = sum(s["records"] for s in source_stats.values()) + openalex_total_papers

    manifest = {
        "total_pairs": len(enabled_ids),
        "analyzable_pairs": len(analyzable_ids),
        "records_scanned": "90B+",
        "toponym_matches": toponym_matches,
        "cl_corpus": _get_cl_corpus_size(),
        "time_span": "2010-2026",
        "num_sources": 7,
        "num_countries": int(extra_map.get("trends_countries", "0")),
        "sources": {
            "trends": {"records": source_stats.get("trends", {}).get("records", 0), "pairs": source_stats.get("trends", {}).get("pairs", 0), "label": "Search Trends", "unit": "datapoints", "extra": f"{extra_map.get('trends_countries', '55')} countries", "color": "#4285F4"},
            "gdelt": {"records": source_stats.get("gdelt", {}).get("records", 0), "pairs": source_stats.get("gdelt", {}).get("pairs", 0), "label": "News Articles", "unit": "articles", "extra": f"{extra_map.get('gdelt_domains', '0')} domains", "color": "#1e3a5f"},
            "wikipedia": {"records": source_stats.get("wikipedia", {}).get("records", 0), "pairs": source_stats.get("wikipedia", {}).get("pairs", 0), "label": "Page Views", "unit": "pageviews", "extra": "monthly", "color": "#636466"},
            "reddit": {"records": source_stats.get("reddit", {}).get("records", 0), "pairs": source_stats.get("reddit", {}).get("pairs", 0), "label": "Posts", "unit": "posts", "extra": f"{extra_map.get('reddit_subreddits', '0')} subreddits", "color": "#FF4500"},
            "youtube": {"records": source_stats.get("youtube", {}).get("records", 0), "pairs": source_stats.get("youtube", {}).get("pairs", 0), "label": "Videos", "unit": "videos", "extra": f"{extra_map.get('youtube_channels', '0')} channels", "color": "#FF0000"},
            "ngrams": {"records": source_stats.get("ngrams", {}).get("records", 0), "pairs": source_stats.get("ngrams", {}).get("pairs", 0), "label": "Book Records", "unit": "records", "extra": "8M+ volumes", "color": "#7c3aed"},
            "openalex": {"records": openalex_total_papers, "pairs": openalex_total_pairs, "label": "Academic Papers", "unit": "papers", "extra": "250M+ works indexed", "color": "#06b6d4"},
        },
        "categories": categories,
        "category_stats": sorted(category_list, key=lambda x: -x["avg_adoption"]),
        "pairs": sorted(pairs_out, key=lambda x: x["id"]),
    }

    log.info(f"  Manifest: {manifest['analyzable_pairs']} analyzable pairs, {manifest['toponym_matches']:,} toponym matches")
    return manifest


def export_trends_countries(enabled_ids: set[int]) -> dict:
    log.info("Exporting trends countries...")
    df = _load("trends")
    if not len(df):
        return {}
    t = df[(df["geo"] != "") & (df["geo"].notna())].copy()
    g = t.groupby(["pair_id", "geo", "variant"])["interest"].sum().reset_index()
    p = g.pivot_table(index=["pair_id", "geo"], columns="variant", values="interest", fill_value=0).reset_index()
    result = {}
    for _, r in p.iterrows():
        pid = int(r["pair_id"])
        if pid not in enabled_ids:
            continue
        ukr = float(r.get("ukrainian", 0))
        rus = float(r.get("russian", 0))
        total = ukr + rus
        if total < 100 or ukr == 0 or rus == 0:
            continue
        numeric = GEO_TO_NUMERIC.get(r["geo"])
        if not numeric:
            continue
        spid = str(pid)
        result.setdefault(spid, {})
        result[spid][numeric] = {"name": GEO_NAMES.get(numeric, r["geo"]), "adoption": round(ukr / total * 100, 1)}
    log.info(f"  Trends countries: {len(result)} pairs")
    return result


def export_holdouts(enabled_ids: set[int]) -> tuple[dict, list]:
    log.info("Exporting holdouts...")
    df = _load("gdelt")
    if not len(df):
        return {}, []
    recent = df[pd.to_datetime(df["date"]).dt.date >= date(2024, 1, 1)].copy()

    # Per-pair holdouts
    g = recent.groupby(["pair_id", "source_domain", "variant"])["count"].sum().reset_index()
    p = g.pivot_table(index=["pair_id", "source_domain"], columns="variant", values="count", fill_value=0).reset_index()
    p["total"] = p.get("russian", 0) + p.get("ukrainian", 0)
    p = p[p["total"] >= 20]
    p["rus"] = p.get("russian", 0)
    p = p[p["rus"] > p.get("ukrainian", 0)]
    p["russian_pct"] = (p["rus"] / p["total"] * 100).round(1)
    p["is_ru"] = p["source_domain"].str.endswith(".ru")

    by_pair = {}
    for pid, grp in p.groupby("pair_id"):
        if pid not in enabled_ids:
            continue
        top = grp.nlargest(50, "total")
        by_pair[str(pid)] = [{"domain": r["source_domain"], "russian_pct": float(r["russian_pct"]),
                              "total": int(r["total"]), "is_ru": bool(r["is_ru"])} for _, r in top.iterrows()]

    # Global holdouts
    g2 = recent.groupby(["source_domain", "variant"])["count"].sum().reset_index()
    p2 = g2.pivot_table(index="source_domain", columns="variant", values="count", fill_value=0).reset_index()
    p2["total"] = p2.get("russian", 0) + p2.get("ukrainian", 0)
    p2 = p2[p2["total"] >= 50]
    p2["rus"] = p2.get("russian", 0)
    p2 = p2[p2["rus"] > p2.get("ukrainian", 0)]
    p2["russian_pct"] = (p2["rus"] / p2["total"] * 100).round(1)
    p2["is_ru"] = p2["source_domain"].str.endswith(".ru")
    top_global = p2.nlargest(100, "total")
    global_list = [{"domain": r["source_domain"], "russian_pct": float(r["russian_pct"]),
                    "total": int(r["total"]), "is_ru": bool(r["is_ru"])} for _, r in top_global.iterrows()]

    return by_pair, global_list


def export_pair_events(enabled_ids: set[int]) -> dict:
    log.info("Exporting pair events...")
    cfg = load_pairs()
    result = {}
    for p in cfg["pairs"]:
        if p["id"] not in enabled_ids:
            continue
        events = p.get("events", [])
        if events:
            result[str(p["id"])] = [
                {"date": e["date"], "label": e["label"], "color": e.get("color", "#0057B8")}
                for e in events
            ]
    return result


def export_domain_origins(enabled_ids: set[int]) -> dict:
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
    g = recent.groupby(["pair_id", "origin", "variant"])["count"].sum().reset_index()
    p = g.pivot_table(index=["pair_id", "origin"], columns="variant", values="count", fill_value=0).reset_index()
    p["total"] = p.get("russian", 0) + p.get("ukrainian", 0)

    result = {}
    for _, r in p.iterrows():
        pid = int(r["pair_id"])
        if pid not in enabled_ids:
            continue
        total = int(r["total"])
        result.setdefault(str(pid), {})[r["origin"]] = {
            "ukr": int(r.get("ukrainian", 0)), "rus": int(r.get("russian", 0)),
            "total": total, "adoption": round(int(r.get("ukrainian", 0)) / total * 100, 1) if total > 0 else 0,
        }
    log.info(f"  Domain origins: {len(result)} pairs")
    return result


def export_analysis() -> dict:
    """Export changepoint analysis from local JSON if available."""
    log.info("Exporting analysis...")
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

    enabled_ids = get_enabled_pair_ids()
    analyzable_ids = get_analyzable_pair_ids()
    control_ids = get_control_pair_ids()
    log.info(f"Pairs: {len(enabled_ids)} enabled, {len(analyzable_ids)} analyzable, {len(control_ids)} control")

    manifest = export_manifest(enabled_ids, analyzable_ids, control_ids)
    timeseries = export_timeseries(enabled_ids)
    trends_countries = export_trends_countries(enabled_ids)
    holdouts_by_pair, holdouts_global = export_holdouts(enabled_ids)
    pair_events = export_pair_events(enabled_ids)
    analysis = export_analysis()
    domain_origins = export_domain_origins(enabled_ids)

    write_json(SITE_DATA_DIR / "manifest.json", manifest)
    write_json(SITE_DATA_DIR / "timeseries.json", timeseries)
    write_json(SITE_DATA_DIR / "trends_countries.json", trends_countries)
    write_json(SITE_DATA_DIR / "domain_origins.json", domain_origins)
    write_json(SITE_DATA_DIR / "holdouts_by_pair.json", holdouts_by_pair)
    write_json(SITE_DATA_DIR / "holdouts.json", holdouts_global)
    write_json(SITE_DATA_DIR / "pair_events.json", pair_events)
    write_json(SITE_DATA_DIR / "analysis.json", analysis)

    log.info("=" * 60)
    log.info("Export complete!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
