"""Export BigQuery data to site JSON files.

Single source of truth: ALL site data is generated from this script.
The site reads only from site/src/data/*.json — nothing is computed at build time.

Usage:
    python -m pipeline.export_site_data
"""

import json
import logging
from pathlib import Path

from google.cloud import bigquery

from pipeline.config import load_pairs

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PROJECT = "kyivnotkiev-research"
DATASET = "kyivnotkiev"
SITE_DATA_DIR = Path(__file__).resolve().parent.parent / "site" / "src" / "data"

client = bigquery.Client(project=PROJECT)


def query(sql: str) -> list[dict]:
    rows = client.query(sql).result()
    return [dict(row) for row in rows]


def get_enabled_pair_ids() -> set[int]:
    cfg = load_pairs()
    return {p["id"] for p in cfg["pairs"] if p.get("enabled", True)}


def get_control_pair_ids() -> set[int]:
    cfg = load_pairs()
    return {p["id"] for p in cfg["pairs"] if p.get("is_control", False)}


def get_analyzable_pair_ids() -> set[int]:
    """Enabled and non-control."""
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
    return [{"date": series[i]["date"], "adoption": smoothed[i]} for i in range(len(series))]


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


# ── Exports ───────────────────────────────────────────────────────────────────

def export_timeseries(enabled_ids: set[int]) -> dict:
    log.info("Exporting timeseries...")
    result = {"events": [
        {"date": "2014-02", "label": "Euromaidan", "color": "#d97706"},
        {"date": "2022-02", "label": "Full-scale war", "color": "#dc2626"},
    ]}

    # Trends (monthly, smoothed)
    log.info("  Trends...")
    rows = query(f"""
        SELECT pair_id, FORMAT_DATE('%Y-%m', date) as month,
            SUM(IF(variant='ukrainian', interest, 0)) as ukr,
            SUM(IF(variant='russian', interest, 0)) as rus
        FROM `{DATASET}.raw_trends`
        WHERE (geo = '' OR geo IS NULL)
        GROUP BY pair_id, month ORDER BY pair_id, month
    """)
    raw = {}
    for r in rows:
        pid = r["pair_id"]
        if pid not in enabled_ids:
            continue
        raw.setdefault(pid, [])
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else None
        raw[pid].append({"date": r["month"], "adoption": adoption})
    for pid, series in raw.items():
        result.setdefault(str(pid), {})
        result[str(pid)]["trends"] = smooth_series(series, window=3)

    # GDELT (monthly)
    log.info("  GDELT...")
    rows = query(f"""
        SELECT pair_id, FORMAT_DATE('%Y-%m', date) as month,
            COUNTIF(variant='ukrainian') as ukr, COUNTIF(variant='russian') as rus
        FROM `{DATASET}.raw_gdelt`
        GROUP BY pair_id, month ORDER BY pair_id, month
    """)
    for r in rows:
        if r["pair_id"] not in enabled_ids:
            continue
        pid = str(r["pair_id"])
        result.setdefault(pid, {}).setdefault("gdelt", [])
        total = r["ukr"] + r["rus"]
        if total > 0:
            result[pid]["gdelt"].append({"date": r["month"], "adoption": round(r["ukr"] / total * 100, 1)})

    # Wikipedia (monthly)
    log.info("  Wikipedia...")
    rows = query(f"""
        SELECT pair_id, FORMAT_DATE('%Y-%m', date) as month,
            SUM(IF(variant='ukrainian', pageviews, 0)) as ukr,
            SUM(IF(variant='russian', pageviews, 0)) as rus
        FROM `{DATASET}.raw_wikipedia`
        GROUP BY pair_id, month ORDER BY pair_id, month
    """)
    for r in rows:
        if r["pair_id"] not in enabled_ids:
            continue
        pid = str(r["pair_id"])
        result.setdefault(pid, {}).setdefault("wikipedia", [])
        total = r["ukr"] + r["rus"]
        if total > 0:
            result[pid]["wikipedia"].append({"date": r["month"], "adoption": round(r["ukr"] / total * 100, 1)})

    # Reddit (annual for consistency across pairs)
    log.info("  Reddit...")
    rows = query(f"""
        SELECT pair_id,
            CAST(EXTRACT(YEAR FROM DATE(created_utc)) AS STRING) as yr,
            COUNTIF(variant='ukrainian') as ukr, COUNTIF(variant='russian') as rus
        FROM `{DATASET}.raw_reddit`
        GROUP BY pair_id, yr HAVING (ukr + rus) >= 2
        ORDER BY pair_id, yr
    """)
    for r in rows:
        if r["pair_id"] not in enabled_ids:
            continue
        pid = str(r["pair_id"])
        result.setdefault(pid, {}).setdefault("reddit", [])
        total = r["ukr"] + r["rus"]
        if total > 0:
            result[pid]["reddit"].append({"date": f"{r['yr']}-01", "adoption": round(r["ukr"] / total * 100, 1)})

    # YouTube: merge BQ data with local yt-dlp CSVs (local has all 54 pairs, 2010-2026)
    log.info("  YouTube (BQ + local CSVs)...")
    import csv
    from collections import defaultdict

    # First load BQ data (annual)
    rows = query(f"""
        SELECT pair_id,
            CAST(EXTRACT(YEAR FROM DATE(published_at)) AS STRING) as yr,
            COUNTIF(variant='ukrainian') as ukr, COUNTIF(variant='russian') as rus
        FROM `{DATASET}.raw_youtube`
        GROUP BY pair_id, yr HAVING (ukr + rus) >= 2
        ORDER BY pair_id, yr
    """)
    yt_data = defaultdict(dict)  # {pid: {year: (ukr, rus)}}
    for r in rows:
        if r["pair_id"] not in enabled_ids:
            continue
        yt_data[r["pair_id"]][r["yr"]] = (r["ukr"], r["rus"])

    # Merge local yt-dlp CSVs (title_matches per year)
    yt_local_dir = Path(__file__).resolve().parent.parent / "data" / "raw" / "youtube"
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

    # Build timeseries
    for pid in sorted(yt_data.keys()):
        spid = str(pid)
        result.setdefault(spid, {}).setdefault("youtube", [])
        for yr in sorted(yt_data[pid].keys()):
            ukr, rus = yt_data[pid][yr]
            total = ukr + rus
            if total > 0:
                result[spid]["youtube"].append({"date": f"{yr}-01", "adoption": round(ukr / total * 100, 1)})

    # Ngrams (yearly)
    log.info("  Ngrams...")
    rows = query(f"""
        SELECT pair_id, year,
            SUM(IF(variant='ukrainian', frequency, 0)) as ukr,
            SUM(IF(variant='russian', frequency, 0)) as rus
        FROM `{DATASET}.raw_ngrams` WHERE year >= 1900
        GROUP BY pair_id, year ORDER BY pair_id, year
    """)
    for r in rows:
        if r["pair_id"] not in enabled_ids:
            continue
        pid = str(r["pair_id"])
        result.setdefault(pid, {}).setdefault("ngrams", [])
        total = r["ukr"] + r["rus"]
        if total > 0:
            result[pid]["ngrams"].append({"date": f"{r['year']}-01", "adoption": round(r["ukr"] / total * 100, 1)})

    # Academic Papers (OpenAlex — replaces Common Crawl)
    log.info("  Academic Papers (OpenAlex local data)...")
    openalex_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "openalex" / "openalex_all_pairs.json"
    if openalex_path.exists():
        with open(openalex_path) as f:
            openalex_data = json.load(f)
        for pair_data in openalex_data:
            pid = pair_data["pair_id"]
            if pid not in enabled_ids:
                continue
            spid = str(pid)
            result.setdefault(spid, {}).setdefault("openalex", [])
            for yr in pair_data["yearly"]:
                total = yr["total"]
                if total > 0:
                    adoption = round(yr["ukrainian_count"] / total * 100, 1)
                    result[spid]["openalex"].append({"date": f"{yr['year']}-01", "adoption": adoption})
        log.info(f"    Loaded {len(openalex_data)} pairs from OpenAlex")
    else:
        log.warning("    No OpenAlex data found — run: python -m pipeline.ingestion.openalex")

    pair_count = len([k for k in result if k != "events"])
    log.info(f"  Timeseries: {pair_count} pairs")
    return result


def export_manifest(enabled_ids: set[int], analyzable_ids: set[int], control_ids: set[int]) -> dict:
    """Single source of truth for all site stats, pair metadata, and category stats.

    The site reads ONLY this file for stats, counts, and pair info.
    Nothing is computed at Astro build time.
    """
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

    # ── OpenAlex stats from local data ──
    openalex_path = Path(__file__).resolve().parent.parent / "data" / "raw" / "openalex" / "openalex_all_pairs.json"
    openalex_total_papers = 0
    openalex_total_pairs = 0
    if openalex_path.exists():
        with open(openalex_path) as f:
            oa_data = json.load(f)
        openalex_total_pairs = len(oa_data)
        openalex_total_papers = sum(
            sum(yr["total"] for yr in p["yearly"]) for p in oa_data
        )

    # ── Per-source stats from BQ ──
    log.info("  Querying per-source stats...")
    stats_rows = query(f"""
        SELECT 'trends' as source, COUNT(*) as records, COUNT(DISTINCT pair_id) as pairs,
            'datapoints' as unit FROM `{DATASET}.raw_trends`
        UNION ALL SELECT 'gdelt', COUNT(*), COUNT(DISTINCT pair_id), 'articles' FROM `{DATASET}.raw_gdelt`
        UNION ALL SELECT 'wikipedia', CAST(SUM(pageviews) AS INT64), COUNT(DISTINCT pair_id), 'pageviews' FROM `{DATASET}.raw_wikipedia`
        UNION ALL SELECT 'reddit', COUNT(*), COUNT(DISTINCT pair_id), 'posts' FROM `{DATASET}.raw_reddit`
        UNION ALL SELECT 'youtube', COUNT(*), COUNT(DISTINCT pair_id), 'videos' FROM `{DATASET}.raw_youtube`
        UNION ALL SELECT 'ngrams', COUNT(*), COUNT(DISTINCT pair_id), 'records' FROM `{DATASET}.raw_ngrams`
        -- OpenAlex stats computed from local data, not BQ
    """)
    source_stats = {r["source"]: {"records": r["records"], "pairs": r["pairs"], "unit": r["unit"]}
                    for r in stats_rows}

    # Extra stats
    extra = query(f"""
        SELECT 'gdelt_domains' as k, CAST(COUNT(DISTINCT source_domain) AS STRING) as v FROM `{DATASET}.raw_gdelt`
        UNION ALL SELECT 'reddit_subreddits', CAST(COUNT(DISTINCT subreddit) AS STRING) FROM `{DATASET}.raw_reddit`
        UNION ALL SELECT 'youtube_channels', CAST(COUNT(DISTINCT channel_id) AS STRING) FROM `{DATASET}.raw_youtube`
        UNION ALL SELECT 'trends_countries', CAST(COUNT(DISTINCT geo) AS STRING) FROM `{DATASET}.raw_trends` WHERE geo != '' AND geo IS NOT NULL
    """)
    extra_map = {r["k"]: r["v"] for r in extra}

    # ── Per-pair adoption from all sources (last 12 months) ──
    log.info("  Querying per-pair adoption...")
    recent_rows = query(f"""
        WITH recent AS (
            SELECT pair_id, variant, COUNT(*) as cnt FROM `{DATASET}.raw_gdelt`
                WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) GROUP BY pair_id, variant
            UNION ALL
            SELECT pair_id, variant, SUM(interest) FROM `{DATASET}.raw_trends`
                WHERE (geo='' OR geo IS NULL) AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) GROUP BY pair_id, variant
            UNION ALL
            SELECT pair_id, variant, SUM(pageviews) FROM `{DATASET}.raw_wikipedia`
                WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) GROUP BY pair_id, variant
            UNION ALL
            SELECT pair_id, variant, COUNT(*) FROM `{DATASET}.raw_reddit`
                WHERE DATE(created_utc) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) GROUP BY pair_id, variant
            UNION ALL
            SELECT pair_id, variant, COUNT(*) FROM `{DATASET}.raw_youtube`
                WHERE DATE(published_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) GROUP BY pair_id, variant
        )
        SELECT pair_id,
            SUM(IF(variant='ukrainian', cnt, 0)) as ukr,
            SUM(IF(variant='russian', cnt, 0)) as rus
        FROM recent GROUP BY pair_id
    """)
    recent_map = {r["pair_id"]: r for r in recent_rows}

    # Total mentions per pair
    total_rows = query(f"""
        WITH all_m AS (
            SELECT pair_id, COUNT(*) as cnt FROM `{DATASET}.raw_gdelt` GROUP BY pair_id
            UNION ALL SELECT pair_id, SUM(interest) FROM `{DATASET}.raw_trends` WHERE (geo='' OR geo IS NULL) GROUP BY pair_id
            UNION ALL SELECT pair_id, SUM(pageviews) FROM `{DATASET}.raw_wikipedia` GROUP BY pair_id
            UNION ALL SELECT pair_id, COUNT(*) FROM `{DATASET}.raw_reddit` GROUP BY pair_id
            UNION ALL SELECT pair_id, COUNT(*) FROM `{DATASET}.raw_youtube` GROUP BY pair_id
            UNION ALL SELECT pair_id, COUNT(*) FROM `{DATASET}.raw_common_crawl` GROUP BY pair_id
        )
        SELECT pair_id, SUM(cnt) as total FROM all_m GROUP BY pair_id
    """)
    total_map = {r["pair_id"]: r["total"] for r in total_rows}

    # Build pairs
    pairs_out = []
    for p in pairs_cfg["pairs"]:
        if p["id"] not in enabled_ids:
            continue
        pid = p["id"]
        recent = recent_map.get(pid, {})
        if pid in control_ids:
            adoption_pct = 0.0
        else:
            ukr = recent.get("ukr", 0)
            rus = recent.get("rus", 0)
            t = ukr + rus
            adoption_pct = round(ukr / t * 100, 1) if t > 0 else 0.0

        pairs_out.append({
            "id": pid,
            "category": p["category"],
            "russian": p["russian"],
            "ukrainian": p["ukrainian"],
            "adoption": adoption_pct,
            "total": total_map.get(pid, 0),
            "is_control": pid in control_ids,
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
            **c,
            "count": len(vals),
            "avg_adoption": round(sum(vals) / len(vals), 1) if vals else 0,
        })

    # Total records = sum across all sources
    total_records = sum(s["records"] for s in source_stats.values()) + openalex_total_papers

    manifest = {
        # ── Counts ──
        "total_pairs": len(enabled_ids),
        "analyzable_pairs": len(analyzable_ids),
        "control_pairs": len(control_ids),
        "total_records": total_records,
        "data_processed_pb": 1.2,
        "time_span": "2010-2026",
        "num_sources": 7,
        "num_countries": int(extra_map.get("trends_countries", "0")),

        # ── Per-source stats ──
        "sources": {
            "trends": {
                "records": source_stats["trends"]["records"],
                "pairs": source_stats["trends"]["pairs"],
                "label": "Search Trends",
                "unit": "datapoints",
                "extra": f"{extra_map.get('trends_countries', '55')} countries",
                "color": "#4285F4",
            },
            "gdelt": {
                "records": source_stats["gdelt"]["records"],
                "pairs": source_stats["gdelt"]["pairs"],
                "label": "News Articles",
                "unit": "articles",
                "extra": f"{extra_map.get('gdelt_domains', '0')} domains",
                "color": "#1e3a5f",
            },
            "wikipedia": {
                "records": source_stats["wikipedia"]["records"],
                "pairs": source_stats["wikipedia"]["pairs"],
                "label": "Page Views",
                "unit": "pageviews",
                "extra": "monthly",
                "color": "#636466",
            },
            "reddit": {
                "records": source_stats["reddit"]["records"],
                "pairs": source_stats["reddit"]["pairs"],
                "label": "Posts",
                "unit": "posts",
                "extra": f"{extra_map.get('reddit_subreddits', '0')} subreddits",
                "color": "#FF4500",
            },
            "youtube": {
                "records": source_stats["youtube"]["records"],
                "pairs": source_stats["youtube"]["pairs"],
                "label": "Videos",
                "unit": "videos",
                "extra": f"{extra_map.get('youtube_channels', '0')} channels",
                "color": "#FF0000",
            },
            "ngrams": {
                "records": source_stats["ngrams"]["records"],
                "pairs": source_stats["ngrams"]["pairs"],
                "label": "Book Records",
                "unit": "records",
                "extra": "8M+ volumes",
                "color": "#7c3aed",
            },
            "openalex": {
                "records": openalex_total_papers,
                "pairs": openalex_total_pairs,
                "label": "Academic Papers",
                "unit": "papers",
                "extra": "250M+ works indexed",
                "color": "#06b6d4",
            },
        },

        # ── Pairs & categories ──
        "categories": categories,
        "category_stats": sorted(category_list, key=lambda x: -x["avg_adoption"]),
        "pairs": sorted(pairs_out, key=lambda x: x["id"]),
    }

    log.info(f"  Manifest: {manifest['analyzable_pairs']} analyzable pairs, "
             f"{manifest['total_records']:,} total records")
    return manifest


def export_trends_countries(enabled_ids: set[int]) -> dict:
    log.info("Exporting trends countries...")
    rows = query(f"""
        SELECT pair_id, geo,
            SUM(IF(variant='ukrainian', interest, 0)) as ukr,
            SUM(IF(variant='russian', interest, 0)) as rus
        FROM `{DATASET}.raw_trends`
        WHERE geo != '' AND geo IS NOT NULL
        GROUP BY pair_id, geo
        HAVING (ukr + rus) >= 100
            AND SUM(IF(variant='ukrainian', interest, 0)) > 0
            AND SUM(IF(variant='russian', interest, 0)) > 0
    """)
    result = {}
    for r in rows:
        if r["pair_id"] not in enabled_ids:
            continue
        pid = str(r["pair_id"])
        numeric = GEO_TO_NUMERIC.get(r["geo"])
        if not numeric:
            continue
        total = r["ukr"] + r["rus"]
        result.setdefault(pid, {})
        result[pid][numeric] = {
            "name": GEO_NAMES.get(numeric, r["geo"]),
            "adoption": round(r["ukr"] / total * 100, 1),
        }
    log.info(f"  Trends countries: {len(result)} pairs")
    return result


def export_holdouts(enabled_ids: set[int]) -> tuple[dict, list]:
    log.info("Exporting holdouts...")
    rows = query(f"""
        WITH d AS (
            SELECT pair_id, source_domain as domain,
                COUNTIF(variant='russian') as rus, COUNTIF(variant='ukrainian') as ukr, COUNT(*) as total
            FROM `{DATASET}.raw_gdelt` WHERE date >= '2024-01-01'
            GROUP BY pair_id, domain HAVING total >= 20
        )
        SELECT pair_id, domain, rus, total,
            ROUND(rus / total * 100, 1) as russian_pct, ENDS_WITH(domain, '.ru') as is_ru
        FROM d WHERE rus > ukr ORDER BY pair_id, total DESC
    """)
    by_pair = {}
    for r in rows:
        if r["pair_id"] not in enabled_ids:
            continue
        pid = str(r["pair_id"])
        by_pair.setdefault(pid, [])
        if len(by_pair[pid]) < 50:
            by_pair[pid].append({"domain": r["domain"], "russian_pct": float(r["russian_pct"]),
                                 "total": r["total"], "is_ru": r["is_ru"]})

    # Global
    rows2 = query(f"""
        WITH d AS (
            SELECT source_domain as domain,
                COUNTIF(variant='russian') as rus, COUNTIF(variant='ukrainian') as ukr, COUNT(*) as total
            FROM `{DATASET}.raw_gdelt` WHERE date >= '2024-01-01'
            GROUP BY domain HAVING total >= 50
        )
        SELECT domain, rus, total, ROUND(rus / total * 100, 1) as russian_pct,
            ENDS_WITH(domain, '.ru') as is_ru
        FROM d WHERE rus > ukr ORDER BY total DESC LIMIT 100
    """)
    global_list = [{"domain": r["domain"], "russian_pct": float(r["russian_pct"]),
                    "total": r["total"], "is_ru": r["is_ru"]} for r in rows2]

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


def export_analysis() -> dict:
    log.info("Exporting analysis...")
    rows = query(f"""
        SELECT pair_id, source, changepoint_date, ci_lower, ci_upper, effect_size
        FROM `{DATASET}.analysis_changepoints` ORDER BY pair_id, source
    """)
    cfg = load_pairs()
    pl = {p["id"]: p for p in cfg["pairs"]}
    return {
        "changepoint_detection": [
            {"pair_id": r["pair_id"],
             "pair": f"{pl.get(r['pair_id'], {}).get('russian', '?')} -> {pl.get(r['pair_id'], {}).get('ukrainian', '?')}",
             "category": pl.get(r["pair_id"], {}).get("category", ""),
             "changepoint_date": str(r["changepoint_date"]),
             "effect_size": round(float(r["effect_size"]), 1) if r["effect_size"] else 0}
            for r in rows
        ],
        "metadata": {"generated": "auto", "source": "bigquery"},
    }


def write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"), default=str)
    log.info(f"  Wrote {path.name} ({path.stat().st_size / 1024:.0f} KB)")


def main():
    log.info("=" * 60)
    log.info("Exporting BigQuery data to site JSON")
    log.info("=" * 60)

    enabled_ids = get_enabled_pair_ids()
    analyzable_ids = get_analyzable_pair_ids()
    control_ids = get_control_pair_ids()
    log.info(f"Pairs: {len(enabled_ids)} enabled, {len(analyzable_ids)} analyzable, {len(control_ids)} control")

    # The manifest is the single source of truth
    manifest = export_manifest(enabled_ids, analyzable_ids, control_ids)
    timeseries = export_timeseries(enabled_ids)
    trends_countries = export_trends_countries(enabled_ids)
    holdouts_by_pair, holdouts_global = export_holdouts(enabled_ids)
    pair_events = export_pair_events(enabled_ids)
    analysis = export_analysis()

    write_json(SITE_DATA_DIR / "manifest.json", manifest)
    write_json(SITE_DATA_DIR / "timeseries.json", timeseries)
    write_json(SITE_DATA_DIR / "trends_countries.json", trends_countries)
    write_json(SITE_DATA_DIR / "holdouts_by_pair.json", holdouts_by_pair)
    write_json(SITE_DATA_DIR / "holdouts.json", holdouts_global)
    write_json(SITE_DATA_DIR / "pair_events.json", pair_events)
    write_json(SITE_DATA_DIR / "analysis.json", analysis)

    log.info("=" * 60)
    log.info("Export complete!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
