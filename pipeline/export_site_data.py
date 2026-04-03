"""Export BigQuery data to site JSON files.

Pulls fresh data from all BQ tables and generates the JSON files
that power the Astro site at site/src/data/.

Usage:
    python -m pipeline.export_site_data
"""

import json
import logging
from pathlib import Path

from google.cloud import bigquery

from pipeline.config import get_enabled_pairs, get_categories, load_pairs

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

PROJECT = "kyivnotkiev-research"
DATASET = "kyivnotkiev"
SITE_DATA_DIR = Path(__file__).resolve().parent.parent / "site" / "src" / "data"

client = bigquery.Client(project=PROJECT)


def query(sql: str) -> list[dict]:
    """Run a BQ query and return list of dicts."""
    rows = client.query(sql).result()
    return [dict(row) for row in rows]


# ── Country code mapping (ISO 2-letter to ISO 3166-1 numeric) ────────────────

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
    "688": "Serbia", "700": "Singapore", "702": "Singapore", "703": "Slovakia",
    "705": "Slovenia", "710": "South Africa", "716": "Zimbabwe", "724": "Spain",
    "752": "Sweden", "756": "Switzerland", "764": "Thailand", "788": "Tunisia",
    "792": "Turkey", "800": "Uganda", "804": "Ukraine", "818": "Egypt",
    "826": "United Kingdom", "834": "Tanzania", "840": "United States",
    "858": "Uruguay", "860": "Uzbekistan", "862": "Venezuela", "704": "Vietnam",
    "784": "UAE", "807": "North Macedonia", "275": "Palestine",
    "887": "Yemen", "894": "Zambia",
}


# ── Export functions ──────────────────────────────────────────────────────────

def export_timeseries() -> dict:
    """Export monthly timeseries per pair per source."""
    log.info("Exporting timeseries...")

    result = {}

    # Global events
    result["events"] = [
        {"date": "2014-02", "label": "Euromaidan", "color": "#d97706"},
        {"date": "2022-02", "label": "Full-scale war", "color": "#dc2626"},
    ]

    # ── Trends (monthly, global only) ──
    log.info("  Trends...")
    rows = query(f"""
        SELECT pair_id, FORMAT_DATE('%Y-%m', date) as month,
            SUM(IF(variant='ukrainian', interest, 0)) as ukr,
            SUM(IF(variant='russian', interest, 0)) as rus
        FROM `{DATASET}.raw_trends`
        WHERE (geo = '' OR geo IS NULL)
        GROUP BY pair_id, month
        ORDER BY pair_id, month
    """)
    for r in rows:
        pid = str(r["pair_id"])
        result.setdefault(pid, {})
        result[pid].setdefault("trends", [])
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else None
        result[pid]["trends"].append({"date": r["month"], "adoption": adoption})

    # ── GDELT (monthly) ──
    log.info("  GDELT...")
    rows = query(f"""
        SELECT pair_id, FORMAT_DATE('%Y-%m', date) as month,
            COUNTIF(variant='ukrainian') as ukr,
            COUNTIF(variant='russian') as rus
        FROM `{DATASET}.raw_gdelt`
        GROUP BY pair_id, month
        ORDER BY pair_id, month
    """)
    for r in rows:
        pid = str(r["pair_id"])
        result.setdefault(pid, {})
        result[pid].setdefault("gdelt", [])
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else None
        result[pid]["gdelt"].append({"date": r["month"], "adoption": adoption})

    # ── Wikipedia (monthly) ──
    log.info("  Wikipedia...")
    rows = query(f"""
        SELECT pair_id, FORMAT_DATE('%Y-%m', date) as month,
            SUM(IF(variant='ukrainian', pageviews, 0)) as ukr,
            SUM(IF(variant='russian', pageviews, 0)) as rus
        FROM `{DATASET}.raw_wikipedia`
        GROUP BY pair_id, month
        ORDER BY pair_id, month
    """)
    for r in rows:
        pid = str(r["pair_id"])
        result.setdefault(pid, {})
        result[pid].setdefault("wikipedia", [])
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else None
        result[pid]["wikipedia"].append({"date": r["month"], "adoption": adoption})

    # ── Reddit (quarterly) ──
    log.info("  Reddit...")
    rows = query(f"""
        SELECT pair_id,
            FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(created_utc), QUARTER)) as quarter,
            COUNTIF(variant='ukrainian') as ukr,
            COUNTIF(variant='russian') as rus
        FROM `{DATASET}.raw_reddit`
        GROUP BY pair_id, quarter
        HAVING (ukr + rus) > 0
        ORDER BY pair_id, quarter
    """)
    for r in rows:
        pid = str(r["pair_id"])
        result.setdefault(pid, {})
        result[pid].setdefault("reddit", [])
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else None
        result[pid]["reddit"].append({"date": r["quarter"], "adoption": adoption})

    # ── YouTube (quarterly) ──
    log.info("  YouTube...")
    rows = query(f"""
        SELECT pair_id,
            FORMAT_DATE('%Y-%m', DATE_TRUNC(DATE(published_at), QUARTER)) as quarter,
            COUNTIF(variant='ukrainian') as ukr,
            COUNTIF(variant='russian') as rus
        FROM `{DATASET}.raw_youtube`
        GROUP BY pair_id, quarter
        HAVING (ukr + rus) > 0
        ORDER BY pair_id, quarter
    """)
    for r in rows:
        pid = str(r["pair_id"])
        result.setdefault(pid, {})
        result[pid].setdefault("youtube", [])
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else None
        result[pid]["youtube"].append({"date": r["quarter"], "adoption": adoption})

    # ── Ngrams (by decade pre-1950, by year after) ──
    log.info("  Ngrams...")
    rows = query(f"""
        SELECT pair_id, year,
            SUM(IF(variant='ukrainian', frequency, 0)) as ukr,
            SUM(IF(variant='russian', frequency, 0)) as rus
        FROM `{DATASET}.raw_ngrams`
        WHERE year >= 1900
        GROUP BY pair_id, year
        ORDER BY pair_id, year
    """)
    for r in rows:
        pid = str(r["pair_id"])
        result.setdefault(pid, {})
        result[pid].setdefault("ngrams", [])
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else None
        date_str = f"{r['year']}-01"
        result[pid]["ngrams"].append({"date": date_str, "adoption": adoption})

    # ── Common Crawl (by crawl) ──
    log.info("  Common Crawl...")
    rows = query(f"""
        SELECT pair_id, FORMAT_DATE('%Y-%m', crawl_date) as month,
            COUNTIF(variant='ukrainian') as ukr,
            COUNTIF(variant='russian') as rus
        FROM `{DATASET}.raw_common_crawl`
        GROUP BY pair_id, month
        HAVING (ukr + rus) > 0
        ORDER BY pair_id, month
    """)
    for r in rows:
        pid = str(r["pair_id"])
        result.setdefault(pid, {})
        result[pid].setdefault("common_crawl", [])
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else None
        result[pid]["common_crawl"].append({"date": r["month"], "adoption": adoption})

    log.info(f"  Timeseries: {len(result) - 1} pairs")  # -1 for events key
    return result


def export_pairs() -> dict:
    """Export pair metadata with adoption stats."""
    log.info("Exporting pairs...")

    pairs_cfg = load_pairs()
    categories_raw = pairs_cfg.get("categories", {})
    pairs_raw = [p for p in pairs_cfg["pairs"] if p.get("enabled", True)]

    # Category colors
    cat_colors = {
        "geographical": "#0057B8",
        "food": "#e6b800",
        "landmarks": "#8B4513",
        "country": "#228B22",
        "institutional": "#4B0082",
        "sports": "#DC143C",
        "historical": "#708090",
        "people": "#FF6347",
    }

    categories = []
    for cat_id, cat_info in categories_raw.items():
        categories.append({
            "id": cat_id,
            "name": cat_info["name"],
            "color": cat_colors.get(cat_id, "#888888"),
        })

    # Get adoption stats from BQ
    adoption_rows = query(f"""
        WITH all_mentions AS (
            SELECT pair_id, variant, COUNT(*) as cnt FROM `{DATASET}.raw_gdelt` GROUP BY pair_id, variant
            UNION ALL
            SELECT pair_id, variant, SUM(interest) FROM `{DATASET}.raw_trends` WHERE (geo='' OR geo IS NULL) GROUP BY pair_id, variant
            UNION ALL
            SELECT pair_id, variant, SUM(pageviews) FROM `{DATASET}.raw_wikipedia` GROUP BY pair_id, variant
            UNION ALL
            SELECT pair_id, variant, COUNT(*) FROM `{DATASET}.raw_reddit` GROUP BY pair_id, variant
            UNION ALL
            SELECT pair_id, variant, COUNT(*) FROM `{DATASET}.raw_youtube` GROUP BY pair_id, variant
            UNION ALL
            SELECT pair_id, variant, COUNT(*) FROM `{DATASET}.raw_common_crawl` GROUP BY pair_id, variant
        )
        SELECT pair_id,
            SUM(IF(variant='ukrainian', cnt, 0)) as ukr_total,
            SUM(IF(variant='russian', cnt, 0)) as rus_total,
            SUM(cnt) as grand_total
        FROM all_mentions
        GROUP BY pair_id
    """)
    adoption_map = {r["pair_id"]: r for r in adoption_rows}

    # Recent adoption (last 12 months of trends)
    recent_rows = query(f"""
        SELECT pair_id,
            SUM(IF(variant='ukrainian', interest, 0)) as ukr,
            SUM(IF(variant='russian', interest, 0)) as rus
        FROM `{DATASET}.raw_trends`
        WHERE (geo='' OR geo IS NULL) AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
        GROUP BY pair_id
    """)
    recent_map = {r["pair_id"]: r for r in recent_rows}

    pairs_out = []
    for p in pairs_raw:
        pid = p["id"]
        stats = adoption_map.get(pid, {})
        recent = recent_map.get(pid, {})
        total = stats.get("grand_total", 0)
        ukr_recent = recent.get("ukr", 0)
        rus_recent = recent.get("rus", 0)
        recent_total = ukr_recent + rus_recent
        adoption_pct = round(ukr_recent / recent_total * 100, 1) if recent_total > 0 else 0.0

        pairs_out.append({
            "id": pid,
            "category": p["category"],
            "russian": p["russian"],
            "ukrainian": p["ukrainian"],
            "adoption": adoption_pct,
            "total": total,
        })

    log.info(f"  Pairs: {len(pairs_out)}")
    return {"categories": categories, "pairs": pairs_out}


def export_trends_countries() -> dict:
    """Export Google Trends adoption by country per pair."""
    log.info("Exporting trends countries...")

    rows = query(f"""
        SELECT pair_id, geo,
            SUM(IF(variant='ukrainian', interest, 0)) as ukr,
            SUM(IF(variant='russian', interest, 0)) as rus
        FROM `{DATASET}.raw_trends`
        WHERE geo != '' AND geo IS NOT NULL
          AND date >= '2020-01-01'
        GROUP BY pair_id, geo
        HAVING (ukr + rus) > 0
    """)

    result = {}
    for r in rows:
        pid = str(r["pair_id"])
        geo = r["geo"]
        numeric = GEO_TO_NUMERIC.get(geo)
        if not numeric:
            continue
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else 0.0
        name = GEO_NAMES.get(numeric, geo)
        result.setdefault(pid, {})
        result[pid][numeric] = {"name": name, "adoption": adoption}

    log.info(f"  Trends countries: {len(result)} pairs")
    return result


def export_countries_by_pair() -> dict:
    """Export GDELT adoption by country per pair."""
    log.info("Exporting GDELT countries...")

    rows = query(f"""
        SELECT pair_id, source_country as geo,
            COUNTIF(variant='ukrainian') as ukr,
            COUNTIF(variant='russian') as rus
        FROM `{DATASET}.raw_gdelt`
        WHERE source_country IS NOT NULL AND source_country != ''
          AND date >= '2024-01-01'
        GROUP BY pair_id, geo
        HAVING (ukr + rus) >= 10
    """)

    result = {}
    for r in rows:
        pid = str(r["pair_id"])
        geo = r["geo"]
        numeric = GEO_TO_NUMERIC.get(geo)
        if not numeric:
            continue
        total = r["ukr"] + r["rus"]
        adoption = round(r["ukr"] / total * 100, 1) if total > 0 else 0.0
        name = GEO_NAMES.get(numeric, geo)
        result.setdefault(pid, {})
        result[pid][numeric] = {"name": name, "adoption": adoption}

    log.info(f"  GDELT countries: {len(result)} pairs")
    return result


def export_holdouts_by_pair() -> dict:
    """Export holdout domains per pair from GDELT."""
    log.info("Exporting holdouts...")

    rows = query(f"""
        WITH domain_stats AS (
            SELECT pair_id, source_domain as domain,
                COUNTIF(variant='russian') as rus,
                COUNTIF(variant='ukrainian') as ukr,
                COUNT(*) as total
            FROM `{DATASET}.raw_gdelt`
            WHERE date >= '2024-01-01'
            GROUP BY pair_id, domain
            HAVING total >= 20
        )
        SELECT pair_id, domain, rus, total,
            ROUND(rus / total * 100, 1) as russian_pct,
            ENDS_WITH(domain, '.ru') as is_ru
        FROM domain_stats
        WHERE rus > ukr
        ORDER BY pair_id, total DESC
    """)

    result = {}
    for r in rows:
        pid = str(r["pair_id"])
        result.setdefault(pid, [])
        if len(result[pid]) < 50:  # top 50 per pair
            result[pid].append({
                "domain": r["domain"],
                "russian_pct": float(r["russian_pct"]),
                "total": r["total"],
                "is_ru": r["is_ru"],
            })

    log.info(f"  Holdouts: {len(result)} pairs")
    return result


def export_holdouts_global() -> dict:
    """Export global holdout domains across all pairs."""
    log.info("Exporting global holdouts...")

    rows = query(f"""
        WITH domain_stats AS (
            SELECT source_domain as domain,
                COUNTIF(variant='russian') as rus,
                COUNTIF(variant='ukrainian') as ukr,
                COUNT(*) as total
            FROM `{DATASET}.raw_gdelt`
            WHERE date >= '2024-01-01'
            GROUP BY domain
            HAVING total >= 50
        )
        SELECT domain, rus, total,
            ROUND(rus / total * 100, 1) as russian_pct,
            ENDS_WITH(domain, '.ru') as is_ru
        FROM domain_stats
        WHERE rus > ukr
        ORDER BY total DESC
        LIMIT 100
    """)

    return [
        {
            "domain": r["domain"],
            "russian_pct": float(r["russian_pct"]),
            "total": r["total"],
            "is_ru": r["is_ru"],
        }
        for r in rows
    ]


def export_pair_events() -> dict:
    """Export pair-specific events from config."""
    log.info("Exporting pair events...")
    pairs_cfg = load_pairs()
    result = {}
    for p in pairs_cfg["pairs"]:
        if not p.get("enabled", True):
            continue
        events = p.get("events", [])
        if events:
            pair_events = []
            for e in events:
                pair_events.append({
                    "date": e["date"],
                    "label": e["label"],
                    "color": e.get("color", "#0057B8"),
                })
            result[str(p["id"])] = pair_events
    return result


def export_analysis() -> dict:
    """Export analysis results from BQ."""
    log.info("Exporting analysis...")

    # Changepoints
    cp_rows = query(f"""
        SELECT pair_id, source, changepoint_date, ci_lower, ci_upper, effect_size
        FROM `{DATASET}.analysis_changepoints`
        ORDER BY pair_id, source
    """)

    pairs_cfg = load_pairs()
    pair_lookup = {p["id"]: p for p in pairs_cfg["pairs"]}

    changepoints = []
    for r in cp_rows:
        p = pair_lookup.get(r["pair_id"], {})
        changepoints.append({
            "pair_id": r["pair_id"],
            "pair": f"{p.get('russian', '?')} -> {p.get('ukrainian', '?')}",
            "category": p.get("category", ""),
            "changepoint_date": str(r["changepoint_date"]),
            "effect_size": round(float(r["effect_size"]), 1) if r["effect_size"] else 0,
        })

    return {
        "changepoint_detection": changepoints,
        "metadata": {"generated": "auto", "source": "bigquery"},
    }


def export_site_stats() -> dict:
    """Export per-source statistics for the site header."""
    log.info("Exporting site stats...")

    rows = query(f"""
        SELECT 'gdelt_articles' as metric, CAST(COUNT(*) AS STRING) as val FROM `{DATASET}.raw_gdelt`
        UNION ALL
        SELECT 'gdelt_domains', CAST(COUNT(DISTINCT source_domain) AS STRING) FROM `{DATASET}.raw_gdelt`
        UNION ALL
        SELECT 'reddit_posts', CAST(COUNT(*) AS STRING) FROM `{DATASET}.raw_reddit`
        UNION ALL
        SELECT 'reddit_subreddits', CAST(COUNT(DISTINCT subreddit) AS STRING) FROM `{DATASET}.raw_reddit`
        UNION ALL
        SELECT 'youtube_videos', CAST(COUNT(*) AS STRING) FROM `{DATASET}.raw_youtube`
        UNION ALL
        SELECT 'youtube_channels', CAST(COUNT(DISTINCT channel_id) AS STRING) FROM `{DATASET}.raw_youtube`
        UNION ALL
        SELECT 'wikipedia_pageviews', CAST(SUM(pageviews) AS STRING) FROM `{DATASET}.raw_wikipedia`
        UNION ALL
        SELECT 'trends_datapoints', CAST(COUNT(*) AS STRING) FROM `{DATASET}.raw_trends`
        UNION ALL
        SELECT 'trends_countries', CAST(COUNT(DISTINCT geo) AS STRING) FROM `{DATASET}.raw_trends` WHERE geo != '' AND geo IS NOT NULL
        UNION ALL
        SELECT 'ngrams_datapoints', CAST(COUNT(*) AS STRING) FROM `{DATASET}.raw_ngrams`
        UNION ALL
        SELECT 'common_crawl_matches', CAST(COUNT(*) AS STRING) FROM `{DATASET}.raw_common_crawl`
        UNION ALL
        SELECT 'common_crawl_domains', CAST(COUNT(DISTINCT domain) AS STRING) FROM `{DATASET}.raw_common_crawl`
        UNION ALL
        SELECT 'total_pairs', CAST(COUNT(DISTINCT pair_id) AS STRING) FROM (
            SELECT pair_id FROM `{DATASET}.raw_trends`
            UNION DISTINCT
            SELECT pair_id FROM `{DATASET}.raw_gdelt`
            UNION DISTINCT
            SELECT pair_id FROM `{DATASET}.raw_wikipedia`
            UNION DISTINCT
            SELECT pair_id FROM `{DATASET}.raw_reddit`
            UNION DISTINCT
            SELECT pair_id FROM `{DATASET}.raw_youtube`
        )
    """)

    stats = {r["metric"]: r["val"] for r in rows}

    # Total verified instances (sum of all source counts)
    total_query = query(f"""
        SELECT
            (SELECT COUNT(*) FROM `{DATASET}.raw_gdelt`) +
            (SELECT COUNT(*) FROM `{DATASET}.raw_reddit`) +
            (SELECT COUNT(*) FROM `{DATASET}.raw_youtube`) +
            (SELECT COUNT(*) FROM `{DATASET}.raw_common_crawl`) as total_records
    """)
    stats["total_records"] = str(total_query[0]["total_records"])

    log.info(f"  Stats: {stats}")
    return stats


def write_json(path: Path, data):
    """Write JSON file with compact formatting."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, separators=(",", ":"), default=str)
    size_kb = path.stat().st_size / 1024
    log.info(f"  Wrote {path.name} ({size_kb:.0f} KB)")


def main():
    log.info("=" * 60)
    log.info("Exporting BigQuery data to site JSON")
    log.info("=" * 60)

    # Export all data
    timeseries = export_timeseries()
    pairs = export_pairs()
    trends_countries = export_trends_countries()
    countries_by_pair = export_countries_by_pair()
    holdouts_by_pair = export_holdouts_by_pair()
    holdouts_global = export_holdouts_global()
    pair_events = export_pair_events()
    analysis = export_analysis()
    site_stats = export_site_stats()

    # Write all files
    write_json(SITE_DATA_DIR / "timeseries.json", timeseries)
    write_json(SITE_DATA_DIR / "pairs.json", pairs)
    write_json(SITE_DATA_DIR / "trends_countries.json", trends_countries)
    write_json(SITE_DATA_DIR / "countries_by_pair.json", countries_by_pair)
    write_json(SITE_DATA_DIR / "holdouts_by_pair.json", holdouts_by_pair)
    write_json(SITE_DATA_DIR / "holdouts.json", holdouts_global)
    write_json(SITE_DATA_DIR / "pair_events.json", pair_events)
    write_json(SITE_DATA_DIR / "analysis.json", analysis)
    write_json(SITE_DATA_DIR / "site_stats.json", site_stats)

    log.info("=" * 60)
    log.info("Export complete!")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
