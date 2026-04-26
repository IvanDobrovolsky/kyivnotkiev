"""Build per-country adoption data from GDELT domain information.

Uses source_domain from raw_gdelt.parquet (the SourceCommonName field
from GDELT GKG) to derive source country via ccTLD and known outlet
mapping. Produces countries_by_pair.json for the site map.

Methodology note for paper:
  Country attribution uses the domain's country-code TLD (ccTLD) where
  available (e.g., .ua = Ukraine, .ru = Russia, .de = Germany). For
  generic TLDs (.com, .org, .net), a curated mapping of 50+ major
  international news outlets is applied. Domains that cannot be
  attributed to a specific country are excluded. This approach covers
  ~70% of all GDELT GKG records in the dataset.

Usage:
    python -m pipeline.ingestion.gdelt_athena_countries
"""

import json
import logging
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


# ccTLD → ISO-3166-1 alpha-2
CCTLD_TO_ISO = {
    "ac": "SH", "ad": "AD", "ae": "AE", "af": "AF", "ag": "AG", "ai": "AI",
    "al": "AL", "am": "AM", "ao": "AO", "ar": "AR", "at": "AT", "au": "AU",
    "az": "AZ", "ba": "BA", "bb": "BB", "bd": "BD", "be": "BE", "bf": "BF",
    "bg": "BG", "bh": "BH", "bi": "BI", "bj": "BJ", "bn": "BN", "bo": "BO",
    "br": "BR", "bs": "BS", "bt": "BT", "bw": "BW", "by": "BY", "bz": "BZ",
    "ca": "CA", "cd": "CD", "cf": "CF", "cg": "CG", "ch": "CH", "ci": "CI",
    "cl": "CL", "cm": "CM", "cn": "CN", "co": "CO", "cr": "CR", "cu": "CU",
    "cy": "CY", "cz": "CZ", "de": "DE", "dj": "DJ", "dk": "DK", "dm": "DM",
    "do": "DO", "dz": "DZ", "ec": "EC", "ee": "EE", "eg": "EG", "er": "ER",
    "es": "ES", "et": "ET", "fi": "FI", "fj": "FJ", "fr": "FR", "ga": "GA",
    "ge": "GE", "gh": "GH", "gm": "GM", "gn": "GN", "gq": "GQ", "gr": "GR",
    "gt": "GT", "gw": "GW", "gy": "GY", "hk": "HK", "hn": "HN", "hr": "HR",
    "ht": "HT", "hu": "HU", "id": "ID", "ie": "IE", "il": "IL", "in": "IN",
    "iq": "IQ", "ir": "IR", "is": "IS", "it": "IT", "jm": "JM", "jo": "JO",
    "jp": "JP", "ke": "KE", "kg": "KG", "kh": "KH", "km": "KM", "kn": "KN",
    "kp": "KP", "kr": "KR", "kw": "KW", "kz": "KZ", "la": "LA", "lb": "LB",
    "lc": "LC", "li": "LI", "lk": "LK", "lr": "LR", "ls": "LS", "lt": "LT",
    "lu": "LU", "lv": "LV", "ly": "LY", "ma": "MA", "mc": "MC", "md": "MD",
    "me": "ME", "mg": "MG", "mk": "MK", "ml": "ML", "mm": "MM", "mn": "MN",
    "mo": "MO", "mr": "MR", "mt": "MT", "mu": "MU", "mv": "MV", "mw": "MW",
    "mx": "MX", "my": "MY", "mz": "MZ", "na": "NA", "ne": "NE", "ng": "NG",
    "ni": "NI", "nl": "NL", "no": "NO", "np": "NP", "nz": "NZ", "om": "OM",
    "pa": "PA", "pe": "PE", "pg": "PG", "ph": "PH", "pk": "PK", "pl": "PL",
    "pr": "PR", "ps": "PS", "pt": "PT", "py": "PY", "qa": "QA", "ro": "RO",
    "rs": "RS", "ru": "RU", "rw": "RW", "sa": "SA", "sb": "SB", "sc": "SC",
    "sd": "SD", "se": "SE", "sg": "SG", "si": "SI", "sk": "SK", "sl": "SL",
    "sn": "SN", "so": "SO", "sr": "SR", "sv": "SV", "sy": "SY", "sz": "SZ",
    "td": "TD", "tg": "TG", "th": "TH", "tj": "TJ", "tm": "TM", "tn": "TN",
    "to": "TO", "tr": "TR", "tt": "TT", "tw": "TW", "tz": "TZ", "ua": "UA",
    "ug": "UG", "uk": "GB", "us": "US", "uy": "UY", "uz": "UZ", "ve": "VE",
    "vn": "VN", "ye": "YE", "za": "ZA", "zm": "ZM", "zw": "ZW",
}

# Compound ccTLDs (checked before simple TLD)
COMPOUND_CCTLD = {
    "co.uk": "GB", "co.jp": "JP", "co.kr": "KR", "co.in": "IN",
    "co.za": "ZA", "co.nz": "NZ", "co.il": "IL", "co.id": "ID",
    "com.au": "AU", "com.br": "BR", "com.tr": "TR", "com.mx": "MX",
    "com.ar": "AR", "com.co": "CO", "com.ua": "UA", "com.ng": "NG",
    "com.pk": "PK", "com.eg": "EG", "com.sg": "SG", "com.my": "MY",
    "com.ph": "PH", "com.bd": "BD", "com.vn": "VN", "com.pe": "PE",
    "com.cn": "CN", "com.tw": "TW", "com.hk": "HK",
    "org.uk": "GB", "org.au": "AU",
    "net.ua": "UA", "net.au": "AU",
}

# Known major outlets with generic TLDs → country
# Sources: Wikipedia, outlet about pages. Only outlets with clear single-country editorial base.
KNOWN_OUTLETS = {
    # US
    "nytimes.com": "US", "washingtonpost.com": "US", "cnn.com": "US",
    "foxnews.com": "US", "usatoday.com": "US", "npr.org": "US",
    "nbcnews.com": "US", "cbsnews.com": "US", "abcnews.go.com": "US",
    "politico.com": "US", "thehill.com": "US", "axios.com": "US",
    "bloomberg.com": "US", "wsj.com": "US", "huffpost.com": "US",
    "yahoo.com": "US", "businessinsider.com": "US", "vox.com": "US",
    "thedailybeast.com": "US", "newsweek.com": "US", "usnews.com": "US",
    "apnews.com": "US", "pbs.org": "US", "latimes.com": "US",
    "chicagotribune.com": "US", "nypost.com": "US", "time.com": "US",
    "foreignpolicy.com": "US", "foreignaffairs.com": "US",
    # UK
    "bbc.com": "GB", "theguardian.com": "GB", "reuters.com": "GB",
    "independent.co.uk": "GB", "dailymail.co.uk": "GB", "ft.com": "GB",
    "economist.com": "GB", "thetimes.co.uk": "GB", "metro.co.uk": "GB",
    # Germany
    "dw.com": "DE", "dw.de": "DE",
    # France
    "france24.com": "FR",
    # Qatar / Middle East
    "aljazeera.com": "QA",
    # Russia
    "rt.com": "RU", "sputniknews.com": "RU", "tass.com": "RU",
    # Ukraine
    "kyivpost.com": "UA", "kyivindependent.com": "UA", "ukrinform.net": "UA",
    "unian.info": "UA", "uatoday.tv": "UA", "112.international": "UA",
    # China
    "globaltimes.cn": "CN", "chinadaily.com.cn": "CN",
    # Japan
    "japantimes.co.jp": "JP",
    # Singapore
    "straitstimes.com": "SG", "channelnewsasia.com": "SG",
    # Israel
    "timesofisrael.com": "IL", "haaretz.com": "IL", "jpost.com": "IL",
    # Turkey
    "dailysabah.com": "TR", "trtworld.com": "TR",
    # Australia
    "abc.net.au": "AU", "smh.com.au": "AU", "sbs.com.au": "AU",
    # Canada
    "globalnews.ca": "CA", "cbc.ca": "CA",
    # Italy
    "ansa.it": "IT",
    # Spain
    "elpais.com": "ES",
    # India
    "thehindu.com": "IN", "ndtv.com": "IN", "hindustantimes.com": "IN",
    # South Korea
    "koreaherald.com": "KR", "en.yna.co.kr": "KR",
    # Brazil
    "oglobo.globo.com": "BR",
    # South Africa
    "news24.com": "ZA",
    # Nigeria
    "punchng.com": "NG",
    # International (skip — not attributable to one country)
    # "euronews.com", "africanews.com" — multinational, skip
}

# ISO numeric → ISO alpha-2 (for manifest.json country names)
ISO_NUM_TO_ALPHA = {
    "004": "AF", "008": "AL", "012": "DZ", "020": "AD", "024": "AO",
    "031": "AZ", "032": "AR", "036": "AU", "040": "AT", "044": "BS",
    "048": "BH", "050": "BD", "051": "AM", "056": "BE", "064": "BT",
    "068": "BO", "070": "BA", "072": "BW", "076": "BR", "084": "BZ",
    "090": "SB", "096": "BN", "100": "BG", "104": "MM", "108": "BI",
    "112": "BY", "116": "KH", "120": "CM", "124": "CA", "144": "LK",
    "148": "TD", "152": "CL", "156": "CN", "170": "CO", "178": "CG",
    "180": "CD", "188": "CR", "191": "HR", "192": "CU", "196": "CY",
    "203": "CZ", "208": "DK", "214": "DO", "218": "EC", "222": "SV",
    "226": "GQ", "231": "ET", "233": "EE", "246": "FI", "250": "FR",
    "268": "GE", "276": "DE", "288": "GH", "300": "GR", "320": "GT",
    "324": "GN", "328": "GY", "332": "HT", "340": "HN", "344": "HK",
    "348": "HU", "352": "IS", "356": "IN", "360": "ID", "364": "IR",
    "368": "IQ", "372": "IE", "376": "IL", "380": "IT", "384": "CI",
    "388": "JM", "392": "JP", "398": "KZ", "400": "JO", "404": "KE",
    "410": "KR", "414": "KW", "417": "KG", "422": "LB", "428": "LV",
    "434": "LY", "440": "LT", "442": "LU", "458": "MY", "484": "MX",
    "496": "MN", "498": "MD", "499": "ME", "504": "MA", "508": "MZ",
    "512": "OM", "524": "NP", "528": "NL", "554": "NZ", "558": "NI",
    "566": "NG", "578": "NO", "586": "PK", "591": "PA", "600": "PY",
    "604": "PE", "608": "PH", "616": "PL", "620": "PT", "630": "PR",
    "634": "QA", "642": "RO", "643": "RU", "682": "SA", "686": "SN",
    "688": "RS", "702": "SG", "703": "SK", "705": "SI", "710": "ZA",
    "716": "ZW", "724": "ES", "752": "SE", "756": "CH", "764": "TH",
    "788": "TN", "792": "TR", "800": "UG", "804": "UA", "818": "EG",
    "826": "GB", "834": "TZ", "840": "US", "858": "UY", "860": "UZ",
    "862": "VE", "704": "VN", "784": "AE", "807": "MK", "275": "PS",
    "887": "YE", "894": "ZM",
}

# Reverse: alpha-2 → country name (from manifest ISO_NUM map)
ALPHA_TO_NAME = {}
for num, alpha in ISO_NUM_TO_ALPHA.items():
    # We'll populate names below
    pass


def domain_to_country(domain: str) -> str:
    """Map GDELT SourceCommonName to ISO alpha-2 country code."""
    if not isinstance(domain, str) or not domain:
        return ""
    domain = domain.lower().strip()

    # Strip www. prefix
    if domain.startswith("www."):
        domain = domain[4:]

    # Check known outlets first (exact match)
    if domain in KNOWN_OUTLETS:
        return KNOWN_OUTLETS[domain]

    # Check compound ccTLDs
    parts = domain.rsplit(".", 2)
    if len(parts) >= 3:
        compound = f"{parts[-2]}.{parts[-1]}"
        if compound in COMPOUND_CCTLD:
            return COMPOUND_CCTLD[compound]

    # Simple ccTLD
    tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
    if tld in CCTLD_TO_ISO:
        return CCTLD_TO_ISO[tld]

    return ""


def main():
    log.info("Building per-country adoption from GDELT SourceCommonName...")

    # Load GDELT data
    table = pq.read_table(ROOT_DIR / "dataset" / "raw_gdelt.parquet")
    for i, field in enumerate(table.schema):
        if "date" in str(field.type):
            table = table.set_column(i, field.name, table.column(i).cast(pa.string()))
    table = table.replace_schema_metadata({})
    df = table.to_pandas()
    log.info(f"Loaded: {len(df):,} rows, {df['pair_id'].nunique()} pairs")

    # Map domains to countries
    df["country"] = df["source_domain"].apply(domain_to_country)
    mapped = df[df["country"] != ""]
    log.info(f"Mapped to country: {len(mapped):,}/{len(df):,} ({len(mapped)/len(df)*100:.1f}%)")
    log.info(f"Countries: {mapped['country'].nunique()}")

    # Aggregate: per pair × per country × variant → sum(count)
    agg = mapped.groupby(["pair_id", "country", "variant"])["count"].sum().reset_index()

    # Build reverse map: alpha-2 → (ISO numeric, name)
    # The site map uses ISO numeric (3-digit padded) as keys
    alpha_to_num = {}
    for num, alpha in ISO_NUM_TO_ALPHA.items():
        alpha_to_num[alpha] = num

    # Country names from the export_site_data GEO_NAMES map
    import pipeline.export_site_data as esd
    geo_names = getattr(esd, "GEO_NAMES", {})

    # Build output keyed by ISO numeric (for the world map)
    result = {}
    for pid in sorted(agg["pair_id"].unique()):
        pair_agg = agg[agg["pair_id"] == pid]
        country_data = {}

        for country_alpha in pair_agg["country"].unique():
            c_data = pair_agg[pair_agg["country"] == country_alpha]
            ukr = int(c_data[c_data["variant"] == "ukrainian"]["count"].sum())
            rus = int(c_data[c_data["variant"] == "russian"]["count"].sum())
            total = ukr + rus
            if total < 10:
                continue

            iso_num = alpha_to_num.get(country_alpha, "")
            if not iso_num:
                continue

            name = geo_names.get(iso_num, country_alpha)
            country_data[iso_num] = {
                "name": name,
                "adoption": round(ukr / total * 100, 1),
                "total": int(total),
            }

        if country_data:
            result[str(pid)] = country_data

    log.info(f"Pairs with country data: {len(result)}")

    # Coverage stats
    for pid in sorted(result.keys(), key=int):
        n_countries = len(result[pid])
        total = sum(v["total"] for v in result[pid].values())
        log.info(f"  Pair {pid}: {n_countries} countries, {total:,} records")

    # Save
    out_path = ROOT_DIR / "site" / "src" / "data" / "countries_by_pair.json"
    with open(out_path, "w") as f:
        json.dump(result, f, separators=(",", ":"))
    log.info(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
