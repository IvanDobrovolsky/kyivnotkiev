"""Build site/src/data/religious.json from the religious crawl outputs.

Aggregates per-source-per-year-per-pair data into a single file the
website consumes for the religious institutions panel.

Output structure:
  {
    "denominations": [
      {
        "id": "constantinople",
        "name": "Ecumenical Patriarchate of Constantinople",
        "short": "Constantinople",
        "url": "https://ec-patr.org/",
        "stance": "Granted autocephaly to OCU, January 2019",
        "switch_year": 2019,
        "totals": {"ru": 18, "ua": 28},
        "ua_pct": 60.9,
        "yearly": [{"year": 2014, "ru": 3, "ua": 0}, ...],
        "pairs": [{"pair_id": 1, "ru_term": "Kiev", "ua_term": "Kyiv",
                   "ru": 18, "ua": 28, "ua_pct": 60.9}, ...]
      },
      ...
    ],
    "switch_timeline": [
      {"id": "constantinople", "switch_year": 2019, "event": "OCU autocephaly"},
      ...
    ]
  }

Usage:
    python -m pipeline.analysis.build_religious_site_data
"""

import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
RAW_DIR = ROOT / "data" / "raw" / "religious"
OUT_PATH = ROOT / "site" / "src" / "data" / "religious.json"


# Hand-curated metadata for each source. Stance + switch_year are paper-ready
# one-liners that explain the political position rather than just the data.
DENOMINATIONS = {
    "constantinople": {
        "name": "Ecumenical Patriarchate of Constantinople",
        "short": "Constantinople",
        "url": "https://ec-patr.org/",
        "stance": "Granted autocephaly to the Orthodox Church of Ukraine in January 2019, breaking with Moscow's claim over Ukrainian Orthodoxy",
        "stance_event": "OCU autocephaly",
        "color": "#0057B8",
    },
    "vatican": {
        "name": "Holy See / Vatican",
        "short": "Vatican",
        "url": "https://www.vatican.va/",
        "stance": "Pope Francis used 'Kyiv' in addresses since at least 2014; rare mentions overall but consistently Ukrainian after 2022",
        "stance_event": "early adopter (~2014)",
        "color": "#FFD700",
    },
    "mospat": {
        "name": "Moscow Patriarchate, Department for External Church Relations",
        "short": "Moscow Patriarchate",
        "url": "https://mospat.ru/en/",
        "stance": "TBD — to be filled in once data is collected",
        "stance_event": None,
        "color": "#D52B1E",
    },
    "wcc": {
        "name": "World Council of Churches",
        "short": "WCC",
        "url": "https://www.oikoumene.org/",
        "stance": "Ecumenical Geneva-based body of 350+ Christian denominations. Moderately Ukrainian-leaning in English news (~74% Kyiv across 17 toponym pairs); shifted clearly after the 2022 invasion",
        "stance_event": "post-2022",
        "color": "#A0A0A0",
    },
    "patriarchia": {
        "name": "Russian Orthodox Church (patriarchia.ru)",
        "short": "ROC general site",
        "url": "https://www.patriarchia.ru/en/",
        "stance": "TBD",
        "stance_event": None,
        "color": "#A52A2A",
    },
}


def load_source(source_key: str) -> list[dict] | None:
    """Load <source>_results.json if present."""
    p = RAW_DIR / f"{source_key}_results.json"
    if not p.exists():
        return None
    try:
        with open(p) as f:
            return json.load(f)
    except Exception:
        return None


def build_denomination(source_key: str, results: list[dict]) -> dict:
    meta = DENOMINATIONS.get(source_key, {})
    yearly = defaultdict(lambda: {"ru": 0, "ua": 0, "pages": 0})
    pairs_out = []
    total_ru = 0
    total_ua = 0
    for r in results:
        for yr in r["yearly"]:
            yearly[yr["year"]]["ru"] += yr["russian_count"]
            yearly[yr["year"]]["ua"] += yr["ukrainian_count"]
            yearly[yr["year"]]["pages"] += yr.get("n_pages", 0)
        ru = r["total_russian"]
        ua = r["total_ukrainian"]
        total_ru += ru
        total_ua += ua
        pairs_out.append({
            "pair_id": r["pair_id"],
            "ru_term": r["russian_term"],
            "ua_term": r["ukrainian_term"],
            "category": r.get("category", ""),
            "ru": ru,
            "ua": ua,
            "ua_pct": round(ua / (ru + ua) * 100, 1) if (ru + ua) else None,
        })

    yearly_list = [
        {"year": y, **counts} for y, counts in sorted(yearly.items())
    ]

    # Try to detect a switch year automatically: the first year with UA > RU
    # in a denomination that previously had any data.
    switch_year = None
    seen_pre_switch = False
    for yr in yearly_list:
        if yr["ua"] > yr["ru"]:
            if seen_pre_switch:
                switch_year = yr["year"]
                break
            elif yr["ua"] > 0 and yr["ru"] == 0:
                # First year ever, all-UA — treat as adopted from start
                switch_year = yr["year"]
                break
        elif yr["ru"] > 0:
            seen_pre_switch = True

    return {
        "id": source_key,
        "name": meta.get("name", source_key.title()),
        "short": meta.get("short", source_key.title()),
        "url": meta.get("url", ""),
        "stance": meta.get("stance", ""),
        "stance_event": meta.get("stance_event"),
        "color": meta.get("color", "#666"),
        "switch_year": switch_year,
        "totals": {"ru": total_ru, "ua": total_ua},
        "ua_pct": round(total_ua / (total_ru + total_ua) * 100, 1) if (total_ru + total_ua) else None,
        "n_pairs_with_mentions": len(results),
        "yearly": yearly_list,
        "pairs": sorted(pairs_out, key=lambda x: -(x["ru"] + x["ua"])),
    }


def main():
    denoms = []
    for source_key in ("constantinople", "vatican", "mospat", "wcc", "patriarchia"):
        results = load_source(source_key)
        if results is None:
            print(f"  skip {source_key}: no results yet")
            continue
        denoms.append(build_denomination(source_key, results))
        print(f"  {source_key}: {len(results)} pairs")

    # Sort: most-data sources first, then alphabetical
    denoms.sort(key=lambda d: (-(d["totals"]["ru"] + d["totals"]["ua"]), d["short"]))

    out = {
        "denominations": denoms,
        "n_denominations": len(denoms),
        "total_mentions": sum(d["totals"]["ru"] + d["totals"]["ua"] for d in denoms),
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nwrote {OUT_PATH}")
    print(f"  {out['n_denominations']} denominations, {out['total_mentions']} total mentions")


if __name__ == "__main__":
    main()
