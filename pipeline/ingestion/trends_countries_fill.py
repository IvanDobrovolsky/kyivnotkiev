"""Fill missing country-level Google Trends data for all enabled pairs.

Queries pytrends for each pair×country combination that's missing from BQ.

Usage:
    python -m pipeline.ingestion.trends_countries_fill
"""

import logging
import time
from datetime import datetime

from google.cloud import bigquery
from pytrends.request import TrendReq

from pipeline.config import load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

PROJECT = "kyivnotkiev-research"
TABLE = f"{PROJECT}.kyivnotkiev.raw_trends"

TARGET_COUNTRIES = [
    "US", "GB", "CA", "AU", "DE", "FR", "IT", "ES", "NL", "PL",
    "MX", "BR", "IN", "JP", "UA", "RU", "TR", "SE", "NO", "IE", "ZA",
]


def get_existing_pairs_countries(client: bigquery.Client) -> set[tuple[int, str]]:
    """Get set of (pair_id, geo) already in BQ."""
    query = f"""
        SELECT DISTINCT pair_id, geo
        FROM `kyivnotkiev.raw_trends`
        WHERE geo != '' AND geo IS NOT NULL
    """
    result = set()
    for row in client.query(query).result():
        result.add((row["pair_id"], row["geo"]))
    return result


def main():
    client = bigquery.Client(project=PROJECT)
    pytrends = TrendReq(hl="en-US", tz=360)

    cfg = load_pairs()
    pairs = [p for p in cfg["pairs"]
             if p.get("enabled", True) and not p.get("is_control", False)]

    existing = get_existing_pairs_countries(client)
    log.info(f"Existing pair×country: {len(existing)}")

    # Find what's missing
    missing = []
    for p in pairs:
        for geo in TARGET_COUNTRIES:
            if (p["id"], geo) not in existing:
                missing.append((p["id"], p["russian"], p["ukrainian"], geo))

    log.info(f"Missing: {len(missing)} pair×country combinations")
    if not missing:
        log.info("Nothing to do!")
        return

    collected = 0
    errors = 0

    for pid, russian, ukrainian, geo in missing:
        try:
            pytrends.build_payload(
                [russian, ukrainian],
                timeframe="2020-01-01 2026-04-01",
                geo=geo,
            )
            df = pytrends.interest_over_time()

            if not df.empty:
                rows = []
                for idx, row in df.iterrows():
                    for variant, term in [("russian", russian), ("ukrainian", ukrainian)]:
                        val = int(row[term])
                        if val > 0:
                            rows.append({
                                "pair_id": pid,
                                "date": idx.strftime("%Y-%m-%d"),
                                "term": term,
                                "variant": variant,
                                "interest": val,
                                "geo": geo,
                                "ingested_at": datetime.now().isoformat(),
                            })
                if rows:
                    errs = client.insert_rows_json(TABLE, rows)
                    if errs:
                        log.warning(f"  BQ error for {pid}/{geo}: {errs[:1]}")
                    else:
                        collected += 1
                        if collected % 20 == 0:
                            log.info(f"  Progress: {collected}/{len(missing)} collected, {errors} errors")

            time.sleep(2)

        except Exception as e:
            errors += 1
            log.warning(f"  {pid} {russian}/{geo}: {e}")
            time.sleep(10)

    log.info(f"Done: {collected} collected, {errors} errors out of {len(missing)} missing")


if __name__ == "__main__":
    main()
