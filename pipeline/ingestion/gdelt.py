"""GDELT ingestion: BigQuery public dataset → our BigQuery tables.

Scans the GDELT Global Knowledge Graph for toponym mentions.
Incremental: only processes dates after the last watermark.
"""

import logging
from datetime import datetime, timezone

from google.cloud import bigquery

from pipeline.config import get_enabled_pairs, get_gcp_config, load_pipeline
from pipeline.ingestion.watermarks import get_watermark, set_watermark

logger = logging.getLogger(__name__)


def _build_regex(pair: dict) -> tuple[str, str]:
    """Build BigQuery RE2 regex patterns for Russian and Ukrainian variants.

    Uses (?i) for case-insensitive and (^|[^a-zA-Z]) / ($|[^a-zA-Z])
    for word boundaries since RE2 doesn't support \\b reliably.
    """
    russian = pair["russian"]
    ukrainian = pair["ukrainian"]

    def _word_boundary_regex(term):
        """Wrap term in RE2-safe word boundaries."""
        import re
        escaped = re.escape(term)
        return rf"(?i)(^|[^a-zA-Z]){escaped}($|[^a-zA-Z])"

    # Special case: "the Ukraine" needs negative lookahead
    if pair.get("filter_pattern"):
        russian_regex = pair["filter_pattern"]
    else:
        russian_regex = _word_boundary_regex(russian)

    ukrainian_regex = _word_boundary_regex(ukrainian)

    # Pair-specific overrides for substring/contamination issues.
    # RE2 doesn't support lookaheads, so these use explicit char classes.
    pid = pair.get("id")
    return russian_regex, ukrainian_regex


def _pair_exclusion_clause(pair: dict) -> str:
    """Return extra SQL WHERE conditions to exclude contamination."""
    pid = pair.get("id")
    clauses = []
    if pid == 3:
        # Odessa: exclude Odessa, Texas sources
        clauses.append("AND NET.HOST(DocumentIdentifier) NOT LIKE '%texas%'")
        clauses.append("AND NET.HOST(DocumentIdentifier) NOT LIKE '%permian%'")
        clauses.append("AND NET.HOST(DocumentIdentifier) NOT LIKE '%midland%'")
    # Pair 1 (Kiev/Kyiv) no longer filters cross-pair terms — parent pairs
    # intentionally capture all uses of the spelling (food, sports, etc.)
    return "\n              ".join(clauses) if clauses else ""


def ingest_pair(pair: dict, client: bigquery.Client, cfg: dict, full_refresh: bool = False):
    """Ingest GDELT data for a single pair into our BigQuery table."""
    pair_id = pair["id"]
    gcp = cfg["gcp"]
    dest_table = f"{gcp['project_id']}.{gcp['bigquery']['dataset']}.raw_gdelt"

    # Check watermark for incremental
    start_date = "2015-01-01"
    if not full_refresh:
        wm = get_watermark(pair_id, "gdelt")
        if wm and wm["status"] == "success":
            start_date = wm["last_fetched"].strftime("%Y-%m-%d")

    russian_regex, ukrainian_regex = _build_regex(pair)
    exclusion = _pair_exclusion_clause(pair)

    query = f"""
        INSERT INTO `{dest_table}`
        (pair_id, date, source_url, source_domain, source_country,
         matched_term, variant, count, ingested_at)

        WITH russian AS (
            SELECT
                DATE(_PARTITIONTIME) AS date,
                DocumentIdentifier AS source_url,
                NET.HOST(DocumentIdentifier) AS source_domain,
                SourceCommonName AS source_country,
                '{pair["russian"]}' AS matched_term,
                'russian' AS variant,
                1 AS count
            FROM `gdelt-bq.gdeltv2.gkg_partitioned`
            WHERE _PARTITIONTIME >= TIMESTAMP('{start_date}')
              AND REGEXP_CONTAINS(
                  CONCAT(IFNULL(V2Themes, ''), ' ', IFNULL(V2Locations, ''),
                         ' ', IFNULL(DocumentIdentifier, '')),
                  r'{russian_regex}'
              )
              {exclusion}
        ),
        ukrainian AS (
            SELECT
                DATE(_PARTITIONTIME) AS date,
                DocumentIdentifier AS source_url,
                NET.HOST(DocumentIdentifier) AS source_domain,
                SourceCommonName AS source_country,
                '{pair["ukrainian"]}' AS matched_term,
                'ukrainian' AS variant,
                1 AS count
            FROM `gdelt-bq.gdeltv2.gkg_partitioned`
            WHERE _PARTITIONTIME >= TIMESTAMP('{start_date}')
              AND REGEXP_CONTAINS(
                  CONCAT(IFNULL(V2Themes, ''), ' ', IFNULL(V2Locations, ''),
                         ' ', IFNULL(DocumentIdentifier, '')),
                  r'{ukrainian_regex}'
              )
              {exclusion}
        ),
        combined AS (
            SELECT * FROM russian
            UNION ALL
            SELECT * FROM ukrainian
        )

        SELECT
            {pair_id} AS pair_id,
            date,
            source_url,
            source_domain,
            source_country,
            matched_term,
            variant,
            count,
            CURRENT_TIMESTAMP() AS ingested_at
        FROM combined
    """

    logger.info(f"Ingesting GDELT pair {pair_id}: {pair['russian']}/{pair['ukrainian']} from {start_date}")

    try:
        job = client.query(query)
        result = job.result()
        row_count = job.num_dml_affected_rows or 0
        logger.info(f"  → {row_count} rows inserted")
        set_watermark(pair_id, "gdelt", row_count)
    except Exception as e:
        logger.error(f"  → FAILED: {e}")
        set_watermark(pair_id, "gdelt", 0, status="failed")
        raise


def run(pair_ids: list[int] | None = None, full_refresh: bool = False):
    """Run GDELT ingestion for specified pairs (or all enabled)."""
    cfg = load_pipeline()
    gcp = cfg["gcp"]
    client = bigquery.Client(project=gcp["project_id"])

    pairs = get_enabled_pairs()
    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    logger.info(f"GDELT ingestion: {len(pairs)} pairs")
    for pair in pairs:
        ingest_pair(pair, client, cfg, full_refresh)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
    run()
