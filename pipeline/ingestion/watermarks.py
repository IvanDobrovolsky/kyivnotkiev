"""Watermark management for incremental ingestion.

Each (pair_id, source) combination has a watermark tracking when it was
last successfully ingested and how many rows were loaded. The pipeline
checks watermarks before fetching to skip pairs that are already fresh.
"""

from datetime import datetime, timedelta, timezone
from google.cloud import bigquery

from pipeline.config import get_gcp_config


def get_client() -> bigquery.Client:
    cfg = get_gcp_config()
    return bigquery.Client(project=cfg["project_id"])


def _table_ref() -> str:
    cfg = get_gcp_config()
    return f"{cfg['project_id']}.{cfg['bigquery']['dataset']}.watermarks"


def get_watermark(pair_id: int, source: str) -> dict | None:
    """Get the watermark for a (pair_id, source) combo.
    Returns dict with last_fetched, row_count, status or None if not found.
    """
    client = get_client()
    query = f"""
        SELECT last_fetched, row_count, status
        FROM `{_table_ref()}`
        WHERE pair_id = @pair_id AND source = @source
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pair_id", "INT64", pair_id),
            bigquery.ScalarQueryParameter("source", "STRING", source),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return None
    row = rows[0]
    return {
        "last_fetched": row.last_fetched,
        "row_count": row.row_count,
        "status": row.status,
    }


def set_watermark(pair_id: int, source: str, row_count: int, status: str = "success"):
    """Upsert a watermark for a (pair_id, source) combo."""
    client = get_client()
    table = _table_ref()
    now = datetime.now(timezone.utc).isoformat()

    # MERGE to upsert
    query = f"""
        MERGE `{table}` T
        USING (SELECT @pair_id AS pair_id, @source AS source) S
        ON T.pair_id = S.pair_id AND T.source = S.source
        WHEN MATCHED THEN
            UPDATE SET last_fetched = @now, row_count = @row_count, status = @status
        WHEN NOT MATCHED THEN
            INSERT (pair_id, source, last_fetched, row_count, status)
            VALUES (@pair_id, @source, @now, @row_count, @status)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("pair_id", "INT64", pair_id),
            bigquery.ScalarQueryParameter("source", "STRING", source),
            bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
            bigquery.ScalarQueryParameter("row_count", "INT64", row_count),
            bigquery.ScalarQueryParameter("status", "STRING", status),
        ]
    )
    client.query(query, job_config=job_config).result()


def is_stale(pair_id: int, source: str, max_age_days: int = 7) -> bool:
    """Check if a watermark is older than max_age_days (or missing)."""
    wm = get_watermark(pair_id, source)
    if wm is None or wm["status"] == "failed":
        return True
    cutoff = datetime.now(timezone.utc) - timedelta(days=max_age_days)
    return wm["last_fetched"] < cutoff


def get_all_watermarks() -> list[dict]:
    """Return all watermarks for status display."""
    client = get_client()
    query = f"SELECT * FROM `{_table_ref()}` ORDER BY source, pair_id"
    return [dict(row) for row in client.query(query).result()]
