"""Query GDELT GKG via Athena for pairs missing from URL-only scan.

These 31 pairs have multi-word terms that don't appear in URLs but DO
appear in article themes/locations/text. We search the full GKG row
(URL + themes + locations + persons + organizations) for matches.

Usage:
    python -m pipeline.ingestion.gdelt_athena_missing
"""

import boto3
import io
import logging
import time
from pathlib import Path

import pandas as pd
import yaml

from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

AWS_REGION = "us-east-1"
S3_OUTPUT = "s3://kyivnotkiev-athena-results/"
OUT_DIR = ROOT_DIR / "data" / "raw" / "gdelt"

athena = boto3.client("athena", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)

# Pairs that were NOT found in URL-only scan
MISSING_PAIR_IDS = {15,20,21,22,24,25,26,28,29,30,31,34,35,52,54,55,56,57,58,
                    60,61,62,70,71,80,83,84,85,86,87,89}

with open(ROOT_DIR / "config" / "pairs.yaml") as f:
    _cfg = yaml.safe_load(f)

PAIRS = []
for p in _cfg["pairs"]:
    if not p.get("enabled") or p.get("is_control"):
        continue
    if p["id"] in MISSING_PAIR_IDS:
        PAIRS.append({"id": p["id"], "russian": p["russian"], "ukrainian": p["ukrainian"]})


def run_query(sql: str) -> str:
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": "gdelt"},
        WorkGroup="primary",
        ResultConfiguration={"OutputLocation": S3_OUTPUT},
    )
    qid = resp["QueryExecutionId"]
    log.info(f"  Query submitted: {qid}")

    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)
        state = r["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(5)

    if state != "SUCCEEDED":
        reason = r["QueryExecution"]["Status"].get("StateChangeReason", "")
        log.error(f"  Query failed: {reason}")
        return ""

    stats = r["QueryExecution"]["Statistics"]
    scanned = stats.get("DataScannedInBytes", 0) / 1e9
    runtime = stats.get("EngineExecutionTimeInMillis", 0) / 1000
    log.info(f"  Done: {scanned:.1f} GB scanned, {runtime:.0f}s runtime")
    return qid


def download_results(qid: str) -> pd.DataFrame:
    bucket = S3_OUTPUT.replace("s3://", "").split("/")[0]
    key = "/".join(S3_OUTPUT.replace("s3://", "").split("/")[1:]) + f"{qid}.csv"
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.StringIO(obj["Body"].read().decode("utf-8")))


def main():
    # Ensure table exists
    log.info("Ensuring GDELT GKG table exists...")
    run_query("""
    CREATE EXTERNAL TABLE IF NOT EXISTS gdelt_gkg (
        gkgrecordid STRING, v21date STRING, v2sourcecollectionidentifier STRING,
        v2sourcecommonname STRING, v2documentidentifier STRING,
        v1counts STRING, v21counts STRING, v1themes STRING, v2enhancedthemes STRING,
        v1locations STRING, v2enhancedlocations STRING, v1persons STRING,
        v2enhancedpersons STRING, v1organizations STRING, v2enhancedorganizations STRING,
        v15tone STRING, v21enhanceddates STRING, v2gcam STRING, v21sharingimage STRING,
        v21relatedimages STRING, v21socialimageembeds STRING, v21socialvideoembeds STRING,
        v21quotations STRING, v21allnames STRING, v21amounts STRING,
        v21translationinfo STRING, v2extrasxml STRING
    )
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
    STORED AS TEXTFILE
    LOCATION 's3://gdelt-open-data/v2/gkg/'
    """)

    # Build a single query that searches URL + themes + locations + persons
    # for all missing pairs. Use UNION ALL for each pair.
    # Search across: v2documentidentifier, v1themes, v2enhancedthemes,
    # v1locations, v2enhancedlocations, v1persons, v2enhancedpersons,
    # v1organizations, v2enhancedorganizations, v21allnames
    log.info(f"Querying {len(PAIRS)} missing pairs across full GKG record...")

    union_parts = []
    for p in PAIRS:
        ru = p["russian"].lower().replace("'", "''")
        ua = p["ukrainian"].lower().replace("'", "''")
        pid = p["id"]

        # Search the concatenation of all text fields
        search_expr = """LOWER(COALESCE(v2documentidentifier,'') || ' ' ||
            COALESCE(v1themes,'') || ' ' || COALESCE(v2enhancedthemes,'') || ' ' ||
            COALESCE(v1locations,'') || ' ' || COALESCE(v2enhancedlocations,'') || ' ' ||
            COALESCE(v1persons,'') || ' ' || COALESCE(v2enhancedpersons,'') || ' ' ||
            COALESCE(v21allnames,''))"""

        union_parts.append(f"""
        SELECT {pid} AS pair_id, v2sourcecommonname AS domain,
               v2documentidentifier AS url, SUBSTR(v21date, 1, 8) AS gkg_date,
               'russian' AS variant
        FROM gdelt_gkg
        WHERE {search_expr} LIKE '%{ru}%'
        """)

        union_parts.append(f"""
        SELECT {pid} AS pair_id, v2sourcecommonname AS domain,
               v2documentidentifier AS url, SUBSTR(v21date, 1, 8) AS gkg_date,
               'ukrainian' AS variant
        FROM gdelt_gkg
        WHERE {search_expr} LIKE '%{ua}%'
        """)

    # This is 62 subqueries but each scans the full table.
    # Athena will optimize this into a single scan with multiple predicates.
    full_sql = " UNION ALL ".join(union_parts)

    log.info(f"  SQL: {len(full_sql):,} chars, {len(union_parts)} UNION parts")
    qid = run_query(full_sql)
    if not qid:
        log.error("Query failed")
        return

    df = download_results(qid)
    log.info(f"Raw results: {len(df):,} rows")

    # Save
    out_path = OUT_DIR / "gdelt_missing_pairs_urls.csv"
    df.to_csv(out_path, index=False)
    log.info(f"Saved: {out_path}")

    # Stats
    for pid in sorted(df["pair_id"].unique()):
        sub = df[df["pair_id"] == pid]
        p = next((p for p in PAIRS if p["id"] == pid), {})
        log.info(f"  Pair {pid} ({p.get('russian','')}/{p.get('ukrainian','')}): {len(sub):,} URLs")


if __name__ == "__main__":
    main()
