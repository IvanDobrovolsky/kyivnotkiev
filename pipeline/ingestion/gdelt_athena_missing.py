"""Query GDELT GKG via Athena for pairs missing from URL-only scan.

Uses a SINGLE scan with OR conditions instead of UNION ALL to avoid
scanning the full table 62 times.

Usage:
    python -m pipeline.ingestion.gdelt_athena_missing
"""

import boto3
import io
import logging
import re
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
    log.info(f"  Query: {qid}")

    while True:
        r = athena.get_query_execution(QueryExecutionId=qid)
        state = r["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(10)

    stats = r["QueryExecution"]["Statistics"]
    scanned = stats.get("DataScannedInBytes", 0) / 1e9
    runtime = stats.get("EngineExecutionTimeInMillis", 0) / 1000
    cost = scanned * 5 / 1000

    if state != "SUCCEEDED":
        reason = r["QueryExecution"]["Status"].get("StateChangeReason", "")
        log.error(f"  FAILED: {reason} (scanned {scanned:.1f}GB, ~${cost:.0f})")
        return ""

    log.info(f"  Done: {scanned:.1f} GB, {runtime:.0f}s, ~${cost:.0f}")
    return qid


def download_results(qid: str) -> pd.DataFrame:
    bucket = S3_OUTPUT.replace("s3://", "").split("/")[0]
    key = "/".join(S3_OUTPUT.replace("s3://", "").split("/")[1:]) + f"{qid}.csv"
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.StringIO(obj["Body"].read().decode("utf-8")))


def main():
    log.info("Ensuring table exists...")
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

    # Single-pass: scan once, extract all matching rows, classify locally
    # Build a big OR of all terms to match
    all_terms = set()
    for p in PAIRS:
        all_terms.add(p["russian"].lower())
        all_terms.add(p["ukrainian"].lower())

    # Concat searchable columns into one field, check with OR conditions
    search_expr = """LOWER(COALESCE(v2documentidentifier,'') || ' ' ||
        COALESCE(v2enhancedthemes,'') || ' ' ||
        COALESCE(v2enhancedlocations,'') || ' ' ||
        COALESCE(v2enhancedpersons,'') || ' ' ||
        COALESCE(v21allnames,''))"""

    # Build WHERE with OR — single scan
    conditions = []
    for term in sorted(all_terms):
        escaped = term.replace("'", "''")
        conditions.append(f"{search_expr} LIKE '%{escaped}%'")

    where = " OR ".join(conditions)

    sql = f"""
    SELECT
        v2sourcecommonname AS domain,
        v2documentidentifier AS url,
        SUBSTR(v21date, 1, 8) AS gkg_date,
        {search_expr} AS searchable
    FROM gdelt_gkg
    WHERE {where}
    """

    log.info(f"Single-pass query for {len(all_terms)} terms ({len(PAIRS)} pairs)...")
    log.info(f"  SQL: {len(sql):,} chars")
    qid = run_query(sql)
    if not qid:
        return

    df = download_results(qid)
    log.info(f"Raw rows: {len(df):,}")

    if df.empty:
        log.info("No results")
        return

    # Classify each row: which pair(s) does it match?
    pair_patterns = []
    for p in PAIRS:
        pair_patterns.append({
            "id": p["id"],
            "russian": p["russian"],
            "ukrainian": p["ukrainian"],
            "ru_re": re.compile(r"\b" + re.escape(p["russian"]) + r"\b", re.IGNORECASE),
            "ua_re": re.compile(r"\b" + re.escape(p["ukrainian"]) + r"\b", re.IGNORECASE),
        })

    results = []
    for _, row in df.iterrows():
        text = str(row.get("searchable", ""))
        url = str(row.get("url", ""))
        for pp in pair_patterns:
            if pp["ru_re"].search(text):
                results.append({
                    "pair_id": pp["id"], "url": url, "domain": row.get("domain", ""),
                    "date": row.get("gkg_date", ""), "variant": "russian",
                    "matched_term": pp["russian"],
                })
            if pp["ua_re"].search(text):
                results.append({
                    "pair_id": pp["id"], "url": url, "domain": row.get("domain", ""),
                    "date": row.get("gkg_date", ""), "variant": "ukrainian",
                    "matched_term": pp["ukrainian"],
                })

    if not results:
        log.info("No pair matches after classification")
        return

    out_df = pd.DataFrame(results).drop_duplicates(subset=["pair_id", "url", "variant"])
    out_path = OUT_DIR / "gdelt_missing_pairs_urls.csv"
    out_df.to_csv(out_path, index=False)
    log.info(f"Saved: {out_path} ({len(out_df):,} pair-URL matches)")

    for pid in sorted(out_df["pair_id"].unique()):
        sub = out_df[out_df["pair_id"] == pid]
        p = next((p for p in PAIRS if p["id"] == pid), {})
        log.info(f"  Pair {pid} ({p.get('russian','')}/{p.get('ukrainian','')}): {len(sub):,}")


if __name__ == "__main__":
    main()
