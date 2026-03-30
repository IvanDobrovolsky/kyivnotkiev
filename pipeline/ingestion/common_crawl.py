"""Common Crawl ingestion: PySpark job for Dataproc.

Scans Common Crawl WARC files for toponym mentions across billions of web pages.
This is the big data core — runs on a Dataproc Spark cluster.

Usage:
    gcloud dataproc jobs submit pyspark common_crawl.py -- --config config/pipeline.yaml
    python -m pipeline.ingestion.common_crawl  # local mode for testing
"""

import argparse
import logging
import re
from datetime import datetime, timezone

import yaml
from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DateType,
    TimestampType,
)

logger = logging.getLogger(__name__)

# Output schema for BigQuery
OUTPUT_SCHEMA = StructType([
    StructField("pair_id", IntegerType(), False),
    StructField("url", StringType(), False),
    StructField("domain", StringType(), False),
    StructField("tld", StringType(), True),
    StructField("matched_term", StringType(), False),
    StructField("variant", StringType(), False),
    StructField("context_snippet", StringType(), True),
    StructField("crawl_id", StringType(), False),
    StructField("crawl_date", DateType(), False),
    StructField("content_language", StringType(), True),
    StructField("ingested_at", TimestampType(), False),
])


def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        return yaml.safe_load(f)


def build_pair_patterns(pairs: list[dict]) -> list[tuple[int, str, str, re.Pattern, re.Pattern]]:
    """Build compiled regex patterns for each pair.
    Returns list of (pair_id, russian_term, ukrainian_term, russian_re, ukrainian_re).
    """
    patterns = []
    for p in pairs:
        if not p.get("enabled", True) or p.get("is_control", False):
            continue
        russian_re = re.compile(rf"\b{re.escape(p['russian'])}\b", re.IGNORECASE)
        ukrainian_re = re.compile(rf"\b{re.escape(p['ukrainian'])}\b", re.IGNORECASE)
        patterns.append((p["id"], p["russian"], p["ukrainian"], russian_re, ukrainian_re))
    return patterns


def extract_matches(text: str, url: str, crawl_id: str, crawl_date: str,
                    lang: str, patterns: list, now: datetime) -> list[Row]:
    """Extract all toponym matches from a single document's text."""
    results = []
    if not text:
        return results

    # Extract domain from URL
    try:
        from urllib.parse import urlparse
        parsed = urlparse(url)
        domain = parsed.hostname or ""
        tld = domain.split(".")[-1] if domain else None
    except Exception:
        domain = ""
        tld = None

    for pair_id, russian_term, ukrainian_term, russian_re, ukrainian_re in patterns:
        # Check Russian variant
        for match in russian_re.finditer(text):
            start = max(0, match.start() - 50)
            end = min(len(text), match.end() + 50)
            snippet = text[start:end].replace("\n", " ").strip()
            results.append(Row(
                pair_id=pair_id,
                url=url,
                domain=domain,
                tld=tld,
                matched_term=russian_term,
                variant="russian",
                context_snippet=snippet,
                crawl_id=crawl_id,
                crawl_date=datetime.strptime(crawl_date[:10], "%Y-%m-%d").date() if crawl_date else None,
                content_language=lang,
                ingested_at=now,
            ))

        # Check Ukrainian variant
        for match in ukrainian_re.finditer(text):
            start = max(0, match.start() - 50)
            end = min(len(text), match.end() + 50)
            snippet = text[start:end].replace("\n", " ").strip()
            results.append(Row(
                pair_id=pair_id,
                url=url,
                domain=domain,
                tld=tld,
                matched_term=ukrainian_term,
                variant="ukrainian",
                context_snippet=snippet,
                crawl_id=crawl_id,
                crawl_date=datetime.strptime(crawl_date[:10], "%Y-%m-%d").date() if crawl_date else None,
                content_language=lang,
                ingested_at=now,
            ))

    return results


def process_crawl(spark: SparkSession, crawl_id: str, pairs: list[dict],
                  output_table: str, project_id: str):
    """Process one Common Crawl monthly crawl."""
    logger.info(f"Processing crawl: {crawl_id}")

    # Common Crawl index is on S3 but accessible via HTTPS
    # We use the columnar index (cc-index) for efficient filtering
    index_path = f"s3a://commoncrawl/cc-index/table/cc-main/warc/crawl={crawl_id}/subset=warc/*.parquet"

    # Read the index — filter to English pages that might contain Ukrainian place names
    try:
        index_df = spark.read.parquet(index_path)
    except Exception as e:
        logger.warning(f"Could not read index for {crawl_id}: {e}")
        return

    # Build a broad keyword filter for the index
    # This dramatically reduces the number of WARC records we need to fetch
    all_terms = []
    for p in pairs:
        if p.get("enabled", True) and not p.get("is_control", False):
            all_terms.extend([p["russian"].lower(), p["ukrainian"].lower()])
    # Deduplicate and take unique root words
    unique_terms = list(set(all_terms))

    # Filter index to English pages containing any of our terms in the URL or content
    # This is a rough filter — we'll do exact matching on the actual content
    keyword_filter = None
    for term in unique_terms[:20]:  # BQ has expression limits
        condition = (
            F.lower(F.col("url_host_name")).contains(term.lower().replace(" ", ""))
            | F.lower(F.col("url_path")).contains(term.lower().replace(" ", ""))
        )
        keyword_filter = condition if keyword_filter is None else (keyword_filter | condition)

    filtered_index = index_df.filter(
        (F.col("content_languages") == "eng")
        & keyword_filter
    ).select("url", "warc_filename", "warc_record_offset", "warc_record_length",
             "content_languages", "fetch_time")

    record_count = filtered_index.count()
    logger.info(f"  {crawl_id}: {record_count} candidate records after index filtering")

    if record_count == 0:
        return

    # For each filtered record, fetch the WARC content and extract matches
    patterns = build_pair_patterns(pairs)
    now = datetime.now(timezone.utc)
    crawl_date_str = crawl_id.replace("CC-MAIN-", "")  # e.g. "2024-10"

    # Broadcast patterns to all workers
    patterns_bc = spark.sparkContext.broadcast(patterns)
    now_bc = spark.sparkContext.broadcast(now)
    crawl_id_bc = spark.sparkContext.broadcast(crawl_id)

    def process_partition(rows):
        """Process a partition of WARC records."""
        import requests
        from warcio.archiveiterator import ArchiveIterator
        from io import BytesIO

        local_patterns = patterns_bc.value
        local_now = now_bc.value
        local_crawl_id = crawl_id_bc.value

        for row in rows:
            warc_url = f"https://data.commoncrawl.org/{row.warc_filename}"
            offset = row.warc_record_offset
            length = row.warc_record_length

            try:
                resp = requests.get(
                    warc_url,
                    headers={"Range": f"bytes={offset}-{offset + length - 1}"},
                    timeout=30,
                )
                if resp.status_code not in (200, 206):
                    continue

                stream = BytesIO(resp.content)
                for record in ArchiveIterator(stream):
                    if record.rec_type == "response":
                        content = record.content_stream().read()
                        try:
                            text = content.decode("utf-8", errors="ignore")
                        except Exception:
                            continue

                        matches = extract_matches(
                            text, row.url, local_crawl_id,
                            crawl_date_str, row.content_languages,
                            local_patterns, local_now,
                        )
                        yield from matches
            except Exception:
                continue

    # Process and write to BigQuery
    results_rdd = filtered_index.rdd.mapPartitions(process_partition)
    results_df = spark.createDataFrame(results_rdd, schema=OUTPUT_SCHEMA)

    match_count = results_df.count()
    logger.info(f"  {crawl_id}: {match_count} toponym matches found")

    if match_count > 0:
        results_df.write.format("bigquery") \
            .option("table", output_table) \
            .option("temporaryGcsBucket", f"{project_id}-dataproc-staging") \
            .mode("append") \
            .save()
        logger.info(f"  {crawl_id}: written to BigQuery")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config/pipeline.yaml")
    parser.add_argument("--crawl-id", help="Process a specific crawl only")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

    # Load config
    with open(args.config) as f:
        pipeline_cfg = yaml.safe_load(f)
    with open("config/pairs.yaml") as f:
        pairs_cfg = yaml.safe_load(f)

    gcp = pipeline_cfg["gcp"]
    project_id = gcp["project_id"]
    output_table = f"{project_id}.{gcp['bigquery']['dataset']}.raw_common_crawl"

    # Initialize Spark
    spark = SparkSession.builder \
        .appName("KyivNotKiev-CommonCrawl") \
        .config("spark.jars.packages",
                "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider") \
        .getOrCreate()

    # Determine which crawls to process
    crawl_ids = [args.crawl_id] if args.crawl_id else pipeline_cfg["pipeline"]["common_crawl"]["crawl_ids"]

    pairs = pairs_cfg["pairs"]

    for crawl_id in crawl_ids:
        process_crawl(spark, crawl_id, pairs, output_table, project_id)

    spark.stop()
    logger.info("Common Crawl ingestion complete")


if __name__ == "__main__":
    main()
