#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Common Crawl Processing Pipeline — AWS Edition
# ============================================================================
#
# Run this on an EC2 spot instance in us-east-1 for free S3 access.
# Or locally (slower, ~5-10x more time due to cross-region S3 reads).
#
# Prerequisites:
#   1. AWS CLI configured: aws configure
#   2. Create S3 bucket: aws s3 mb s3://kyivnotkiev-cc-results --region us-east-1
#   3. Athena table created (run setup_athena.sql in Athena console)
#   4. GCP credentials for BigQuery: gcloud auth application-default login
#   5. Python deps: pip install boto3 warcio requests google-cloud-bigquery pyyaml
#
# Usage:
#   ./run.sh                    # Process all configured crawls
#   ./run.sh CC-MAIN-2024-10    # Process one specific crawl
#
# Cost estimate (28 crawls):
#   Athena: ~$5-10 (index queries)
#   EC2 spot: ~$5-15 (c5.xlarge @ $0.05/hr × ~100-300 hrs)
#   Total: ~$15-25
#
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
cd "$PROJECT_ROOT"

RESULTS_BUCKET="s3://kyivnotkiev-cc-results"
CRAWL="${1:-all}"

echo "================================================"
echo "Common Crawl Processing Pipeline"
echo "================================================"
echo "Project root: $PROJECT_ROOT"
echo "Results bucket: $RESULTS_BUCKET"
echo ""

# Ensure bucket exists
aws s3 mb "$RESULTS_BUCKET" --region us-east-1 2>/dev/null || true

if [ "$CRAWL" = "all" ]; then
    CRAWLS=$(python3 -c "from pipeline.ingestion.cc_aws.config import CRAWL_IDS; print(' '.join(CRAWL_IDS))")
else
    CRAWLS="$CRAWL"
fi

echo "Crawls to process: $(echo $CRAWLS | wc -w | tr -d ' ')"
echo ""

for CRAWL_ID in $CRAWLS; do
    echo "================================================"
    echo "Processing: $CRAWL_ID"
    echo "================================================"

    # Phase 1: Query Athena index
    echo "[Phase 1] Querying CC index via Athena..."
    python3 -m pipeline.ingestion.cc_aws.query_index --crawl "$CRAWL_ID"

    # The Athena results are saved as CSV in the results bucket
    # Find the latest result file
    RESULT_FILE=$(aws s3 ls "$RESULTS_BUCKET/" --recursive | grep ".csv" | sort | tail -1 | awk '{print $4}')

    if [ -z "$RESULT_FILE" ]; then
        echo "  No results found for $CRAWL_ID, skipping"
        continue
    fi

    echo "[Phase 2] Processing WARC records..."
    python3 -m pipeline.ingestion.cc_aws.process_warcs \
        --input "${RESULTS_BUCKET}/${RESULT_FILE}"

    echo "  Done: $CRAWL_ID"
    echo ""
done

echo "================================================"
echo "All crawls processed!"
echo "================================================"
