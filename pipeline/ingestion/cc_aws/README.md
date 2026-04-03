# Common Crawl Processing — AWS Pipeline

Scans Common Crawl web archives (2013–2026) for Ukrainian toponym mentions across all 54 enabled pairs.

## Architecture

```
CC Index (Parquet on S3)  →  Athena queries  →  Candidate URLs (CSV)
                                                       ↓
WARC files (S3)  ←  HTTP range requests  ←  EC2 spot instance
                                                       ↓
                                              BigQuery (matches)
```

## Cost Estimate

| Component | Cost |
|-----------|------|
| Athena index queries (28 crawls) | ~$5-10 |
| EC2 spot c5.xlarge (~100-300 hrs) | ~$5-15 |
| S3 requests | ~$1-2 |
| **Total** | **~$15-25** |

## Setup (one-time)

1. **Create AWS account** (free tier)

2. **Configure CLI:**
   ```bash
   aws configure  # enter access key, secret, region=us-east-1
   ```

3. **Create S3 results bucket:**
   ```bash
   aws s3 mb s3://kyivnotkiev-cc-results --region us-east-1
   ```

4. **Create Athena table** (run in Athena console):
   ```sql
   -- Copy contents of setup_athena.sql
   ```

5. **Set up GCP credentials** (for BigQuery writes):
   ```bash
   gcloud auth application-default login
   ```

## Usage

### Option A: Run on EC2 spot instance (recommended)
```bash
./launch_spot.sh
```
Self-terminates when done. Monitor via:
```bash
aws ec2 describe-instances --filters 'Name=tag:Name,Values=kyivnotkiev-cc-processor'
aws s3 ls s3://kyivnotkiev-cc-results/logs/
```

### Option B: Run locally (slower, cross-region S3 reads)
```bash
# Process all 28 crawls
./run.sh

# Process one crawl
./run.sh CC-MAIN-2024-10
```

### Option C: Step by step
```bash
# 1. Query index
python -m pipeline.ingestion.cc_aws.query_index --crawl CC-MAIN-2024-10

# 2. Process WARC records
python -m pipeline.ingestion.cc_aws.process_warcs --input s3://kyivnotkiev-cc-results/result.csv

# 3. Dry run (test without BQ writes)
python -m pipeline.ingestion.cc_aws.process_warcs --input result.csv --limit 100 --dry-run
```

## Crawl Coverage

28 crawls selected (2 per year, 2013–2026):
- **2013–2018**: Historical baseline (pre-#KyivNotKiev)
- **2019**: Campaign year
- **2022**: Full-scale invasion
- **2023–2026**: Post-invasion adoption tracking

## Domain Filtering

~100 curated news/media domains including:
- Major English news (BBC, CNN, NYT, Reuters, AP, Guardian, etc.)
- Ukrainian English media (Kyiv Independent, Ukrinform, etc.)
- Travel & food sites (for food/landmark pairs)
- Sports sites (for sports pairs)
- Plus all `.ua` TLD pages
