# Common Crawl Processing -- AWS Pipeline

> **DEPRECATED**: This pipeline has been replaced by [OpenAlex](../../README.md) (`make ingest-openalex`) for academic/web text coverage. Kept for historical reference only.

Scans Common Crawl web archives (2013--2026) for Ukrainian toponym mentions.

## Architecture

```
CC Index (Parquet on S3)  ->  Athena queries  ->  Candidate URLs (CSV)
                                                       |
WARC files (S3)  <-  HTTP range requests  <-  EC2 spot instance
                                                       |
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

1. Configure AWS CLI: `aws configure` (region=us-east-1)
2. Create S3 results bucket: `aws s3 mb s3://kyivnotkiev-cc-results --region us-east-1`
3. Create Athena table (see `setup_athena.sql`)
4. Set up GCP credentials: `gcloud auth application-default login`

## Usage

```bash
# Run on EC2 spot instance (recommended, self-terminates when done)
./launch_spot.sh

# Run locally (slower, cross-region S3 reads)
./run.sh                     # All 28 crawls
./run.sh CC-MAIN-2024-10     # One crawl
```

## Crawl Coverage

28 crawls selected (2 per year, 2013--2026):
- **2013--2018**: Historical baseline (pre-#KyivNotKiev)
- **2019**: Campaign year
- **2022**: Full-scale invasion
- **2023--2026**: Post-invasion adoption tracking

See also: [../../README.md](../../README.md) | [../../../README.md](../../../README.md)
