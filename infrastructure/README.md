# Infrastructure

GCP infrastructure managed by Terraform. One command deploys everything.

## Resources

```mermaid
graph TD
    subgraph GCP["GCP: kyivnotkiev-research"]
        style GCP fill:#1a1a2e,stroke:#0057B8,color:#e2e8f0

        subgraph Storage
            style Storage fill:#1a1a2e,stroke:#f59e0b,color:#e2e8f0
            BQ["BigQuery<br/>kyivnotkiev dataset"]
            GCS["Cloud Storage<br/>kyivnotkiev-research-data"]
            AR["Artifact Registry<br/>Docker images"]
        end

        subgraph Compute["Compute (on demand)"]
            style Compute fill:#1a1a2e,stroke:#8b5cf6,color:#e2e8f0
            DP["Dataproc<br/>Spark cluster 2-8 nodes"]
            CR["Cloud Run<br/>orchestrator"]
        end

        subgraph IAM
            style IAM fill:#1a1a2e,stroke:#06b6d4,color:#e2e8f0
            SA["Service Account<br/>kyivnotkiev-pipeline"]
        end
    end

    BQ --> DP
    GCS --> DP
    SA --> BQ & GCS & DP & CR
```

## BigQuery Tables

| Table | Partitioned | Clustered | Description |
|-------|------------|-----------|-------------|
| `raw_gdelt` | DAY (date) | pair_id, variant | News media mentions (39.6M) |
| `raw_reddit` | MONTH (created_utc) | pair_id, variant, subreddit | Reddit posts/comments (22K) |
| `raw_wikipedia` | MONTH (date) | pair_id, variant | Pageviews (573M) |
| `raw_trends` | -- | pair_id, variant | Google Trends interest (152K) |
| `raw_ngrams` | -- | pair_id, variant | Book frequency 1900--2019 (11.6K) |
| `raw_youtube` | MONTH (published_at) | pair_id, variant | Video metadata (14.5K) |
| `raw_openalex` | -- | pair_id, variant | Academic papers (379K) |
| `watermarks` | -- | -- | Ingestion state tracking |
| `analysis_adoption` | -- | pair_id, source | Computed adoption ratios |
| `analysis_changepoints` | -- | -- | Detected change points |
| `v_cross_source` | -- | -- | View: cross-source comparison |
| `v_latest_adoption` | -- | -- | View: most recent ratios |

## Deploy

```bash
make infra            # Deploy
make infra-plan       # Preview changes
make infra-destroy    # Tear down
```

## Deferred Resources

Files ending in `.tf.deferred` are not deployed by default (cost control):

| File | Resource | When to enable |
|------|----------|---------------|
| `dataproc.tf.deferred` | Spark cluster (4-8 workers) | Before Reddit bulk jobs |
| `cloud_run.tf.deferred` | Orchestrator service | After Docker image is built |

Rename to `.tf` and run `make infra` to deploy.

## Cost

| Resource | Monthly Cost |
|----------|-------------|
| BigQuery storage (~10GB) | ~$0.20 |
| BigQuery queries (free tier 1TB) | $0 |
| GCS storage (~200GB) | ~$4 |
| Dataproc (on-demand, ~4hrs) | ~$20-50 one-time |
| **Total steady-state** | **~$5/month** |

See also: [../README.md](../README.md) | [../pipeline/README.md](../pipeline/README.md)
