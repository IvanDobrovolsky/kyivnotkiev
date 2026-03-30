# Pipeline

Incremental data pipeline on GCP. Reads `config/pairs.yaml`, checks watermarks, fetches only what's new.

## Flow

```mermaid
flowchart TD
    subgraph Config
        style Config fill:#1a1a2e,stroke:#e6b800,color:#fff
        pairs["pairs.yaml<br/>71 pairs, 8 categories"]
    end

    subgraph Ingestion
        style Ingestion fill:#16213e,stroke:#e6b800,color:#fff
        orch["orchestrator.py"]
        wm["watermarks.py<br/>skip if fresh"]
        gdelt["gdelt.py<br/>BQ → BQ"]
        cc["common_crawl.py<br/>PySpark on Dataproc"]
        reddit["reddit.py<br/>Arctic Shift"]
        wiki["wikipedia.py<br/>Pageviews API"]
        trends["trends.py<br/>pytrends"]
        ngrams["ngrams.py<br/>Books API"]
        yt["youtube.py<br/>Data API v3"]
    end

    subgraph Analysis
        style Analysis fill:#0f3460,stroke:#e6b800,color:#fff
        adopt["adoption.py<br/>ratios per pair"]
        cp["changepoint.py<br/>PELT + bootstrap CIs"]
        cat["categories.py<br/>Kruskal-Wallis"]
        hold["holdouts.py<br/>who still uses old spellings"]
        reg["regression.py<br/>logistic model"]
    end

    subgraph Figures
        style Figures fill:#1a1a2e,stroke:#e6b800,color:#fff
        cross["crossover plots"]
        heat["heatmaps"]
        choro["choropleths"]
        modern["dashboard figures"]
    end

    pairs --> orch
    orch --> wm
    wm -->|stale| gdelt & cc & reddit & wiki & trends & ngrams & yt
    wm -->|fresh| skip["skip"]
    gdelt & cc & reddit & wiki & trends & ngrams & yt -->|BigQuery| adopt
    adopt --> cp & cat & hold & reg
    cp & cat & hold & reg --> cross & heat & choro & modern
```

## Modules

### `ingestion/`

| Module | Source | Scale | Method |
|--------|--------|-------|--------|
| `gdelt.py` | GDELT GKG | 42B articles | BQ public → BQ (SQL) |
| `common_crawl.py` | Common Crawl WARC | TB-scale | PySpark on Dataproc |
| `reddit.py` | Reddit via Arctic Shift | 50M+ comments | Spark on zst dumps |
| `wikipedia.py` | Wikimedia API | Pageviews + edits | REST API |
| `trends.py` | Google Trends | Weekly interest | pytrends |
| `ngrams.py` | Google Books | 1800–2019 | REST API |
| `youtube.py` | YouTube Data API v3 | Video metadata | REST API |
| `orchestrator.py` | — | — | Coordinates all above |
| `watermarks.py` | — | — | Tracks freshness per (pair, source) |

### `analysis/`

| Module | What | Statistical Method |
|--------|------|-------------------|
| `adoption.py` | Ukrainian spelling ratio over time | Count-based |
| `changepoint.py` | When did the shift happen? | PELT, bootstrap 95% CIs |
| `categories.py` | Do categories differ? | Kruskal-Wallis H, pairwise Mann-Whitney |
| `holdouts.py` | Who still uses old spellings? | Domain-level aggregation |
| `regression.py` | What predicts adoption speed? | Logistic regression |
| `events.py` | Impact of geopolitical events | Pre/post comparison |

### `figures/`

| Module | Output |
|--------|--------|
| `crossover.py` | Per-pair adoption curves with crossover dates |
| `heatmap.py` | All pairs × time heatmap |
| `choropleth.py` | Geographic adoption maps |
| `category_curves.py` | Category-level trend comparison |
| `event_overlay.py` | Event markers on timelines |
| `modern.py` | Publication-ready dashboard figures |

## Incremental Design

```
Pair added in pairs.yaml    → fetched across all 7 sources
Pair disabled               → excluded from analysis, data preserved in BQ
Pair already fresh          → skipped (watermark < 7 days old)
```

No data is ever deleted. Disabling a pair just filters it from analysis views.
