# Pipeline

Incremental data pipeline. Reads `config/pairs.yaml`, checks watermarks, fetches only what's new. Data published to HuggingFace.

## Flow

```mermaid
flowchart TD
    subgraph Config
        style Config fill:#1a1a2e,stroke:#0057B8,color:#e2e8f0
        pairs["pairs.yaml<br/>59 pairs, 8 categories"]
    end

    subgraph Ingestion
        style Ingestion fill:#1a1a2e,stroke:#f59e0b,color:#e2e8f0
        orch["orchestrator.py"]
        wm["watermarks.py<br/>skip if fresh"]
        gdelt["gdelt.py<br/>BQ → BQ"]
        reddit["reddit.py<br/>Arctic Shift"]
        wiki["wikipedia.py<br/>Pageviews API"]
        trends["trends.py<br/>pytrends"]
        ngrams["ngrams.py<br/>Books API"]
        yt["youtube.py<br/>Data API v3"]
        oa["openalex.py<br/>REST API"]
        ol["openlibrary.py<br/>REST API"]
    end

    subgraph Analysis
        style Analysis fill:#1a1a2e,stroke:#8b5cf6,color:#e2e8f0
        adopt["adoption.py<br/>ratios per pair"]
        cp["changepoint.py<br/>PELT + bootstrap CIs"]
        cat["categories.py<br/>Kruskal-Wallis"]
        hold["holdouts.py<br/>who still uses old spellings"]
        reg["statistical_tests.py<br/>OLS regression"]
    end

    subgraph CL["CL Pipeline"]
        style CL fill:#1a1a2e,stroke:#06b6d4,color:#e2e8f0
        extract["extract texts"]
        annotate["Llama 3.1 70B annotation"]
        finetune["XLM-RoBERTa-large fine-tuning"]
        export["HF dataset + model"]
    end

    subgraph Figures
        style Figures fill:#1a1a2e,stroke:#059669,color:#e2e8f0
        cross["crossover plots"]
        heat["heatmaps"]
        choro["choropleths"]
        modern["dashboard figures"]
    end

    pairs --> orch
    orch --> wm
    wm -->|stale| gdelt & reddit & wiki & trends & ngrams & yt & oa & ol
    wm -->|fresh| skip["skip"]
    gdelt & reddit & wiki & trends & ngrams & yt & oa & ol --> adopt
    adopt --> cp & cat & hold & reg
    adopt -->|80,141 texts| extract --> annotate --> finetune --> export
    cp & cat & hold & reg --> cross & heat & choro & modern
```

## Modules

### `ingestion/`

| Module | Source | Scale | Method |
|--------|--------|-------|--------|
| `gdelt.py` | GDELT GKG | 39.6M articles | BQ public dataset (SQL) |
| `reddit.py` | Reddit via Arctic Shift | 22.6K posts | Spark on zst dumps |
| `wikipedia.py` | Wikimedia API | 573M pageviews | REST API |
| `trends.py` | Google Trends | 151K datapoints | pytrends |
| `ngrams.py` | Google Books | 11.6K, 1900--2019 | REST API |
| `youtube.py` | YouTube Data API v3 | 14.5K videos | REST API |
| `openalex.py` | OpenAlex | 381K papers | REST API |
| `openlibrary.py` | Open Library | 1.9K titles | REST API |
| `orchestrator.py` | -- | -- | Coordinates all above |
| `watermarks.py` | -- | -- | Tracks freshness per (pair, source) |

### `cl/`

Transformer-based discourse analysis. See [cl/README.md](cl/README.md).

### `analysis/`

| Module | What | Statistical Method |
|--------|------|-------------------|
| `adoption.py` | Ukrainian spelling ratio over time | Count-based |
| `changepoint.py` | When did the shift happen? | PELT, bootstrap 95% CIs |
| `categories.py` | Do categories differ? | Kruskal-Wallis H, pairwise Mann-Whitney |
| `holdouts.py` | Who still uses old spellings? | Domain-level aggregation |
| `statistical_tests.py` | What predicts adoption speed? | OLS regression (nested models) |
| `events.py` | Impact of geopolitical events | Pre/post comparison |

### `figures/`

| Module | Output |
|--------|--------|
| `crossover.py` | Per-pair adoption curves with crossover dates |
| `heatmap.py` | All pairs x time heatmap |
| `choropleth.py` | Geographic adoption maps |
| `category_curves.py` | Category-level trend comparison |
| `event_overlay.py` | Event markers on timelines |
| `modern.py` | Publication-ready dashboard figures |

## Incremental Design

No data is ever deleted. Disabling a pair just filters it from analysis views.

- Pair added in `pairs.yaml` -> fetched across all 8 sources
- Pair disabled -> excluded from analysis, data preserved locally
- Pair already fresh -> skipped (watermark < 7 days old)

See also: [../README.md](../README.md) | [cl/README.md](cl/README.md) | [../infrastructure/README.md](../infrastructure/README.md)
