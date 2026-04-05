<p align="center">
  <img src="logo.svg" width="80" alt="Kyiv chestnut">
</p>

<h1 align="center">#KyivNotKiev</h1>

<p align="center">
  <strong>A Large-Scale Computational Study of Ukrainian Toponym Adoption</strong><br>
  90B+ records scanned, 40M+ toponym matches, 29K ML-analyzed texts, 7 sources.
</p>

<p align="center">
  <a href="https://kyivnotkiev.org">kyivnotkiev.org</a> ·
  <a href="pipeline/README.md">Pipeline</a> ·
  <a href="pipeline/cl/README.md">Computational Linguistics</a> ·
  <a href="infrastructure/README.md">Infrastructure</a> ·
  <a href="dataset/README.md">Dataset</a> ·
  <a href="https://huggingface.co/datasets/KyivNotKiev/corpus">HuggingFace</a>
</p>

---

| Metric | Value |
|--------|-------|
| Records scanned | **90B+** across GDELT, Wikipedia, OpenAlex, Reddit, YouTube, Ngrams, Trends |
| Toponym matches | **40M+** (39.6M news articles, 573M pageviews tracked, 379K papers, 152K trends, 22K posts, 14.5K videos) |
| Toponym pairs | **55** enabled across **8** categories |
| Data sources | **7** (GDELT, Google Trends, Wikipedia, Reddit, YouTube, Google Books Ngrams, OpenAlex) |
| CL corpus | **29,938** texts, DeBERTa-v3-large F1=88.8% |
| Time span | **2010--2026** (Ngrams: 1900--2019) |
| Countries | **55** with per-country adoption data |
| Infrastructure | **GCP** (BigQuery, GCS, Cloud Run) + vast.ai GPU |
| Reproducibility | `make reproduce` -- one command, full pipeline |

## Architecture

```mermaid
graph LR
    subgraph Sources["Data Sources"]
        style Sources fill:#1a1a2e,stroke:#0057B8,color:#e2e8f0
        GDELT["GDELT<br/>39.6M articles"]
        OA["OpenAlex<br/>379K papers"]
        Reddit["Reddit<br/>22K posts"]
        Wiki["Wikipedia<br/>573M pageviews"]
        Trends["Google Trends<br/>152K datapoints"]
        Ngrams["Ngrams<br/>11.6K books 1900-2019"]
        YT["YouTube<br/>14.5K videos"]
    end

    subgraph Pipeline["Python + BigQuery Pipeline"]
        style Pipeline fill:#1a1a2e,stroke:#f59e0b,color:#e2e8f0
        Ingest["Incremental<br/>Ingestion"]
        Transform["Normalize<br/>+ Validate"]
        Analyze["Change-point<br/>Detection"]
    end

    subgraph CL["CL Pipeline"]
        style CL fill:#1a1a2e,stroke:#8b5cf6,color:#e2e8f0
        Extract["Extract<br/>29,938 texts"]
        Annotate["Llama 3.1 70B<br/>Annotation"]
        Finetune["DeBERTa-v3-large<br/>F1=88.8%"]
    end

    subgraph Storage["GCP"]
        style Storage fill:#1a1a2e,stroke:#06b6d4,color:#e2e8f0
        BQ["BigQuery<br/>warehouse"]
        GCS["Cloud Storage<br/>data lake"]
    end

    subgraph Output["Output"]
        style Output fill:#1a1a2e,stroke:#059669,color:#e2e8f0
        Figures["Figures"]
        Paper["Paper"]
        Web["Website"]
        HF["HuggingFace"]
    end

    GDELT & OA & Reddit & Wiki & Trends & Ngrams & YT --> Ingest
    Ingest --> BQ
    Ingest --> GCS
    BQ --> Transform --> Analyze
    BQ --> Extract --> Annotate --> Finetune
    Analyze --> Figures & Paper & Web
    Finetune --> HF & Paper
```

## Quick Start

```bash
uv sync
make infra
make reproduce
```

## Key Commands

| Command | What it does |
|---------|-------------|
| `make ingest` | Incremental ingestion -- skips fresh pairs |
| `make ingest-pair ID=1` | Ingest one pair across all sources |
| `make analyze` | All analysis: adoption, changepoints, regression, holdouts |
| `make figures` | Generate publication figures from BigQuery |
| `make cl-all` | Full CL pipeline: extract, balance, classify, finetune, export |
| `make status` | Show watermarks -- what's been fetched |
| `make reproduce` | Full end-to-end reproduction |

## Citation

```bibtex
@article{dobrovolskyi2026kyivnotkiev,
  title={{#KyivNotKiev}: A Large-Scale Computational Study of Ukrainian Toponym Adoption},
  author={Dobrovolskyi, Ivan},
  year={2026}
}
```
