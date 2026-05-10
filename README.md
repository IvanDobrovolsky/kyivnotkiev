<p align="center">
  <img src="logo.svg" width="80" alt="Kyiv chestnut">
</p>

<h1 align="center">#KyivNotKiev</h1>

<p align="center">
  <strong>A Large-Scale Computational Study of Ukrainian Toponym Adoption</strong><br>
  Large-scale data analysis and machine learning across 9 independent sources.
</p>

<p align="center">
  <a href="https://kyivnotkiev.org">kyivnotkiev.org</a> ·
  <a href="https://huggingface.co/datasets/KyivNotKiev/toponym-adoption-data">Dataset</a> ·
  <a href="https://www.bbc.com/ukrainian/news-45718643">BBC Coverage</a>
</p>

---

| Metric | Value |
|--------|-------|
| Records scanned | **90B+** |
| Toponym matches | **359M** |
| Toponym pairs | **58** |
| Data sources | **9** (News, Trends, Wiki, Reddit, YouTube, Books, Academic, Telegram, Religious) |
| Time span | **2010–2026** (Books: 2000–2022) |
| FP rate | **0.9%** (verified on 19,250 samples) |
| CL corpus | **93K** verified English texts |

## Data Sources

| Source | Records | Description |
|--------|---------|-------------|
| News (GDELT) | 38.5M | URL-level toponym matching in global news articles |
| Trends | 180K | Google search interest across 55 countries |
| Wiki | 320M | Wikipedia pageviews for spelling-variant redirects |
| Reddit | 21.7K | Posts and comments from r/ukraine, r/worldnews, r/europe |
| YouTube | 21.7K | Video titles and descriptions |
| Books (Ngrams) | 12.2K | Google Books corpus 37 (English 2022) |
| Academic (OpenAlex) | 381K | Scholarly paper titles and abstracts |
| Telegram | 38.7K | Public channel messages (177 channels, 118-term systematic search) |
| Religious | 3.6K | Moscow Patriarchate, WCC, Constantinople, Vatican press |

All source data is on HuggingFace: [`KyivNotKiev/toponym-adoption-data`](https://huggingface.co/datasets/KyivNotKiev/toponym-adoption-data)

## Pipeline

```
pairs.yaml (58 pairs, matching rules)
    ↓
Ingestion scripts (pipeline/ingestion/)
    ↓
Post-filter (pipeline/analysis/post_filter.py — homonym + NER disambiguation)
    ↓
Export (pipeline/export_site_data.py → site/src/data/*.json)
    ↓
Statistical tests (pipeline/analysis/recompute_stats.py)
    ↓
Site (Astro static build → Cloudflare Pages)
```

### Key Scripts

| Script | Purpose |
|--------|---------|
| `pipeline/ingestion/gdelt_stream.py` | GDELT GKG URL scanning |
| `pipeline/ingestion/gdelt_fetch_articles.py` | Article body extraction via trafilatura |
| `pipeline/ingestion/gdelt_athena_countries.py` | Per-country adoption from GDELT domains |
| `pipeline/ingestion/trends.py` | Google Trends collection |
| `pipeline/ingestion/wikipedia.py` | Wikipedia pageview tracking |
| `pipeline/ingestion/reddit.py` | Reddit search + Arctic Shift historical |
| `pipeline/ingestion/youtube_ytdlp.py` | YouTube via yt-dlp (no API key) |
| `pipeline/ingestion/ngrams.py` | Google Books Ngrams |
| `pipeline/ingestion/telegram_search.py` | Systematic Telegram channel discovery |
| `pipeline/ingestion/religious.py` | Religious institution scraping |
| `pipeline/analysis/post_filter.py` | Unified disambiguation (regex + NER) |
| `pipeline/analysis/recompute_stats.py` | Statistical tests (KW, Wilcoxon, OLS) |
| `pipeline/analysis/statistical_tests.py` | Bootstrap CIs, Pettitt changepoints |
| `pipeline/export_site_data.py` | Generate site JSON from parquets |
| `pipeline/colab/fetch_youtube_transcripts.ipynb` | Colab notebook for YouTube transcripts |

## Site

4 pages built with Astro, deployed on Cloudflare Pages:
- `/` — Pair card grid with sort/filter
- `/pair/:id` — Per-pair detail with 9-source charts
- `/llm` — AI audit (72 LLMs, TAS heatmap)
- `/sources` — Data source descriptions
- `/methodology` — Statistical analysis, confusion matrix, benchmarks

## Reproducibility

Source data is on HuggingFace (9 parquets). The pipeline reads from local `dataset/` parquets which mirror HF. To reproduce:

```bash
# Download data from HuggingFace
python -c "
from huggingface_hub import snapshot_download
snapshot_download('KyivNotKiev/toponym-adoption-data', repo_type='dataset', local_dir='dataset/')
"

# Generate site data
python -m pipeline.export_site_data

# Build site
cd site && npm install && npm run build
```

## Citation

```bibtex
@article{dobrovolskyi2026kyivnotkiev,
  title={{#KyivNotKiev}: A Large-Scale Computational Study of Ukrainian Toponym Adoption},
  author={Dobrovolskyi, Ivan},
  year={2026}
}
```
