---
language:
- en
license: cc-by-4.0
task_categories:
- text-classification
task_ids:
- topic-classification
- sentiment-classification
tags:
- linguistics
- ukraine
- toponyms
- language-policy
- kyivnotkiev
size_categories:
- 10K<n<100K
---

# KyivNotKiev Computational Linguistics Corpus

A balanced, labeled corpus of texts containing Ukrainian and Russian toponym variants
(e.g., "Kyiv" vs "Kiev"), annotated with context categories and sentiment.

## Dataset Description

- **Curated by:** Ivan Dobrovolskyi
- **Language:** Primarily English
- **License:** CC-BY 4.0
- **Paper:** [forthcoming]
- **Website:** https://kyivnotkiev.org

## Dataset Summary

29,938 texts across 55 Ukrainian-Russian toponym pairs from 4 sources
(Reddit, YouTube, GDELT news articles). Each text is labeled with:
- **Context category**: politics, war_conflict, sports, culture_arts, food_cuisine, travel_tourism, academic_science, history, business_economy, general_news
- **Sentiment**: positive, neutral, negative
- **Variant**: which toponym form (russian/ukrainian) appears in the text

## Intended Uses

- Studying language policy adoption in media and social platforms
- Training toponym context classifiers
- Analyzing sentiment differences between spelling variants
- Cross-source and temporal analysis of naming conventions

## Dataset Structure

### Data Fields
- `pair_id`: Integer ID of the toponym pair
- `text`: The full text content
- `variant`: "russian" or "ukrainian" — which spelling form appears
- `source`: Data source (reddit, youtube, gdelt)
- `year`: Publication year
- `context_label`: Annotated context category
- `context_confidence`: Annotation confidence (0-1)
- `sentiment_label`: Sentiment annotation
- `sentiment_score`: Sentiment score (-1 to 1)
- `word_count`: Number of words in text
- `matched_term`: The specific toponym form found in text

### Splits
| Split | Count |
|-------|-------|
| train | 23,950 |
| validation | 2,993 |
| test | 2,993 |

## Balance Report

See `balance_report.json` for detailed per-pair, per-source, per-variant distributions
and documented shortfalls.

## Collection Methodology

1. **Reddit**: Titles and bodies from Arctic Shift API + Reddit search (2010-2026)
2. **YouTube**: Video titles and descriptions via yt-dlp (2010-2026)
3. **GDELT**: News article bodies fetched from URLs using trafilatura (2010-2026)
4. **Balancing**: Stratified sampling by pair × source × variant × year stratum
5. **Annotation**: Llama 3.1 70B-Instruct with human validation on 200 random samples
6. **Fetch transparency**: All GDELT URL fetch attempts logged in `fetch_log.parquet`

## Citation

```bibtex
@article{dobrovolskyi2026kyivnotkiev,
  title={#KyivNotKiev: A Large-Scale Computational Study of Ukrainian Toponym Adoption},
  author={Dobrovolskyi, Ivan},
  year={2026}
}
```
