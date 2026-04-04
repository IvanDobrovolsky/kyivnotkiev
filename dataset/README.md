---
license: cc-by-4.0
task_categories:
  - text-classification
language:
  - en
tags:
  - linguistics
  - sociolinguistics
  - language-policy
  - ukraine
  - toponymy
  - corpus-linguistics
size_categories:
  - 1M<n<10M
---

# KyivNotKiev Dataset

**The world's largest computational dataset tracking how English-language media adopts Ukrainian spellings.**

Part of the research project: [kyivnotkiev.org](https://kyivnotkiev.org)

## Dataset Description

This dataset measures the adoption of Ukrainian-derived English spellings (e.g., "Kyiv" instead of "Kiev") across 7 independent data sources spanning 2010-2026. It covers 55 toponym pairs across 8 categories.

## Sources

| Source | Rows | Description | Time Range |
|--------|------|-------------|------------|
| Google Trends | 151,538 | Search interest (global + 55 countries) | 2010-2026 |
| GDELT | 39,589,666 | News article mentions | 2015-2026 |
| Wikipedia | 14,952 | Article pageviews | 2015-2026 |
| Reddit | 22,556 | Post mentions | 2007-2026 |
| YouTube | 14,515 | Video title mentions | 2006-2026 |
| Google Books Ngrams | 11,640 | Book frequency | 1900-2019 |
| OpenAlex | 689 | Academic paper mentions | 2010-2026 |

**Total: 39,805,556 rows**

## Files

- `raw_trends.parquet` — Google Trends data (pair_id, date, term, variant, interest, geo)
- `raw_gdelt.parquet` — GDELT news mentions (pair_id, date, source_domain, variant, count)
- `raw_wikipedia.parquet` — Wikipedia pageviews (pair_id, date, page_title, variant, pageviews)
- `raw_reddit.parquet` — Reddit posts (pair_id, date, subreddit, variant, score)
- `raw_youtube.parquet` — YouTube videos (pair_id, date, channel_title, variant, view_count)
- `raw_ngrams.parquet` — Google Books frequency (pair_id, year, term, variant, frequency)
- `openalex.parquet` — Academic papers (pair_id, year, russian_count, ukrainian_count)
- `pairs.json` — Toponym pair definitions with categories
- `analysis.json` — Statistical test results
- `metadata.json` — Dataset metadata

## Categories

| Category | Pairs | Example |
|----------|-------|---------|
| Geographical | 24 | Kiev → Kyiv |
| Food & Cuisine | 5 | Chicken Kiev → Chicken Kyiv |
| Landmarks | 6 | Kiev Pechersk Lavra → Kyiv Pechersk Lavra |
| Country-Level | 1 | the Ukraine → Ukraine |
| Institutional | 6 | Kiev National University → Kyiv National University |
| Sports | 5 | Dynamo Kiev → Dynamo Kyiv |
| Historical | 6 | Vladimir the Great → Volodymyr the Great |
| People | 2 | Vladimir Zelensky → Volodymyr Zelenskyy |

## Privacy

- GDELT: domain-level aggregation only (no raw article URLs)
- Reddit: no post body text, only metadata (subreddit, date, score)
- YouTube: no video URLs, only channel name and metadata

## Citation

```bibtex
@article{dobrovolskyi2026kyivnotkiev,
  title={Measuring Real-Time Toponymic Change: A Multi-Source Computational Framework for Tracking Ukrainian Spelling Adoption},
  author={Dobrovolskyi, Ivan},
  journal={Computational Linguistics},
  year={2026},
  publisher={MIT Press}
}
```

## License

CC-BY-4.0
