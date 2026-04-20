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

# #KyivNotKiev: A Large-Scale Computational Study of Ukrainian Toponym Adoption -- Dataset

**The world's largest computational dataset tracking how English-language media adopts Ukrainian spellings.**

Part of the research project: [kyivnotkiev.org](https://kyivnotkiev.org)

## Dataset Description

This dataset measures the adoption of Ukrainian-derived English spellings (e.g., "Kyiv" instead of "Kiev") across 8 independent data sources spanning 2010--2026. It covers 59 toponym pairs across 8 categories with 628M+ toponym matches from 90B+ scanned records.

## Sources

| Source | Records | Description | Time Range |
|--------|---------|-------------|------------|
| GDELT | 38.6M | News article mentions | 2015--2026 |
| Wikipedia | 589M | Article pageviews | 2015--2026 |
| Google Trends | 206K | Search interest (global + 55 countries) | 2010--2026 |
| OpenAlex | 381K | Academic paper mentions | 2010--2026 |
| Reddit | 33K | Post mentions | 2007--2026 |
| YouTube | 33K | Video title mentions | 2006--2026 |
| Google Books Ngrams | 13K | Book frequency | 1900--2019 |
| Open Library | 1.9K | Book title mentions | 1900--2025 |

## Files

- `raw_gdelt.parquet` -- GDELT news mentions (pair_id, date, source_domain, variant, count)
- `raw_trends.parquet` -- Google Trends data (pair_id, date, term, variant, interest, geo)
- `raw_wikipedia.parquet` -- Wikipedia pageviews (pair_id, date, page_title, variant, pageviews)
- `raw_reddit.parquet` -- Reddit posts (pair_id, date, subreddit, variant, score)
- `raw_youtube.parquet` -- YouTube videos (pair_id, date, channel_title, variant, view_count)
- `raw_ngrams.parquet` -- Google Books frequency (pair_id, year, term, variant, frequency)
- `openalex.parquet` -- Academic papers (pair_id, year, russian_count, ukrainian_count)
- `pairs.json` -- Toponym pair definitions with categories
- `analysis.json` -- Statistical test results
- `metadata.json` -- Dataset metadata

## Categories

| Category | Pairs | Example |
|----------|-------|---------|
| Geographical | 26 | Kiev -> Kyiv |
| Food & Cuisine | 4 | Borsch -> Borscht |
| Landmarks | 7 | Kiev Pechersk Lavra -> Kyiv Pechersk Lavra |
| Country-Level | 0 | *(disabled)* |
| Institutional | 6 | Kiev National University -> Kyiv National University |
| Sports | 3 | Dynamo Kiev -> Dynamo Kyiv |
| Historical | 6 | Vladimir the Great -> Volodymyr the Great |
| People | 7 | Vladimir Zelensky -> Volodymyr Zelenskyy |

## Privacy

- GDELT: domain-level aggregation only (no raw article URLs)
- Reddit: no post body text, only metadata (subreddit, date, score)
- YouTube: no video URLs, only channel name and metadata

## License

CC-BY-4.0
