# KyivNotKiev: Computational Analysis of Global Ukrainian Toponym Adoption

## Paper Title

**"#KyivNotKiev: A Computational Analysis of Global English-Language Adoption of Ukrainian Toponyms (2015-2026)"**

## Abstract

This study presents the first large-scale computational analysis of how English-language usage of Ukrainian place names has shifted from Russian-derived to Ukrainian-derived spellings over a 11-year period (2015-2026). Using Google Trends data, change-point detection algorithms, and geographic diffusion modeling, we analyze 40+ toponym pairs across 7 categories: geographical, food & cuisine, landmarks & heritage, country-level framing, institutional, sports & entertainment, and historical & ethnographic. We identify a three-wave adoption pattern driven by the #KyivNotKiev campaign (2018), media style guide changes (2019), and the full-scale Russian invasion (2022), and demonstrate that adoption speed varies significantly by category, with geographical names shifting fastest and food terms showing the greatest resistance to change.

## Research Gap

- Existing studies focus on toponymic changes _within_ Ukraine (street renaming, decommunization)
- The #KyivNotKiev campaign is well-documented journalistically but not computationally analyzed
- No publication applies change-point detection, geographic diffusion modeling, or category-level analysis to the _international_ adoption of Ukrainian toponyms in English
- This is a novel contribution to computational sociolinguistics

### Key existing work (not overlapping):
- Gnatiuk & Melnychuk (2023) -- de-Russification of Ukrainian street names (domestic focus)
- Onomastica NLP study -- transformer-based classification of 23,536 rural hodonym types (classification, not adoption tracking)
- Transparent Cities database -- 7,800 renamed toponyms across 83 Ukrainian cities (domestic administrative tracking)
- Google Ngrams -- can show Kiev/Kyiv frequency in books, but no systematic analysis with change-point detection has been published

## Research Questions

**RQ1:** When did the crossover from Russian-derived to Ukrainian-derived English spellings occur for each toponym, and what geopolitical events triggered them?

**RQ2:** How does adoption speed vary across the 7 toponym categories?

**RQ3:** What is the geographic diffusion pattern -- which countries adopted Ukrainian spellings first, and which still lag?

**RQ4:** Is there a measurable correlation between specific geopolitical events and adoption acceleration?

## Data Sources (Multi-Source Strategy)

Three complementary data sources provide triangulated evidence across different dimensions of toponym adoption.

### Source 1: GDELT (Primary -- News Media Usage)

**Global Database of Events, Language and Tone** -- the largest open database of world news.

- **Scale:** 42 billion words of news content, 152 languages, updated every 15 minutes
- **Coverage:** 2015-2026, print + broadcast + web news media worldwide
- **Access:** Google BigQuery (1TB/month free tier)
- **Key field:** `Location` stores the exact spelling used in the article text (e.g., "Kiev" vs "Kyiv"), while `FeatureID` links different spellings to the same place
- **Strengths:** Absolute counts (not relative), full text search, source metadata (country, language, outlet), massive scale
- **Query approach:** Count articles mentioning each spelling variant per week, per source country
- **Cost:** Free (within BigQuery free tier; partition queries by date to stay under 1TB/month)

### Source 2: Google Trends (Secondary -- Public Search Interest)

**Google Trends** -- worldwide, English language, 2015-2026

- **Scale:** Relative search interest (0-100 scale), weekly granularity
- **Coverage:** 11-year window with geographic breakdown by country
- **Access:** `pytrends` (unofficial API) with rate limiting + retry logic, or SerpApi ($50-400/mo for scale)
- **Strengths:** Measures public awareness/interest, not just media usage; geographic granularity
- **Limitations:** Relative scale (not absolute counts), max 5 terms per comparison, aggressive rate limiting on free tier
- **Fallback:** Manual CSV download from Google Trends web interface
- **Pairs with zero volume:** Document as "below detection threshold"

### Source 3: Google Books Ngram Viewer (Tertiary -- Published Books)

**Google Ngrams** -- English-language published books corpus

- **Scale:** Millions of books, 1500-2022
- **Coverage:** Long historical baseline for academic/publishing adoption
- **Access:** Free, bulk download available
- **Strengths:** Shows adoption in formal/published contexts; long time horizon
- **Limitations:** Ends at 2022, significant publication lag, books ≠ current usage
- **Use case:** Historical context + academic adoption angle

### Why multi-source?

| Dimension | GDELT | Google Trends | Ngrams |
|---|---|---|---|
| What it measures | Media usage | Public search interest | Published book usage |
| Scale | Absolute counts | Relative (0-100) | Relative frequency |
| Time range | 2015-2026 | 2015-2026 | 1500-2022 |
| Geographic granularity | Source country | Searcher country | None |
| Update frequency | 15 minutes | Weekly | Yearly |
| Best for | Primary analysis, RQ1-4 | Public awareness (RQ3) | Historical context |

Triangulation across sources strengthens findings and is more publishable than single-source analysis.

---

## Categories & Toponym Pairs

### Category 1: Geographical

All cities (major, conflict zone, secondary), rivers, regions, and administrative areas.

#### Major Cities
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 1 | Kiev | Kyiv | Capital -- the flagship case |
| 2 | Kharkov | Kharkiv | Second largest city, major battle site |
| 3 | Odessa | Odesa | Major port, cultural capital |
| 4 | Lvov | Lviv | Western cultural center |
| 5 | Zaporozhye | Zaporizhzhia | Nuclear plant, frontline city |
| 6 | Nikolaev | Mykolaiv | Southern city |
| 7 | Dnepropetrovsk | Dnipro | Renamed city (also decommunization) |
| 8 | Vinnitsa | Vinnytsia | Central Ukraine |
| 9 | Rovno | Rivne | Western Ukraine |
| 10 | Chernobyl | Chornobyl | Nuclear disaster site |

#### Conflict Zone Cities
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 11 | Lugansk | Luhansk | Occupied since 2014 |
| 12 | Donetsk | Donetsk | Control case -- same in both languages |
| 13 | Mariupol | Mariupol | Control case -- massive 2022 visibility |
| 14 | Kherson | Kherson | Control case -- same in both |

#### Rivers & Geographic Features
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 15 | Dnieper | Dnipro | Major river through Kyiv |
| 16 | Dniester | Dnister | Western Ukraine river |

#### Regions & Administrative
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 17 | Donbass | Donbas | Spelling simplification |
| 18 | Crimea | Krym | Contested peninsula |
| 19 | Transcarpathia | Zakarpattia | Western region |
| 20 | Podolia | Podillia | Historical region |

### Category 2: Food & Cuisine
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 21 | Chicken Kiev | Chicken Kyiv | Most globally known food item |
| 22 | Kiev cake | Kyiv cake | Traditional dessert |
| 23 | Borscht | Borshch | UNESCO-listed, cultural ownership dispute |

### Category 3: Landmarks & Heritage Sites
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 24 | Kiev Pechersk Lavra | Kyiv Pechersk Lavra | UNESCO World Heritage Site |
| 25 | Saint Sophia Cathedral Kiev | Saint Sophia Cathedral Kyiv | UNESCO site |
| 26 | Chernobyl Exclusion Zone | Chornobyl Exclusion Zone | Historical/tourism landmark |

### Category 4: Country-Level Framing
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 27 | the Ukraine | Ukraine | Definite article removal = sovereignty |

### Category 5: Institutional
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 28 | Kiev National University | Kyiv National University | Taras Shevchenko University |
| 29 | Kharkov University | Kharkiv University | Karazin University |
| 30 | Kiev Polytechnic | Kyiv Polytechnic | Igor Sikorsky KPI |
| 31 | Kiev Patriarchate | Kyiv Patriarchate | Orthodox church split |

### Category 6: Sports & Entertainment
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 32 | Dynamo Kiev | Dynamo Kyiv | Champions League visibility |
| 33 | Shakhtar Donetsk | Shakhtar Donetsk | Control -- unchanged |
| 34 | Kiev ballet | Kyiv ballet | Cultural export |

### Category 7: Historical & Ethnographic
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 35 | Kievan Rus | Kyivan Rus | Historical state -- identity claim |
| 36 | Cossack | Kozak | Ethno-historical term |
| 37 | Little Russia | Ukraine | Colonial vs sovereign framing |

**Total: 37 toponym pairs across 7 categories**

---

## Methodology

### Step 1: Data Collection

#### 1a: GDELT via BigQuery (primary)
- Query GDELT GKG (Global Knowledge Graph) for article counts mentioning each spelling variant
- Aggregate by week, by source country, by source language (filter to English)
- Use `_PARTITIONTIME` to limit scan costs within free tier
- Store results as Parquet files per toponym pair

#### 1b: Google Trends via pytrends (secondary)
- Pull weekly interest data for each pair: worldwide + by country (30+ countries)
- Rate limiting: 1 request per 10 seconds, exponential backoff on 429s
- 11-year window: January 2015 -- March 2026
- Store raw data as CSV per toponym pair
- Pairs with zero search volume documented as "below detection threshold"

#### 1c: Google Ngrams (tertiary)
- Download pre-built ngram datasets for toponym pairs
- Extract yearly frequency for each spelling variant
- Use as historical baseline context

### Step 2: Change-Point Detection

For each toponym pair, apply:

1. **PELT (Pruned Exact Linear Time)** -- finds optimal number of change points
2. **Bayesian Online Change Point Detection (BOCPD)** -- provides probability distribution over change point locations
3. **CUSUM (Cumulative Sum)** -- simpler alternative for validation

Libraries: `ruptures` for PELT, custom implementation for BOCPD

For each pair, identify:
- The exact week when Ukrainian spelling overtook Russian spelling
- Whether the change was gradual or sudden (step function vs ramp)
- Confidence intervals on the change point

### Step 3: Geographic Diffusion Analysis

For the top 5 pairs (Kyiv, Kharkiv, Odesa, Lviv, Zaporizhzhia):
- Pull country-level data for 30+ countries
- Identify crossover date per country
- Map the diffusion pattern: which countries adopted first vs last
- Visualize as animated choropleth map or static heatmap

### Step 4: Category Analysis

- Compute mean crossover date per category
- Test hypothesis: geographical names shifted before food terms
- Statistical significance testing between categories
- Document pairs that have NOT crossed over (e.g., "Chicken Kiev" likely still dominates)

### Step 5: Event Correlation

Overlay geopolitical event timeline:

| Date | Event |
|---|---|
| 2014 Feb | Euromaidan revolution |
| 2014 Mar | Crimea annexation |
| 2018 Oct | #KyivNotKiev campaign launched by MFA |
| 2019 Aug-Oct | AP, WSJ, BBC adopt "Kyiv" |
| 2019 Sep | Wikipedia switches to "Kyiv" |
| 2022 Feb 24 | Full-scale Russian invasion |
| 2022-2023 | Various media/organizations switch remaining toponyms |

Cross-correlate search trend inflection points with events using Granger causality or lagged correlation.

---

## Expected Findings

### Finding 1: Three-Wave Adoption Pattern
- **Wave 1 (2018-2019):** Kyiv only, triggered by #KyivNotKiev campaign + media style guide changes
- **Wave 2 (Feb 2022):** All major city names shift simultaneously, triggered by invasion
- **Wave 3 (2022-2023):** Secondary cities, food terms, landmarks -- slower diffusion

### Finding 2: Category Hierarchy
- Geographical names --> fastest (institutional + media pressure)
- Historical terms (Kyivan Rus) --> medium (academic adoption)
- Food terms (Chicken Kyiv) --> slowest (consumer inertia, restaurant menus, packaging)
- "the Ukraine" --> surprisingly fast (media awareness)

### Finding 3: Geographic Diffusion
- Baltic states and Poland --> first adopters (regional solidarity)
- UK/US --> rapid after media style guide changes
- Non-English European countries --> varied
- Some countries may never have switched

### Finding 4: The "Chicken Kiev" Problem
- "Chicken Kiev" likely still dominates in searches even in 2026
- Food terms are the most resistant to toponym de-Russification
- Practical implications: menus, packaging, recipe databases

---

## Visualizations

### Chart 1: Flagship Crossover
"Kiev" vs "Kyiv" worldwide search interest (2015-2026). Two lines crossing. The iconic chart.

### Chart 2: Toponym Heatmap
37 toponym pairs x time. Color = ratio of Ukrainian/Russian spelling. Visual wave propagating from early adopters to laggards.

### Chart 3: Geographic Choropleth
World map showing crossover date by country. Color gradient from green (early) to red (not switched).

### Chart 4: Category Adoption Curves
7 lines (one per category) showing mean adoption ratio over time.

### Chart 5: Event-Driven Spikes
Search volume for select pairs with vertical lines marking geopolitical events.

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python 3.11+ | Core language |
| google-cloud-bigquery | GDELT queries via BigQuery |
| pytrends | Google Trends API wrapper |
| ruptures | Change-point detection (PELT) |
| pandas + numpy + pyarrow | Data processing + Parquet I/O |
| plotly + matplotlib | Visualization |
| geopandas + folium | Geographic mapping |
| scipy | Statistical tests (Granger causality, correlation) |
| Streamlit | Interactive dashboard (optional) |

---

## Project Structure

```
kyivnotkiev/
├── docs/
│   └── SPEC.md                        # This document
├── data/
│   ├── raw/
│   │   ├── gdelt/                     # Raw GDELT BigQuery exports (Parquet)
│   │   ├── trends/                    # Raw Google Trends CSVs
│   │   └── ngrams/                    # Google Ngrams data
│   ├── processed/                     # Cleaned, merged datasets
│   └── toponym_pairs.json             # Definition of all 37 pairs with metadata
├── src/
│   ├── __init__.py
│   ├── config.py                      # Shared config, paths, constants
│   ├── pipeline/
│   │   ├── __init__.py
│   │   ├── collect_gdelt.py           # GDELT BigQuery data collection
│   │   ├── collect_trends.py          # Google Trends data collection (pytrends)
│   │   ├── collect_ngrams.py          # Google Ngrams data collection
│   │   └── preprocess.py              # Normalize, merge, validate raw data
│   ├── analysis/
│   │   ├── __init__.py
│   │   ├── changepoint.py             # PELT + BOCPD + CUSUM change-point detection
│   │   ├── geographic.py              # Country-level diffusion analysis
│   │   ├── categories.py              # Category-level aggregation + stats
│   │   └── events.py                  # Event correlation (Granger causality)
│   └── viz/
│       ├── __init__.py
│       ├── crossover.py               # Flagship crossover charts
│       ├── heatmap.py                 # Toponym x time heatmap
│       ├── choropleth.py              # Geographic diffusion maps
│       ├── category_curves.py         # Category adoption curves
│       └── event_overlay.py           # Event-driven spike charts
├── scripts/
│   ├── run_collect.py                 # CLI: run all data collection
│   ├── run_analysis.py                # CLI: run all analysis steps
│   ├── run_viz.py                     # CLI: generate all figures
│   └── run_pipeline.py               # CLI: end-to-end pipeline
├── tests/
│   ├── test_config.py
│   ├── test_changepoint.py
│   ├── test_geographic.py
│   └── test_preprocess.py
├── paper/
│   └── figures/                       # Publication-ready charts (output)
├── requirements.txt
├── pyproject.toml
└── Makefile                           # make collect / make analyze / make viz / make all
```

---

## Target Venues

| Venue | Notes |
|---|---|
| Ukrainian university journal (Category B) | Fast review, culturally relevant |
| Digital Humanities journals | Interdisciplinary, welcomes computational approaches |
| arXiv preprint (cs.CL or cs.CY) | Immediate visibility |
| UNLP 2027 (ACL Anthology) | Ukrainian NLP conference |
| Computational Linguistics journals | If results are strong |

---

## Timeline

| Day | Tasks |
|---|---|
| 1 | GDELT BigQuery collection (primary corpus, all 37 pairs) |
| 2 | Google Trends collection (all 37 pairs x worldwide + 30 countries) + Ngrams |
| 3 | Change-point detection + category analysis |
| 4 | Geographic diffusion + event correlation |
| 5 | Charts + paper writing |
| 6 | Review + submit |
