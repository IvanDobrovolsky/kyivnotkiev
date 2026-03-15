# KyivNotKiev: Computational Analysis of Global Ukrainian Toponym Adoption

## Paper Title

**"#KyivNotKiev: A Computational Analysis of Global English-Language Adoption of Ukrainian Toponyms (2015-2025)"**

## Abstract

This study presents the first large-scale computational analysis of how English-language usage of Ukrainian place names has shifted from Russian-derived to Ukrainian-derived spellings over a 10-year period (2015-2025). Using Google Trends data, change-point detection algorithms, and geographic diffusion modeling, we analyze 40+ toponym pairs across 7 categories: geographical, food & cuisine, landmarks & heritage, country-level framing, institutional, sports & entertainment, and historical & ethnographic. We identify a three-wave adoption pattern driven by the #KyivNotKiev campaign (2018), media style guide changes (2019), and the full-scale Russian invasion (2022), and demonstrate that adoption speed varies significantly by category, with geographical names shifting fastest and food terms showing the greatest resistance to change.

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

## Data Source

**Google Trends** -- worldwide, English language, 2015-2025 (10-year window)

- Free, public, well-established in computational sociolinguistics research
- Weekly granularity
- Geographic breakdown by country
- Comparison of up to 5 terms simultaneously

### Data collection notes:
- `pytrends` has aggressive rate limiting; plan for batched collection with delays
- Manual CSV download as fallback
- Some obscure pairs may have zero search volume -- document as "below detection threshold"
- Store raw data as CSV per toponym pair

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

- Use `pytrends` (Google Trends API wrapper) with rate limiting and retry logic
- Fallback: manual CSV download from Google Trends web interface
- Pull weekly interest data for each pair: worldwide + by country (30+ countries)
- 10-year window: January 2015 -- March 2025
- Filter to English-language results where possible
- Store raw data as CSV per toponym pair
- Pairs with zero search volume documented as "below detection threshold"

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
- "Chicken Kiev" likely still dominates in searches even in 2025
- Food terms are the most resistant to toponym de-Russification
- Practical implications: menus, packaging, recipe databases

---

## Visualizations

### Chart 1: Flagship Crossover
"Kiev" vs "Kyiv" worldwide search interest (2015-2025). Two lines crossing. The iconic chart.

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
| pytrends | Google Trends API wrapper |
| ruptures | Change-point detection (PELT) |
| pandas + numpy | Data processing |
| plotly + matplotlib | Visualization |
| geopandas + folium | Geographic mapping |
| scipy | Statistical tests (Granger causality, correlation) |
| Streamlit | Interactive dashboard (optional) |

---

## Project Structure

```
kyivnotkiev/
├── docs/
│   └── SPEC.md                   # This document
├── data/
│   ├── raw/                      # Raw Google Trends CSV downloads
│   ├── processed/                # Cleaned, merged datasets
│   └── toponym_pairs.json        # Definition of all 37 pairs with metadata
├── src/
│   ├── collect.py                # Google Trends data collection
│   ├── changepoint.py            # PELT + BOCPD change-point detection
│   ├── geographic.py             # Country-level analysis + diffusion mapping
│   ├── categories.py             # Category-level aggregation
│   ├── events.py                 # Event correlation analysis
│   └── visualize.py              # All chart generation
├── notebooks/
│   ├── 01_exploration.ipynb      # Initial data exploration
│   ├── 02_changepoints.ipynb     # Change-point analysis
│   └── 03_geographic.ipynb       # Geographic diffusion
├── paper/
│   └── figures/                  # Publication-ready charts
├── requirements.txt
└── app.py                        # Optional Streamlit dashboard
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
| 1 | Data collection (pytrends for all 37 pairs x worldwide + 30 countries) |
| 2 | Change-point detection + category analysis |
| 3 | Geographic diffusion + event correlation |
| 4 | Charts + paper writing |
| 5 | Review + submit |
