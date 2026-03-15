# #KyivNotKiev: Measuring the Global Adoption of Ukrainian Toponyms in English Through Computational Analysis of Search Trends (2015–2025)

## Paper Title
**"#KyivNotKiev: A Computational Analysis of Global English-Language Adoption of Ukrainian Toponyms (2015–2025)"**

## The Gap
- One paper exists (Taylor & Francis, 2023) studying Ukrainian vs Russian search trends WITHIN Ukraine
- The #KyivNotKiev campaign is well-documented journalistically
- Wikipedia has detailed historical timelines of media style guide changes
- **NOBODY has computationally analyzed the ENGLISH-LANGUAGE worldwide adoption using change-point detection, geographic diffusion modeling, or category-level analysis**
- This is a 100% original computational sociolinguistics contribution

## Research Questions

**RQ1:** When did the crossover from Russian-derived to Ukrainian-derived English spellings occur for each Ukrainian toponym, and what geopolitical events triggered them?

**RQ2:** How does the adoption speed vary across toponym categories (city names, food terms, cultural landmarks)?

**RQ3:** What is the geographic diffusion pattern — which countries adopted Ukrainian spellings first, and which still lag?

**RQ4:** Is there a measurable correlation between specific geopolitical events (Euromaidan 2014, #KyivNotKiev campaign 2018, full-scale invasion 2022) and adoption acceleration?

## Data Source
**Google Trends API** — worldwide, English language, 2015–2025 (10-year window)
- Free, public, well-established in computational sociolinguistics research
- Weekly granularity
- Geographic breakdown by country
- Comparison of up to 5 terms simultaneously

## Toponym Pairs (30+ pairs across 6 categories)

### Category 1: Major Cities (highest visibility)
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 1 | Kiev | Kyiv | Capital — the flagship case |
| 2 | Kharkov | Kharkiv | Second largest city, major battle site |
| 3 | Odessa | Odesa | Major port, cultural capital |
| 4 | Lvov | Lviv | Western cultural center |
| 5 | Zaporozhye | Zaporizhzhia | Nuclear plant, frontline city |
| 6 | Nikolaev | Mykolaiv | Southern city |
| 7 | Dnepropetrovsk | Dnipro | Renamed city (also de-communization) |
| 8 | Vinnitsa | Vinnytsia | Central Ukraine |
| 9 | Rovno | Rivne | Western Ukraine |
| 10 | Chernobyl | Chornobyl | Historical — nuclear disaster site |

### Category 2: Conflict Zone Cities (expected spike after 2022)
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 11 | Lugansk | Luhansk | Occupied since 2014 |
| 12 | Donetsk | Donetsk (unchanged) | Control case — same in both languages |
| 13 | Mariupol | Mariupol (unchanged) | Control case — same, but massive 2022 visibility |
| 14 | Kherson | Kherson (unchanged) | Control case — same in both |

### Category 3: Food & Cuisine (cultural soft power)
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 15 | Chicken Kiev | Chicken Kyiv | Most globally known food item |
| 16 | Kiev cake | Kyiv cake | Traditional dessert |
| 17 | Borscht (Russian claim) | Borshch (Ukrainian) | UNESCO-listed, cultural ownership dispute |

### Category 4: Landmarks & Historical Sites
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 18 | Kiev Pechersk Lavra | Kyiv Pechersk Lavra | UNESCO World Heritage Site |
| 19 | Kievan Rus | Kyivan Rus | Historical state — identity claim |

### Category 5: Country-Level
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 20 | "the Ukraine" | "Ukraine" | Definite article removal = sovereignty |

### Category 6: Rivers & Geographic Features
| # | Russian-derived | Ukrainian-derived | Significance |
|---|---|---|---|
| 21 | Dnieper | Dnipro | Major river |

## Methodology

### Step 1: Data Collection (1 day)
- Use pytrends (Google Trends unofficial API) or manual download
- Pull weekly interest data for each pair, worldwide + by country
- 10-year window: January 2015 – March 2025
- Filter to English-language results where possible
- Store as CSV per toponym pair

### Step 2: Change-Point Detection (1 day)
For each toponym pair, apply:
- **PELT (Pruned Exact Linear Time)** algorithm — finds optimal number of change points
- **Bayesian Online Change Point Detection (BOCPD)** — provides probability distribution over change point locations
- **CUSUM (Cumulative Sum)** — simpler alternative for validation
- Python: `ruptures` library for PELT, custom implementation for BOCPD

For each pair, identify:
- The exact week when Ukrainian spelling overtook Russian spelling
- Whether the change was gradual or sudden (step function vs ramp)
- Confidence intervals on the change point

### Step 3: Geographic Diffusion Analysis (1 day)
For the top 5 pairs (Kyiv, Kharkiv, Odesa, Lviv, Zaporizhzhia):
- Pull country-level data for 30+ countries
- Identify crossover date per country
- Map the diffusion pattern: which countries adopted first → last
- Expected pattern: Ukraine → Baltics/Poland → Western Europe → US/UK → rest of world
- Visualize as animated choropleth map or static heatmap

### Step 4: Category Analysis (0.5 day)
- Compute mean crossover date per category
- Test hypothesis: city names shifted before food terms
- Expected finding: cities (external pressure from media/governments) before food (consumer/cultural inertia)
- "Chicken Kiev" likely STILL dominates "Chicken Kyiv" — document this lag

### Step 5: Event Correlation (0.5 day)
Overlay geopolitical event timeline:
- **2014 Feb**: Euromaidan revolution
- **2014 Mar**: Crimea annexation
- **2018 Oct**: #KyivNotKiev campaign launched by MFA
- **2019 Aug-Oct**: AP, WSJ, BBC adopt "Kyiv"
- **2019 Sep**: Wikipedia switches to "Kyiv"
- **2022 Feb 24**: Full-scale invasion
- **2022-2023**: Various media/organizations switch remaining toponyms

Cross-correlate search trend inflection points with these events using Granger causality or simple lagged correlation.

## Expected Findings

### Finding 1: Three-Wave Adoption Pattern
- Wave 1 (2018-2019): Kyiv only, triggered by #KyivNotKiev campaign + media style guide changes
- Wave 2 (Feb 2022): All major city names shift simultaneously, triggered by invasion
- Wave 3 (2022-2023): Secondary cities, food terms, landmarks — slower diffusion

### Finding 2: Category Hierarchy
- City names → fastest (institutional pressure)
- Historical terms (Kyivan Rus) → medium (academic adoption)
- Food terms (Chicken Kyiv) → slowest (consumer inertia, restaurant menus, packaging)
- "the Ukraine" → surprisingly fast (media awareness)

### Finding 3: Geographic Diffusion
- Baltic states and Poland → first adopters (regional solidarity)
- UK/US → rapid after media style guide changes
- Non-English European countries → varied (French media debated longer)
- Some countries may never have switched (countries without strong Ukraine ties)

### Finding 4: The "Chicken Kiev" Problem
- "Chicken Kiev" likely still dominates in searches even in 2025
- Food terms are the most resistant to toponym de-Russification
- This has practical implications: menus, packaging, recipe databases

## Paper Structure
1. **Introduction** — The #KyivNotKiev campaign, why computational analysis matters
2. **Related Work** — Existing Ukrainization research, Google Trends in sociolinguistics, computational approaches to language policy
3. **Data & Methodology** — Google Trends collection, change-point detection algorithms, geographic analysis
4. **Results** — Change-point dates per toponym, category analysis, geographic diffusion
5. **Discussion** — Three-wave pattern, category hierarchy, practical implications
6. **Limitations** — Google Trends sampling, English-only, search ≠ usage
7. **Conclusion** — Quantitative evidence for the success of digital de-Russification campaign

## Hero Charts for the Paper

### Chart 1: "Kiev" vs "Kyiv" Worldwide (2015-2025)
The iconic crossover chart. Two lines crossing around 2019-2022. Everyone understands this instantly.

### Chart 2: Heatmap — 30 toponym pairs × time
Visual wall showing the wave of adoption. Color = ratio of Ukrainian/Russian spelling. The wave propagates from top (Kyiv, earliest) to bottom (Chicken Kyiv, latest).

### Chart 3: Geographic Choropleth Map
World map showing crossover date by country. Color gradient from green (early adopters) to red (haven't switched).

### Chart 4: Category Adoption Curves
5 lines (one per category) showing mean adoption over time. City names rise first, food terms last.

### Chart 5: Event-Driven Spikes
Search volume for "Kharkiv" vs "Kharkov" with vertical lines marking key battle events (Aug 2022 counteroffensive = "Kharkiv" spike).

## Tech Stack
- **Python 3.11+**
- **pytrends** — Google Trends API wrapper
- **ruptures** — change-point detection (PELT algorithm)
- **pandas + numpy** — data processing
- **plotly + matplotlib** — visualization
- **geopandas + folium** — geographic mapping
- **scipy** — statistical tests (Granger causality, correlation)
- **Streamlit** — interactive dashboard (optional, for demo)

## Project Structure
```
kyivnotkiev/
├── README.md
├── requirements.txt
├── data/
│   ├── raw/                    # Raw Google Trends CSV downloads
│   ├── processed/              # Cleaned, merged datasets
│   └── toponym_pairs.json      # Definition of all 30+ pairs with metadata
├── src/
│   ├── collect.py              # Google Trends data collection
│   ├── changepoint.py          # PELT + BOCPD change-point detection
│   ├── geographic.py           # Country-level analysis + diffusion mapping
│   ├── categories.py           # Category-level aggregation
│   ├── events.py               # Event correlation analysis
│   └── visualize.py            # All chart generation
├── notebooks/
│   ├── 01_exploration.ipynb    # Initial data exploration
│   ├── 02_changepoints.ipynb   # Change-point analysis
│   └── 03_geographic.ipynb     # Geographic diffusion
├── paper/
│   └── figures/                # Publication-ready charts
└── app.py                      # Optional Streamlit dashboard
```

## Timeline
- **Day 1:** Data collection (pytrends for all 30 pairs × worldwide + 30 countries)
- **Day 2:** Change-point detection + category analysis
- **Day 3:** Geographic diffusion + event correlation
- **Day 4:** Charts + paper writing
- **Day 5:** Review + submit to Ukrainian university journal or arXiv

## Target Venues
- **Ukrainian university journal** (Category B) — fast, culturally relevant
- **Digital Humanities journals** — interdisciplinary, welcomes this type
- **arXiv preprint** (cs.CL or cs.CY) — immediate visibility
- **UNLP 2027** — save for next year's Ukrainian NLP conference (ACL Anthology)
- **Computational Linguistics journals** — if results are strong enough

## Why This Paper Is Uniquely Yours
- You are Ukrainian, living in the US — you bridge both perspectives
- You're a technologist applying computational methods to your own culture's story
- The #KyivNotKiev campaign is personally meaningful, not abstract
- No one else has done this computational analysis — verified through extensive search
- The paper connects to your broader research theme: language, identity, and computation

## Open Source
Publish to GitHub as `kyivnotkiev` — the data, code, and results all public.
Contribution to both NLP community and Ukrainian cultural discourse.
