# Paper Notes — CL Methodology Section

## Annotation Model Selection

Texts were annotated using Claude Haiku 4.5 (Anthropic, 2025). Model selection was validated
empirically against two independent human annotators on a stratified sample of 220 texts
(20 per context class, oversampling model-disagreement cases at ~30%).

**Key result:** LLM-human agreement (Cohen's κ = 0.56–0.69) exceeds inter-annotator
agreement between the two human raters (κ = 0.52). On the 130 texts where both humans
agreed (59% of samples), the LLM matched their consensus in 86.2% of cases.

**Implication:** The LLM annotations are at least as reliable as any individual human
annotator. The moderate κ reflects inherent task ambiguity across 11 context categories
(e.g., "academic paper about history" can reasonably be labeled either `academic` or
`history`), not annotation error.

**Cost-efficiency:** Full corpus annotation (36,791 texts) cost ~$29 via API. Using a
larger model (e.g., Claude Opus, GPT-4o) would cost 10-20x more with no measurable
improvement in agreement — the human-human ceiling (κ = 0.52) is already exceeded.

All 36,791 labels are published in the dataset for independent verification and
re-annotation.

## Corpus Balancing Strategy

The CL corpus uses stratified balanced sampling to prevent any single pair, source,
variant, or time period from dominating the classifier's training signal.

### Why balance?

Without balancing, the raw data is heavily skewed:
- **Source skew:** OpenAlex contributes 42% of raw texts (academic papers), while GDELT
  contributes only 2.4%. An unbalanced corpus would learn "academic" as the default class.
- **Pair skew:** Pair 1 (Kiev/Kyiv) has 50x more data than pair 83 (Olha of Kyiv). The
  classifier would overfit to high-volume pairs.
- **Variant skew:** Some pairs have 90%+ Russian-variant texts. The classifier needs
  exposure to both variants in all contexts.
- **Temporal skew:** Post-2022 invasion content overwhelms pre-2022 data. The classifier
  must recognize context across all time periods.

### How balancing works

Sampling is stratified across four dimensions simultaneously:
1. **pair_id** — each pair gets representation
2. **source** — each data source (Reddit, YouTube, OpenAlex, GDELT, Religious) contributes
3. **variant** — Russian and Ukrainian forms are balanced per pair
4. **year_stratum** — four temporal periods (2010-2013, 2014-2017, 2018-2021, 2022-2026)

For each cell in this 4D grid, we sample up to `MAX_PER_VARIANT_PER_SOURCE / 4` texts
(default: 125 per stratum). Cells with fewer available texts contribute all they have;
these shortfalls are documented in the balance report.

### Quality filters applied before balancing

1. **Pair term verification** — text must contain the Russian or Ukrainian form of the
   toponym. Removes 5,867 texts (13.8%) where the search API matched on metadata
   (URL, tags) not text content.
2. **Latin-script filter** — majority of characters must be Latin. Removes Cyrillic-only texts.
3. **Minimum length** — text must be ≥ 20 characters.
4. **Odessa TX filter** — removes texts about Odessa, Texas (regex pattern match).
5. **English language detection** — langdetect filter for English-language texts.
6. **Deduplication** — same pair_id + text content = same document.

### Resulting corpus

| Metric | Value |
|--------|-------|
| Total texts | 36,791 |
| Pairs represented | 57 of 59 enabled (missing: 62 Shevchenko, 89 Sviatohirsk — insufficient source data) |
| Context classes | 11 (politics, war_conflict, sports, culture_arts, food_cuisine, travel_tourism, academic_science, history, business_economy, general_news, religion) |
| Variant split | 51% Russian / 49% Ukrainian |
| Train / Val / Test | 29,396 / 3,675 / 3,675 (80/10/10 stratified) |
| Smallest test class | business_economy (53 samples) |

## Russia Filter — EU Sanctions List

Domain classification uses the EU sanctions list (Council Regulation 833/2014, Article 2f)
to identify Russian state media. This is a legally verifiable classification, not an
editorial judgment.

Three tiers:
1. **EU-sanctioned state media** — 20 domains found in data (725K mentions, 21% Ukrainian adoption).
   Based on Council Decisions 2022/351 through 2025/394 (7 waves of sanctions).
2. **Russian domains** (.ru/.su) — 2,042 domains (4.5M mentions, 26% Ukrainian adoption).
3. **All other domains** — 51K domains (34.3M mentions, 48% Ukrainian adoption).

Key finding: **27pp adoption gap** between EU-sanctioned state media (21%) and global
media (48%).

## Scope: Latin-Script Media

The GDELT data source indexes articles in all languages. Our analysis measures adoption
across all Latin-script media worldwide, not exclusively English-language sources. This is
appropriate because:

1. The Ukrainian→Russian spelling transition (Kiev→Kyiv, Kharkov→Kharkiv) uses the same
   Latin-script forms across English, French, Italian, Spanish, Portuguese, German, and
   other European languages.
2. The UN, EU, ICAO, and other international bodies adopted "Kyiv" as the standard form
   across all languages, not just English.
3. We cannot reliably determine article language from GDELT domain-level data alone.

**Limitation:** Non-English media may use Russian-derived forms for language-specific
reasons (e.g., historical convention in Italian), not political preference. This is
acknowledged as a limitation and does not affect the core finding that adoption varies
systematically by context.
