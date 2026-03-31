# Data Quality Audit Report

**Generated:** 2026-03-30
**Dataset:** kyivnotkiev-research.kyivnotkiev
**Total rows:** 38.7M GDELT + 24.6K Trends + 14.9K Wikipedia + 22.5K Reddit + 12.1K Ngrams

---

## 1. 24tv.ua Holdout Investigation

### Finding: LEGITIMATE -- 24tv.ua runs a full Russian-language mirror

24tv.ua appears as 66.6% "Kiev" in GDELT (40,934 Kiev vs 20,528 Kyiv since 2024). The user suspected false positives, but the data is **mostly correct**:

| Category | Count | % of Kiev matches |
|---|---|---|
| Russian-language section (`/ru/` in URL) | 23,928 | 58.5% |
| Ukrainian section, no "kiev" in URL | 17,005 | 41.5% |
| Ukrainian section, "kiev" in URL | 1 | 0.0% |

**Explanation:** 24tv.ua operates a full Russian-language mirror at `24tv.ua/ru/...`. These articles legitimately use "Kiev" (the Russian transliteration standard for Russian-language content). This accounts for 58.5% of the "Kiev" matches.

**But there is a problem:** 41.5% of "Kiev" matches (17,005 articles) come from the Ukrainian-language section with NO "kiev" in the URL. These URLs look like:
- `https://24tv.ua/geopolitics/rosiya-pogrozhuye-pivdenniy-koreyi...`
- `https://24tv.ua/agenti-atesh-vlashtuvali-seriyu-diversiy...`
- `https://24tv.ua/naystrashnishi-momenti-viyni-osobisti-istoriyi...`

These are Ukrainian-language articles. **GDELT is matching "Kiev" from its V2Themes/V2Locations metadata**, not from the article text or URL. GDELT's GKG extracts geographic locations using its own taxonomy, and it internally tags Kyiv as "Kiev" in its V2Locations field regardless of what the article actually says. This is a **systematic GDELT bias** -- the GKG geocoder uses legacy "Kiev" internally.

### Verdict: RED FLAG for the paper

The GDELT ingestion pipeline matches on `CONCAT(V2Themes, V2Locations, DocumentIdentifier)`. The V2Locations field uses GDELT's own geocoding which historically uses "Kiev". **This means GDELT data systematically over-counts "Kiev" because the matching includes GDELT's own metadata, not just article content.** This affects ALL pairs where GDELT extracts locations/themes using legacy Russian-origin spellings.

---

## 2. Per-Pair Cross-Source Consistency

Pairs with >30% gap between GDELT and Trends (2024 data):

| pair_id | Name | GDELT % Ukr | Trends % Ukr | Wiki % Ukr | Gap | Issue |
|---|---|---|---|---|---|---|
| 8 | Vinnytsia/Vinnitsa | 0.7% | 79.6% | 96.5% | **78.9** | GDELT V2Locations bias |
| 43 | Kremenchuk/Kremenchug | 11.8% | 86.3% | 91.4% | **74.5** | GDELT V2Locations bias |
| 44 | Kropyvnytskyi/Kirovograd | 0.2% | 70.1% | 96.0% | **69.9** | GDELT V2Locations bias |
| 42 | Uzhhorod/Uzhgorod | 4.1% | 72.9% | 98.6% | **68.8** | GDELT V2Locations bias |
| 18 | Crimea/Krym | 57.5% | 0.0% | 50.0% | **57.5** | "Krym" has no English search volume |
| 23 | Borshch/Borscht | 57.6% | 0.7% | 50.0% | **56.9** | "Borshch" not searched; different concept |
| 6 | Mykolaiv/Nikolaev | 8.2% | 55.2% | 100.0% | **47.0** | GDELT V2Locations bias |
| 40 | Zhytomyr/Zhitomir | 48.2% | 92.6% | 93.3% | **44.4** | GDELT V2Locations bias |
| 10 | Chornobyl/Chernobyl | 38.6% | 0.8% | 0.1% | **37.8** | "Disaster brand" -- all sources agree |

### Pattern: GDELT systematically under-reports Ukrainian adoption for city names

For city pairs (8, 43, 44, 42, 6, 40), GDELT shows 0.2-48% Ukrainian adoption while Trends and Wikipedia show 70-98%. The explanation is the V2Locations matching bug described above.

---

## 3. Kiev/Kyiv GDELT Anomaly -- Deep Dive

### Year-by-year GDELT adoption for pair 1 (Kiev/Kyiv):

| Year | Total | Kyiv count | Kyiv % |
|---|---|---|---|
| 2015 | 1,425,219 | 288,442 | 20.2% |
| 2016 | 1,625,732 | 308,116 | 19.0% |
| 2017 | 1,528,010 | 284,700 | 18.6% |
| 2018 | 1,263,172 | 266,716 | 21.1% |
| 2019 | 955,712 | 231,135 | 24.2% |
| 2020 | 683,415 | 182,196 | 26.7% |
| 2021 | 663,707 | 159,865 | 24.1% |
| **2022** | **2,385,618** | **976,501** | **40.9%** |
| 2023 | 2,294,832 | 637,313 | 27.8% |
| 2024 | 1,840,691 | 540,591 | 29.4% |
| 2025 | 1,656,181 | 471,884 | 28.5% |
| 2026 | 317,215 | 88,459 | 27.9% |

### Key observations:

1. **The 2022 spike to 40.9% is real** -- the invasion drove genuine adoption.
2. **But it reverted to ~28% by 2023** -- this reversion is suspicious and likely reflects the V2Locations bias reasserting itself as the initial wave of "Kyiv"-using Western coverage was diluted by GDELT's internal geocoding.
3. **Trends shows 51% Kyiv, Wikipedia shows 87.8% Kyiv** -- the gap with GDELT (28.9%) is enormous.

### Source breakdown (2024, pair 1):

| Media type | Kiev count | Kyiv count |
|---|---|---|
| Russian media | 295,065 | 136 |
| Ukrainian media | 266,688 | 160,939 |
| Other (Western + rest) | 2,151,400 | 939,859 |

Ukrainian media: 62.4% of their mentions are tagged "Kiev" in GDELT. But as shown above, much of this comes from /ru/ sections and V2Locations contamination.

### URL-level analysis (2024, pair 1):

- Of "Kiev"-variant matches: 9.9% have "kiev" in the URL; 90.1% do not
- Of "Kyiv"-variant matches: 4.2% have "kiev" in the URL (likely cross-contamination)
- **Most matches come from V2Themes/V2Locations, not URLs**

### Verdict: GDELT data for Kiev/Kyiv is UNRELIABLE as a measure of media adoption

The V2Locations field uses GDELT's geocoder which tags articles about Kyiv with the legacy "Kiev" location code. The matching regex `(?i)\bKiev\b` then picks this up. **GDELT is measuring its own internal taxonomy, not what journalists actually write.**

---

## 4. Pairs at 0% GDELT Adoption

### GDELT results for pairs 54, 59, 63, 66, 67:

Only pair 54 (Babyn Yar / Babi Yar) has ANY GDELT data: 44 rows, all "russian" variant.

Pairs 59 (Halychyna), 63 (Klychko), 66 (Pyrizhky), 67 (Syrnyky) have **zero GDELT rows**.

### Trends and Wikipedia data:

| pair_id | Name | Trends Ukr | Trends Rus | Wiki Ukr views | Wiki Rus views |
|---|---|---|---|---|---|
| 54 | Babyn Yar | 198 | 1,971 | 3,816,973 | 3,816,973 |
| 59 | Halychyna | 0 | 13,518 | 39,683 | 4,262,269 |
| 63 | Klychko | 0 | 1,239 | 10,075,748 | 10,075,748 |
| 66 | Pyrizhky | 0 | 309 | 4,561 | 1,508,267 |
| 67 | Syrnyky | 0 | 378 | 4,898 | 679,592 |

### Verdict: These are GENUINE zero-adoption cases

- **Halychyna** (59): "Galicia" is overwhelmingly preferred in English. "Halychyna" is a Ukrainian-language term with essentially zero English usage. This is not an adoption failure -- it is a term that never existed in English.
- **Klychko** (63): "Vitaliy Klychko" vs "Vitali Klitschko" -- the Ukrainian spelling has zero search traction. Klitschko built his brand in boxing under the German/Russian spelling.
- **Pyrizhky** (66) and **Syrnyky** (67): Niche food terms with near-zero English-language presence in either variant.
- **Babyn Yar** (54): Wikipedia maps BOTH variants to the same "Babi Yar" page (see Section below). Trends shows 10:1 Russian preference. The Holocaust memorial site's name is deeply established.

### Wikipedia data quality issue for these pairs:

Pairs 54 and 63 show IDENTICAL pageviews for both variants because Wikipedia has only ONE article (e.g., "Babi Yar", "Vitali Klitschko") mapped to both variants. **This makes Wikipedia useless for measuring adoption for these pairs.**

---

## 5. Top Holdout Domains Deep Dive

### Top 25 "Kiev"-using domains (2024+, 1000+ articles):

| Domain | Total | Kiev count | Kiev % | Type |
|---|---|---|---|---|
| 24tv.ua | 61,462 | 40,934 | 66.6% | Ukrainian (with /ru/ section) |
| www.zazoom.it | 39,663 | 39,048 | 98.4% | Italian aggregator |
| ria.ru | 35,359 | 35,349 | 100.0% | Russian state media |
| lenta.ru | 33,039 | 33,029 | 100.0% | Russian state media |
| pda.kp.ru | 32,735 | 32,724 | 100.0% | Russian state media |
| tass.ru | 30,028 | 30,017 | 100.0% | Russian state media |
| news.mail.ru | 29,340 | 29,329 | 100.0% | Russian aggregator |
| aif.ru | 26,210 | 26,194 | 99.9% | Russian media |
| unn.ua | 35,388 | 24,849 | 70.2% | Ukrainian (with /ru/ section) |
| www.obozrevatel.com | 36,422 | 24,275 | 66.6% | Ukrainian (no /ru/ URL marker) |
| tsn.ua | 37,131 | 22,550 | 60.7% | Ukrainian (with /ru/ section) |
| focus.ua | 31,286 | 22,047 | 70.5% | Ukrainian (no /ru/ URL marker) |
| gazeta.ua | 29,967 | 19,958 | 66.6% | Ukrainian (with /ru/ section) |
| www.mk.ru | 19,480 | 19,471 | 100.0% | Russian media |
| www.unian.net | 19,329 | 18,518 | 95.8% | Ukrainian news wire |
| www.merkur.de | 17,758 | 17,752 | 100.0% | German media |
| www.n-tv.de | 16,062 | 16,062 | 100.0% | German media |

### Ukrainian media /ru/ section breakdown:

| Domain | Kiev from /ru/ | Kiev from UA section | % from /ru/ |
|---|---|---|---|
| 24tv.ua | 23,928 | 17,006 | 58.5% |
| unn.ua | 14,440 | 10,409 | 58.1% |
| gazeta.ua | 11,488 | 8,470 | 57.6% |
| tsn.ua | 10,439 | 12,111 | 46.3% |
| www.obozrevatel.com | 0 | 24,275 | 0.0% |
| focus.ua | 0 | 22,047 | 0.0% |
| glavcom.ua | 0 | 16,935 | 0.0% |
| korrespondent.net | 0 | 12,409 | 0.0% |
| interfax.com.ua | 0 | 10,196 | 0.0% |

### Critical findings:

1. **Sites with /ru/ sections** (24tv.ua, unn.ua, gazeta.ua, tsn.ua): About half their "Kiev" matches are from legitimate Russian-language content. The other half is V2Locations contamination.
2. **Sites WITHOUT /ru/ URL markers** (obozrevatel.com, focus.ua, glavcom.ua, korrespondent.net, interfax.com.ua): ALL their "Kiev" matches come from Ukrainian-language pages. **These are almost certainly false positives from GDELT's V2Locations geocoder.**
3. **German media** (merkur.de, n-tv.de): 100% "Kiev". German media genuinely still uses "Kiew" (German for Kiev), and GDELT's regex catches this.
4. **zazoom.it**: Italian aggregator using "Kiev" -- Italian media uses "Kiev" as standard.

---

## 6. Crimea Special Analysis

### Top domains for Crimea/Krym (pair 18, 2024+, 20+ articles):

| Domain | Variant | Count |
|---|---|---|
| kafanews.com | ukrainian (Krym) | 8,887 |
| 24tv.ua | ukrainian (Krym) | 6,891 |
| kafanews.com | russian (Crimea) | 5,401 |
| sevastopol.su | ukrainian (Krym) | 4,829 |
| 24tv.ua | russian (Crimea) | 4,249 |
| ria.ru | ukrainian (Krym) | 4,214 |
| sevastopol.su | russian (Crimea) | 3,626 |
| www.n-tv.de | russian (Crimea) | 3,242 |
| www.n-tv.de | ukrainian (Krym) | 3,055 |

### This pair is FUNDAMENTALLY BROKEN

- **"Crimea" is the English word. "Krym" is the Ukrainian/Russian word.** Measuring English-language adoption of "Krym" over "Crimea" makes no linguistic sense. English speakers say "Crimea" regardless of political stance.
- **ria.ru shows as 4,214 "Krym" matches** -- Russian state media is NOT using "Krym" by choice. GDELT's V2Locations is matching "Krym" from location metadata because GKG also includes native-language location names.
- Google Trends shows 0% for "Krym" -- nobody searches for it in English.
- Wikipedia maps BOTH variants to the same "Crimea" article.

### Verdict: Pair 18 (Crimea/Krym) should be EXCLUDED from the paper or analyzed separately with heavy caveats

---

## 7. Wikipedia Data Quality: Same-Page Problem

**20 pairs map BOTH variants to the same Wikipedia article:**

| pair_id | Page title | Problem |
|---|---|---|
| 5 | Zaporizhzhia | Only Ukrainian page exists |
| 7 | Dnipro | Only Ukrainian page exists |
| 17 | Donbas | Only Ukrainian page exists |
| 18 | Crimea | Neither variant is the page title |
| 21 | Chicken Kiev | Russian variant IS the page title |
| 23 | Borscht | Russian variant IS the page title |
| 25 | Saint Sophia's Cathedral, Kyiv | Ukrainian variant IS the page title |
| 27 | Ukraine | Ukrainian variant IS the page title |
| 35 | Kievan Rus' | Russian variant IS the page title |
| 36 | Cossacks | Neither variant matches |
| 51-56, 60-63 | Various | Various |

**These pairs show 50/50 adoption in Wikipedia because the same pageview count is assigned to both variants.** This is a data bug -- the ingestion pipeline should check whether the Wikipedia redirect actually resolves to the Ukrainian or Russian form.

---

## 8. Reddit Data Quality

### Volume and structure:

- 22,506 total rows across ~50 pairs
- Data goes back to 2007 (earliest: pair 8, 2007-07-03)
- Most pairs have 100-1000 posts total

### The 100-cap problem:

Reddit data is **capped at exactly 100 posts per (pair, subreddit, variant) combination**. For example, pair 1 in r/europe has exactly 100 "russian" and 100 "ukrainian" posts. This is an artifact of the Pushshift/Reddit API query limit.

**This means:**
1. Volume comparisons within subreddits are meaningless -- they are all capped
2. The data only measures presence/absence and temporal distribution, not relative frequency
3. High-volume subreddits (r/worldnews, r/europe) are severely under-sampled

### Pre/post invasion comparison (Reddit):

| pair_id | Name | Pre-invasion Ukr% | Post-invasion Ukr% | Pre N | Post N |
|---|---|---|---|---|---|
| 1 | Kyiv/Kiev | 20.9% | 59.6% | 516 | 904 |
| 2 | Kharkiv/Kharkov | 40.3% | 70.1% | 216 | 652 |
| 3 | Odesa/Odessa | 11.5% | 56.6% | 305 | 753 |
| 5 | Zaporizhzhia | 16.7% | 68.5% | 30 | 587 |
| 7 | Dnipro | 50.7% | 81.2% | 140 | 483 |
| 8 | Vinnytsia | 51.6% | 68.6% | 31 | 280 |
| 11 | Luhansk/Lugansk | 2.5% | 53.6% | 204 | 265 |
| 17 | Donbas/Donbass | 21.6% | 57.1% | 273 | 645 |
| 18 | Crimea/Krym | 21.0% | 19.3% | 143 | 482 |

Reddit shows a **strong and consistent invasion effect** across most pairs, with typical jumps of 20-40 percentage points. The Crimea/Krym pair shows NO invasion effect, confirming it is linguistically different from the others.

### Verdict: Reddit data is USABLE but WEAK

The 100-cap means it cannot measure volume, only direction. Good for qualitative confirmation of trends, not for standalone quantitative claims. Sample sizes are small but the invasion effect is so large it is statistically significant even with N~500.

---

## 9. Ngrams Data

12,120 rows of Google Books Ngram data. Contains historical frequency data going back to 1900. This is a solid long-term baseline showing the historical dominance of Russian-origin spellings in published English text.

---

## 10. GDELT Invasion Effect (Before/After Feb 24, 2022)

| pair_id | Name | Pre-invasion Ukr% | Post-invasion Ukr% | Delta |
|---|---|---|---|---|
| 1 | Kyiv/Kiev | 21.7% | 31.7% | +10.0 |
| 2 | Kharkiv/Kharkov | 83.0% | 97.7% | +14.7 |
| 3 | Odesa/Odessa | 9.3% | 23.4% | +14.1 |
| 4 | Lviv/Lvov | 96.8% | 98.1% | +1.3 |
| 5 | Zaporizhzhia | 20.8% | 85.8% | +65.0 |
| 7 | Dnipro | 99.4% | 99.9% | +0.5 |
| 8 | Vinnytsia/Vinnitsa | 0.3% | 1.1% | +0.8 |
| 9 | Rivne/Rovno | 91.6% | 96.7% | +5.1 |
| 11 | Luhansk/Lugansk | 71.3% | 73.1% | +1.8 |
| 17 | Donbas/Donbass | 13.5% | 35.1% | +21.6 |
| 18 | Crimea/Krym | 55.7% | 56.0% | +0.3 |
| 38 | Chernihiv/Chernigov | 71.2% | 82.8% | +11.6 |
| 40 | Zhytomyr/Zhitomir | 50.4% | 51.4% | +1.0 |
| 41 | Cherkasy/Cherkassy | 69.0% | 75.3% | +6.3 |
| 45 | Ternopil/Tarnopol | 99.9% | 99.9% | +0.0 |

**GDELT shows a muted invasion effect** compared to Trends and Reddit. This is consistent with the V2Locations contamination -- GDELT's geocoder continues to use "Kiev" regardless of what year it is, dampening the apparent adoption.

---

## 11. Chernobyl: The "Disaster Brand" Effect

| Year | Total | Chornobyl count | Chornobyl % |
|---|---|---|---|
| 2015 | 21,319 | 13 | 0.1% |
| 2016 | 36,596 | 48 | 0.1% |
| 2017 | 36,076 | 111 | 0.3% |
| 2018 | 33,874 | 320 | 0.9% |
| 2019 | 27,460 | 124 | 0.5% |
| 2020 | 33,246 | 34 | 0.1% |
| 2021 | 33,128 | 25 | 0.1% |
| **2022** | **18,848** | **655** | **3.5%** |
| 2023 | 33,184 | 145 | 0.4% |
| 2024 | 34,846 | 133 | 0.4% |
| 2025 | 24,358 | 286 | 1.2% |
| 2026 | 6,489 | 63 | 1.0% |

Trends: 0.8% Chornobyl. Wikipedia: 0.1% (Chornobyl article has 43K views vs Chernobyl disaster's 74.4M views).

**Chernobyl is a "frozen brand."** Unlike Kyiv (which had existing usage momentum), Chernobyl is so deeply embedded in global consciousness (HBO series, disaster literature, nuclear policy) that renaming adoption is essentially zero. The 2022 invasion spike (Russian forces entering the exclusion zone) was temporary.

**This is a powerful finding for the paper** -- it demonstrates that adoption follows a "brand strength" gradient, not a uniform political response.

---

## 12. Fastest vs Slowest Adopters (GDELT 2024)

### Fastest (>95% Ukrainian in GDELT):
1. Ternopil/Tarnopol (45): 99.9% -- small city, no legacy brand
2. Chernivtsi/Chernovtsy (39): 99.9% -- small city
3. Dnipro/Dnepropetrovsk (7): 99.8% -- **major success story** (complete rebrand)
4. Lviv/Lvov (4): 98.4% -- already adopted pre-2022
5. Kharkiv/Kharkov (2): 98.2% -- invasion drove final adoption

### Slowest (<30% Ukrainian in GDELT):
1. Kiev/Kyiv (1): 28.9% -- **but this is inflated by V2Locations bug**
2. Chornobyl/Chernobyl (10): 38.6% -- disaster brand
3. Donbas/Donbass (17): 30.1% -- conflict zone branding
4. Vinnytsia/Vinnitsa (8): 0.7% -- **almost certainly a data bug** (Trends=79.6%, Wiki=96.5%)

---

## 13. Key Findings and Recommendations

### For the paper:

1. **CRITICAL: The GDELT V2Locations matching bug must be disclosed and addressed.** The pipeline matches on `CONCAT(V2Themes, V2Locations, DocumentIdentifier)`, and V2Locations contains GDELT's own geocoded location names, which use legacy Russian-origin spellings. Options:
   - Re-run ingestion matching ONLY on `DocumentIdentifier` (URL text)
   - Re-run matching on article titles extracted from GDELT
   - Disclose the limitation and present GDELT as a "floor" estimate
   - **Best option:** Filter out matches where the term appears ONLY in V2Locations and not in the URL or title

2. **Wikipedia same-page problem affects 20 pairs.** These pairs show artificial 50/50 splits. Must either fix the Wikipedia ingestion (check redirect target) or exclude these pairs from Wikipedia analysis.

3. **Reddit 100-cap limits quantitative analysis.** Reddit data cannot measure adoption rates, only directional shifts. Disclose this.

4. **Remove or heavily caveat pair 18 (Crimea/Krym).** "Krym" is not an English word. This pair measures something fundamentally different from the others.

5. **Pairs 59, 63, 66, 67 (Halychyna, Klychko, Pyrizhky, Syrnyky) have near-zero data.** These terms barely exist in English. Consider dropping them or grouping them as "zero-adoption" examples.

### Findings that STRENGTHEN the paper:

1. **The invasion effect is robust across all three independent sources** (GDELT, Reddit, Trends), despite each source's individual biases.
2. **The "brand strength" gradient is clear:** Cities with strong pre-existing English-language identities (Chernobyl, Kiev) resist renaming far more than lesser-known places (Ternopil, Chernivtsi).
3. **Dnipro is the most successful rebrand** -- it went from a long Russian name (Dnepropetrovsk) to a short Ukrainian one (Dnipro), achieving 99.8% adoption.
4. **Russian-language media is 100% holdout** -- ria.ru, tass.ru, lenta.ru show 0% adoption, providing a clear control group.
5. **German media is a notable Western holdout** -- merkur.de and n-tv.de show 100% "Kiev" (German: "Kiew").

### Red flags that could get the paper rejected:

1. **The GDELT V2Locations bias is the #1 risk.** If a reviewer discovers that GDELT is matching its own metadata rather than article text, it undermines the core finding. Must fix or disclose prominently.
2. **Ukrainian media with /ru/ sections conflate two phenomena:** (a) genuine Russian-language content and (b) GDELT metadata matching. The paper must distinguish between these.
3. **Wikipedia data for 20 pairs is meaningless** (same page mapped to both variants). If this is not caught, a reviewer could invalidate the entire Wikipedia analysis.
4. **The Reddit 100-cap makes volume claims impossible.** Any claim about "Reddit users adopted Kyiv at rate X" could be challenged.
5. **Chernobyl/Chornobyl Wikipedia comparison is misleading** -- "Chernobyl disaster" (74M views) vs "Chornobyl" (43K views) are different articles with different scopes, not just different spellings of the same place.

### Policy-actionable findings for MFA:

1. **Name length matters:** Dnipro (6 chars) replaced Dnepropetrovsk (15 chars) at 99.8%. Zaporizhzhia (12 chars, hard to spell) is at only 83.5%. **Shorter, easier-to-spell Ukrainian names adopt faster.**
2. **Brand entrenchment is the main barrier:** Chernobyl, Kiev, and Babi Yar resist because they are embedded in Western cultural memory, not because of political resistance.
3. **German-language media is an untapped advocacy target** -- 100% holdout suggests no diplomatic engagement.
4. **Russian-language media will never adopt** -- 0% adoption confirms this is a political, not linguistic, choice.
5. **The invasion was the single most effective adoption event** -- MFA should leverage ongoing geopolitical attention while it lasts.

---

## 14. Data Volume Summary

| Source | Rows | Pairs covered | Date range |
|---|---|---|---|
| GDELT | 38,707,124 | ~30 | 2015-2026 |
| Google Trends | 24,570 | ~35 | 2004-2026 |
| Wikipedia | 14,903 | ~35 | 2015-2026 |
| Reddit | 22,506 | ~50 | 2007-2026 |
| Google Ngrams | 12,120 | ~5 | 1900-2019 |
| **Total** | **38,781,223** | | |

GDELT dominates at 99.8% of all data. The paper should acknowledge this asymmetry.
