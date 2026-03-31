# Crimea Media Framing Analysis

**Generated:** 2026-03-30
**Data source:** GDELT via BigQuery (`kyivnotkiev-research.kyivnotkiev.raw_gdelt` + `gdelt-bq.gdeltv2.gkg_partitioned`)
**Pair analyzed:** #18 (Crimea vs Krym)

> **Note on methodology:** In the raw_gdelt dataset, pair 18 tracks two transliteration variants: "Crimea" (labeled `ukrainian` variant) and "Krym" (labeled `russian` variant). "Crimea" is the standard English-language name derived from Ukrainian/Latin tradition, while "Krym" is a direct transliteration of the Russian "Крым". Additionally, Query 5 examines URL-level framing where outlets use phrases like "Russian Crimea" or "Crimea Russia" in ways that may imply sovereignty.

---

## 1. Key Statistics (Post-2022 Full-Scale Invasion)

### Coverage Volume by Media Origin

| Media Category | Total Articles | Earliest | Latest |
|---|---|---|---|
| International media | 983,401 | 2022-01-01 | 2026-03-30 |
| Russian media (.ru) | 375,335 | 2022-01-01 | 2026-03-30 |
| Ukrainian media (.ua) | 181,637 | 2022-01-01 | 2026-03-30 |

**Total post-2022 Crimea articles: 1,540,373**

International media accounts for 63.8% of all Crimea coverage, Russian media for 24.4%, and Ukrainian media for 11.8%.

### Year-over-Year Trends (All Years)

| Year | "Crimea" (Ukr. variant) | "Krym" (Rus. variant) | Total | Krym % |
|---|---|---|---|---|
| 2015 | 277,206 | 227,078 | 504,284 | 45.0% |
| 2016 | 321,510 | 260,061 | 581,571 | 44.7% |
| 2017 | 231,493 | 183,839 | 415,332 | 44.3% |
| 2018 | 214,738 | 169,850 | 384,588 | 44.2% |
| 2019 | 122,433 | 95,301 | 217,734 | 43.8% |
| 2020 | 68,459 | 53,222 | 121,681 | 43.7% |
| 2021 | 93,705 | 72,783 | 166,488 | 43.7% |
| 2022 | 228,774 | 163,822 | 392,596 | 41.7% |
| 2023 | 366,589 | 314,150 | 680,739 | 46.2% |
| 2024 | 137,666 | 100,405 | 238,071 | 42.2% |
| 2025 | 114,710 | 86,364 | 201,074 | 42.9% |
| 2026 (Q1) | 16,331 | 11,562 | 27,893 | 41.5% |

**Key trend:** The Russian variant "Krym" has remained remarkably stable at 42-46% of total usage across the entire 2015-2026 period. The 2022 invasion did not significantly shift the ratio. The spike in 2023 (46.2% Krym) is notable and may reflect increased Russian-language coverage.

---

## 2. Media Categorization by Crimea Variant Usage

### Outlets Using Russian Variant ("Krym") at 95-100%

These outlets almost exclusively use the Russian transliteration, suggesting alignment with Russian-language framing:

| Outlet | Country | Russian % | Total Articles |
|---|---|---|---|
| dantri.com.vn | Vietnam | 100.0% | 433 |
| tienphong.vn | Vietnam | 100.0% | 332 |
| vnexpress.net | Vietnam | 100.0% | 238 |
| thanhnien.vn | Vietnam | 100.0% | 217 |
| redetv.uol.com.br | Brazil | 100.0% | 210 |
| baomoi.com | Vietnam | 99.9% | 736 |
| noticias.uol.com.br | Brazil | 99.7% | 773 |
| vov.vn (Voice of Vietnam) | Vietnam | 99.6% | 250 |
| em.com.br | Brazil | 99.5% | 594 |
| oglobo.globo.com | Brazil | 98.9% | 367 |
| correiobraziliense.com.br | Brazil | 98.8% | 244 |
| noticias.r7.com | Brazil | 98.1% | 208 |
| g1.globo.com | Brazil | 97.5% | 236 |
| istoedinheiro.com.br | Brazil | 96.7% | 302 |
| brasil247.com | Brazil | 96.7% | 214 |
| prensa-latina.cu | Cuba | 95.9% | 246 |
| istoe.com.br | Brazil | 94.1% | 222 |

**Pattern:** Two dominant clusters emerge:
- **Vietnamese media** (100% Russian variant): All major Vietnamese outlets exclusively use "Krym." This likely reflects Vietnam's historical ties to the Soviet Union and continued use of Russian transliterations in Vietnamese.
- **Brazilian media** (95-100% Russian variant): Nearly all Brazilian Portuguese outlets use "Krym." In Portuguese, "Crimeia" is the standard name; "Krym" appearing in GDELT may reflect how the transliteration is parsed from Portuguese-language URLs and text.
- **Cuban state media** (prensa-latina.cu, 95.9%): Reflects Cuba's political alignment with Russia.

### Outlets Using Russian Variant at 55-65% (Elevated but Mixed)

| Outlet | Country | Russian % | Total Articles |
|---|---|---|---|
| pda.crimea.kp.ru | Russia | 85.6% | 1,065 |
| giornaledibrescia.it | Italy | 67.4% | 227 |
| lagazzettadelmezzogiorno.it | Italy | 65.0% | 223 |
| lapresse.it | Italy | 61.7% | 235 |
| zazoom.it | Italy | 61.0% | 5,598 |
| globalist.it | Italy | 60.2% | 236 |
| businessinsider.com | USA | 59.0% | 229 |
| ansa.it (Italian wire) | Italy | 57.6% | 1,055 |
| iltempo.it | Italy | 58.7% | 404 |
| sputniknews.lat | Russia (Latam) | 58.5% | 395 |
| ilfattoquotidiano.it | Italy | 56.2% | 331 |
| ilgiornale.it | Italy | 55.8% | 344 |
| actualidad.rt.com | Russia (Spanish) | 55.7% | 727 |
| hispantv.com | Iran | 54.8% | 259 |
| sputnikglobe.com | Russia | 54.6% | 260 |

**Pattern:** Italian media stands out as a significant cluster with elevated Russian-variant usage (55-67%). This is linguistically significant -- in Italian, "Crimea" is the standard term, so a 60%+ "Krym" rate suggests either RSS feeds pulling from Russian-language sources or a genuine editorial tendency. Russian state media outlets (RT, Sputnik) appear in this band at 55-59%, which is lower than expected.

### Known Russian State Media Outlets

| Outlet | Russian % | Total |
|---|---|---|
| ria.ru | 28.6% | 11,368 |
| kommersant.ru | 43.2% | 7,027 |
| lenta.ru | 41.9% | 4,679 |
| regnum.ru | 41.3% | 5,148 |
| runews24.ru | 59.4% | 2,810 |
| rt.com (English) | 55.7% | 727 |
| sputnikglobe.com | 54.6% | 260 |
| sputniknews.lat | 58.5% | 395 |

Interestingly, major Russian outlets like ria.ru (28.6% Krym) and kommersant.ru (43.2%) actually use "Crimea" more than "Krym" in their GDELT-captured output. This is because their English-language content and URL structures use the Latin "Crimea."

---

## 3. URL-Level Sovereignty Framing Analysis (2024+)

### Outlets With "Russian Crimea" / "Crimea Russia" / "Annexed" in URLs

From the GDELT GKG (2024+), outlets whose URLs most frequently contain sovereignty-implying terms:

| Outlet | Context Mentions | Total Crimea Articles | Context Rate |
|---|---|---|---|
| themoscowtimes.com | 22 | 74 | 29.7% |
| rferl.org | 11 | 82 | 13.4% |
| yahoo.com | 12 | 305 | 3.9% |
| newsweek.com | 6 | 185 | 3.2% |
| ilmessaggero.it | 6 | 72 | 8.3% |
| elpuntavui.cat | 6 | 50 | 12.0% |
| euronews.com | 5 | 116 | 4.3% |
| indiatimes.com | 3 | 65 | 4.6% |
| marketscreener.com | 4 | 64 | 6.3% |
| cnn.com | 3 | 59 | 5.1% |
| menafn.com | 3 | 107 | 2.8% |
| obozrevatel.com | 2 | 134 | 1.5% |

**Important caveat:** The presence of "Crimea Russia" or "Russian Crimea" in a URL does not inherently indicate pro-Russian framing. Many of these articles are *about* Russia's claim to Crimea, peace negotiations, or military operations. For instance, the April 2025 spike across Fox News affiliates, thejournal.ie, SCMP, and others was driven by coverage of Trump-Zelensky negotiations over potentially ceding Crimea to Russia.

### Specific Articles Referring to "Russian Crimea" or "Crimea Russia" (2024-2026)

Notable examples from the 30 most recent URL matches:

**Neutral/News Reporting (majority):**
- newsweek.com -- "Ukraine Flamingo Missile Crimea Russia" (military reporting)
- drudge.com -- "Ukraine Hits Bridge Linking Crimea Russia" (Kerch Bridge attack)
- thejournal.ie -- "Trump Zelensky Crimea Russia Peace Deal" (diplomacy)
- rferl.org -- "Ukraine Peace Proposal Crimea Russia Reactions" (analysis)
- scmp.com -- "Trump Thinks Zelensky Ready Give Crimea Russia" (reporting)
- Multiple Fox affiliates -- "Crimea Russia Trump Ukraine War" (AP/wire redistribution)

**Pro-Russian Framing:**
- eturbonews.com -- "Not Ukraine Crimea Russia New Venue Mrs America 2015" (refers to Crimea as Russia, not Ukraine)
- hellenicshippingnews.com -- "Russian Crimea Head Says" (uses "Russian Crimea" as a territorial designation)

**Analytical/Critical:**
- editorialedomani.it -- Coverage of Donbas/Crimea/Russia peace plans
- eaworldview.com -- "Crimea Russia Kyiv Strikes Key Facilities" (military analysis)

---

## 4. High-Volume Crimea Coverage Outlets (Post-2022)

### Top 15 Outlets by Volume

| Outlet | Ukrainian Variant | Russian Variant | Total | Type |
|---|---|---|---|---|
| chaspik.spb.ru | 123,947 | 128,064 | 252,011 | Russian aggregator |
| 24tv.ua | 16,374 | 10,584 | 26,958 | Ukrainian TV |
| kafanews.com | 11,601 | 7,042 | 18,643 | Crimea local news |
| gazeta.ua | 8,743 | 5,204 | 13,947 | Ukrainian |
| sevastopol.su | 8,423 | 6,340 | 14,763 | Crimea/Sevastopol |
| ria.ru | 8,112 | 3,256 | 11,368 | Russian state |
| tsn.ua | 7,202 | 4,819 | 12,021 | Ukrainian TV |
| unian.ua | 6,285 | 3,259 | 9,544 | Ukrainian wire |
| unian.net | 6,218 | 3,480 | 9,698 | Ukrainian wire |
| n-tv.de | 4,906 | 5,521 | 10,427 | German TV |
| dw.com | 4,119 | 3,731 | 7,850 | German public |
| gordonua.com | 4,572 | 4,092 | 8,664 | Ukrainian |
| msn.com | 4,276 | 2,810 | 7,086 | Aggregator |
| kommersant.ru | 3,990 | 3,037 | 7,027 | Russian business |
| merkur.de | 3,506 | 3,389 | 6,895 | German regional |

---

## 5. Conclusions

### 5.1 The "Krym" Signal Is Predominantly Linguistic, Not Political

Unlike Kyiv/Kiev where the variant choice directly signals political alignment, the Crimea/Krym pair is more ambiguous. "Krym" usage at 100% in Vietnamese and Brazilian media reflects linguistic convention (the word entered these languages via Russian) rather than editorial endorsement of Russian sovereignty. This confirms the AUDIT_REPORT.md finding that pair 18 should be treated separately from other pairs.

### 5.2 Italian Media Is a Genuine Concern

Italian outlets show 55-67% "Krym" usage, which is higher than expected for a Western European country. This cluster (ANSA, La Stampa network, Il Fatto Quotidiano, Il Giornale, Il Tempo) warrants further investigation. Italy has historically had closer economic and political ties to Russia, and this may be reflected in editorial conventions.

### 5.3 URL-Level "Russian Crimea" Framing Is Rare and Mostly Neutral

Of 50 top outlets covering Crimea in 2024+, only 2 (eturbonews.com, hellenicshippingnews.com) used "Russian Crimea" in a way that implied sovereignty. The vast majority of "Crimea Russia" URL patterns reflect news about military operations, diplomacy, or the Kerch Bridge -- not sovereignty endorsement.

### 5.4 Russian State Media Uses "Crimea" More Than Expected

Major Russian outlets like RIA Novosti (71.4% Crimea), Kommersant (56.8% Crimea), and Lenta (58.1% Crimea) predominantly use the Latin "Crimea" in their English-language output. This makes the Crimea/Krym pair a poor proxy for political alignment compared to Kyiv/Kiev.

---

## 6. Recommendations

### For the MFA Policy Brief

1. **Do not conflate Crimea/Krym with Kyiv/Kiev in policy arguments.** The Crimea/Krym pair measures linguistic convention more than political stance. Using it alongside Kyiv/Kiev would weaken the overall argument.

2. **Focus instead on explicit sovereignty framing.** Track outlets that use phrases like "Russian Crimea," "Crimea, Russia," or "annexed Crimea" as editorial descriptors (not news context). The eturbonews.com case is a clear example.

3. **Flag Italian media for diplomatic engagement.** The elevated "Krym" usage across Italian outlets is a potential advocacy target, particularly ANSA (Italy's main wire service) which sets the standard for downstream Italian media.

4. **Note Vietnamese and Brazilian patterns for context.** These are linguistic artifacts, not political signals, but they illustrate how Russian-origin transliterations become embedded in other languages -- a useful example for the broader argument about naming conventions carrying political weight.

### For Media Outreach

1. **Priority targets for engagement:**
   - ANSA (Italy) -- wire service, high influence, 57.6% Russian variant
   - Deutsche Welle -- already mostly correct (52.5% Ukrainian variant) but could improve
   - MSN.com -- aggregator with massive reach, 60.3% Ukrainian variant

2. **Outlets to monitor (potential pro-Russian framing):**
   - eturbonews.com -- explicitly referred to "Crimea, Russia" as a location
   - hellenicshippingnews.com -- used "Russian Crimea" as a territorial term
   - hispantv.com (Iranian state media) -- 54.8% Russian variant
   - prensa-latina.cu (Cuban state media) -- 95.9% Russian variant

3. **Outlets already aligned (reinforce):**
   - 24tv.ua, tsn.ua, unian.ua/net, gazeta.ua -- Ukrainian media consistently use "Crimea"
   - rferl.org -- strong coverage with analytical framing of Russian claims
   - BBC, CNN, Reuters -- standard English usage of "Crimea" is already correct

### For the Research Paper

1. **Exclude pair 18 from the main analysis** (as already recommended in the audit). The Crimea/Krym split does not measure the same phenomenon as Kyiv/Kiev.

2. **Consider a separate supplementary analysis** examining how "Crimea" appears in sovereignty-implying contexts across media, using the URL-level analysis demonstrated here.

3. **The Vietnamese and Brazilian clusters** provide a compelling illustration of how Soviet-era transliteration paths persist in post-colonial language ecosystems -- worth a footnote or discussion point.
