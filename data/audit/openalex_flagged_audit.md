# OpenAlex flagged-works audit — academic holdouts table

Date: 2026-09-07. Scope: 8 works flagged by a user on the site's "academic holdouts"
exhibit (papers allegedly still using Russian spellings). Sources: local
`data/cl/raw/openalex/all_pairs.parquet` (41,762 rows; 39,713 distinct openalex_ids)
plus live OpenAlex API (and HAL API for item 7). Exhibit logic audited:
`pipeline/export_site_data.py::export_openalex_holdouts` (metalinguistic both-variants
filter, `OPENALEX_COLLISIONS`, `_non_en_title` function-word filter, `_tkey` title dedupe).

Category codes: (a) English-text usage · (b) non-English language's own convention ·
(c) different referent / frozen nomenclature · (d) metalinguistic mention ·
(e) duplicate record of another listed work.

## Summary table

| # | openalex_id | Language | Year | What it is | Category | Recommended action | Current export code |
|---|-------------|----------|------|------------|----------|--------------------|---------------------|
| 1 | W4411445331 | en | 2024 | EMPIAR cryo-EM **dataset** (EMBL-EBI): antibody T5-1E08 + influenza HA trimer of strain **A/Kiev/1/57** | (c) frozen strain nomenclature | **exclude-wrong-referent** | still KEPT — needs fix |
| 2 | W4408689132 | en | 2024 | Sibling EMPIAR dataset, antibody T5-1E11, same strain (DOI empiar-12307) | (c) — **not (e): not a duplicate** | **exclude-wrong-referent** | still KEPT — needs fix |
| 3 | W4409771702 | en | 2024 | Sibling EMPIAR dataset, antibody T5-1E08 UCA, same strain (DOI empiar-12309) | (c) — **not (e): not a duplicate** | **exclude-wrong-referent** | still KEPT — needs fix |
| 4 | W4408506764 | en (UA journal) | 2023 | Art-history article; title's "Kiev Russian" is a garbled translation of **Київська Русь / Kyivan Rus** | (c) different referent (medieval polity) + translation artifact | **exclude-wrong-referent** | still KEPT — needs fix |
| 5 | W4387918963 | en | 2022 | English article in a Portuguese journal with bilingual EN\|PT double title; **English half uses "Kyiv's"**, "Kiev" only in the PT half | (b) Portuguese exonym | **exclude-as-non-English** | already excluded (both-variants filter) |
| 6 | W4399752334 | uk | 2024 | Ukrainian-language imagology article; "KIEV" only inside the quoted French book title «La petite princesse des neiges Anne de Kiev» | (b) + (d) quoted French title in a uk-language work | **exclude-as-non-English** | already excluded (fw filter) |
| 7 | W4391076553 | uk | 2024 | Ukrainian-language HAL article (K. Vetochnikov, 1686 Kyiv Metropolis jurisdiction); "Kiev" was in HAL's **French catalogue title**; OpenAlex now shows the Ukrainian title as primary | (b) French exonym in catalogue metadata | **exclude-as-non-English** (do NOT link the French version — see below) | already excluded (fw filter) |
| 8 | W7134494333 | pt (inferred; API 404) | 2024 | Portuguese **news headline** (Slovakia threatens Kyiv over Russian gas transit), from the W7-billion bulk batch; **deleted from OpenAlex** — API 404, zero title-search hits; the site's dead link is the openalex.org work URL itself | (b) + non-academic + defunct record | **exclude / remove** (self-resolves on re-export and re-ingest) | already excluded (fw filter) |

## Per-item detail

### 1–3. W4411445331 / W4408689132 / W4409771702 — the "A/Kiev/1/57" EMPIAR trio
- All three are EMPIAR raw-micrograph **dataset depositions** (OpenAlex `type: dataset`,
  source "EMPIAR dataset", EMBL-EBI), not papers: DOIs 10.6019/empiar-11952, -12307,
  -12309. One study series, three distinct antibodies (T5-1E08, T5-1E11, T5-1E08 UCA).
- The user's suspicion "a different Kiev" is close but not exact: **A/Kiev/1/57** is the
  WHO influenza strain designation (H1N1 isolated in Kyiv in 1957). Strain names are
  immutable identifiers frozen at isolation time — the token can never become "Kyiv",
  and the referent of the matched string is a virus strain, not a contemporary naming
  choice for the city. Zero evidential value for the holdouts exhibit.
- The user's "duplicate" call on items 2–3 is **wrong**: three distinct works, distinct
  DOIs — do NOT collapse-duplicates; exclude each on referent grounds instead.
- Implementable guard (parquet lacks the `type` field): title regex
  `\bA/Kiev/\d+/\d+\b` (influenza strain pattern) added to the holdout exclusion, or a
  per-id blocklist alongside `OPENALEX_COLLISIONS`.

### 4. W4408506764 — "Masters of Kiev Russian"
- National Academy of Managerial Staff of Culture and Arts Herald (Kyiv), 2023, DOI
  10.32461/2226-3209.4.2023.324755. The English title fragment "Masters of Kiev
  Russian" is a machine-garbled rendering of "майстрів Київської Русі" — the abstract's
  own English text says "**Kyivan Rus**".
- The matched "Kiev" therefore names the medieval polity (Kyivan Rus), not the modern
  city, inside a broken translation produced by a *Ukrainian* journal — the opposite of
  a Russian-spelling holdout. Exclude-wrong-referent.
- Guard suggestion: exclude titles matching `\bKiev(an)?\s+Rus` from the pair-1 holdout
  exhibit (historical-polity referent), plus per-id entry for this work (its exact
  string "Kiev Russian" is nonstandard).

### 5. W4387918963 — bilingual EN|PT double title
- Political Observer — Revista Portuguesa de Ciência Política, 2022. OpenAlex stores
  the double title: "…from **Kyiv's** independence… | …da independência de **Kiev** à
  agressão de Moscovo…". The English half already uses the Ukrainian spelling; the
  "Kiev" hit comes only from the Portuguese half, where Kiev is the standard exonym.
- As a holdout it is a false positive; as a datapoint the English text is actually
  *adoption* evidence. The current metalinguistic filter already drops it from the
  exhibit (title carries both variants) — right outcome, mislabeled reason. For series
  counting, note the parquet row is tagged variant=russian; strictly it attests both.

### 6. W4399752334 — Anna Yaroslavna article (FOLIUM)
- Language `uk` per API. Ukrainian-language comparative-literature article; the only
  Latin-script "KIEV" is inside the quoted title of M.-C. Monchaux's French children's
  book «La petite princesse des neiges Anne de Kiev». French convention inside a
  Ukrainian text, and a mention (quoted title) rather than a use. Exclude-as-non-English.

### 7. W4391076553 — HAL record, 1686 Kyiv Metropolis
- HAL hal-04405308, author Konstantinos Vetochnikov, `language: uk`. At ingestion time
  the record's display title was HAL's French catalogue title "La juridiction de la
  métropole de Kiev… [en ukrainien]"; OpenAlex now serves the Ukrainian title
  ("Чи насправді у 1686 році відбулася зміна юрисдикції Київської митрополії ?") as
  primary. The "Kiev" was French catalogue metadata, not English usage.
- The user suggested linking the French version. It exists — HAL hal-04406334 =
  OpenAlex **W4391089265**, "La «concession» de la métropole de Kiev au patriarche de
  Moscou en 1686 : Analyse canonique" (`language: fr`) — but it is a *French-language*
  work whose "Kiev" is the standard French exonym, so it does not belong in an
  English-usage holdouts table either. Recommendation: plain exclude-as-non-English,
  no alternate-version link.

### 8. W7134494333 — "Eslováquia ameaça Kiev…"
- Portuguese news headline (Slovakia/Fico threatening Kyiv with retaliation for ending
  Russian gas transit — the Dec-2024 story). Part of OpenAlex's recent high-id
  (W ≥ 5×10⁹) bulk ingestion of catalogue/news/database content.
- The record has been **deleted upstream**: `GET /works/W7134494333` → HTTP 404 (plain
  not-found, not a merge redirect), and title search returns 0 hits. The site's dead
  landing page is explained by the export using `openalex_id` as the link URL — the
  openalex.org page itself is gone.
- Action: nothing manual needed beyond re-export (the `_non_en_title` filter already
  catches it: "com", "por", "ameaça"); it drops out of the parquet entirely on the next
  OpenAlex re-ingestion. The stale deployed site data should be refreshed.

## Why 4 of 8 flags were already fixed
The current `export_openalex_holdouts` guards (metalinguistic both-variants filter and
the Romance/Germanic function-word filter, whose comment cites item 8's exact title)
post-date the deployed site data the user was reading. Items 5–8 vanish on re-export.
Items 1–4 are the real residue: **English-titled wrong-referent cases have no guard**
(frozen strain nomenclature; Kyivan-Rus polity). A per-id blocklist constant next to
`OPENALEX_COLLISIONS`, or the two title regexes suggested above, closes the gap.

## Class question (i): non-English titles in the corpus
Sample: 300 random rows (`random_state=42`) of all_pairs.parquet; script check +
langdetect + manual adjudication of every flagged title.

- Cyrillic-script titles: **10/300**
- Latin-script, genuinely non-English natural language (manually confirmed out of 58
  langdetect flags): **25/300** — French, Turkish, German, Italian, Spanish,
  Portuguese, Indonesian, plus romanized Ukrainian/Russian book-review titles
- Botanical-Latin taxon records ("Lipandra polysperma (L.) S. Fuentes, Uotila &
  Borsch" type — language-neutral nomenclature, its own contamination class): **10/300**
- langdetect false positives (English, mostly ALL-CAPS Ukrainian-journal titles): 23

**Clearly non-English titles: ~35/300 = 11.7%** (binomial 95% CI ≈ 8.4–15.9%);
**15.0%** if the botanical-Latin records are counted as non-English. Two incidental
wrong-referent families surfaced by the sample: Turkish "Kazak" titles are about
*Kazakh* Turkic culture (not Cossacks), and the botanical "Borsch" is the taxonomist
Thomas Borsch (already known: `OPENALEX_COLLISIONS` excludes the borscht slug).

## Class question (ii): duplicate OpenAlex records beyond the flagged ones
Method: distinct openalex_ids (39,687 with a ≥5-char normalized title), titles
normalized (NFKC, lowercase, punctuation→space); groups with >1 distinct id.

**1,380 normalized-title groups covering 5,316 distinct ids (13.4% of works).**
Decomposition:
- 11 botanical taxon-name groups = 2,176 ids (ChecklistBank-style records in the
  W7-billion batch; all pair-64/"Borsch" collisions, biggest group n=999).
- 33 groups are ≤12-char generic titles — homonym collisions, not duplicate records
  (e.g. "Odessa" ×18 across 1963–2022, genuinely different works).
- Remainder ≈ 1,336 groups / ~3,000 ids: journal front-matter repeated per issue and
  genuine same-work multi-record duplication.

Five examples (beyond the flagged items — none of which, note, were actually
duplicates):
1. "Multiple large explosions hit center of Ukraine's capital Kyiv for first time in
   months" — **9 records**, 2022 news-wire ingestion (W4304108588, W4304108591,
   W4304108544, W4307015520, W4304108543, …).
2. "Inverse modelling-based reconstruction of the Chernobyl source term…" —
   W2118937917 + W4232605331 (2007; classic MAG/Crossref double record).
3. "Ukrainian Nationalism in the 1990s: A Minority Faith" — W1979166454 + W331464272
   (1997 book, two records).
4. "Scientific Bulletin of the Odessa National Economic University" — **42 records**
   (issue-level front-matter, one identical title per issue, 2018–2019).
5. "HYGIENIC PERIODICALS AND THE SHAPING OF DISCOURSE OF PUBLIC HEALTH IN LVIV…" —
   W3004351442 + W7117639583: an old-range id and a W7-batch id for the same article,
   i.e. the 2025–26 bulk batch **re-ingested existing works under new ids**.

Implications: the exhibit already collapses exact-title duplicates
(`drop_duplicates("_tkey")`), so the holdouts table is protected; the openalex *series*
counts are not deduplicated and multi-count such works. Near-duplicates with differing
titles (the EMPIAR trio pattern) escape any title-based dedupe. Separately, the
W ≥ 5×10⁹ id stratum is 5,256 of 39,713 distinct ids (13.2%) and is dominated by
catalogue/database/news junk (romanized Russian library records "Drevnij Kiev",
taxon records, PT news), at least one member of which OpenAlex has since deleted —
worth quarantining or re-validating wholesale at the next refresh.
