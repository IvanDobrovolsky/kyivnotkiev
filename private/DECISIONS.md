# Methodology decisions

A record of what was changed, why, and what the numbers were. Each entry states the
evidence that forced the decision, so a later reader can re-open it on the same terms.

Last updated 2026-08-26.

---

## 1. GDELT mention counts were rebuilt from scratch

**The old series was not measuring English spelling.** `dataset/raw_gdelt.parquet` held
20,164,767 "mentions" produced by matching `DocumentIdentifier OR AllNames` and storing
only a collapsed `matched_term`. Three defects, all measured against the rebuilt pull of
12,495,018 rows:

| defect | measurement |
|---|---|
| `AllNames` dominates | **78.9%** of rows matched only `AllNames` — a canonicalised NER field, not text an outlet wrote |
| Documents were not English | **88.7%** were machine-translated from another language |
| Substring matching | `LIKE '%borsch%'` also matched "borscht", reporting 27% Ukrainian where the attested figure is 79.6% |

`AllNames` normalises entity names across languages, so a Ukrainian article's
"Володимир Великий" surfaced as "Vladimir The Great" and was counted as *Russian*
usage. That is a variant flip, not a miss, and it is invisible in totals.

**The URL column was never persisted**, so no row could be re-adjudicated. A re-query
was mandatory; there was nothing to patch.

### The rebuild

`pipeline/ingestion/gdelt_mentions.py` — `query` → `download` → `clean` → `final` → `export`.

- Persist `DocumentIdentifier` on every row. Never aggregate away provenance.
- Never collapse the OR: `url_match` and `allnames_match` are separate booleans.
- Variant comes **only** from the URL path, which is CMS-authored text. `AllNames` is a
  volume denominator and is never summed with the attested series.
- Word-boundary regex, never `LIKE '%x%'`.
- Nothing filtered server-side; `TranslationInfo` is captured raw so filtering stays auditable.

**Cost is flat in the number of pairs.** BigQuery bills columns × partitions, not matches,
so a one-pair and a 24-pair query measured byte-for-byte identical at 667,884,946,710 bytes
(0.607 TiB). Always query every pair in one pass; per-pair querying would cost ~14.6 TiB for
the same data. Results land in `kyivnotkiev-bq.gdelt_v2.mentions_raw` so the scan never repeats.

---

## 2. Only natively-English documents count

`TranslationInfo` is `srclc:<lang>;eng:<engine>` when GDELT translated a document, and NULL
when it was already English. Verified, not assumed: `rus` 4,226,908 · `ukr` 3,572,132 ·
**NULL (English) 1,413,221**.

**This filter is mandatory, not conservative.** Restricting to rows where the spelling is
readable in the URL either way, the Ukrainian share differs between English and translated
documents by a **mean of 28.6 points**:

| pair | UA% English | UA% translated | gap |
|---|---|---|---|
| kharkiv | 98.2 | 19.5 | 78.7 |
| borscht | 79.6 | 5.2 | 74.3 |
| luhansk | 88.0 | 15.1 | 73.0 |
| kyiv | 78.6 | 33.7 | 44.9 |
| chornobyl | 5.3 | 4.5 | 0.8 |

By source language: `rus` 16.8% Ukrainian, `ukr` 77.3%, English 67.8%. GDELT translates body
text but never rewrites URLs, so `lenta.ru/kiev-...` looks "attested" while attesting
*Russian-language* usage. Including translated rows would make the series move with GDELT's
monthly language mix rather than with English usage.

**The cost is statistical power, not bias.** Retention varies 85-fold by pair — ternopil 1.1%,
lviv 1.4%, kharkiv 2.7% versus kyivan-rus 93.9%. Eight pairs fall under 100 usable rows and
must lean on YouTube, Reddit and OpenAlex.

---

## 3. Attested vs unattested

Of 1,401,638 unique English articles:

- **Attested (332,676, 23.7%)** — the spelling appears in the URL path.
- **Unattested (1,068,962, 76.3%)** — matched only via `AllNames`, which is canonicalised, so
  the spelling is unknown. These contribute nothing to the metric.

Attestation is **not random**. It is a property of each outlet's CMS: mean 23.5% per domain,
std 18.1, range 0–100%. **111 domains sit at 0%, covering 68,429 English articles** —
`interfax.com.ua` (13,425), `globalsecurity.org` (6,792), `tass.com` (4,091),
`charter97.org` (2,343) all use numeric URLs and are structurally invisible, while
`unian.info` is 82% visible. TASS being absent bounds what the holdout analysis can claim
about state media, and it is why `sputniknews.com` dominated the holdout tables: its CMS
uses slugs, so it was the only state outlet eligible.

Attestation also splits by pair shape — single-token cities land in slugs (bakhmut 89%,
donbas 70%, chornobyl 66%), multi-word person names almost never do (zelenskyy 1.4%,
mykola-hohol 1.2%, serhii-korolyov 1.3%). Report that split as a finding.

---

## 4. Article text is the only authority

`pipeline/ingestion/gdelt_fetch_texts.py` → `pipeline/cl/corpus/gdelt_verified.py`.

A slug is written once and never revised; `AllNames` is canonicalised. Both are retrieval
hints, not evidence. Three rules:

1. **Reclassify, don't reconcile.** `url_claimed` is kept for auditing only.
2. **Retrieval without usage is not an observation.** An article can match GDELT and never use
   either spelling — the entity came from a headline extraction dropped, a nav menu, a
   related-links block. Removed from the texts *and* the series, because counting it would
   count GDELT's guess rather than an author's choice.
3. **One record per article**, deduplicated on body hash.

The series is a **derived view** of the surviving records, so the chart and the corpus cannot
disagree.

**URL vs body agreement is 96.6%** on babyn-yar (254 of 263 strict comparisons) and 100% on
volodymyr-the-great — a validation the URL metric never had. A body containing *both* spellings
is tracked separately as `enriched`, not as a disagreement: it does not contradict the slug, it
exceeds it. Conflating the two initially made the URL metric look far worse than it is (99
apparent disagreements on babyn-yar were really 9 disagreements plus 90 both-spelling bodies).
Disagreements run both ways: `unian.info/kiev/...-kyiv-stuck-in-traffic-jams` is a legacy
section path over a body saying Kyiv 3×; `sbnation`'s `dynamo-kyiv` slug sits over a body
saying Kiev.

### Fetch guards, all found on a 37-URL trial

- **Parked domains answer 200 with their current homepage.** Two `uatoday.tv` articles dated
  2015 and 2016 both returned identical text reading "Enemy losses on 26.08.2026" — the
  domain now redirects to `unian.info/`. Worse than a 404 because it looks like data.
  Detected structurally: a deep path collapsing to a homepage.
- **Body hashing** for syndicated wire copy and GDELT re-records. Exact hashing only — the
  same column ran on six sites and four copies differ in site chrome. Near-duplicate
  detection needs shingling, and belongs before clustering, not before fetch.

### Measured yields

**32% of URLs become unique usable texts** (not the 48% first quoted, which counted duplicates
and stale redirects as successes). Extraction yield rises with recency: 40% in 2017, 68% in
2023. Throughput is 11.8 url/s at concurrency 32 — the attested set is ~8 hours, attested plus
unattested ~33 hours.

**No distributed fetching.** Median 3 URLs per domain across 9,404 domains means the limit is
per-domain courtesy, not parallelism, and Dataflow would bill for workers idle on network IO.

### The two bias terms fail in orthogonal ways

- **URL-attested**: outlet-selection bias, *no* survivorship — nothing is fetched, so every row
  is present by construction.
- **Body-attested**: no outlet selection, but **variant-correlated survivorship**. Controlling
  for year, Ukrainian-variant URLs survive ~14 points more often than Russian ones (5 of 6
  years; thin cells, directional).

Their overlap — attested URLs successfully fetched — is the calibration set, the only place
URL variant, body variant and fetch outcome are all observed. That makes inverse-probability
weighting per (year, variant, domain) estimable rather than assumed.

**Measured on babyn-yar at volume**: the same attested pool reads 28.7% Ukrainian across all
1,071 URLs, and **46.4% across the 353 that survived fetching — a +17.7 point survivorship
shift.** Any verified-text series carries this and must report it or reweight. Because rot rises
with age, the inflation is larger in older years, so a rising Ukrainian trend is understated
rather than manufactured by it.

**A small-sample result that did not survive.** On volodymyr-the-great (7 unattested records)
unattested articles looked 63 points more Ukrainian than attested ones. At n=2,207 on babyn-yar
they are **7.4 points less** Ukrainian. The n=7 signal was noise; do not generalise attested-vs-
unattested composition from a thin pair.

---

## 5. Holdout tables

One rule set for **every** source: `HOLDOUT_SINCE = 2022-01-01`, `HOLDOUT_CAP = 100`,
`HOLDOUT_VARIANTS = (russian, both)`, `HOLDOUT_PER_DOMAIN = 3`.

- Holdouts are evidence of *current* usage. An outlet that switched in 2019 is not a holdout
  today, so the window starts at the invasion.
- Ukrainian-only rows are not holdouts and are excluded.
- **The per-source cap matters**: `sputniknews.com` was 77 of 100 rows for donbas and 66 for
  kyiv, so the panel showed one publisher rather than who still uses the old spelling. Capping
  at 3 per domain took donbas from 13 to 54 distinct domains and kyiv from 24 to 58. YouTube
  is capped per channel for the same reason.
- Wikipedia yields 1 row per pair because Wikipedia has one page per spelling. Inherent.

News holdout rows are validated against the rebuilt attested set (53.8% survive) and the
variant is taken from the rebuilt data, never the stale file.

### OpenAlex exclusions

`borscht` and `ihor-sikorsky` are excluded from academic holdouts because a word-boundary
match cannot separate them from something that is not the toponym:

- **`borscht`** — "Borsch" is the plant taxonomist T. Borsch. 2,228 of 3,484 Russian-variant
  papers since 2022 are botanical taxonomy and **not one contains "borscht"**.
- **`ihor-sikorsky`** — "Igor Sikorsky Kyiv Polytechnic Institute" is a university's official
  English name, so matches are affiliation strings. Its top-cited hit is *"Socrative as a
  Formative Assessment Tool"*.

Excluded rather than filtered by heuristic. **Both also contaminate the OpenAlex adoption
series**, which is a separate outstanding fix.

---

## 6. The world adoption map was deleted

It asserted country-level comparability it could not support:

- built on **6.02%** of the data (20,019 of 332,676 articles)
- **78.3%** of domains carry no ccTLD and are unmappable
- **47%** of country cells held under 30 articles; live cells included Sri Lanka n=10 → 100%

Dropping `.com` does not remove a random 78% — it removes nearly every global and US outlet
while keeping countries where local-TLD publishing is normal, so "United Kingdom 99.7%" and
the US figure come from different kinds of outlet and are not comparable. A ccTLD is a
registration fact, not audience and not editorial origin.

Rebuilding from pre-filter domains would not help: it reimports the machine-translated rows
the language filter exists to remove, and `.com` stays unresolvable.

`domain_origins` has the same disease — post-filter it is 99.7% "intl", with 16 of 24 pairs
having only that bucket. Still rendered on the uk page; not yet removed.

---

## 7. Known limitations to state, not fix

- **Outlet invisibility.** TASS, `interfax.com.ua` and 109 other domains contribute zero
  because their CMS uses numeric URLs.
- **Corpus class balance.** Fetch survivorship favours Ukrainian-spelling articles by ~14
  points, so the text corpus skews UA. Fix by stratified sampling at training time.
- **Corpus recency skew.** Extraction yield is 40% for 2017 and 68% for 2023, so the corpus
  over-represents recent years. The timeseries is unaffected — dead URLs keep their mention.
- **Odessa, Texas / Florida / Delaware.** 6,424 of 50,003 `odesa` rows (12.85%) carry explicit
  US-state markers; top domains `sfgate.com` (1,853), `chron.com` (1,665). Parked by decision:
  the spelling is the Russian-derived form regardless of the town's location, so it is not
  cleanly a false positive.
- **`chicken kiev` → `kyiv`.** 33 of 162,073 rows (0.02%). Accepted — kyiv is an umbrella term.
  Cause is leftmost-match: in `/chicken-dinner-recipes-kiev-diane-sauce/` the word "kiev" is
  not adjacent to "chicken", so the longer alternative never fires.
- **Surface-form coverage.** Each pair carries one Russian form. `olexandr usyk` (54 URLs) and
  `aleksandr usyk` (3) are absent from `pairs.yaml`. Negligible against 5,695 `oleksandr`, but
  the same one-form-per-pair limit applies to every pair.
- **GEG (`geg_gcnlapi`) was evaluated and not adopted.** It preserves surface forms and
  disambiguates referents via `wikipediaUrl` (`Odessa,_Texas` vs `Odesa`;
  `Chernobyl_(miniseries)` vs `Chernobyl_disaster`), covers 2016-07-17 → 2026-06-18, and costs
  1.513 TiB. Rejected for now because its `date` is the processing date rather than publication
  date, and archive index pages inflate counts — one `moonofalabama.org` monthly archive
  contributed 32 "Kiev" mentions.

---

## 8. Findings that changed how results are read

- **Usyk's Russian-form series was fabricated.** The old data showed 85% Russian in 2015 and
  74.7% in 2018; the attested figure is 0% and 2%. `AllNames` labelled 16,904 rows "Alexander
  Usyk" whose URLs say `oleksandr` 350 times and `alexander` zero times. Across all 41,651
  usyk URLs: `oleksandr` 5,695, `alexander` 11. **Audit any pair whose old series looked
  strongly Russian before believing it.**
- **KPI's naming policy lags its own scholars.** 57 of 59 OpenAlex Sikorsky papers mention the
  polytechnic, and papers begin in 2016 when the university took the name. Igor vs Ihor runs
  31–0 for 2018-2021, then 20–6 for 2022-2025. The official English name is frozen as "Igor"
  while its authors switched — the mirror of the MFA place-name campaign, where policy led.
- **`volodymyr-the-great` is two discourses, not one rename.** Of 17 verified records, the
  Russian form appears in coverage of Moscow's monument, Putin's 1,000-year ceremony and
  Trump-Helsinki columns; the Ukrainian form in St. Volodymyr the Great churches in Utica and
  Coventry and a Ukrainian navy corvette. The apparent 2019 switch may be a change in what
  gets covered. UA share moved 16.2% → 43.8% once text was read, because 6 of the 17 were
  invisible to the URL-based series and 5 of those 6 are Ukrainian.


---

## 9. Data layout (agreed 2026-08-26)

Four stages, each a function of the previous, each output regenerable from the one
before it. **Only `raw` is expensive**; everything downstream is a recompute.

```
collect  ->  <source>_raw.parquet         everything the provider returned, untouched
process  ->  <source>_processed.parquet   cleaned + regex-matched; correct records only
split    ->  pairs/<slug>.parquet         all sources stacked, unbalanced, carries source
balance  ->  (later)                      training set
```

### Why the filter lives in `process`, not `collect`
The matcher has already changed twice — word boundaries, then punctuation, after
`«Volodymyr, the Great!»` scored as containing no spelling. Because `raw` is retained
in full, a matcher fix re-runs `process(raw)` over local parquet in minutes. If `raw`
were pre-filtered, the same fix would mean refetching 1.4M URLs.

Two consequences:
- **`raw` stores full uncapped text.** Capping is a processing decision; capping in
  `raw` would make the cap length unchangeable without a refetch.
- **`raw` keeps the junk.** The Vladimir Putin videos, the Borsch taxonomy papers and
  the Odessa-Texas articles are what the provider returned. They are the evidence that
  the filtering did something, and without them it cannot be audited.

### There is no `verified` column
It was doing work it had not earned — the word implies a judgement, while the reality
is a regex outcome that was twice wrong. `processed` carries `ua_hits`, `ru_hits` and
the `variant` derived from them. The evidence is the data; the reader decides.

Auditing is done against the **matcher**, not the corpus, which is a few hundred rows
rather than hundreds of thousands:
- store the matched **span**, not only a count — 50 spans tell you whether the matcher
  works; 50 documents tell you almost nothing
- store a **near_miss** flag: text contains one word of the phrase but no full match.
  That is exactly the signal that exposed the comma bug (144 rows containing
  "volodymyr" scoring zero).

### Which source has which tier
| source | raw | processed | in pairs/ |
|---|---|---|---|
| gdelt, youtube, reddit, openalex | yes | yes | yes |
| wikipedia, trends, ngrams | yes | yes | **no** — no document, only counts |
| telegram | yes | — | no — 80% Cyrillic, measures Ukrainian-language channels |

A pageview is not a document. Count-only sources feed the adoption series and cannot
appear in a text corpus; forcing empty files there would be a lie about coverage.

### `pairs/<slug>.parquet`
The union of every source's `processed` rows for that pair. Unbalanced by design —
balancing is a training-time decision, not a storage one. **Carries `source`**: it is
required at evaluation time, because the first question about any cluster is whether
it merely rediscovered the source. Dropping it is the feature-matrix step's job.

### Publishing
The same files go to HuggingFace unchanged, `raw` included, so the study is
reproducible from the repo alone after a wiped laptop. Publishing is an allowlist
(see §7): a file is published because it is named, never because of its extension.
