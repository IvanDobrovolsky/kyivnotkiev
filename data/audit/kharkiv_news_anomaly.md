# Kharkiv news-holdout anomaly — audit

Scope: why `site/src/data/holdouts_by_pair.json` shows `kharkiv.news = 1` against
`kharkiv.news_articles = 47`, and why the two keys disagree so wildly across pairs.
Read-only investigation; no code or data was changed.

Verified against the shipped JSON, `pipeline/export_site_data.py`,
`data/cl/corpus/gdelt_verified/*.parquet`, `dataset/raw_gdelt.parquet`,
`data/raw/gdelt/mentions_v2/gdelt_mentions_final.parquet` and
`site/src/components/PairShell.astro`.

---

## 1. `news` and `news_articles` are not two versions of the same thing

They are different objects, from different tables, built by different code, measuring
different populations. Comparing `1` to `47` is a category error before it is anything else.

| | `news` | `news_articles` |
|---|---|---|
| Written at | `pipeline/export_site_data.py:1441-1444` (inside `export_holdouts`, def at `:1424`) | `pipeline/export_site_data.py:1868-1873` (inside `main`) |
| Unit of a row | **an outlet** — `{name, russian_pct, total}` | **an article** — `{domain, url, variant, text_preview, month}` |
| Source table | `_load("gdelt")` → `dataset/raw_gdelt.parquet` | `data/cl/corpus/gdelt_verified/<slug>.parquet` |
| What that table is | a per-(pair, month, variant, term, domain) **count** aggregate | one row per fetched **article**, with body text |
| How the variant is decided | **the spelling in the URL slug** | **the spelling in the article body** (`body_ua` / `body_ru`) |
| Selection | `date >= "2025-01"`; group by (pair, domain, variant); `total = russian + ukrainian`; keep `total >= 5` **and** `rus_pct > 50`; `nlargest(50, "total")` | `variant in HOLDOUT_VARIANTS`; `HOLDOUT_SINCE <= date <= STUDY_END_DATE`; umbrella regex exclusion; lead dedup; context dedup; `head(HOLDOUT_PER_DOMAIN)` per domain; `head(HOLDOUT_CAP)` |

### The provenance of `dataset/raw_gdelt.parquet` is the whole story

It is a straight aggregation of the v2 URL-attested mention set. Confirmed numerically:

```
gdelt_mentions_final.parquet  →  group by (pair, month, variant, url_term, domain)
  = 131,830 rows, count sum 332,676
dataset/raw_gdelt.parquet
  = 131,830 rows, count sum 332,676      ← exact match
```

`gdelt_mentions_final.parquet` carries a `url_term` column and the term is present in the
URL on **500/500** sampled kharkiv rows. So the `news` table can only ever see outlets whose
URL slugs spell the place name out.

Consequence, measured:

* `tass.com` appears **0 times in all 131,830 rows** of `raw_gdelt.parquet` (its URLs are
  numeric, e.g. `tass.com/politics/1234567`).
* `tass.com` is the **single largest** Russian-form kharkiv publisher in the verified
  corpus: **73 of the 139** qualifying 2025 rows.
* `globalsecurity.org` appears once in the whole of `raw_gdelt` (for `chornobyl`); it is
  **22 of 139** kharkiv 2025 rows in the verified corpus.

Domain sets for kharkiv, 2025, Russian form:

| pair | body-verified rows / domains | URL-attested rows / domains | domains in common |
|---|---|---|---|
| kharkiv | 140 / 31 | 27 / **5** | 3 |
| kyiv | 943 / 173 | 1,659 / 155 | 87 |
| donbas | 106 / 46 | 147 / 34 | 21 |
| odesa | 190 / 102 | 2,706 / 375 | 40 |

In the shipped JSON the two keys for kharkiv name **disjoint** outlet sets:

```
kharkiv: news domains=1  article domains=30  overlap=0
donbas : news domains=7  article domains=43  overlap=3
odesa  : news domains=37 article domains=72  overlap=5
kyiv   : news domains=41 article domains=61  overlap=22
```

Only 5 of 24 pairs get a `news` key at all; 20 get `news_articles`.

---

## 2. Funnel A — how kharkiv gets to 47 `news_articles`

From `data/cl/corpus/gdelt_verified/kharkiv.parquet`, replaying `export_site_data.py:1838-1873`
exactly (constants: `HOLDOUT_SINCE=2025-01-01`, `STUDY_END_DATE=2025-12-31`,
`HOLDOUT_VARIANTS=('russian',)`, `HOLDOUT_PER_DOMAIN=3`, `HOLDOUT_CAP=100`).

| # | stage | line | kharkiv | kyiv | donbas | odesa |
|---|---|---|---|---|---|---|
| 0 | verified parquet rows (all years/variants) | — | 13,120 | 89,552 | 13,609 | 10,723 |
| 1 | `variant.isin(HOLDOUT_VARIANTS)` | 1841 | 888 | 12,167 | 2,118 | 3,065 |
| 2 | `date >= HOLDOUT_SINCE` | 1842 | 182 | 1,362 | 174 | 298 |
| 3 | `date <= STUDY_END_DATE` (drops 2026) | 1843 | 140 | 943 | 106 | 190 |
| 4 | umbrella exclusion | 1844-1846 | 139 | 930 | 106 (no regex) | 190 (no regex) |
| 5 | `drop_duplicates("_lead")` | 1850-1853, 1862 | 139 | 930 | 106 | 190 |
| 6 | `drop_duplicates("_ctx")` | 1858-1861, 1863 | 139 | 925 | 103 | 190 |
| 7 | `groupby("domain").head(3)` | 1865 | **47** | 276 | 66 | 141 |
| 8 | `head(HOLDOUT_CAP)` | 1866 | **47** | **100** | **66** | **100** |

Matches the shipped JSON exactly (47 / 100 / 66 / 100).

**The binding constraint is stage 7, the per-domain cap.** kharkiv's 139 surviving rows sit
on only 30 distinct domains, and are extremely concentrated:

```
tass.com               73     ← 3 kept
globalsecurity.org     22     ← 3 kept
russiaherald.com        6     ← 3 kept
en.kremlin.ru / batonrougepost.com / philippinetimes.com / heraldglobe.com   3 each
wrp.org.uk / wsws.org / calcuttanews.net                                     2 each
20 further domains                                                           1 each
```

95 of 139 rows (68.3%) come from two domains. `head(3)` discards 92 of them. 30 domains,
capped at 3, with 20 of them holding a single row = 47. Nothing is over-cutting; the number
is what the cap is designed to produce.

Other stages are near-no-ops for kharkiv: the umbrella regex is
`\b(?:Metalist\s+Kharkiv|Metalist\s+Kharkov)\b` and removes 1 row; both dedup passes remove 0.
Verified-wrong drops are **already applied upstream** — of the 57 audited drop URLs for
kharkiv (127 kyiv, 89 donbas), **0** are still present in the verified parquet, checked both
raw and scheme/`www`-normalised. That stage is clean.

## 2b. Funnel B — how kharkiv gets to 1 `news`

From `dataset/raw_gdelt.parquet` via `_load("gdelt")`, replaying `:1430-1444`.

| # | stage | line | kharkiv | kyiv | donbas | odesa |
|---|---|---|---|---|---|---|
| A | 2025 rows (all variants) | 1432 | 966 | 8,656 | 783 | 1,788 |
| B | distinct (pair, domain) groups | 1433-1434 | 602 | 1,680 | 560 | 955 |
| C | `total >= 5` | 1437 | 43 | 884 | 28 | 89 |
| D | `rus_pct > 50` | 1437 | **1** | **41** | **7** | **37** |
| E | `nlargest(50, "total")` → `news` | 1439 | **1** | **41** | **7** | **37** |

kharkiv's entire 2025 Russian-form presence in this table is **27 mentions across 5 domains**:

```
sputnikglobe.com      23 ru /   0 ua   → the only row that clears total>=5
mangalorean.com        1 /  1
nakedcapitalism.com    1 /  0
newkerala.com          1 /  1
plenglish.com          1 /  0
```

So `news = 1` is arithmetically correct *for that table*. The table is the problem, not the
threshold: it cannot see tass.com, globalsecurity.org, en.kremlin.ru, wsws.org or 26 other
outlets that demonstrably published "Kharkov" in 2025, because those outlets do not put the
place name in their URLs.

---

## 3. Is the 2025-only window binding? No.

Russian-form ("Kharkov") records in `data/cl/corpus/gdelt_verified/kharkiv.parquet`, by year:

| year | RU rows | distinct domains | tass.com | globalsecurity.org | after `head(3)` per domain |
|---|---|---|---|---|---|
| 2022 | 190 | 66 | 24 | 0 | 96 |
| 2023 | 110 | 51 | 35 | 6 | 69 |
| 2024 | 176 | 60 | 47 | 18 | 88 |
| **2025** | **140** | **31** | **73** | **22** | **48** |
| 2026 (partial, excluded by `STUDY_END_DATE`) | 42 | 11 | 15 | 18 | 15 |

Full variant breakdown for context (`both` is excluded by `HOLDOUT_VARIANTS`):

| year | russian | both | ukrainian |
|---|---|---|---|
| 2022 | 190 | 75 | 3,857 |
| 2023 | 110 | 93 | 1,206 |
| 2024 | 176 | 33 | 3,503 |
| 2025 | 140 | 13 | 1,063 |

2025 is in line with 2022-2024 in volume (140 vs 190/110/176). What **has** changed is
concentration: distinct RU-form domains fall 66 → 51 → 60 → **31**, and tass.com's share
rises 12.6% → 31.8% → 26.7% → **52.1%**. That is a real, reportable finding — residual
"Kharkov" in English news has consolidated into a handful of Russian state and defence
outlets — and it is exactly what the per-domain cap then compresses to 47.

For the `news` key the equivalent per-year figures from `raw_gdelt.parquet` are
2022: 73 ru / 7,361 ua, 2023: 30 / 1,560, 2024: 85 / 6,961, 2025: 27 / 1,515 — i.e. that
table has been missing ~80% of kharkiv's Russian-form news every year, not just in 2025.

---

## 4. Verdict: `news_articles` is data; `news` is a defect

**4a. The 47 vs 100 vs 66 spread in `news_articles` is DATA, not a defect.**
The funnel is identical for every pair and every stage behaves as designed. kharkiv lands at
47 because it has 30 qualifying domains and a `HOLDOUT_PER_DOMAIN = 3` cap; kyiv lands at 100
because it has 165 and hits `HOLDOUT_CAP`; donbas lands at 66 because it has 43 domains and
never reaches the cap. No filter over-cuts, no stage misbehaves, verified-wrong drops are
correctly pre-applied.

**4b. The `news` key is a defect — three compounding faults.**

**D1 — wrong table (the substantive defect).** `export_holdouts` still reads the URL-slug
table the project already retired for the series. The exporter says so itself at
`pipeline/export_site_data.py:676-678`:

> *"Verified-text series only. There is no legacy fallback: a pair without a verified build
> shows nothing rather than a series derived from URL spellings, which is a different
> measurement and was silently standing in for the same one."*

That migration landed for `export_timeseries` (`:679-699`, reads `gdelt_verified/*_series.parquet`)
and for `news_articles` (`:1831-1873`), but **not** for `export_holdouts` (`:1430-1444`) or for
`holdouts_global` (`:1655-1666`). Both still call `_load("gdelt")`. The result is a table that
systematically omits the outlets the exhibit exists to name (0/30 overlap for kharkiv), and
whose per-pair yield tracks outlet URL conventions rather than spelling behaviour.

**D2 — the homonym filter cannot fire on this table.** `_apply_homonym_filters`
(`:381-404`) matches `pairs.yaml` `homonym_filters` against the **`source_domain` column
only** (`:394-396`). Those patterns are written for article text (`texas`, `permian`,
`midland`, `call of duty`, `world of tanks`). Against a domain string they almost never match:
**128 rows removed out of 131,830** across all pairs (kyiv 34, odesa 69, chornobyl 7,
donbas 17, kharkiv 1). `apply_source_filters` — which owns `referent_filter`, including
odesa's entire US-homonym apparatus — is invoked only for YouTube (`:250`) and never for
GDELT. Measured effect: of odesa's 3,981 Russian-form 2025 counts in this table, **2,302
(57.8%)** come from 15 hand-identified Odessa, **Texas** outlets. `site/src/data/holdouts.json`
ships with `newswest9.com` (445), `firstalert7.com` (404), `b93.net`, `kbat.com`,
`1025kiss.com`, `iheart.com`, `975kgkl.com`, `965therock.com`, `103kkcn.com`, `oaoa.com` as
its top "Russian-spelling holdout" outlets. These are West Texas radio and TV stations.
The table is also pre-aggregated with no `url` column, so the 1,409 individually
verified-wrong URLs in `data/audit/holdout_ai_verification.json` **cannot** be applied to it
even in principle.

**D3 — it is dead data.** `site/src/components/PairShell.astro:1268-1274` routes
`gdelt → drawNewsHoldouts`, which reads `news_articles` only (`:1294`). `drawHoldoutTable` is
registered for `reddit`/`youtube`/`wikipedia`/`openalex` only, so its
`sourceToKey.gdelt = 'news'` mapping (`:1595`) and the whole `holdoutKey === 'news'` render
branch (`:1625-1653`) are unreachable. `site/src/data/holdouts.json` is not imported by any
file under `site/src`. Additionally `export_holdouts(enabled_slugs)` is called **twice** at
`:1814-1815` (`holdouts_by_pair, _ = …` then `_, holdouts_global = …`), doing the full
aggregation twice to keep one half of each result.

Net: nothing user-visible is currently wrong on the pair pages, but the repo ships a
`news` key and a `holdouts.json` whose contents contradict the study's own stated
measurement and are majority-false-positive for at least one pair.

---

## 5. Minimal fix (proposed, NOT applied)

### Option A — minimal and aligned with the project's own precedent: delete it

The `news` key is unrendered, is built from a measurement `:676-678` already declares invalid
for this purpose, and has an unfixable false-positive problem. Remove it rather than repair it.

* `pipeline/export_site_data.py:1429-1444` — delete the `gdelt = _load("gdelt")` block and the
  `by_pair[slug]["news"] = [...]` assignment. Note `gdelt` is reused at `:1657`, so delete that
  block too.
* `pipeline/export_site_data.py:1654-1668` — delete the `global_list` block; return
  `by_pair, []`, or change the signature to return `by_pair` only and drop `holdouts_global`.
* `pipeline/export_site_data.py:1814-1815` — collapse the double call to a single
  `holdouts_by_pair = export_holdouts(enabled_slugs)`.
* `pipeline/export_site_data.py:1895` — drop `write_json(SITE_DATA_DIR / "holdouts.json", …)`
  and delete `site/src/data/holdouts.json`.
* `site/src/components/PairShell.astro:1595` and `:1625-1653` — remove the now-unreferenced
  `gdelt: 'news'` mapping and the dead `holdoutKey === 'news'` branch.

### Option B — if an outlet-level table is actually wanted, rebuild it from the verified corpus

Replace `:1430-1444` with an aggregation over the same frame `news_articles` already uses,
so the two keys can never disagree again. Sketch (drop-in for the block at `:1430-1444`,
placed after the verified loader so `_vdf` per slug is in scope, or as its own pass over
`data/cl/corpus/gdelt_verified/*.parquet`):

```python
_v = pd.read_parquet(_vf)
_v = _v[(_v.date >= HOLDOUT_SINCE) & (_v.date <= STUDY_END_DATE)]
_agg = (_v.assign(ru=_v.variant.eq("russian"), ua=_v.variant.eq("ukrainian"))
          .groupby("domain")[["ru", "ua"]].sum())
_agg["total"] = _agg.ru + _agg.ua
_agg["rus_pct"] = (_agg.ru / _agg.total * 100).round(1)
_agg = _agg[(_agg.total >= 5) & (_agg.rus_pct > 50)].nlargest(50, "total")
by_pair.setdefault(_slug, {})["news"] = [
    {"name": d, "russian_pct": float(r.rus_pct), "total": int(r.total)}
    for d, r in _agg.iterrows()]
```

This inherits the body-text classification, the umbrella exclusion and the upstream
verified-wrong drops for free. Expected kharkiv result: `tass.com` (73 ru / 0 ua, 100%),
`globalsecurity.org` (22 / 0), `russiaherald.com` (6 / 0) — the outlets that actually still
write "Kharkov", instead of one Sputnik masthead.

**Do not** simply relax `total >= 5` or widen `HOLDOUT_SINCE` on the current table. Neither
touches the cause: the outlets are absent from the table entirely, at every threshold and in
every year.

### Separate, unrelated to this anomaly but found on the way

`pipeline/export_site_data.py:394-396` applies text-shaped `homonym_filters` to the
`source_domain` column. It silently no-ops for the patterns it was written for. If the GDELT
count table survives in any form, this should route through
`pipeline.filters.apply_source_filters` like YouTube does at `:250`, or the filter should be
documented as domain-only and the text patterns moved out of `homonym_filters`.
