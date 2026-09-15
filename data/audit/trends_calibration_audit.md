# Google Trends calibration audit

Date: 2026-09-05. Auditor: independent read-only pass over `pipeline/ingestion/trends.py`,
`pipeline/export_site_data.py`, `data/raw/trends*`, `site/src/data/timeseries.json`,
`site/src/data/manifest.json`, `site/src/components/PairShell.astro`. Python: `/opt/anaconda3/bin/python`.
Nothing in `data/` was modified except this file. Findings are ordered by severity: defects first.

---

## DEFECTS

### D1 (high) — `smooth_series` fabricates trends adoption for unmeasured months and distorts measured ones by up to 79 pp on 6 pairs

The calibration factor itself is sound (see C1–C2), but the adoption series the site ships is
post-processed by `smooth_series` (`pipeline/export_site_data.py:627` → `:454-490`). When a
pair trips `needs_smoothing = null_pct > 0.1 or jump_rate > 0.05` (`:468`), the function
forward-fills every undefined month (both variants at 0 → adoption undefined) from the last
measured value — seeded with the FIRST measured value for leading months (`:476`) — then
applies a centred moving average (window 7 when `null_pct > 0.3` or `jump_rate > 0.1`, `:472`).
The chart draws these points identically to measured ones (`site/src/components/PairShell.astro:767,777`
breaks the line only on `adoption == null`; painted months are not null).

Measured impact (site series vs. raw calibrated ratio recomputed from
`data/raw/trends/trends_world_monthly.parquet`, replicating `export_site_data.py:601-623`):

| pair | raw undefined months | painted (fabricated) months | max distortion of a MEASURED month |
|---|---|---|---|
| oleksandr-usyk | 76/192 (39.6%) | 76 | 78.95 pp (2015-12: raw 92.15 → shown 13.2) |
| volodymyr-zelenskyy | 116/192 (60.4%) | 116 | 33.21 pp (2022-02: raw 67.91 → shown 34.7) |
| bakhmut | 105/192 (54.7%) | 105 | 42.66 pp (2022-05: raw 98.06 → shown 55.4) |
| feodosiia | 127/192 (66.1%) | 127 | 0 (all zeros) |
| dynamo-kyiv | 0 (jump_rate 12.0%) | 0 | 43.60 pp (window-7 averaging) |
| donbas | 6/192 (jump_rate 6.3%) | 6 | 23.78 pp |

Worst concrete instances:

- **oleksandr-usyk**: the site shows **100.0% adoption for 2010-01 through 2012-05**, 31 months
  BEFORE the first measured month (2012-08, itself a single-month quantisation spike at 100%).
  "Oleksandr Usyk" search adoption in 2010 was never measured; the value is backfill of
  `non_null[0]` (`export_site_data.py:476`). 2017-01 shows a fabricated 77.9%.
- **volodymyr-zelenskyy / bakhmut**: the 2022 switch point — the study's central object — is
  dragged down and smeared: zelenskyy 2022-02 raw 67.91% → shown 34.7%; bakhmut 2022-05
  raw 98.06% → shown 55.4%; bakhmut 2022-04 raw 0.0% (measured) → shown 41.7%.
- The remaining 18 pairs, including all 5 audit pairs of §C3/C4, are byte-identical to the raw
  calibrated ratio (max |site − raw| = 0.00), so this defect is confined to the 6 pairs above.

The raw undefined months themselves are a Trends artifact worth naming: a solo series is
normalised to its own peak, so after Feb-2022 a term like "Volodymyr Zelenskyy" has a peak so
high that all pre-2019 months round to 0 — the pre-history is genuinely unmeasurable at this
normalisation, and painting it hides that.

Contrast with the codebase's own doctrine for count sources: `smooth_ratio_series` explicitly
"refuse[s] to draw a number rather than draw one the data cannot support"
(`export_site_data.py:443-445`) and preserves capped/measured-zero nulls — but trends is
excluded from that path (`COUNT_BASED_SOURCES`, `:416`, and the guard at `:894`), and gets the
legacy forward-fill smoother instead.

### D2 (medium) — feodosiia's below-floor verdict misstates its own evidence; the joint calls contradict the k=0 assignment

`calibration_report.csv` row `feodosiia`: *"Feodosiia below floor: 2/192 non-zero months and
0 in every joint window"* (template at `pipeline/ingestion/trends.py:209-210`). The second
clause is false. The cached joint responses (`data/raw/trends_serp/`):

| joint window | Feodosiya (RU) | Feodosiia (UA) |
|---|---|---|
| joint_feodosiia_2010-01_2011-04.json | 0 | **100** |
| joint_feodosiia_2010-01_2011-10.json | 185 | 0 |
| joint_feodosiia_2011-01_2012-12.json | 0 | **100** |
| joint_feodosiia_2016-08_2018-07.json | below_floor (empty response) |

The run's own `tried` string records `2011-01:zero(ru=0,ua=100)` — the UA side was the
NON-zero one in that window. The below-floor branch (`trends.py:197-214`) never checks which
side the joint call zeroed; it picks the weak side purely by solo non-zero month count
(Feodosiia 2/192 vs Feodosiya 65/192) and then emits a note asserting a condition
(`0 in every joint window`) that did not hold. The code comment at `trends.py:190-196`
claims "two independent signals agree"; for feodosiia they conflict.

Substantively: both spellings of this 67k-population town sit at or under the reporting floor
(solo: 65/192 and 2/192 non-zero; joint: never both non-zero; one window below floor entirely).
The defensible output is "unresolvable — no calibration possible", i.e. a gap; the pipeline
instead pins k = 0.0 and the site shows a **confident, unbroken 0.0% line for all 192 months**,
127 of which have no measurement on either side (see D3). The three other below-floor pairs do
not share this defect: kazymyr-malevych's joint windows zeroed the UA side all three times
(ua=0 vs ru=6853/5676/5396) consistent with its 2/192 solo months, and ihor-sikorsky /
serhii-korolyov have literally empty UA solo responses (SerpApi "no results", cached as
`below_floor` at `trends.py:88-91`).

Also misleading in the same row: `2016-08:missing_cols` — that window's joint response was a
below-floor empty response, not a malformed one; the `missing_cols` label (`trends.py:178`)
conflates the two.

### D3 (medium) — inconsistent no-data handling: 127 unmeasured feodosiia months painted as 0.0%, while serhii-korolyov's 6 equivalent months honestly gap

Same root as D1, called out separately because it inverts a policy the export otherwise
enforces. Months where BOTH variants are 0 produce `adoption = None`
(`export_site_data.py:623`). Two paths then diverge:

- `null_pct ≤ 0.1`: nulls are dropped (`:470`), then re-inserted by the gap-fill loop
  (`:822-878`, runs ≤ `MAX_ZERO_FILL_RUN = 6`, `:31`) as
  `{"adoption": null, "measured_zero": true}` → the chart breaks the line
  (`PairShell.astro:767`). This happened to serhii-korolyov (6 months), dnipro-river (2010-07),
  volodymyr-the-great (1 month). Honest.
- `null_pct > 0.1`: `smooth_series` forward-fills the nulls BEFORE the gap-fill loop can see
  them → feodosiia's 127 no-data months (66.1% of the series) are painted `adoption: 0.0`
  with `ukr: 0.0, rus: 0.0` and no marker; e.g. `{"date":"2010-01","ukr":0.0,"rus":0.0,"adoption":0.0}`
  in `site/src/data/timeseries.json`. Indistinguishable from a measured zero.

So the pair with the MOST missing data gets the most confident-looking series. Verified counts:
feodosiia site series 192/192 non-null, all 0.0; serhii-korolyov 186 non-null + 6
measured_zero nulls.

### D4 (latent, currently inactive) — two silent k=1 fallbacks in the export

- `export_site_data.py:595-596`: if `interest_calibrated` is absent from the loaded trends
  frame, it is silently set equal to `interest` — adoption then becomes the ratio of two
  INDEPENDENTLY peak-normalised 0-100 indexes, i.e. an assumed k = 1, with no warning logged.
- `export_site_data.py:617-621`: a per-month `KeyError` on the calibrated pivot falls back to
  the raw display-scale values (`cu, cru = ukr, rus`), again k = 1 for that month.

Verified NOT triggered by current data: the parquet carries `interest_calibrated` on all
9,216 rows, the implied factor `interest_calibrated / interest` is constant per pair
(min = max across every UA row) and equals `calibration_report.csv` for all 20 measured pairs,
and both pivots at `:598-601` are built from the same groupby so their (pair, month) keys are
identical. But a future parquet written without the column would flip every pair to k = 1
silently. A hard failure or logged warning would be safer than a default.

---

## OBSERVATIONS (minor, no action forced)

- **O1 — hard 0.0% where truth is "< detection floor".** The four below-floor pairs
  (feodosiia, ihor-sikorsky, serhii-korolyov, kazymyr-malevych) display exact 0.0% adoption
  (manifest `pair_source_stats.*.trends.adoption` = 0.0) with no "below reporting threshold"
  qualifier anywhere in the data layer. The k=0 doctrine ("below floor is a measurement") is
  documented in `trends.py:160-165`, but the UI cannot distinguish "measured ~0" (mykola-hohol,
  k=0.004574, real ratio) from "undetectable" (k forced to 0).
- **O2 — scale-mixing weights in summary adoption.** Both the manifest's per-pair trends
  adoption (`export_site_data.py:1570-1578`) and the browser popover
  (`PairShell.astro:976-987`, last-12-months window `SINCE='2025-01'` at `:960`) weight the
  monthly calibrated ratio by `ukr + rus` in the DISPLAY scale — the sum of two independently
  peak-normalised indexes. Months where either variant nears its own peak are overweighted
  regardless of true volume. Bounded within-pair bias, not a factor error (kyiv manifest 20.8%
  vs unweighted lifetime mean 23.7%).
- **O3 — 100%-months are floor artifacts of the common variant.** Months where the RU solo
  value is 0 while UA > 0 force adoption = 100%: bakhmut 17 months (2024+), oleksandr-usyk 3,
  ternopil 1 (2010-01). For bakhmut post-2023 this is plausibly real (Artemovsk search volume
  is dead); as numbers they are "≥ x" bounds, since RU sits under ITS floor there. The site
  shows them (usyk 2012-08: 100.0).
- **O4 — determinism holds only through the disk cache.** Trends the instrument is
  non-deterministic (documented at `trends.py:37-40`; repeat calls differ by 1-2 points).
  `call()` (`trends.py:76-99`) caches every response, so rebuilds are reproducible — verified
  in C2 — but the guarantee lives in `data/raw/trends_serp/` (94 files). Losing that directory
  and refetching would move every k by a few percent (see C5) and could flip feodosiia's
  coin-toss verdict (D2).

---

## CONFIRMATIONS

### C1 — How the factor is computed (audit question 1)

Per pair (`pipeline/ingestion/trends.py:147-227`):

1. Two SOLO SerpApi Trends calls, one per spelling, full study span 2010-01-01..2025-12-31
   (`:150-172`); each series is Google-normalised to its own peak (0-100, monthly).
2. Candidate 24-month calibration windows (`CAL_MONTHS`, `:66`) are scored by the WEAKER
   side's rolling mass after per-series peak-normalisation — `calibration_windows`
   (`:114-144`), up to 3 distinct candidates, best first.
3. One JOINT call per candidate (`:175-176`) — both terms in one request, so both land on a
   common scale. First window where joint AND solo sums are all positive wins (`:180-187`).
4. **k = (j_uk / j_ru) × s_ru / s_uk** (`:188`), where j_* are the joint response's sums over
   the window and s_* the solo series' sums over the same window. Derivation: with
   solo_x = true_x / peak_x × 100, this algebraically equals peak_UA / peak_RU — the factor
   that puts the UA solo series on the RU solo scale. Applied as `uk__cal = uk_s × k` (`:224`),
   exported as `interest_calibrated` (`:239-246`); RU's calibrated value is its own solo value.
5. If UA's solo response is empty, or no window resolves both sides and the weak side has
   ≤ `BELOW_FLOOR_MAX_NONZERO = 10` of 192 non-zero solo months (`:67`, `:201`): k = 0.0,
   explicit zero UA series, `below_floor: true` (`:160-169`, `:197-214`). If both sides carry
   volume but no window resolves them, the pair is dropped with `joint_quantised_to_zero`
   (`:215-217`) — no pair is currently in that state.
6. Site adoption per month = `cu / (cu + cru) × 100` on the calibrated columns
   (`export_site_data.py:623`), then `smooth_series` (`:627` — see D1).

The module's own validation anchor reproduces: chornobyl Nov-2024 calibrated
**15.29%** vs direct joint measurement **15.38%** (docstring `trends.py:33-36` claims
15.3 vs 15.5) — two independent routes agree within 0.1 pp; the site shows 15.29.

### C2 — Determinism and coverage (audit question 2)

- **Coverage: 24/24 enabled pairs** (config/pairs.yaml via `pipeline/config.py`) have a trends
  series in the parquet, in `calibration_report.csv`, and on the site (192 months each in
  `site/src/data/timeseries.json`). 20 pairs have a measured k; 4 are pinned k=0 below-floor
  (feodosiia — contested, see D2; ihor-sikorsky; serhii-korolyov; kazymyr-malevych).
- **No pair falls back to a default/assumed factor** (the k=1 fallbacks of D4 are dead paths
  on current data). k=0 is an explicit, reported verdict, not a silent default.
- **Replay determinism: exact.** Re-running `collect_pair` for all 24 pairs against the disk
  cache only (network blocked) reproduces `calibration_report.csv` bit-for-bit: 24/24 match on
  k, window, ok, below_floor — 0 mismatches. Repeat-fetch stability: the same chornobyl window
  was fetched twice on 2026-08-28 (`joint_cal_2024_2025.json` 18:08, `joint_chornobyl_2024-01_2025-12.json`
  18:22) and yields identical k to 6 decimals.
- The parquet-implied factor (`interest_calibrated / interest` on UA rows) is a single constant
  per pair and equals the report for all 20 measured pairs.

### C3 — Independent recomputation of k for 5 pairs (audit question 3)

Own parser over the raw SerpApi JSONs in `data/raw/trends_serp/` (no pipeline code imported),
formula per C1, windows per `calibration_report.csv`:

| pair | window | j_uk/j_ru | s_ru | s_uk | k recomputed | k pipeline | rel. diff |
|---|---|---|---|---|---|---|---|
| kyiv | 2021-09..2023-08 | 0.51265 | 431 | 335 | 0.659558 | 0.659558 | +6.1e-07 |
| chornobyl | 2024-01..2025-12 | 0.03206 | 107 | 380 | 0.009028 | 0.009028 | −1.4e-05 |
| mykola-hohol | 2022-03..2024-02 | 0.00188 | 1339 | 550 | 0.004574 | 0.004574 | −8.0e-05 |
| borscht | 2024-01..2025-12 | 2.58166 | 1845 | 1303 | 3.655538 | 3.655538 | +3.7e-08 |
| dnipro-river | 2021-12..2023-11 | 0.85366 | 558 | 792 | 0.601441 | 0.601441 | +4.0e-07 |

All five agree to ≤ 8e-05 relative — differences are the 6-decimal rounding of the report.
The recorded `joint_ratio` column also matches to 5 decimals in all five cases.

### C4 — Cross-source consistency (audit question 4)

Site series (`site/src/data/timeseries.json`), overlapping non-null months only. For all 5
audit pairs the site trends series equals the raw calibrated ratio to 0.00 (none are smoothed),
so this tests the calibration itself, not D1's smoothing.

| pair | vs | n | Pearson r | Spearman ρ | mean trends | mean other | offset |
|---|---|---|---|---|---|---|---|
| kyiv | gdelt | 131 | +0.693 | +0.800 | 23.7 | 78.4 | −54.8 |
| kyiv | youtube | 192 | +0.820 | +0.887 | 18.2 | 45.9 | −27.7 |
| chornobyl | gdelt | 131 | +0.802 | +0.564 | 0.6 | 10.2 | −9.5 |
| chornobyl | youtube | 192 | +0.855 | +0.509 | 0.5 | 3.8 | −3.3 |
| mykola-hohol | gdelt | 131 | +0.440 | +0.562 | 0.0 | 0.3 | −0.2 |
| mykola-hohol | youtube | 192 | −0.016 | +0.075 | 0.0 | 0.3 | −0.3 |
| borscht | gdelt | 131 | +0.238 | +0.222 | 68.9 | 69.5 | −0.6 |
| dnipro-river | gdelt | 131 | +0.703 | +0.613 | 26.6 | 43.9 | −17.4 |

- **No red-flag divergence.** Every correlation is positive except mykola-hohol vs youtube,
  where r ≈ 0 is a degenerate-variance artifact, not disagreement: the trends series is pinned
  at ~0% (range 0–1.06, sd 0.11 pp, because k=0.004574 — UA search volume is ~1/500 of RU),
  while youtube's is 0–6.1%. Both sources agree adoption ≈ 0; there is no signal left to
  correlate. Borscht's weak +0.24 comes with near-identical means (68.9 vs 69.5) — level
  agreement, noisy monthly co-movement (GDELT's borscht corpus is known-contaminated by
  Borscht Belt homonyms per `data/audit` history).
- **Offsets are one-directional and population-shaped**: trends (global search) reads LOWER
  UA-adoption than gdelt (English news text) everywhere, hugely so for kyiv (−54.8 pp) —
  consistent with search queries lagging editorial style guides; the expected constant offset,
  not a calibration error.
- **Not verifiable here:** borscht and dnipro-river have NO youtube series in
  `timeseries.json` at all (absent key), so trends-vs-youtube cannot be checked for them from
  the site data. Stated as such rather than substituted.

### C5 — Window-choice sensitivity of k is ≤ 7%

Alternative joint windows cached by an earlier run allow measuring how much k depends on the
window (same solo series, different joint pull):

| pair | alt window | k_alt / k_used |
|---|---|---|
| chornobyl (repeat fetch, same window) | 2024-01..2025-12 | 1.000 |
| luhansk | 2021-09..2023-08 | 0.999 |
| dnipro-river | 2022-08..2024-07 | 0.992 |
| lviv | 2018-02..2020-01 | 0.984 |
| babyn-yar | 2021-04..2023-03 | 0.943 |
| odesa | 2024-01..2025-12 | 1.030 |
| kyivan-rus | 2024-01..2025-12 | 0.929 |

Worst case ±7% on k ⇒ ≤ ~1.8 pp on an adoption share at 50% (error scales as
Δk·a·(1−a)), smaller at the extremes where most pairs sit. The calibration is robust to
window choice at the precision the site claims.

### C6 — Edge handling summary (audit question 5)

- One variant below floor for the whole span → k=0, UA series explicit zeros; site shows a
  0.0% line, not a gap (4 pairs; qualifier absent — O1; feodosiia's verdict contested — D2).
- Both variants zero in a month → adoption null; shown as a real line-break gap
  (`measured_zero` + `PairShell.astro:767`) for the 3 pairs below the 10% null threshold, but
  PAINTED as forward-filled values for the 5 pairs above it — D1/D3.
- Common variant momentarily at 0 with UA > 0 → forced 100% months (O3).
- Trends is exempt from `MIN_COUNT_THRESHOLD` pruning by construction (its `ukr`/`rus` are
  index sums, `export_site_data.py:925-931`), so below-floor pairs remain on the site — the
  intended behaviour per `trends.py:160-165`.

---

## Verdict

The calibration factor pipeline itself is correct, deterministic given its cache, fully
covered, independently reproducible to 5+ decimals, and cross-source consistent. The defects
are downstream of it: `smooth_series` fabricates or distorts the shipped adoption series for
6 of 24 pairs (worst: 31 months of invented 100% for oleksandr-usyk; the 2022 switch points of
bakhmut and volodymyr-zelenskyy flattened by up to 43 pp), feodosiia's k=0 verdict contradicts
its own joint-call evidence, and two silent k=1 fallbacks wait in the export for the day the
calibrated column goes missing.
