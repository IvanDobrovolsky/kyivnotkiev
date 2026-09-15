# Outlet concentration on the Russian form — checked, mostly an artifact

Prompted by kharkiv's news holdout table showing 47 entries where other pairs
show 100. The cause there is real: kharkiv's 2025 Russian-form rows sit on 31
domains, 52.1% of them tass.com, and the per-domain cap of 3 compresses that
to 47. Row volume did not fall (140 in 2025 vs 190/110/176 in 2022-24) — the
outlets consolidated.

## Does that generalise? No.

Raw counts suggested it did: distinct domains carrying the Russian form fell
189 -> 129 -> 97 -> 92 across 2022-2025, and mean top-outlet share rose from
11.3% to 16.3%.

Normalising by volume and running the Ukrainian form as a control removes it:

| side | 2022 | 2023 | 2024 | 2025 | change |
|------|------|------|------|------|--------|
| Russian form, domains per 100 rows   | 32.0 | 31.0 | 30.1 | 24.7 | -23% |
| Ukrainian form, domains per 100 rows | 13.6 | 14.7 | 13.2 | 10.5 | -23% |

Both sides lose breadth at the same rate, so this is GDELT's coverage or our
dedup changing over time, not the Russian form retreating into fewer outlets.
Do not report the aggregate trend as a finding.

## What survives

kharkiv as a per-pair outlier. Its 2025 top-outlet share (52.1%) is the
highest of 14 measurable pairs and 1.6x the next (luhansk 31.6%, zelenskyy
30.7%). Per-pair concentration is worth reporting; the corpus-wide trend is
not.

Numbers: data/audit/outlet_concentration.json
Kharkiv funnel: data/audit/kharkiv_news_anomaly.md
