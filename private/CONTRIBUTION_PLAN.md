# NAACL 2027 contribution plan — decided 2026-08-30

Verdict from the verified literature sweep (full report in the session task
output; every citation below was fetched from a primary source, not recalled).

## The paper: one narrative, three layers

**Guard-clustered contexts → hierarchical event-linked adoption model → over
audited instruments.** Each pillar covers the others' weakness.

1. **Outcome-preserving clustering** (the variant guard, generalized).
   Novelty that survived prior art: a merge-time veto on an OUTCOME variable's
   distribution across components, outcome never a feature. Nearest neighbours,
   all verified: Wagstaff 2001 (pairwise, pre-specified), Davidson & Ravi 2005
   (merge constraints, instance-level), Chierichetti 2017 fairlets (forces
   mixing — the opposite), Hennig 2010 (geometry-only GMM merging), Brückner
   LChange 2024 (similarity-only merging — the published baseline we ablate
   against). To build: variance-preservation proof, τ by permutation null,
   no-leakage argument, ablation grid, synthetic failure-mode study.
2. **Hierarchical Bayesian adoption curves**: y_{p,k,s,t} ~ Bin, logistic
   curves with pooled asymptotes over 24 pairs × guarded contexts × 8 sources,
   PRE-SPECIFIED event dates (AP Stylebook, invasion, Wikipedia rename, game
   release) so change-points become event-attribution tests. Nearest: Amato
   PNAS 2018 (one corpus, no hierarchy/contexts/measurement), Stewart &
   Eisenstein EMNLP 2018, CausalImpact, Caughey & Warshaw 2015. Combination
   unpublished. numpyro, laptop-feasible.
3. **Measurement layer** (the estimator, folded in): per-source emission
   models — YouTube bounded-sample capture (mark-recapture with heterogeneity;
   NO prior capture-recapture treatment of repeated API queries exists),
   Trends interval-censored quantisation, GDELT translation-layer bias
   ([[finding_gdelt_translation_bias]] — 87.2% vs 12-62% by source language,
   unpublished) and AllNames coverage gaps. Full estimator = second paper for
   ICWSM/JQD.

## Experiment set, dependency order (fits Oct 12)
1. Reddit ground-truth recall (bounded search vs complete PullPush) — 2-3 d
2. Repeated-pass YouTube capture histories, ~40 stratified windows — 3-4 d
   (checkpoints store deduplicated unions only; must recollect)
3. Trends recovery check vs West CIKM 2020 as named baseline — 1-2 d
4. Guard ablation + synthetic study + permutation τ — 3-4 d
5. Hierarchical model + LOO for 2019-priming and 2014-vs-2022 — 1-2 wk
6. Context annotation (≥300 items, 2 annotators, α) — start day 1, external

## Corrections to project memory made alongside
- "Zhu & Jurgens NAACL 2021 Cox PH" does not exist; verified paper is Zhu &
  Jurgens EMNLP 2021 (idiolects). Fixed in plan_paper_strategy.
