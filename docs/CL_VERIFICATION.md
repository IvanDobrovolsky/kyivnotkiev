# Verifying collocations and clusters, pair by pair

Same shape as the holdout verification: one agent per pair, evidence read from
that pair's own corpus, every verdict persisted the moment it is made, patterns
measured corpus-wide before anything is applied, and whatever changes gets
re-verified.

Zeros and oddities are hypotheses, not results. A term that looks like junk in
one pair can be real vocabulary in another, so every verdict is scoped to its
pair.

## What each agent checks

For its pair, read `site/src/data/cl_keyness.json` and `site/src/data/cl_clusters.json`
(what the site actually ships) and `data/stats/<slug>/records.parquet` (the
evidence).

**Every collocation chip**
1. Is the term real vocabulary, or scrape/nav/platform debris?
2. Does it belong to this pair's referent, or a homonym (Odessa TX, the Kiev
   destroyer, a wrestler)?
3. Is the gloss true of the rows the term actually appears in?
4. Does the quoted example contain the term and come from a source the pair
   plots?
5. Is it on the correct spelling side?

**Every cluster**
1. Does the label/gloss describe what the texts are?
2. Does the example carry at least one of the cluster's terms?
3. Is it a discourse register, or an artifact — one channel's spam, one
   syndication farm, a single year?
4. Does the source mix mean the cluster is a platform artifact (>=90% one
   source is a flag, not a verdict)?

## Output — one file per pair, written incrementally

`data/audit/cl_verification/<pair>.json`

```json
{"pair": "kyiv",
 "collocations": [
   {"term": "dds", "side": "ru", "verdict": "wrong_referent",
    "note": "World of Warships tier-VII destroyer, 205/213 rows",
    "suggested_gloss": null}],
 "clusters": [
   {"label": "hbo · series", "verdict": "gloss_wrong",
    "note": "the miniseries, not the disaster",
    "suggested_gloss": "the HBO miniseries"}]}
```

Verdicts: `ok`, `junk`, `wrong_referent`, `gloss_wrong`, `example_wrong`,
`wrong_side`, `artifact`.

## How verdicts become changes

* `junk` / `wrong_referent` -> `data/audit/term_blocklist.json` ({pair: [terms]}),
  read by the exporter. A wrong referent that also pollutes the series needs a
  `config/pairs.yaml` pattern instead — that is a corpus fix, not a display fix,
  and must be measured corpus-wide first with `pipeline.audit.validate_patterns`.
* `gloss_wrong` -> corrected gloss into `collocation_glosses.json` or
  `cluster_glosses.json` ({pair: {label: gloss}}), both read by the exporter.
* `example_wrong` -> fix the selection rule, never the stored text.
* `artifact` -> report it; dropping a real-but-ugly cluster hides a finding.

## Convergence

Re-export, then re-run `pipeline.audit.gloss_coverage` and re-verify anything
that appears which was not verified before. Removing terms promotes new ones
into the top 10, and those have never been looked at.
