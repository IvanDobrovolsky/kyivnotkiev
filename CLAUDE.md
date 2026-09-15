# Working in this repo

A computational-linguistics study measuring English adoption of Ukrainian vs
Russian transliterations across **24 pairs**, 7 sources, 2010–2025.

## Non-obvious things that will bite you

**24 pairs, not 25.** `config/pairs.yaml` holds 47 entries; 23 are
`enabled: false`. borscht is retired (its raw parquet is kept deliberately —
see `data/stats/borscht/ABANDONED.md`). Anything that globs `data/store/pairs/`
or `data/stats/` without checking `enabled` will process it and report 25.

**The site auto-deploys on push to `main`, and the site is built from committed
JSON.** Pushing pipeline code alone changes nothing a reader sees — you must run
`pipeline.export_site_data` and commit `site/src/data/*.json`. Run
`cd site && npm run build` first; a pre-push hook verifies the committed tree
and must never be bypassed.

**Keyness is computed WITHIN SOURCE. Verify it the same way.** Pooling the
sources reverses results through Simpson's paradox, because the two spelling
sides have different source mixes. A chip that looks like 1.2× pooled can be
1.9×–2.5× in every individual source.

**Never run `pipeline.stats.clusters` in parallel.** UMAP OOMs above ~40K texts.
`pipeline.export_site_data` takes an flock, so two chains queue rather than
interleave — do not remove it.

**Curated content lives in `data/audit/` and is hand-written, not generated:**
`collocation_glosses.json`, `cluster_names.json`, `cluster_glosses.json`,
`term_blocklist.json`. Nothing regenerates them. They are whitelisted in
`.gitignore` (which needs `/data/*`, not `/data/`, or git will not descend).

**Cluster names resolve by term identity as well as exact label.** Labels are the
top two c-TF-IDF terms and move on every rebuild; an exact-key lookup silently
orphaned 62 of 129 clusters, rendering as raw statistical labels.

## Commands

```bash
./pipeline/run/full_rebuild.sh        # keyness -> clusters -> export -> audits (~3.5h)
./pipeline/run/reddit_converge.sh     # drive Reddit holdouts to convergence

python -m pipeline.stats.analyze_pair --all       # or --pair <slug>
python -m pipeline.stats.clusters --pair <slug>   # one at a time
python -m pipeline.export_site_data

python -m pipeline.audit.holdout_convergence      # exits 1 while any table can improve
python -m pipeline.audit.gloss_coverage           # exits 1 on any unglossed chip
```

Secrets: `YOUTUBE_API_KEY` is in the macOS keychain —
`security find-generic-password -s YOUTUBE_API_KEY -w`. There is no `.env`.

## What "done" looks like

```
pairs      24    green 24    amber 0
holdouts   36 full, 60 pool-exhausted, improvable 0    CONVERGED
chips      372   glossed 372
clusters   128   named and described 128
```

A pair is green (`data_ready`) only when its YouTube census is complete —
every year 2010–2025, both variants, ≥12 months AND ≥40 windows per checkpoint
in `data/cl/raw/youtube_census/.checkpoints/`. Stats staleness is judged by
the sha1 of the store file recorded in `analysis.json`, not by mtime.

## Conventions

Commit each meaningful change separately. Never add AI attribution to commits.
Write comments that explain *why*, with the measurement that motivated them.
