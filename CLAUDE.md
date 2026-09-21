# Working in this repo

A computational-linguistics study measuring English adoption of Ukrainian vs
Russian transliterations across **24 pairs**, 7 sources, 2010–2025.

## Non-obvious things that will bite you

**24 pairs, not 25.** `config/pairs.yaml` holds 47 entries; 23 are
`enabled: false`. Anything that globs `data/store/pairs/` or `data/stats/`
without checking `enabled` will process retired borscht and report 25.

**borscht's raw layer is NOT on this disk any more.** The retirement rests on a
72.8% Ukrainian-form figure. `data/store/pairs/borscht.parquet` was rewritten
2026-09-14 down to 1,500 GDELT-only rows reading 90.6%, so that figure no longer
reproduces locally. The full 21,428-row multi-source file (gdelt 1,500 ·
openalex 28 · reddit 4,730 · youtube 15,170, 72.8% UA) survives only in
`~/kyivnotkiev-archive/` and on HuggingFace. `data/stats/borscht/ABANDONED.md`
is likewise untracked — `.gitignore` whitelists `/data/audit/*.md` only. The
argument itself is preserved in the tracked `notes:` field on the borscht entry
in `config/pairs.yaml`.

**Three data paths answer the same question differently, and all three are
live.** Ask how much GDELT data a pair has:

| path | role | zaporizhzhia before 2026-09-20 |
|---|---|---|
| `data/store/` | documented source of truth, published to HF | 0 rows |
| `dataset/` | what `export_site_data._load` actually reads for gdelt, trends, wikipedia, ngrams | present |
| `data/cl/corpus/gdelt_verified/` | substituted into pair files by `migrate.py:397-417` | 7,475 rows |

None is wrong on its own terms. The store tier is fixed now, but until the paths
are collapsed every GDELT number has two possible provenances. The pair-file
substitution also bypasses `apply_study_scope`, which is why 2026-dated rows
reach keyness while the `_processed` tier is correctly clamped.

**Attribution is re-derived in two places, with different key shapes.**
`flag_rows()` normalises `[-_]+ → " "` before looking a term up; `cmd_final()`
re-derives attribution from the URL and must do the same. It did not, so a URL
yielding `volodymyr-zelenskyy` missed a map keyed `volodymyr zelenskyy` and
**every multi-word pair resolved to `("", "")`** — the 2026-09-12 run lost 11 of
24 enabled pairs and left 17,103 blank-slug rows. All 13 single-word pairs were
unaffected, so it read as thin data rather than breakage. Fixed in `26f805c8`,
which now raises instead of writing an unresolvable row. Sort term fragments
longest-first in both places or `kyiv` steals `kyivan rus`.

**Do not re-query GDELT.** The 47-pair scan is done: `gdelt_v2.mentions_raw`
holds 14,394,987 rows including zaporizhzhia, scanned 2026-09-12 at 0.609 TiB.
Re-reading the table is cheap; the scan must never be repeated. Before any new
scan check the month's billed bytes —
``SELECT SUM(total_bytes_billed) FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT`` —
because the free tier is 1 TiB/month and September 2026 was already at 0.609.
Always query every pair in one pass; per-pair costs 24× the same bytes.

**HuggingFace is published by hand and lags every rebuild.**
`pipeline.store.publish` is explicitly not part of `full_rebuild.sh`. It uploads
`data/store/pairs/*.parquet` wholesale, so publishing a degraded local file
destroys the remote copy — this is how the good borscht would be lost. Diff
against `~/kyivnotkiev-archive/hf-snapshot-2026-09-11/MANIFEST.json` after
publishing so a silent loss shows up as a named file, not a size delta.

**The site is built from committed JSON.** Pushing pipeline code alone changes
nothing a reader sees — run `pipeline.export_site_data` and commit
`site/src/data/*.json`. Run `cd site && npm run build` first; the pre-push hook
verifies the committed tree and must never be bypassed. The deploy trigger
itself is Cloudflare, configured in their dashboard and **not visible anywhere
in this repo** — there is no `.github/`, `vercel.json` or `netlify.toml`. After
a deploy-affecting push, curl the live site for a marker string; pushed ≠
deployed.

**Keyness is computed WITHIN SOURCE. Verify it the same way.** Pooling the
sources reverses results through Simpson's paradox, because the two spelling
sides have different source mixes. A chip that looks like 1.2× pooled can be
1.9×–2.5× in every individual source.

**Never run `pipeline.stats.clusters` in parallel.** The 15,000-text cap is real
and enforced in code; the ~40K OOM figure that motivates it is a source comment
with no recorded benchmark. `pipeline.export_site_data` takes an flock, so two
chains queue rather than interleave — do not remove it.

**Curated content lives in `data/audit/` and is hand-written, not generated:**
`collocation_glosses.json`, `cluster_names.json`, `cluster_glosses.json`,
`term_blocklist.json`. Nothing regenerates them. They are whitelisted in
`.gitignore` (which needs `/data/*`, not `/data/`, or git will not descend).

**Cluster names resolve by term identity as well as exact label.** Labels are the
top two c-TF-IDF terms and move on every rebuild; an exact-key lookup once
orphaned 62 of the then-129 clusters, rendering as raw statistical labels. Two
current clusters still resolve to no curated entry (chornobyl `radiation ·
thyroid`, kyivan-rus `ukraine · armor`) and are invisible only because the
display merge folds them into larger cards.

**No gate is enforced by anything.** `full_rebuild.sh` sets `-u` but not `-e`,
and both audit exit codes are discarded into the log. The pre-push hook checks
the build, i18n parity and a runtime smoke — it calls none of the audits. Every
green number holds because a human read the log.

**`green_check`'s sha1 comparison can never be true.** It compares a 40-character
`hexdigest()` against the 16-character value `analyze_pair.py:155` writes, so it
is False for every pair and silently falls back to the row-count check. A scrub
that rewrites text without changing row count would still print PASS. `green_check`
is also absent from `full_rebuild.sh`.

## Counting rules that do not fall out of the obvious query

- **372 chips = 352 (`ua` + `ru`) + 20 (`solo`).** Counting only ua+ru gives 352.
  feodosiia and serhii-korolyov ship solo chips only, with a different key shape.
- **128 clusters is the post-merge display count.** 135 exist on disk across the
  24 enabled pairs; the site number and the stats number will never agree.
- **The `solo` path skips the document-share guard** — no `MIN_DF_RATIO`, no
  per-source grouping. A different gate (≥5 distinct hosts) substitutes.

## Commands

```bash
./pipeline/run/full_rebuild.sh        # keyness -> clusters -> export -> audits (~3.5h)
./pipeline/run/reddit_converge.sh     # drive Reddit holdouts to convergence

python -m pipeline.stats.analyze_pair --all       # or --pair <slug>
python -m pipeline.stats.clusters --pair <slug>   # one at a time
python -m pipeline.export_site_data

python -m pipeline.audit.green_check              # 24/24 green; NOT in the rebuild chain
python -m pipeline.audit.holdout_convergence      # exits 1 while any table can improve
python -m pipeline.audit.gloss_coverage           # exits 1 on any unglossed chip
python -m pipeline.audit.site_invariants          # checks cluster desc, but not name

python -m pipeline.store.migrate --source <name> --dry-run   # then without --dry-run
python -m pipeline.store.publish --dry-run                   # READ THE HF WARNING ABOVE
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
in `data/cl/raw/youtube_census/.checkpoints/`.

Read these as artifact-presence claims, not statistical-strength ones. "green"
says the census finished and the files exist; it says nothing about keyness
quality — serhii-korolyov is green with `interpretable: false` and zero usable
sources. "CONVERGED" includes 16 of 96 tables that ship zero rows and pass only
because their pool is also zero. Stats staleness is judged by the sha1 recorded
in `analysis.json` **in the exporter**; see the `green_check` note above for why
the audit does not actually do this.

## Conventions

Commit each meaningful change separately. Never add AI attribution to commits —
note that 225 commits from 2026-08-25 to `ff61c868` carry a `Claude-Session:`
trailer injected by a different client; nothing in this repo adds it, and
commits made here come out clean.

Write comments that explain *why*, with the measurement that motivated them.
