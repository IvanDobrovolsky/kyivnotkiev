# borscht — retired, do not rebuild

Disabled in `config/pairs.yaml` (see the retirement note there) and absent from
the site's 24 pairs. The figures in this directory are **stale by definition**
and must not be quoted: nothing regenerates them, and `analyze_pair --all` skips
the pair by design.

The pair was dropped because the contrast does not hold. "Borscht" is not the
Russian spelling against a Ukrainian "borsch" — Russian speakers use *borscht*
constantly, and the -t is an artefact of the German/Yiddish route the word took
into English. Measuring adoption needs a pair where each form belongs to one
convention; this one does not.

## Two corrections to the original version of this file (2026-09-21)

**This directory was NOT all produced before the retirement.** The pair was
retired 2026-09-11. `records.parquet` is dated 2026-09-13 18:21 and this file
2026-09-13 20:08 — both after. Only `analysis.json` (09-04) and the clustering
output (09-11) predate it. Do not read a file's presence here as evidence of
what the retirement decision saw.

**The raw layer is no longer kept here.** The original text promised
`data/store/pairs/borscht.parquet` was retained so the call stays
re-examinable. That file was rewritten 2026-09-14 16:23 and now holds **1,500
GDELT-only rows reading 90.6% Ukrainian-form** — not the evidence the
retirement rests on. The full multi-source layer is **21,428 rows** (gdelt
1,500 · openalex 28 · reddit 4,730 · youtube 15,170) at **72.8%**, matching
`data/audit/language_gate_disclosure.md`, and it survives in exactly two places:

- `~/kyivnotkiev-archive/hf-snapshot-2026-09-11/pairs/borscht.parquet`
  (sha256 `c36beb7b02654664b589a3a73e675c7386c1e204f1f8763c8c7f8571a0b8c837`)
- HuggingFace `KyivNotKiev/toponym-adoption-data`, revision `8b0be97a`

`pipeline.store.publish` uploads `pairs/*.parquet` wholesale, so publishing the
current local store would overwrite the HuggingFace copy with the 1,500-row one
and leave the archive as the only record. Restore before republishing:

```
cp ~/kyivnotkiev-archive/hf-snapshot-2026-09-11/pairs/borscht.parquet \
   data/store/pairs/borscht.parquet
```

The argument for the retirement does not depend on this file — it is preserved
in the tracked `notes:` field on the borscht entry in `config/pairs.yaml`.

Zaporizhzhia replaced it in the active set.
