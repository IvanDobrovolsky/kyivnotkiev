# Next session — GDELT to site

State as of 2026-08-29 00:5x. Two processes are running; do not kill them.

```
58347  build_gdelt_verified --all      parent, 0% cpu — waiting on its child
 └─ 26794  gdelt_fetch_texts --pair volodymyr-zelenskyy --unattested
```

A parent at 0% CPU here is normal. Check `pgrep -P <pid>` before assuming a hang.

## Order of work

1. **Let the zelenskyy fetch finish.** Last pair. ~8h at 13 url/s when measured.
   Check: `pgrep -f gdelt_fetch_texts`.

2. **Retry the released URLs.** 60,534 were unlocked from the ledger after the
   fix in `47c87ef6` — they were recorded as fetched without ever being
   requested. They are unlocked but NOT queued: the running job computed its
   target list at startup. A fresh run picks them up automatically, no flags.
   Concentrated in kyiv (28,674), luhansk (7,325), kharkiv (7,240), odesa
   (6,497). Roughly a third returned 200 on sample retry, so expect ~20k
   recovered. These matter because per-outlet claims depend on them.

3. **Build verified for zelenskyy** — 23 of 24 pairs are built; it is the only
   one missing. The parent process may do this itself once the fetch ends.

4. **Decide the `neither` rows.** 36,810 successful fetches (9.2%) contain
   neither spelling — link rot and redirects. Per the plan they should be
   dropped from both the texts and the derived series, since a body that does
   not name the thing cannot evidence how it was named. Not yet applied.

5. **Process → store → site.**
   ```
   python -m pipeline.store.migrate --source gdelt
   python -m pipeline.store.migrate --source all      # rebuilds pairs/
   python -m pipeline.export_site_data                 # ~15 min, background it
   cd site && npm run build
   ```
   **Verify every page is non-empty before pushing.** `npm run build` exits 0 on
   zero-byte output — that is how the site went down earlier today.
   ```
   for f in index about sources methodology llm; do ...; done
   ```

6. **Publish to HF** via `pipeline/store/publish.py`. Do not use
   `rebuild.py::push_hf` — it targets a `data/` prefix that does not exist in the
   repo.

## Not doing

- **YouTube.** Chornobyl is done at day depth and empirically near-saturated: a
  12-hour split on 8 random days gained 1.14x for 2x the quota. The agreed next
  step is a split-and-measure descent (month by default, descend only where
  splitting a month yields substantially more), not uniform deeper collection.
  Design agreed, not built.
- Stale site artifacts: four JSONs from April/May still render on pair and
  methodology pages. Left in place pending a decision.
