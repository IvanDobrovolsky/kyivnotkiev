# Adding zaporizhzhia — runbook

Measured by a four-agent survey of the ingestion code and the data on disk.
Cost is $0.00: the BigQuery scan is 0.609 TiB and September's 1 TiB free tier
is untouched (last job on the project was 26 Aug).

## Traps, in the order they would bite

1. **borscht must be ENABLED for the scan, then disabled again.** The regex is
   built from the enabled set and the write is WRITE_TRUNCATE. Scanning with
   borscht retired yields a 48-term regex where the other 23 pairs were built
   with 50, changing leftmost-match attribution, and permanently overwrites the
   borscht raw layer the retirement analysis rests on.
2. **Never scan zaporizhzhia alone.** Identical byte cost — BigQuery bills
   columns x partitions, the WHERE clause is free — but under the joint regex
   REGEXP_EXTRACT takes the LEFTMOST match, and zaporizhzhia keeps only 6,323
   of 8,799 slug-attested URLs (71.9%); 2,476 go to kyiv (1,575), lviv (185),
   kharkiv (132), donbas (102), odesa (99), chornobyl (90), luhansk (88),
   bakhmut (41). Every other pair paid that tax. A solo scan would hand this
   pair ~39% more URLs and make its adoption figure incomparable.
3. **Clear the fetch ledger for this pair's URLs before fetching.**
   gdelt_fetch_texts filters against ONE global ledger
   (data/raw/gdelt/texts/fetched_urls.txt). 2,297 of the pair's URLs are already
   in it, fetched under other pairs, and 1,544 already have text tagged to
   volodymyr-zelenskyy (565), dnipro-river (410), kyiv (232), kharkiv (88),
   odesa (70), chornobyl (62), luhansk (61). Skipped silently, they never reach
   this pair's corpus — and the loss is pair- and domain-correlated, so it
   skews rather than thins.
4. **The URL-slug series is not the study metric.** The metric is
   data/cl/corpus/gdelt_verified/<slug>.parquet, where the article BODY decides
   the variant. Both the attested and unattested pools must be fetched, because
   the series and the corpus are derived from the same records.

## Steps

0. DONE — collector restored from git (f17e4bf7^, deleted as a dead module) and
   the four overwrite-in-place artefacts backed up to
   data/raw/gdelt/mentions_v2/pre_zaporizhzhia_backup/ (548 MB).
1. Re-enable borscht in config/pairs.yaml.
2. Dry-run: expect 669,552,904,550 bytes, under the script's own ceiling.
3. Run the ONE joint scan, all enabled pairs.
4. download -> final -> export.
5. Clear this pair's URLs from fetched_urls.txt.
6. python -m pipeline.build_gdelt_verified --pair zaporizhzhia (attested, then
   --unattested, then gdelt_verified).
7. Re-disable borscht.
8. Audit the new corpus for referent contamination BEFORE trusting a number.
   Measured on the co-occurrence pool: ZAZ car 0.1%, Cossack Sich 0.05%,
   football club 0.006%, against nuclear plant 48.0% and oblast 36.6% — all
   legitimate referents of the same place. Re-measure on the real corpus rather
   than inheriting that figure; the odesa precedent was 10,772 of 22,439 records
   being US towns, moving adoption 67.5% -> 34.2%.
9. pipeline.rebuild, then a verified clean build before any push.

## Other layers

Reddit (21,405 rows), OpenAlex (448), Trends (2,142), Ngrams (26) and the LLM
audit (216) are ALREADY COLLECTED under pair_slug=zaporizhzhia. YouTube is in
progress by census (~5,500 search calls, fits one day at week depth).

Wikipedia must be EXCLUDED for this pair: both variants resolve to one article
and return identical pageviews, producing a hard-coded 50.0% line. Already
handled — the exporter now drops any wikipedia series whose two variants are
identical.

## Known defect this pair will have until multi-form matching exists

"Zaporizhia" (one z) was the MAJORITY Ukrainian-derived English spelling before
2022 — 2.7:1 over the two-z form in GDELT 2015-21, 5.9:1 on Reddit 2014-17 —
and is near-absent after. Matching only "Zaporizhzhia" therefore hides most of
the pre-2022 Ukrainian side and overstates the 2022 jump: GDELT would publish
68.4% -> 95.0% where the truth is 86.5% -> 95.5%. Error by source: GDELT -18
points, Reddit -22, YouTube -37.
