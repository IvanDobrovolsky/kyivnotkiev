# Data Quality TODOs

## 1. Fill Data Gaps for All Pairs

### Sources to query more data from:
- **Reddit**: Search broader subreddits for pairs 70 (Vladimir the Great), 71 (Prince of Kiev), 72 (Bakhmut), 58 (Odessa Nat'l Univ). Target: r/ukraine, r/worldnews, r/combatfootage, r/UkrainianConflict, r/AskHistorians
- **YouTube**: yt-dlp search for pairs 70, 71, 72 — war footage channels have Bakhmut content
- **OpenAlex**: Already expanded to 500/variant (19,786 texts). Some pairs inherently sparse (sports clubs, niche landmarks)
- **GDELT**: 20 compound-name pairs have no GDELT data (multi-word names don't match in URL text). Structural limitation — document, don't fix.

### Priority gaps (starred pairs):
- Pair 72 (Bakhmut): Reddit ❌, YouTube ❌ — MUST FIX, this is a starred pair
- Pair 70 (Vladimir the Great): Reddit ❌, YouTube ❌ — inherently sparse, document as finding
- Pair 61 (Zelenskyy): Ngrams ❌ — expected (too new for books), no fix possible

## 2. Remove / Fix Problem Pairs

### Remove pair 27 (the Ukraine → Ukraine):
- NOT a toponym — it's a grammatical article construction
- Different phenomenon from transliteration (Kiev→Kyiv)
- CL reviewers will challenge why this is in a toponym study
- Action: set `enabled: false` in pairs.yaml, re-export

### Fix pair 3 (Odessa → Odesa) regex:
- 3.4% contamination from Odessa, Texas (pop. 115K)
- Current regex matches any "Odessa" regardless of context
- Fix: Add negative context filter for Texas-specific keywords (Permian, Midland, Texas, TX, meteorite)
- OR: Add positive filter requiring Ukraine/Ukrainian/Black Sea/port context within same text
- Action: Update GDELT query with disambiguation, re-export, document the filter
- Note: This is a partial fix — some Odessa TX texts won't have clear markers

### Multilingual scope:
- Limit analysis to English-language sources ONLY
- Remove cross-lingual GDELT TLD analysis from core claims (keep as surface observation)
- Reason: Campaign targeted English media. French/Spanish/German have different transliteration systems
- "pollo de Kiev" ≠ "Chicken Kiev" — each language needs separate study
- Action: Filter GDELT to .com, .org, .net, .co.uk, .au, .ca, .ie (English TLDs) OR lang detection
- Keep current data but add English-only adoption metric alongside

## 3. Add Open Library Source

### What: Book title adoption (Internet Archive's Open Library)
- API: https://openlibrary.org/search.json?q={term}&first_publish_year={year}
- Free, no auth, covers all years including 2020-2025
- Measures: How many published books use each spelling in their title per year
- Finding already confirmed: Kiev→Kyiv crossover in book titles happened in 2019

### Implementation:
- Create `pipeline/ingestion/openlibrary.py`
- Query all 59 pairs (minus removed ones) for years 2010-2025
- Store: pair_id, year, term, variant, book_count
- Add to BigQuery as `raw_openlibrary`
- Add to export pipeline and manifest
- Source count: 7 → 8 (GDELT, Trends, Wikipedia, Reddit, YouTube, OpenAlex, Ngrams, Open Library)
- OR: Replace Ngrams with Open Library (both measure books, OL is more current)

### Decision needed: Keep Ngrams + add Open Library (8 sources) OR replace Ngrams with Open Library (still 7)?
- Argument for keeping both: Ngrams = word frequency inside books, OL = book title choice. Different signals.
- Argument for replacing: Ngrams stops 2019, OL goes to 2025. One book source is enough.
- Recommendation: Replace Ngrams with Open Library. One clean book source covering full period.

## 4. Re-run After Changes

After all fixes:
1. Update pairs.yaml (disable pair 27)
2. Re-export from BigQuery (make export-site)
3. Re-run CL extraction (make cl-extract)
4. Re-run GDELT async with Odessa filter (make cl-gdelt)
5. Re-balance corpus (make cl-balance)
6. Re-annotate on vast.ai (~$5-8, ~45 min)
7. Re-run encoder benchmark (keep existing robustness, just retrain best model)
8. Re-export to HuggingFace
9. Rebuild paper
