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

### ~~Remove pair 27 (the Ukraine → Ukraine):~~ DONE
- ~~NOT a toponym — it's a grammatical article construction~~
- ~~Action: set `enabled: false` in pairs.yaml, re-export~~
- **Status:** Disabled in pairs.yaml, purged from all site JSON files.

### ~~Fix pair 3 (Odessa → Odesa) regex:~~ DONE
- ~~3.4% contamination from Odessa, Texas~~
- **Status:** GDELT/Reddit now exclude Odessa TX sources. Verified: <0.02% Texas contamination in sampled data.

### Multilingual scope:
- Limit analysis to English-language sources ONLY
- Remove cross-lingual GDELT TLD analysis from core claims (keep as surface observation)
- Reason: Campaign targeted English media. French/Spanish/German have different transliteration systems
- "pollo de Kiev" ≠ "Chicken Kiev" — each language needs separate study
- Action: Filter GDELT to .com, .org, .net, .co.uk, .au, .ca, .ie (English TLDs) OR lang detection
- Keep current data but add English-only adoption metric alongside

## ~~3. Add Open Library Source~~ DONE
- **Status:** `pipeline/ingestion/openlibrary.py` created. Open Library is now the 8th source (1,920 records).
- **Decision:** Kept both Ngrams + Open Library (8 sources). Ngrams = word frequency inside books, OL = book title choice.

## 4. Re-run After Changes

After corpus cleanup (pending manual annotation):
1. ~~Update pairs.yaml (disable pair 27)~~ DONE
2. ~~Re-export site data~~ DONE (now runs from parquets: `make export-site`)
3. Strip contaminated texts from CL corpus (~9.3% have no pair term in text)
4. Re-annotate cleaned corpus on vast.ai
5. Re-run encoder benchmark with 11 classes (religion added)
6. Re-export to HuggingFace
7. Rebuild paper
