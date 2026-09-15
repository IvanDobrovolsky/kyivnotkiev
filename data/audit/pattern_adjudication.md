# Drop-pattern adjudication — the 13 "review" patterns

Source: `data/audit/pattern_validation.json` → `review` (2–10% corpus impact each).
Method: each pattern re-applied with `re.I` to `title + " | " + text` on the pair's
`data/store/pairs/<slug>.parquet` (match counts reproduce the manifest exactly);
8–10 matched documents read in full per pattern, plus a targeted census of the
leak class each pattern is at risk of (English rows, prose-vs-URL usage,
prose-vs-work-title usage, referent).

Decision rules applied:
- The study measures **English** usage. Removing non-English documents is legitimate;
  removing English documents that merely *quote* a foreign word/tag is not.
- Hashtags are a spelling choice by the poster → keep, unless the matched set is bot/spam.
- Frozen brands / work titles / different referents are not choices → dropping is correct.
- A URL containing the form is not evidence; the row's prose decides.
- Uncertain → REJECT.

**Result: 2 ACCEPT, 11 REJECT.**

| # | pair | pattern | matches | share | verdict |
|---|------|---------|--------:|------:|---------|
| 1 | luhansk | `#lugansk\b` | 1,923 | 2.98% | REJECT |
| 2 | donbas | `(?:^\|\s)#\w*donbass\w*` | 4,055 | 4.75% | REJECT |
| 3 | chicken-kyiv | `[а-яё]{4,}` | 434 | 5.93% | REJECT |
| 4 | dynamo-kyiv | `\b(özet\|maçi\|maçı\|rakibi\|golü\|türkçe\|spiker\|şov\|futbol)\b` | 761 | 2.17% | REJECT |
| 5 | babyn-yar | `op\.?\s*113[^\n]{0,25}babi yar` | 209 | 3.16% | **ACCEPT** |
| 6 | babyn-yar | `symphony no\.?\s*13[^\n]{0,25}babi yar` | 233 | 3.52% | REJECT |
| 7 | varenyky | `https?://[^\s)]*vareniki` | 289 | 6.97% | REJECT |
| 8 | oleksandr-usyk | `[Ѐ-ӿ]{3,}` | 982 | 2.20% | REJECT |
| 9 | feodosiia | `\b(?:serangan\|ukraina\|krimea\|jembatan\|pelabuhan)\b` | 39 | 2.05% | REJECT |
| 10 | ihor-sikorsky | `\b(helikopter\|penemu\|sejarah\|kisah\|pencipta)\b` | 128 | 4.87% | REJECT |
| 11 | ihor-sikorsky | `\bhelic[oó]ptero\b` | 75 | 2.86% | REJECT |
| 12 | serhii-korolyov | `,\s*sergei korolev[^,]{0,25},` | 110 | 7.73% | REJECT |
| 13 | mykola-hohol | `nikolai\s*gogol\s*edit` | 436 | 4.06% | **ACCEPT** |

---

## 1. luhansk — `#lugansk\b`

- **matches** 1,923 (2.98% of 64,459) — youtube 1,882 · reddit 35 · gdelt 6
- **variant of matched rows**: russian 1,694 · both 229
- **leak census**: 230 matched rows are Latin-script English-density documents.

Representative matches:

    - `gdelt` 2017-02-08 — cross-referenced news and research resources about municipalities of Lugansk Oblast, Ukraine images: google yahoo YouTube contact us updated Mon. Janu…
    - `youtube` 2022-05-09 — Ejército ruso bombardea escuela en la región de Lugansk | De Pisa y Corre El ejército ruso bombardeo una escuela en la región de #Lugansk, se estima q…
    - `youtube` 2025-03-21 — Donbass Lugansk🕯️ #countryballs #memes #edit #ukraine #map #ua_ball #history #Donbass #Lugansk

**REJECT** — a hashtag is the poster's spelling choice and the matched set is not
uniformly bot/spam: it contains English aggregator prose that uses "Lugansk"
throughout and English-language meme uploads whose only text is the hashtag itself.

## 2. donbas — `(?:^|\s)#\w*donbass\w*`

- **matches** 4,055 (4.75% of 85,448) — youtube 3,769 · reddit 271 · gdelt 15
- **variant of matched rows**: russian 3,032 · both 972 · ukrainian 51
- **leak census**: 1,401 matched rows (34.5%) are English-density Latin-script documents.

Representative matches:

    - `gdelt` 2015-04-11 — French military intelligence rules out ‘Russian invasion plans’ for Ukraine Published 11 Apr, 2015 13:02 | Updated 11 Apr, 2015 16:49France’s intellig… [RT article; matches only because it embeds a tweet reading "93% of #Donbass Civilians Say Alienated from Kiev Forever"]
    - `youtube` 2023-01-03 — Ukraine #dpr #lpr #donbass #ukraine #ukrainewar
    - `youtube` 2022-03-02 — 🇷🇺🚀💥🇺🇦 Now, In Berdyansk...🙏 #ukraineunderattack #ukrainewar #ukrainerussiawar #happychip At dawn on February 28, Russian troops took control of the c…

**REJECT** — it deletes full English news articles for quoting a tweet's hashtag, and
the remaining hashtag rows are the poster's own spelling choice (many carry `#donbass`
and `#donbas` side by side), not bot noise.

## 3. chicken-kyiv — `[а-яё]{4,}`

- **matches** 434 (5.93% of 7,319) — youtube 358 · reddit 70 · gdelt 6
- **leak census**: 104 of 434 matched rows are ≥95% Latin-script (i.e. English documents
  that merely quote one Cyrillic word); only 234 are majority-Cyrillic.

Representative matches:

    - `gdelt` 2021-04-03 — [The Moscow Times, entirely in English] Years before I ever tasted it, I was pre-conditioned to adore schnitzel. Maria in “The Sound of Music” listed it along with “cream-colored ponies” and… [matched on the single word "отбивная"]
    - `reddit` 2023-12-03 — [r/ukraine, English] 7:39 EET; The Sun is Rising Over Kyiv on the 648th Day of the Full-Scale Invasion. Ukrainian Honey Cookies! + Charities # 🇺🇦 Слава Україні! 🇺🇦 …
    - `youtube` 2021-08-01 — "Чорний серпень" – до чого готуватися? // Реальна політика з Євгенієм Кисельовим … 30 років "Chicken Kyiv speech" Джорджа Буша …

**REJECT** — the same pattern was already rejected for `ternopil`; here it removes the
out-of-scope Cyrillic rows *and* ~104 English rows whose only Cyrillic is a quoted word
or a slogan (the Moscow Times feature and the r/ukraine daily thread are real usage).

## 4. dynamo-kyiv — `\b(özet|maçi|maçı|rakibi|golü|türkçe|spiker|şov|futbol)\b`

- **matches** 761 (2.17% of 35,000) — youtube 712 · reddit 37 · gdelt 12
- **leak class**: `futbol` is not Turkish-exclusive — it occurs in "**Futbol** Club Barcelona"
  and in the multilingual SEO tag blocks that English football channels append.

Representative matches:

    - `gdelt` 2015-03-12 — [Bleacher Report, English] Romelu Lukaku and Steven Naismith Combine for Fine Everton Goal vs. Dynamo Kiev "Lukaku yıkılmıyor ve Naısmıth'e golü attırıyor https://t.co/XKgmM1CuP… [matched on an embedded Turkish tweet]
    - `youtube` 2022-05-22 — [English] Ter Stegen & Neto | FC Barcelona: Goalkeeper Training … matches against Atlético Madrid, Valencia, Dynamo Kyiv … [matched on "Futbol Club Barcelona"]
    - `youtube` 2021-02-18 — DYNAMO KYIV - CLUB BRUGGE! | AVRUPA LİGİ MAÇI 2021 JOKERELLOW kanalına hepiniz hoşgeldiniz :) … [genuinely Turkish]

**REJECT** — the Turkish-specific terms are safe but `futbol` drags in English documents
(a Bleacher Report match report, Barcelona's own English club description, English
match-preview tag salads), each a real "Dynamo Kiev"/"Dynamo Kyiv" usage.

## 5. babyn-yar — `op\.?\s*113[^\n]{0,25}babi yar`   ✅ ACCEPT

- **matches** 209 (3.16% of 6,624) — youtube 201 · reddit 7 · openalex 1
- **variant of matched rows**: russian 208 · both 1
- **census**: median text length 546 chars; 201 rows are catalogue/track metadata
  ("Provided to YouTube by … ℗ 2006 Arts Music … Auto-generated by YouTube"). Only 8 rows
  contain any of massacre/ravine/Nazi/Holocaust/memorial, and only **4 rows** use
  "Babi Yar" as a place name in English prose — all four are programme notes for the
  same work (two of them duplicate uploads by one composer).

Representative matches:

    - `openalex` 1995-01-01 — Symphony no. 13 op. 113 : "Babi Yar"
    - `youtube` 2018-11-30 — Shostakovich: Symphony No. 13, Op. 113"Babi Yar": 4. Largo - "Fears" · Sergei Koptchak · NHK Symphony Orchestra … Auto-generated by YouTube.
    - `youtube` 2025-01-04 — Eero Järvilehto: Against War -suite … "a country whose Babi Yar monument was destroyed by the Russian army" [the residual cost: 4 such rows]

**ACCEPT** — the opus number only ever occurs inside Shostakovich's frozen work title, so
205 of 209 rows are non-authored recording metadata rather than anyone's spelling choice;
the 4 programme-note exceptions echo the title they are annotating. (Documented cost: 4 rows.)

## 6. babyn-yar — `symphony no\.?\s*13[^\n]{0,25}babi yar`   ❌

- **matches** 233 (3.52% of 6,624) — youtube 189 · gdelt 32 · reddit 10 · openalex 2
- **census**: 43 matched rows mention massacre/ravine/Nazi/Holocaust/memorial; 37 are
  long-form; **27 rows use "Babi Yar" in prose outside any work-title context** (17 of
  them more than once). Named outlets in the matched set: The New Yorker, Daily Kos,
  American Thinker, Tablet, The Forward, Chicago Tribune, Chicago Sun-Times, SMH.

Representative matches:

    - `gdelt` 2022-05-23 — [New Yorker] Remembering Babyn Yar. I always find Masha Gessen’s work thought-provoking, and their piece about Babyn Yar, where German soldiers massacred tens of th…
    - `gdelt` 2026-08-03 — [Daily Kos] … a ravine on the outskirts of the Ukrainian capital of Kyiv known as “Babi yar” in Russian and “Babyn yar” in Ukrainian …
    - `youtube` 2014-12-31 — Symphony No. 13 In B Flat Minor, Op. 113, "Babi Yar": V. Allegretto (A Career) (Shostakovich) … Auto-generated by YouTube.

**REJECT** — unlike the opus number, "Symphony No. 13" occurs in ordinary journalism, so
the pattern deletes ~27 English articles that are *about the massacre* and merely mention
the symphony — including the one document that explicitly contrasts the Russian and
Ukrainian spellings.

## 7. varenyky — `https?://[^\s)]*vareniki`

- **matches** 289 (6.97% of 4,149) — youtube 278 · reddit 11
- **census**: strip every URL from the matched rows and **54 still contain "vareniki"**
  in running text; 51 of those are English.

Representative matches:

    - `reddit` 2017-07-21 — Swapping out kefir with greek yogurt in dough? Hi all. So I'm trying to make vareniki (pretty much dumplings), and I'm hoping to use this recipe…
    - `reddit` 2024-07-16 — Vareniki … Russian vareniki are like Italian ravioli: little pockets of pastry are filled with cheese. Serves 6 …
    - `youtube` 2017-11-23 — Ленивые вареники Ингредиенты: 150 г творога … [Russian-language, URL-only case the pattern was written for]

**REJECT** — the rule that a URL is not evidence cuts both ways: 54 of the 289 rows
(incl. an FDA-style recall notice, r/Cooking, r/FoodDiaries and "How to Make Vareniki,
Russian Dumplings") use the form in the author's own prose and happen to also link a
recipe URL.

## 8. oleksandr-usyk — `[Ѐ-ӿ]{3,}`

- **matches** 982 (2.20% of 44,597) — youtube 954 · reddit 20 · gdelt 8
- **census**: 287 of 982 matched rows are ≥95% Latin-script; only 272 are majority-Cyrillic.

Representative matches:

    - `gdelt` 2023-04-27 — [novinite.com, English war digest] Day 428 of the invasion of Ukraine. Summary of key events in the last 24 hours: - NATO: We have delivered 230 tanks… [matched on a quoted "СЛАВА"]
    - `youtube` 2018-02-09 — [English] Murat Gassiev Vs Oleksandr Usyk | EA Sports UFC 3. Two of boxing's young prospects step in the octagon… [matched on an auto-translated Russian paragraph appended below the English one]
    - `youtube` 2015-12-13 — Александр Усик vs Педро Родригес Нокаут Бокс … Oleksandr Usyk vs Pedro Rodriguez [the bilingual case]

**REJECT** — same failure mode as #3: 29% of the matches are English documents carrying a
Cyrillic tag block, quoted slogan or machine-translated appendix, and they are real
"Oleksandr Usyk"/"Alexander Usyk" usages.

## 9. feodosiia — `\b(?:serangan|ukraina|krimea|jembatan|pelabuhan)\b`

- **matches** 39 (2.05% of 1,906) — youtube 39. All 39 inspected individually.
- **breakdown**: 13 Russian real-estate listings (matched on "ukraina" inside a URL slug),
  8 Indonesian, 7 Azerbaijani, 1 Uzbek, **7 English**, 3 other.

Representative matches:

    - `youtube` 2023-12-27 — [English, Kanal13] 20% of Russian Black Sea Fleet was destroyed in 4 months … the strike on the large landing ship Novocherkassk in the port of Feodosiia, Crimea … [matched on the "#ukraina" tag]
    - `youtube` 2023-12-28 — [Indonesian, the target class] Berisi Senjata dari Iran, Kapal Novocherkassk Ditenggelamkan Ukraina, 33 Tentara Rusia Raib…
    - `youtube` 2020-10-05 — [Russian real-estate ad] Продажа дома 120 кв.м … к-р Украина, ул Чкалова Феодосия https://tavridadom.ru/…-feodosiya-ukraina-chkalova-ul/

**REJECT** — `ukraina` is not Indonesian-exclusive: it appears in the multilingual tag
blocks of English news channels, and 7 of the 39 matches are English reports on the
Feodosiia strike that use the Ukrainian form.

## 10. ihor-sikorsky — `\b(helikopter|penemu|sejarah|kisah|pencipta)\b`

- **matches** 128 (4.87% of 2,626) — youtube 127 · reddit 1; all variant=russian
- **leak class**: `helikopter` is Turkish/German/Dutch/Indonesian *and* appears in the
  multilingual keyword lists English channels paste in. 5 English rows leak.

Representative matches:

    - `youtube` 2016-06-03 — [English] Helicopter Urban Landing. A helicopter is a type of rotorcraft… it was not until 1942 that a helicopter designed by Igor Sikorsky reached full-scale production…
    - `youtube` 2017-07-22 — [English] HELICOPTER inventions Part 2 … Hubschrauber 直升机,HÉLICOPTÈRE,ВЕРТОЛЕТ,…,HELİKOPTER,… [English body, multilingual word list]
    - `reddit` 2024-11-25 — [Turkish, the target class] İlk Başarılı Helikopter Uçuşu 1939'da: Igor Sikorsky tarafından icat edildi

**REJECT** — the Indonesian-only terms are safe, but `helikopter` deletes English
"Igor Sikorsky" documents whose description carries a multilingual keyword list.

## 11. ihor-sikorsky — `\bhelic[oó]ptero\b`

- **matches** 75 (2.86% of 2,626) — youtube 74 · reddit 1; all variant=russian
- **leak class**: `#helicoptero` in hashtag blocks of English videos. 5 English rows leak.

Representative matches:

    - `youtube` 2020-04-29 — [English] IGOR SIKORSKY | the father of the helicopter | American engineer … Aviation pioneer Igor Ivanovich Sikorsky was born May 25, 1889 in Kiev, Russia…
    - `youtube` 2019-04-14 — [English] Most Expensive Helicopters and How Much They Cost … Helicopters were developed and built during the first half-century of flight…
    - `youtube` 2011-05-21 — [Portuguese, the target class] Igor Sikorsky e o desenvolvimento do helicóptero

**REJECT** — mostly Portuguese/Spanish as intended, but it also deletes a full English
biography of Sikorsky and other English uploads that merely tag `#helicoptero`.

## 12. serhii-korolyov — `,\s*sergei korolev[^,]{0,25},`

- **matches** 110 (7.73% of 1,423) — gdelt 47 · youtube 47 · reddit 16; all variant=russian
- **census**: 91 of 110 matched rows (83%) are English-density prose.

Representative matches:

    - `gdelt` 2025-04-17 — [The Independent] Vladimir Putin has drawn a parallel between Elon Musk and a key figure in the Soviet space race, Sergei Korolev, praising the SpaceX founder as a visionary.
    - `gdelt` 2021-04-28 — [New Statesman] … The same applied to the man who put him in space, Sergei Korolev, whose identity was kept secret until after his death.
    - `youtube` 2020-01-05 — [the intended target: a comma-separated keyword list] …cosmonaut, gagarin, soviet union (country), first man in space, rocket, sergei korolev, science, space exploration…

**REJECT** — the most damaging pattern in the batch: it was evidently aimed at YouTube
keyword lists but the comma-appositive it encodes is standard English journalistic
syntax, so it deletes The Independent, New Statesman, The Hindu, The National and
Space.com articles — the pair's core evidence.

## 13. mykola-hohol — `nikolai\s*gogol\s*edit`   ✅ ACCEPT

- **matches** 436 (4.06% of 10,737) — youtube 436, all variant=russian
- **census**: 435 of 436 are Bungou Stray Dogs fan-edit uploads (the anime character
  Nikolai Gogol — a different referent from the writer); the follow-on token is
  `| nikol…`, `#bungou…`, `| bsd…`, `| decay…` etc. Exactly **1** row is about the
  writer, and it is Portuguese (a Dead Souls book review where "edit" comes from
  *Editora*), i.e. out of the study's English scope either way. No English document
  about the writer is touched.

Representative matches:

    - `youtube` 2022-04-02 — hatchback - nikolai gogol edit. a late birthday edit for nikolai gogol : ) !!S P O I L E R S A H E A D!! audio credit: … app being used: alight motion
    - `youtube` 2024-11-09 — Decay of Angel.. Nikolai Gogol edit #nikolaigogol #bsd #bungoustraydogs #edit #anime #animeedit
    - `youtube` 2024-01-31 — [the single non-BSD row, Portuguese] Almas Mortas | Nikolai Gogol | Uma Família Que Lê … Livro: Almas Mortas | Nikolai Gogol Editora: Mimética

**ACCEPT** — the matched rows name an anime character, not the writer; the sole exception
is a non-English book video. This is the cleanest wrong-referent pattern of the 13.

---

## Accepted patterns

```python
ACCEPTED = [
    ("babyn-yar",    r"op\.?\s*113[^\n]{0,25}babi yar"),
    ("mykola-hohol", r"nikolai\s*gogol\s*edit"),
]
```

## Recurring failure modes among the 11 rejections

1. **Language keywords are not language detectors.** `futbol`, `helikopter`, `helicóptero`,
   `ukraina` all appear inside the multilingual tag/keyword blocks that English YouTube
   channels paste into descriptions, and inside English proper nouns
   ("Futbol Club Barcelona"). (#4, #9, #10, #11)
2. **Cyrillic-presence tests delete English documents that quote Cyrillic.** 24% (chicken-kyiv)
   and 29% (usyk) of matches are ≥95% Latin-script. (#3, #8)
3. **Hashtag patterns delete the spelling choice they should be measuring**, and they also
   catch English news articles that embed a tweet. (#1, #2)
4. **Title/keyword-list anchors that are also ordinary English syntax** (`, Sergei Korolev,`)
   or ordinary journalistic phrasing (`Symphony No. 13 … Babi Yar`) delete the pair's best
   evidence. (#6, #12)
5. **URL anchors** catch rows whose prose independently uses the form. (#7)

If any of these are wanted after all, the fixable ones are #10 (drop `helikopter`, keep the
Indonesian-only terms), #4 (drop `futbol`, keep the Turkish-only terms), and #6/#7/#12
(re-anchor to require that the form does *not* also occur in running prose outside the
title / URL).
