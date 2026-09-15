# Collocations review — cl_keyness.json + cl_clusters.json (site-shipped)

Date: 2026-08-25 (session run; data files dated 9 Sep in repo)
Scope: every displayed keyness chip (361 chips across 23 pairs) and every shipped cluster
label/gloss (24 pairs incl. feodosiia, which has clusters but no keyness). Read-only audit;
no pipeline runs. Classifications were cross-checked against the shipped JSON term-by-term
(zero mismatches) and root causes verified against `data/stats/<slug>/records.parquet` and
`data/stats/<slug>/clusters/summary.json`.

Classes: **S** signal · **F** redundant-family · **G** generic-noise · **A** artifact.
Verdicts: keep / merge (into family head) / drop (stopword or filter) / fix (pipeline change,
not a stopword).

---

## CONSOLIDATED FINDINGS (deliverables a–e)

### (e) Overall noise estimate — headline number

**189 of 361 displayed chips (52.4%) are classes 2–4.**
Breakdown: signal 172 (47.6%) · redundant-family 29 (8.0%) · generic-noise 78 (21.6%) ·
artifact 82 (22.7%). Artifact is the largest single defect class — this is not a stopword
problem first; it is a referent/markup/language-gate problem first.

### (d) Five worst pairs by noise share (chips, both sides combined)

| rank | pair | chips | noise | share | dominant failure |
|---|---|---|---|---|---|
| 1 | borscht | 20 | 18 | **90%** | RU side 10/10 = Goldman Sachs analyst **Matthew Borsch** surname reservoir (231 GDELT docs); UA side 8/10 STOP-gap generics (him, never, every…) |
| 2 | lviv | 18 | 15 | **83%** | RU side 8/8 = **Prince Georgy Lvov** (1917 Provisional Government PM) person-homonym reservoir (trotsky, mensheviks, rsdlp, karenina…) |
| 3 | dynamo-kyiv | 20 | 16 | **80%** | RU side 9/10 Turkish pirate-stream + Dutch Eredivisie leakage (canl, izle, tegen, wedstrijd…) |
| 3 | luhansk | 20 | 16 | **80%** | RU side 7/10 Spanish/Vietnamese leakage + comment-handle debris (rusos, hoy, vov, saggy); UA side war-generic boilerplate |
| 3 | bakhmut | 10 | 8 | **80%** | UA-only list is entirely ukraine/russia family + war generics (war, forces, news, near, city) |

Next: kyiv 75% (RU side 9/10 non-English + template echo), donbas 70% (RU side 8/10 French
openalex leakage). Cleanest: serhii-korolyov 10%, chornobyl 12%, ihor-sikorsky 20%,
chicken-kyiv 20%.

### (a) Consolidated stopword additions (exact lists, four tiers)

**Tier 1 — STOP gaps in `pipeline/stats/keyness.py` (function words/contractions that produced
shipped chips):**
`him`, `never`, `every`, `always`, `something`, `didn't` (+ curly variant `didn’t`), `often`,
`big`, `near`, `books`, `million`, `army`, `home`, `live`, `local`, `world`, `europe`.
Rounding out the same families so they never recur: `doesn't`, `wasn't`, `weren't`, `isn't`,
`aren't`, `won't`, `can't`, `couldn't`, `wouldn't`, `shouldn't`, `anything`, `everything`,
`nothing`, `someone`, `everyone`.

**Tier 2 — display-tier generics for the export `_junk` list in `pipeline/export_site_data.py`
(real words, zero pair information; they occupy 40+ chips across the nine war pairs):**
`war`, `forces`, `military`, `news`, `city`, `region`, `national`, `united`, `president`,
`company`, `club`, `round`, `group`, `cup`, `equipment`, `comments`, `posted`.
(Leave them in the underlying keyness stats; suppress at display only.)

**Tier 3 — platform / markup / outlet tokens (extend the existing outlet stoplist and `_junk`):**
`sprite`, `bar-`, `icon-sub` (better: strip the markup — see (c)), `soccerbot`,
`boardgamegeek`, `bgg`, `wotreplays`, `yourmegafile`, `webrip`, `auto-generated`, `amuseio`,
`background-image`, `linear-gradient`, `background-position`, `wikipedia`, `hmu`,
`ukrinform`, `dezeen`, `suspilne`, `kyivindependent`, `babynyar`.
(`ukrinform`/`dezeen`/`suspilne`/`kyivindependent` are outlet names — the STOP list already
bans reuters/afp/bbc/unian etc.; this is the same policy applied consistently. `babynyar` and
`kyivindependent` are URL fragments.)

**Tier 4 — non-English function words that leaked past `NON_EN_STOP` (all produced shipped
chips):**
`que`, `qui`, `pas`, `une`, `contre`, `russes`, `russie`, `ukrainien`, `militaire`,
`l'ukraine`, `sono`, `ucrania`, `ukrayna`, `ancak`, `izle`, `canli`, `canl`, `een`, `tegen`,
`niet`, `voor`, `wedstrijd`, `ich`, `cho`, `hoy`, `desde`, `rusos`, `rusas`, `vov`, `mai`.
**Root fix, not whack-a-mole:** a document-level language-ID gate (English-only) before
keyness/clustering. The tokenizer's accented-word drop demonstrably does not catch
ASCII-spelled French/Spanish/Dutch/Turkish (`que`, `qui`, `pas`, `voor`, `izle`…), and the
Turkish dotless-ı splits `canlı` into the fragment `canl`. This is the same English-filter
lesson already proven for GDELT translations.

### (b) Family-merge groups seen across pairs (display-time lemma merge)

Morphological families occupying multiple chips:
- `ukraine · ukrainian` — bakhmut, donbas, kharkiv, kyiv, luhansk, lviv, ternopil, varenyky,
  volodymyr-the-great, volodymyr-zelenskyy (10 pairs)
- `russia · russian` — bakhmut, donbas, kharkiv, kyiv, kyivan-rus, luhansk, lviv,
  volodymyr-the-great, volodymyr-zelenskyy (9 pairs)
- `viking · vikings` — kyivan-rus (also its cluster label `vikings · viking`)
- `german · germans` — dnipro-river
- `drone · drones` — dnipro-river
- `helicopter · helicopters` — ihor-sikorsky
- `artist · artists` and `painting · paintings` — kazymyr-malevych
- `cook · cooking` — chicken-kyiv
- `theatre · theater` — mykola-hohol (cluster label)
- `crimea · crimean` — feodosiia (cluster label); `russian · russia` — borscht (cluster label)

Same-referent name splits (merge as one chip or drop the weaker half):
- `tyson` + `fury` and `anthony` + `joshua` — oleksandr-usyk (one person each)
- `kievan` + `rus` — volodymyr-the-great (one phrase)
- `vladimir` + `great` — volodymyr-the-great (halves of the pair's own RU form; see self-echo)

### (c) Per-pair filters needed (root-cause fixes, verified against records.parquet)

1. **borscht — surname referent filter.** 231 GDELT docs (100% of them variant=russian) quote
   **Matthew Borsch, Goldman Sachs managed-care analyst**; the word-boundary match on `borsch`
   catches his surname, and ALL TEN shipped RU chips (morgan, penetration, taylor, model,
   premium, justin, deutsche, wolfe, cash, piper) are US health-insurance earnings-call
   vocabulary. Drop docs matching `Matthew Borsch` / `Borsch` within a window of
   analyst|Goldman|Sachs|research note. Also: porn-spam density filter (the 2,141-text
   `xxx · sex` cluster), `yourmegafile`/`webrip` piracy templates (807 texts, 98.4% "ua" —
   inflates UA share), and lingzhi/cordyceps MLM spam (403 texts). Keep Street Fighter's
   "Borscht Dynamite" (Zangief move, 16 docs) — genuine, and a good register nugget.
2. **lviv — person referent filter (RU side).** 158 of 5,082 RU-side docs mention **Prince
   Lvov** (1917); the entire shipped RU chip list (trotsky, mensheviks, srs, karenina,
   bolshevism, marxism, rsdlp, rasputin) is Russian-revolution/Tolstoy prose where "Lvov" is a
   person (Prince Georgy Lvov; the Lvov character in Anna Karenina), not the city. Filter
   `prince (georg[iy]\w* )?lvov`, `lvov government`, provisional-government contexts, and the
   Karenina character.
3. **odesa — the tiered US-homonym fix already designed** (finding_odesa_us_reservoir): Texas/
   Missouri/Florida city contexts (crappie/bluegill fishing, school-district and college
   sports, police blotters — e.g. the `police · crousore` cluster is Odessa, MO, incl. a
   person named "Odessa Maughan"), porn/personals spam (`anal · tits`, `looking · host`
   clusters), Gen V/The Boys fandom (`marie · alt` cluster: godolkin/homelander), and
   greyhound/horse racing names ("Magic Sprite"). The RU chip list is 10/10 noise until this
   lands; the UA list is already 10/10 signal.
4. **Reddit r/soccer markup strip (dynamo-kyiv, luhansk, lviv, kyiv, odesa, ternopil).** Match
   threads embed `[](#sprite1-p8)`, `(#bar-2-green)`, `(#icon-…)` flair markup; this produced
   the `sprite · league` / `sprite · bar-` / `sprite · posted` cluster labels in FIVE pairs
   and lviv's cluster-7 term list. Strip `\[\]\(#[\w-]+\)` and `\(#[\w-]+\)` before
   tokenising/clustering. (luhansk records show 81 such texts; `\bsprite\b` alone misses
   `sprite1`.)
5. **Inline-CSS strip (odesa, any scraped HTML).** `background-image`, `linear-gradient`,
   `background-position`, `deg` appear as cluster label material (`ukraine ·
   background-image`, 384 texts). HTML-to-text cleaner gap.
6. **Template/near-duplicate collapse before keyness.** kyiv's `jamiroquai` chip (73 docs, 68
   RU) is one producer-bio/lineup template echoed; ihor-sikorsky's example store returned the
   same Rensselaer bio twice; the BGG "RECOMMENDATIONS FOR [user]" bot template produced
   `boardgamegeek`/`boardgame` chips in kyiv and kharkiv (kharkiv's `boardgame` is literally
   the URL query param `subtype=boardgame`). Near-dup collapse (minhash or exact-prefix) fixes
   all three.
7. **feodosiia — booking-spam template filter.** The `country · guest` cluster (91 of 472
   texts) is cheap-hotels.club / accomodation.today "Book now" templates.
8. **serhii-korolyov — namesake musician filter.** The `composer · youtube` cluster (37
   texts) is auto-generated topic channels (amuse.io) for a musician Korolyov, not the
   designer.
9. **luhansk — decide the SEC-prospectus register.** `company · shares` cluster (1,431 texts)
   is securities-filing boilerplate (Ivanhoe Electric etc.) that names Luhansk in sanctions
   risk-factors; it also drove the UA chip `company`. Genuine usage, distinct register —
   either label it honestly ("sanctions boilerplate") or cap its keyness contribution.
10. **Solo/phrase pairs — mask the variant's individual words, not just the full phrase.**
    The phrase-only mask leaks the pair's own label back as its top "collocations":
    `sikorsky`, `malevich`, `korolev`, `gogol`, `nikolai`, `usyk`, `zelenskyy`, `volodymyr`,
    `vladimir`, `great`, `dnipro`, `dnieper`, `kyiv` (dynamo-kyiv) all shipped as chips.
    Nine self-echo chips would disappear by masking each word of each variant (word-boundary,
    per word, when the term is multi-word).
11. **chornobyl — keep the hvh/CS:GO texts** (cheat configs literally named `chernobyl.lua`
    on neverlose.cc/onetap — genuine RU-form usage, 130 YouTube docs) but fix the
    cluster-merge label bug so `hvh · youtube` can never name the 7,246-text mainstream
    disaster cluster (see systemic defects below).

### Systemic cluster-label defects (beyond single pairs)

1. **Shortest-label-wins on gloss-merge is backwards.** When two clusters share a gloss and
   spelling regime they merge and keep the SHORTER label; junk labels are short. Result:
   chornobyl's main cluster (7,246 texts, overwhelmingly mainstream 1986-disaster coverage)
   carries hover-label `hvh · youtube` inherited from a 470-text CS:GO-cheat cluster. Keep the
   label of the LARGER constituent instead.
2. **No stopword/markup guard on cluster top_terms.** Labels are built from raw c-TF-IDF
   terms, so `sprite`, `bar-`, `background-image`, `it's`, `i'm`, `comments` reach labels; the
   keyness `_junk` list (tractorlinks, sundayschool…) is NOT applied to cluster labels, so
   feodosiia ships `sundayschool · tractorlinks` — a token combination the export code
   already knows is junk.
3. **`non-English` bucket hides real registers.** dynamo-kyiv cluster 7 (402 texts, 100% UA)
   is CyberLiveArena esports simulations — a genuinely UA-spelling register — flattened to
   "non-English coverage".
4. **Gloss keyword rules mis-fire on gaming clusters.** kyivan-rus `paradox · ukraine`
   (Crusader Kings/Roblox, 510 texts) and `game · civs` (Civilization/AoE, 332 texts) both
   gloss as "Kyivan Rus history" because their term lists contain kievan+rus. Games deserve
   their own gloss rule (`paradox`, `crusader`, `civ`, `roblox`, `evony` already exists for
   volodymyr pairs).
5. **Family-dup labels**: `russian · russia` (borscht), `vikings · viking` (kyivan-rus),
   `crimea · crimean` (feodosiia), `theatre · theater` (mykola-hohol) — the two label slots
   say one thing.
6. **Porn labels ship to readers**: odesa `anal · tits` and borscht `xxx · sex` are visible
   legend text on a site with policy audiences. Even before referent filtering, junk-cluster
   glosses should fall back to a neutral "off-topic/spam" name, not raw terms.

### Systemic tooltip (ex.t) defects

1. **Wrong-source examples.** The example picker searches `variant == side` in parquet order
   (GDELT first) with no source constraint, so chips scored on Reddit+YouTube quote GDELT
   prose: ALL TEN bakhmut chips say "Distinctive in: Reddit, YouTube" then quote GDELT.
2. **Unrepresentative first-match examples** (verified counts): `stray` (mykola-hohol) is
   Bungo Stray Dogs anime (1,476 docs) but the example is a Tamil theatre company "Stray
   Factory"; `polytechnic` (ihor-sikorsky) is 589/613 Kyiv Polytechnic but the example is
   Rensselaer; odesa `pussy`/`sex` examples are sanitized gdelt art-reviews while the driver
   is the porn-spam cluster; kyivan-rus `union` example shows European Union for a
   Soviet-Union signal; odesa `sprite` example is a racing-greyhound name.
3. **Fallback can quote the OPPOSITE spelling's document** when the side has no match in the
   stratified sample — the "verbatim example from this pair's records" can be a document that
   uses the other variant.
4. **Mojibake ships**: kyiv `qui` example renders "s'arrÃªte Ã 20 mm … Ã©tÃ© effectuÃ©"
   (double-encoded UTF-8) in the visible tooltip.
5. **Wrong caption for `src=[]` chips on two-sided pairs.** `_prov` only finds sources whose
   per-source top-25 contains the word, so robust-but-truncated terms get `src=[]`; the site
   then shows the SOLO caption "Distinctive of the dominant form, measured against the
   cross-pair background" — factually wrong for two-sided pairs. Affects ~40 chips including
   all 10 borscht RU chips.
6. **Mid-word truncation at snippet edges by construction** (45 chars before / 60 after):
   "…re in Nazi Germany", "…ween Presidents". Cosmetic; trim to word boundaries.

---

## PER-PAIR REVIEW

(Sections follow in slug order. Every displayed chip is listed with class and verdict;
tooltip and cluster-label notes follow each table.)

### babyn-yar — "Babyn Yar" vs "Babi Yar" (gdelt, openalex, reddit, youtube)

UA chips (7 S / 3 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| supervisory | 3.6 | S | keep | BYHMC Supervisory Board — memorial-institution governance rides the UA spelling |
| sharansky | 3.1 | S | keep | Natan Sharansky chaired BYHMC |
| fuks | 2.8 | S | keep | Pavel Fuks, BYHMC funder |
| magocsi | 2.8 | S | keep | Magocsi co-edited *Babyn Yar: History and Memory* — academic UA usage |
| odesa | 2.8 | S | keep | UA-form bundle: "Babyn Yar" texts also write "Odesa" |
| babynyar | 2.7 | A | drop | URL fragment (babynyar.org) + concatenated self-echo of the pair term |
| ukrinform | 2.6 | A | drop | outlet name / wire attribution — same policy as reuters/unian in STOP |
| dezeen | 2.5 | A | drop | outlet self-reference boilerplate ("Read more Dezeen comments") |
| kiyanovska | 2.5 | S | keep | Marianna Kiyanovska, *The Voices of Babyn Yar* |
| zelenskiy | 2.4 | S | keep | name-form bundle: Reuters-style "Zelenskiy" travels with the UA place form |

RU chips (6 S / 3 G / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| hitler | 14.6 | S | keep | Holocaust-history register owns "Babi Yar" |
| kiev | 12.7 | S | keep | RU-form bundle |
| symphony | 10.9 | S | keep | Shostakovich's 13th ("Babi Yar") — the pair's strongest register story |
| shostakovich | 10.3 | S | keep | same |
| army | 10.2 | G | drop | generic; its example is a Danish war-film review |
| million | 9.9 | G | drop | number-word (museum-cost prose) |
| einsatzgruppen | 9.6 | S | keep | WWII historiography |
| nazis | 9.2 | S | keep | same register |
| him | 9.2 | G | drop | pronoun — STOP has his/her but not him |
| comments | 9.0 | A | drop | platform word (Reddit thread mechanics) |

Tooltips: `odesa`'s example is an unrelated pension story (contains the term, tells the reader
nothing); `army`/`him` examples expose them as noise (film-review prose). Seven UA chips have
`src=[]` and show the wrong "measured against the cross-pair background" caption. Left-edge
mid-word truncation throughout ("…re in Nazi Germany", "…ween Presidents").

Clusters: all four sane. `symphony · shostakovich` (366) is exactly what a discourse label
should be; `ukraine · jewish` (4,036, gloss "war & news coverage") fine; `war · jews` (231)
awkward phrasing but honest; `ukraine · israel` (97, peak 2019) fine. No junk labels.

### bakhmut — "Bakhmut" vs "Artemovsk" (reddit, youtube; RU chip list empty)

UA chips (2 S-weak / 2 F / 6 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| ukraine | 95.7 | S | keep (weak) | Bakhmut-spelling texts are mainstream war news; "Artemovsk" sits in the pro-RU sphere — this chip carries that weakly |
| russian | 79.9 | S | keep (weak) | same contrast |
| ukrainian | 74.1 | F | merge → ukraine | morphological sibling |
| russia | 62.1 | F | merge → russian | sibling |
| war | 57.5 | G | drop | war-news boilerplate, appears on every war pair |
| forces | 56.2 | G | drop | same |
| news | 50.1 | G | drop | same |
| near | 43.0 | G | drop | function-ish word ("near Bakhmut") |
| military | 42.5 | G | drop | boilerplate |
| city | 41.5 | G | drop | boilerplate |

Verdict on the pair: the displayed list says nothing Bakhmut-specific. The interesting
contrast (what vocabulary keeps "Artemovsk" alive — LPR/DPR-sphere talk) is exactly the side
that shipped empty: gdelt has only 4 Artemovsk docs, so the robust two-source rule starves the
RU list while reddit+youtube generics flood the UA list.

Tooltips: all ten chips read "Distinctive in: Reddit, YouTube" and then quote GDELT prose —
the wrong-source defect at 100% on this pair (mining-prospectus and OSCE-report snippets that
had nothing to do with the score).

Clusters: `russian · ukraine` (12,865) + `city · situation` (820 — daily "Bakhmut direction"
update channels, genuine). Labels generic but honest; nothing junk.

### borscht — "Borscht" vs "Borsch" (gdelt, openalex, reddit) — worst pair on the site

UA chips (2 S / 8 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| him | 34.6 | G | drop | pronoun, STOP gap |
| never | 23.8 | G | drop | STOP gap |
| soup | 23.5 | S | keep | culinary register |
| every | 22.8 | G | drop | STOP gap |
| didn't | 21.9 | G | drop | contraction — `_junk` has don't but not didn't |
| something | 21.5 | G | drop | STOP gap |
| jewish | 20.6 | S | keep | Borscht Belt / Jewish-food register — the pair's real UA story |
| trump | 20.4 | G | drop | venue prose ("headlined at Trump Plaza" — Jackie Mason), reads as politics, isn't |
| always | 19.2 | G | drop | STOP gap |
| music | 19.2 | G | drop | Catskills-entertainment residue, generic |

RU chips (10 A — the single worst list shipped):

| term | z | class | verdict | why |
|---|---|---|---|---|
| morgan | 10.9 | A | drop via filter | **Matthew Borsch, Goldman Sachs managed-care analyst** — 231 GDELT docs (all variant=russian) quote him in US health-insurance earnings coverage; `\bborsch\b` word-boundary match catches the surname |
| penetration | 9.0 | A | drop via filter | same reservoir ("small group penetration") |
| taylor | 8.9 | A | drop via filter | same reservoir (entertainment/finance wire prose) |
| model | 7.2 | A | drop via filter | same |
| premium | 6.6 | A | drop via filter | same ("23 percent premium to acquire Humana") |
| justin | 6.6 | A | drop via filter | same |
| deutsche | 6.1 | A | drop via filter | same (Deutsche Bank analyst Q&A) |
| wolfe | 6.1 | A | drop via filter | same (Wolfe Research) |
| cash | 6.1 | A | drop via filter | same |
| piper | 6.0 | A | drop via filter | same (Piper Jaffray) |

Not one RU chip is about the soup. The fix is the referent filter in consolidated (c)1, not
stopwords. All ten have `src=[]` → wrong solo caption in the tooltip as well.

Clusters (12): junk visible labels — `xxx · sex` (2,141 texts: porn spam, and it ships as
legend text), `post · yourmegafile` (807, 98.4% "ua" — piracy templates that inflate the UA
share), `babushka · lingzhi` (403 — mushroom-coffee MLM spam), `adjustment · attack` (224 —
actually Street Fighter V Zangief patch notes: "Heavy Borscht Dynamite"; genuine usage, opaque
label — relabel "Street Fighter (Borscht Dynamite)"), family-dup `russian · russia`. Good:
`soup · beets`, `catskills · belt`, `comedy · brooks`, `film · music`, `ukraine · ukrainian`.

### chicken-kyiv — "Chicken Kyiv" vs "Chicken Kiev" (gdelt, reddit, youtube; UA list empty)

RU chips (8 S / 1 F / 1 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| chicken | 64.3 | S | keep (note) | standalone "chicken" outside the masked phrase — borderline self-echo, but it is the cooking register |
| recipe | 25.8 | S | keep | the dish stays "Kiev" in recipes — the pair's finding, visible in a chip |
| food | 22.9 | S | keep | same register |
| butter | 19.9 | S | keep | the dish's defining ingredient |
| garlic | 18.8 | S | keep | same |
| cooking | 18.6 | S | keep | same |
| breast | 16.0 | S | keep | same |
| cook | 14.6 | F | merge → cooking | morphological sibling |
| cheese | 14.2 | S | keep | supermarket-product register (Cub Foods outbreak texts) |
| home | 14.1 | G | drop | generic; example is boarding-house prose |

UA side displays "no distinctive vocabulary — usage too scattered": honest.

Tooltips: fine on the whole; `chicken`'s example (Antioch Farms salmonella outbreak) is
genuinely the register. `home`'s example is irrelevant.

Clusters (9 shipped 8): `recipe · food` (2,945, ua_pct 5.6 — the dish keeps the old spelling;
this is the site's argument in one number). `ref · keys` (305) is **Team Fortress 2 item
trading** — "Strange"/"killstreak"/"haunted" Chicken Kiev cosmetic traded for keys/refined
metal; genuine register, opaque label — relabel "TF2 item trading". `play · let's` (300)
let's-play/TikTok gaming, fine. Cooking/war tiered glosses fine.

### chornobyl — "Chornobyl" vs "Chernobyl" (gdelt, openalex, reddit, youtube) — cleanest big pair

UA chips (5 S / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| heartofchornobyl | 6.4 | S | keep | STALKER 2's official hashtag — the game put the UA spelling into circulation |
| xess | 6.1 | S | keep | upscaling tech talk around STALKER 2 |
| dlss | 6.0 | S | keep | same |
| fsr | 4.8 | S | keep | same |
| gamescom | 2.7 | S | keep | games-industry register |
| kyivindependent | 2.0 | A | drop | URL fragment from Reddit donation boilerplate (19 docs) |

RU chips (9 S / 1 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| nuclear | 109.6 | S | keep | the 1986-disaster register owns the RU spelling |
| power | 76.0 | S | keep | "nuclear power" |
| radiation | 73.5 | S | keep | same |
| disaster | 71.1 | S | keep | same |
| plant | 67.7 | S | keep | same |
| world | 64.7 | G | drop | generic |
| ukraine | 64.6 | S | keep (weak) | geographic/news frame vs the UA side's gaming frame |
| zone | 60.5 | S | keep | exclusion zone |
| reactor | 60.3 | S | keep | same |
| accident | 54.3 | S | keep | same |

Tooltips: sane; `heartofchornobyl`'s example is a hashtag block (spammy-looking but honestly
what the data is).

Clusters: one serious defect — the biggest displayed cluster carries hover-label
`hvh · youtube` (7,246 texts). That label belongs to a 470-text CS:GO cheat niche (configs
literally named `chernobyl.lua` on neverlose.cc/onetap; verified 130 YouTube docs); the
gloss-merge folded it into the 6,776-text mainstream disaster cluster and the
shortest-label-wins rule kept the junk title. Gloss text ("the 1986 disaster") is right;
the label must take the larger constituent's. `comments · world` (2,272) is a junk label
(platform words; raw top_terms even contain "it's"/"i'm"). The rest are excellent:
`stalker · heart` (2024 game, 98.5% UA — the cleanest register flip on the site),
`game · stalker` (2007 game), `hbo · series`, `beat · type` ("Chernobyl type beat" music),
`vudu · itunes` storefronts, `ukraine · nuclear` war coverage.

### dnipro-river — "Dnipro River" vs "Dnieper River" (gdelt, reddit, youtube)

UA chips (6 S / 1 F / 2 G / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| russian | 32.9 | S | keep (weak) | war-news frame around the UA name |
| ukrainian | 26.0 | S | keep (weak) | family head (no "ukraine" chip here) |
| forces | 22.4 | G | drop | war boilerplate |
| kherson | 21.7 | S | keep | the river's war geography — genuinely pair-specific |
| news | 19.1 | G | drop | boilerplate |
| drones | 18.1 | S | keep | war-tech register of the UA name |
| oblast | 18.1 | S | keep | UA administrative term travelling with the UA spelling — a real marker |
| dnipro | 17.9 | A | fix (mask) | self-echo: phrase mask covers "Dnipro River" only, standalone "Dnipro" leaks back |
| drone | 17.7 | F | merge → drones | sibling |
| avdiivka | 17.2 | S | keep | UA-form toponym bundle |

RU chips (6 S / 1 F / 1 G / 2 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| kiev | 16.8 | S | keep | RU-form bundle |
| dnieper | 15.8 | A | fix (mask) | self-echo: standalone "Dnieper" through the phrase-only mask |
| soviet | 12.3 | S | keep | nostalgia/history register of the old name |
| wikipedia | 9.9 | A | drop | platform/URL token |
| german | 9.6 | S | keep | WWII register (the Dnieper line) |
| empire | 8.5 | S | keep | imperial-history register (example is junk — an Islam rant; replace) |
| europe | 7.7 | G | drop | generic |
| germans | 6.9 | F | merge → german | sibling |
| panzer | 6.6 | S | keep | WWII register — "Dnieper" lives in military history |
| smolensk | 6.5 | S | keep | upper-Dnieper geography, RU bundle |

Tooltips: `empire`'s example is an incoherent Islam-empire rant — swap for a Russian-Empire
snippet; `wikipedia`'s is a bare URL. Others fine.

Clusters: `ukraine · russia` (13,821) war coverage; `fishing · planet` (338, 95.6% UA) is the
**Fishing Planet** video game — a Ukrainian-made fishing sim with an official Dnipro River
level; label is accurate and the cluster is genuine signal (Ukrainian devs → UA spelling).
No junk.

### donbas — "Donbas" vs "Donbass" (gdelt, openalex, reddit, youtube)

UA chips (4 S / 2 F / 4 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| ukraine | 48.2 | S | keep (weak) | family head |
| russia | 41.0 | S | keep (weak) | family head |
| russian | 41.0 | F | merge → russia | sibling |
| ukrainian | 39.9 | F | merge → ukraine | sibling |
| kyiv | 26.3 | S | keep | UA-form bundle |
| war | 25.4 | G | drop | boilerplate |
| forces | 24.5 | G | drop | boilerplate |
| putin | 23.5 | S | keep (weak) | war-news actor |
| president | 21.4 | G | drop | boilerplate |
| military | 21.1 | G | drop | boilerplate |

RU chips (2 S / 8 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| kiev | 16.2 | S | keep | RU-form bundle ("Kiev government" framing) |
| russie | 15.8 | A | drop (lang gate) | French — the "Donbass" corpus is heavy with French openalex/reddit texts; measures French convention, not English |
| pas | 12.2 | A | drop (lang gate) | French function word |
| qui | 11.4 | A | drop (lang gate) | French function word |
| russes | 10.3 | A | drop (lang gate) | French |
| contre | 10.2 | A | drop (lang gate) | French |
| l'ukraine | 10.1 | A | drop (lang gate) | French |
| ukrainien | 10.1 | A | drop (lang gate) | French |
| lugansk | 9.8 | S | keep | RU-form bundle — doubled-s texts also write "Lugansk" |
| militaire | 9.8 | A | drop (lang gate) | French |

This is the translation-filter finding reproduced inside one chip list: the "Donbass" side is
substantially *French discourse*, so its "distinctive vocabulary" is French. The two real
chips (kiev, lugansk) are the RU-forms-travel-together bundle.

Tooltips: French chips quote French sentences on an English-language site (wrong-language
examples by construction).

Clusters: single cluster `ukraine · ukrainian` (12,936 — the whole corpus). A one-cluster map
tells the reader nothing; either suppress the panel or force k>=3 with junk-guarded labels.

### dynamo-kyiv — "Dynamo Kyiv" vs "Dynamo Kiev" (gdelt, reddit, youtube)

UA chips (4 S / 5 G / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| uefa | 33.0 | S | keep | continental-football register |
| europa | 22.1 | S | keep | Europa League |
| liga | 19.9 | S | keep | football register |
| kyiv | 19.7 | A | fix (mask) | self-echo: standalone "Kyiv" through the "Dynamo Kyiv" phrase mask |
| club | 19.6 | G | drop | sports boilerplate |
| round | 18.4 | G | drop | boilerplate |
| group | 17.7 | G | drop | boilerplate |
| cup | 17.5 | G | drop | boilerplate |
| local | 17.3 | G | drop | generic (fan-comment prose) |
| shakhtar | 16.4 | S | keep | Ukrainian-football bundle |

RU chips (1 G / 9 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| aguero | 5.8 | G | drop | one-off name from 2015-16 Man City ties; no register |
| canl | 5.4 | A | drop (lang gate) | Turkish "canlı" (live) with dotless-ı split into a fragment — pirate-stream spam ("canlı maç izle") |
| een | 4.1 | A | drop (lang gate) | Dutch (Eredivisie threads) |
| izle | 3.9 | A | drop (lang gate) | Turkish "watch" — stream-spam |
| tegen | 3.9 | A | drop (lang gate) | Dutch |
| mon | 3.8 | A | drop | lower-cased acronym MON (Nigerian honour, Brown Ideye prose) |
| canli | 3.4 | A | drop (lang gate) | ASCII-ised Turkish |
| niet | 3.1 | A | drop (lang gate) | Dutch |
| wedstrijd | 2.9 | A | drop (lang gate) | Dutch "match" |
| voor | 2.9 | A | drop (lang gate) | Dutch |

The RU list is 90% pirate-stream/Eredivisie leakage; the true story (wire-service inertia on
"Dynamo Kiev") never surfaces. Language gate is the fix.

Tooltips: Turkish/Dutch chips quote Turkish/Dutch spam links (taraftarium/macizle URLs) — the
site displays link-farm text as evidence.

Clusters (11): `sprite · league` (1,240) and `sprite · posted` (395) are **Reddit r/soccer
markup** (`[](#sprite1-p8)`, `(#bar-…)` flair codes — verified in records); `sprite · posted`
ships as visible legend text. Six of eleven labels lean on `league` (league · live, sprite ·
league, chelsea · league, league · group, league · team, league · barcelona) — near-duplicate
labels a reader cannot tell apart. Two `non-English` clusters, one of which (402 texts, 100%
UA) is actually **CyberLiveArena esports simulations** — a genuine UA-spelling register hidden
by the bucket. `chelsea · league`, `league · barcelona` etc. are fine as football coverage.

### feodosiia — "Feodosiia" vs "Feodosiya" (clusters only; no keyness shipped)

No chips to review (keyness thresholds not met — corpus 472 texts). Clusters:

| label | size | verdict | why |
|---|---|---|---|
| russian · ukraine (war gloss) | 192 | keep | genuine post-2022 coverage |
| crimea · crimean | 102 | keep, merge label family | crimean is a sibling; also contains openalex spider-taxonomy leakage (`haplodrassus`) |
| country · guest | 91 | drop cluster (filter) | cheap-hotels.club / accomodation.today booking-spam templates ("Book now") |
| sundayschool · tractorlinks | 68 | drop cluster (filter) | known-junk tokens (already in the keyness `_junk` list, but cluster labels bypass it) |
| moonbeam · club | 19 | keep (opaque) | genuine: DJ duo Moonbeam live at Beach Club 117, Feodosiya 2010 — pre-annexation resort culture; below any sensible display floor though |

19-text clusters shouldn't ship at all; a size floor (e.g. >=5% of corpus) would drop
`moonbeam · club` and make room for honest labels on the rest.

### ihor-sikorsky — solo "Igor Sikorsky" (RU form dominant; measured vs cross-pair background)

Solo chips (8 S / 1 F / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| helicopter | 35.2 | S | keep | the register that froze the RU form: American aviation |
| sikorsky | 26.6 | A | fix (mask) | self-echo: surname through the phrase-only mask |
| helicopters | 20.6 | F | merge → helicopter | sibling |
| aircraft | 20.5 | S | keep | aviation register |
| flight | 17.2 | S | keep | same |
| rotor | 16.6 | S | keep | same |
| polytechnic | 13.6 | S | keep | **Igor Sikorsky Kyiv Polytechnic Institute** — 589 of 613 polytechnic docs are KPI; the institution's frozen "Igor" name is a documented finding (institution lags practice) |
| connecticut | 13.4 | S | keep | Sikorsky Aircraft, Stratford CT — corporate register |
| vertical | 10.9 | S | keep | "vertical flight" |
| pilot | 10.3 | S | keep | aviation register |

Tooltips: `polytechnic`'s example is one of the FOUR Rensselaer docs instead of the 589 KPI
docs — the single most unrepresentative example on the site; the duplicate Rensselaer bio also
shows the corpus carries exact-dup templates. Fix the example picker (prefer majority context)
and near-dup collapse.

Clusters: all sane — `helicopter · aircraft` (1,094), `ukraine · university` (447 = KPI),
war coverage (185), `connecticut · state` (69), `aircraft · russian` (37, Ilya Muromets
bomber history). Below-floor sizes on the last two, but labels are honest.

### kazymyr-malevych — solo "Kazimir Malevich" (RU form dominant)

Solo chips (7 S / 2 F / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| art | 63.3 | S | keep | the art-world register that catalogues him under the RU form |
| painting | 31.9 | S | keep | same |
| artist | 31.3 | S | keep | same |
| artists | 30.9 | F | merge → artist | sibling |
| exhibition | 30.2 | S | keep | museum register |
| works | 28.7 | S | keep | no singular sibling present |
| paintings | 28.6 | F | merge → painting | sibling |
| malevich | 27.1 | A | fix (mask) | self-echo: surname through the phrase-only mask |
| abstract | 24.4 | S | keep | suprematism/abstraction |
| gallery | 22.5 | S | keep | museum register |

Tooltips: fine (Tretyakov/Costakis prose is exactly the register).

Clusters: all sane and specific — `art · work` (2,096), `black · square` (247),
`ukraine · ukrainian` war-reclamation coverage (94), `hadid · architecture` (77 — Zaha
Hadid's Malevich-inspired work, genuine), `million · art` (56, auctions). Raw top_terms
contain the debris token `square'` (apostrophe fragment) — harmless here but shows the label
tokenizer lacks the keyness tokenizer's apostrophe handling.

### kharkiv — "Kharkiv" vs "Kharkov" (gdelt, openalex, reddit, youtube)

UA chips (2 S / 2 F / 6 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| ukraine | 115.1 | S | keep (weak) | family head; war-news frame |
| russian | 102.9 | S | keep (weak) | family head |
| ukrainian | 82.4 | F | merge → ukraine | sibling |
| russia | 74.8 | F | merge → russian | sibling |
| city | 59.0 | G | drop | boilerplate |
| forces | 57.5 | G | drop | boilerplate |
| war | 56.0 | G | drop | boilerplate |
| region | 55.2 | G | drop | boilerplate |
| military | 49.0 | G | drop | boilerplate |
| news | 46.8 | G | drop | boilerplate |

RU chips (6 S / 4 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| boardgamegeek | 8.0 | A | drop | BGG recommendation-bot link template (platform token) |
| boardgame | 8.0 | A | drop | literally the URL query param `subtype=boardgame` in the same bot links |
| battlegroup | 7.9 | S | keep | WWII military-history/gaming register — where "Kharkov" survives |
| wot | 7.8 | S | keep | World of Tanks "Kharkov" map — the strongest single register chip on this side |
| bgg | 6.1 | A | drop | site abbreviation |
| wotreplays | 5.8 | A | drop | site name |
| cod | 5.7 | S | keep | Call of Duty WWII register |
| amx | 5.4 | S | keep | WoT tank talk |
| worldoftanks | 5.4 | S | keep (note) | subreddit-shaped token, but it names the game that keeps "Kharkov" alive |
| dlc | 5.2 | S | keep | gaming register |

The RU story here is real and important — "Kharkov" lives on in WWII wargaming — but four of
ten chips are the platform's name rather than the register.

Tooltips: `battlegroup`'s example is an unrelated r/MilitaryPorn caption (Helmand 2007);
`wot`'s example ("Kharkov is Coming to WoT") is perfect. Bot-template examples on the BGG
chips expose the template-echo cause.

Clusters: good set — war coverage (9,241), `ukraine · university` (502; MBBS students),
`ukraine · music` (470), `battle · german` (309, WWII incl. Graviteam tactics games),
`league · match` (288, Metalist football), war-coverage old-spelling tier (375; raw terms
include `toastmasters`/`bonomo` — small mixed bag). No junk visible labels.

### kyiv — "Kyiv" vs "Kiev" (gdelt, openalex, reddit, youtube)

UA chips (4 S / 2 F / 4 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| ukraine | 184.1 | S | keep (weak) | family head |
| russian | 125.0 | S | keep (weak) | family head |
| ukrainian | 121.8 | F | merge → ukraine | sibling |
| russia | 121.4 | F | merge → russian | sibling |
| war | 93.9 | G | drop | boilerplate |
| military | 73.2 | G | drop | boilerplate |
| zelensky | 70.9 | S | keep | name-form bundle around the UA spelling |
| missiles | 70.6 | S | keep | war-tech register (strike coverage) |
| news | 69.4 | G | drop | boilerplate |
| city | 68.9 | G | drop | boilerplate |

RU chips (1 S / 9 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| que | 24.7 | A | drop (lang gate) | Spanish/French — plain `que` is NOT in NON_EN_STOP (only accented `qué` is) |
| dota | 19.2 | S | keep | the 2017 **Kiev Major** esports register — genuine and pair-specific |
| boardgamegeek | 18.6 | A | drop | BGG bot template (same as kharkiv) |
| boardgame | 18.4 | A | drop | same template |
| ucrania | 14.5 | A | drop (lang gate) | Spanish |
| une | 13.1 | A | drop (lang gate) | French |
| ukrayna | 11.2 | A | drop (lang gate) | Turkish |
| sono | 11.2 | A | drop (lang gate) | Italian |
| qui | 11.1 | A | drop (lang gate) | French; example ships MOJIBAKE ("s'arrÃªte Ã 20 mm… Ã©tÃ©") |
| jamiroquai | 10.9 | A | drop (dedupe) | one producer-bio/lineup template echoed across 73 docs (68 RU) |

The flagship pair's RU side is 9/10 junk — non-English leakage plus two template echoes. The
one keeper (`dota`) is genuinely good.

Tooltips: the `qui` example is double-encoded UTF-8 shown verbatim — the single ugliest
tooltip on the site. `sono` quotes Italian, `ucrania`/`ukrayna` quote openalex titles.

Clusters: `ukraine · russian` (9,989) war; `ukraine · city` (936) travel/civic; `ukraine ·
chicken` (847) cross-talk from the chicken-kyiv reservoir (fine — glossed cooking);
`sprite · league` (485) Reddit-markup football (junk label, gloss "football coverage" saves
the visible row); `camera · ukraine` (299) is the Soviet **Kiev camera brand** (kiev/camera/
film/lens/shutter in raw terms) — genuine register, would deserve its own gloss ("Kiev
cameras"); `ukraine · children` (264) humanitarian/medical. Solid set apart from the sprite
label.

### kyivan-rus — "Kyivan Rus" vs "Kievan Rus" (gdelt, openalex, reddit, youtube)

UA chips (4 S / 1 G — thin list, z 2.0–2.8):

| term | z | class | verdict | why |
|---|---|---|---|---|
| bakhmut | 2.8 | S | keep | modern-war UA bundle around the UA spelling |
| drohobych | 2.8 | S | keep | UA toponym bundle |
| moldovan | 2.7 | G | drop | tangential (Transnistria prose) |
| frontline | 2.4 | S | keep | modern-war register |
| sumy | 2.0 | S | keep | UA toponym bundle |

RU chips (5 S / 2 F / 3 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| russia | 37.7 | S | keep | the RU-anchored history frame — the pair's whole argument |
| russian | 31.6 | F | merge → russia | sibling |
| history | 28.3 | S | keep | history register |
| empire | 27.9 | S | keep | imperial-genealogy framing |
| viking | 24.2 | S | keep | Rurik/Norse-origin framing |
| vikings | 22.9 | F | merge → viking | sibling |
| world | 22.9 | G | drop | generic |
| europe | 22.1 | G | drop | generic |
| khan | 21.8 | S | keep | Mongol-period framing |
| union | 21.1 | G | drop | ambiguous token; the example even shows *European* Union for what should be a Soviet-Union signal |

Tooltips: `union` example wrong-sense (EU); `viking`/`khan` examples excellent.

Clusters: two MIS-GLOSSES — `paradox · ukraine` (510: Crusader Kings/Paradox Interactive +
Roblox) and `game · civs` (332: Civilization/Age of Empires) both gloss as **"Kyivan Rus
history"** because their term lists contain kievan+rus; they are gaming clusters and should
gloss as such. `mongol · khan` (387), `vikings · viking` (255, family-dup label), `kyiv ·
ukraine` (419, church/cathedral heritage), war coverage (7,323) all fine.

### luhansk — "Luhansk" vs "Lugansk" (gdelt, openalex, reddit, youtube)

UA chips (2 S / 2 F / 6 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| russian | 90.1 | S | keep (weak) | family head |
| ukraine | 87.2 | S | keep (weak) | family head |
| russia | 72.3 | F | merge → russian | sibling |
| ukrainian | 65.7 | F | merge → ukraine | sibling |
| war | 52.3 | G | drop | boilerplate |
| forces | 51.8 | G | drop | boilerplate |
| company | 49.9 | G | drop | driven by the SEC-prospectus reservoir (see filters (c)9) |
| military | 43.7 | G | drop | boilerplate |
| united | 39.8 | G | drop | "United Nations" fragment |
| news | 38.0 | G | drop | boilerplate |

RU chips (2 S / 1 G / 7 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| afu | 13.8 | S | keep | **the best RU chip on the site**: pro-Russian outlets' "AFU shelled the LPR" framing — the old spelling's political register in one token |
| motor | 8.8 | G | drop | mixed reservoir ("Bakhmutka motor road" + car prose); no register |
| mbbs | 8.6 | S | keep | Lugansk State Medical University's Indian-student pipeline — genuine RU-form register |
| vov | 8.1 | A | drop (lang gate) | Vietnamese broadcaster VOV — whole non-English docs |
| rusos | 5.4 | A | drop (lang gate) | Spanish (the Romance-reservoir finding, live on the site) |
| rusas | 5.4 | A | drop (lang gate) | Spanish |
| hoy | 5.4 | A | drop (lang gate) | Spanish |
| yug | 5.3 | A | drop | random business-name fragment (Bukhta Yug trade centre, Yerevan) |
| desde | 5.0 | A | drop (lang gate) | Spanish |
| saggy | 4.9 | A | drop | blog commenter handle ("Posted by: Saggy", Moon-of-Alabama comment section) |

Tooltips: `vov` quotes Vietnamese, `rusos`/`desde` quote Spanish; `saggy`'s example is
comment-section debris shown as evidence.

Clusters: war coverage (9,777); `company · shares` (1,431) = securities-prospectus boilerplate
naming Luhansk in sanctions risk-factors (verified: 38 SEC prospectus docs and kin) — honest
but opaque label, relabel "sanctions/legal boilerplate"; `sprite · league` (471) = Reddit
r/soccer markup (Zorya Luhansk match threads; verified 81 texts with `sprite1` codes) — junk
visible label, strip markup.

### lviv — "Lviv" vs "Lvov" (gdelt, openalex, reddit, youtube)

UA chips (3 S / 2 F / 5 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| ukraine | 96.6 | S | keep (weak) | family head |
| ukrainian | 60.6 | F | merge → ukraine | sibling |
| city | 51.9 | G | drop | boilerplate |
| russian | 45.9 | S | keep (weak) | family head |
| war | 41.0 | G | drop | boilerplate |
| russia | 39.8 | F | merge → russian | sibling |
| kyiv | 33.8 | S | keep | UA-form bundle |
| region | 32.1 | G | drop | boilerplate |
| world | 31.2 | G | drop | generic |
| national | 30.7 | G | drop | generic |

RU chips (8 A — all one referent bug):

| term | z | class | verdict | why |
|---|---|---|---|---|
| trotsky | 7.8 | A | drop via filter | **"Lvov" here is a PERSON**: Prince Georgy Lvov, first PM of the 1917 Provisional Government — verified 158 RU-side docs; revolution-history prose |
| mensheviks | 4.7 | A | drop via filter | same reservoir |
| srs | 3.8 | A | drop via filter | same (Socialist Revolutionaries) |
| karenina | 3.4 | A | drop via filter | the Lvov CHARACTER in *Anna Karenina* — Tolstoy discussion threads |
| bolshevism | 3.3 | A | drop via filter | same reservoir |
| marxism | 3.0 | A | drop via filter | same |
| rsdlp | 2.1 | A | drop via filter | same |
| rasputin | 2.0 | A | drop via filter | same |

Every single RU chip describes people named Lvov, not the city. A reader sees "the old
spelling's discourse is Bolshevik history" — historically resonant-looking and entirely an
artifact of the person-homonym reservoir. Highest-priority referent filter after odesa.

Tooltips: all RU examples are Russian-revolution or Tolstoy prose — they "read as evidence"
precisely because the contamination is coherent, which makes this the most misleading list on
the site (borscht's finance chips at least look wrong).

Clusters: mostly fine — war coverage split 97.4%-UA (6,119) vs mixed (1,336; raw terms include
Reddit `sprite` markup), `ukraine · music` (769), `ukraine · city` (364), `league · club`
(359, football; raw terms include `icon-sub` markup), `apartment · ukraine` (278, rental
listings), `region · development` (204, openalex regional-development papers). Labels honest;
markup strip would clean the two term lists.

### mykola-hohol — solo "Nikolai Gogol" (RU form dominant)

Solo chips (7 S / 1 G / 2 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| gogol | 35.0 | A | fix (mask) | self-echo: surname alone evades the "nikolai gogol" phrase mask |
| fyodor | 24.9 | S | keep | Russian-literary-canon register (Dostoevsky pairing) |
| overcoat | 23.6 | S | keep | *The Overcoat* |
| nose | 23.2 | S | keep | *The Nose* |
| literature | 22.4 | S | keep | canon register |
| dostoevsky | 22.3 | S | keep | canon register |
| nikolai | 20.8 | A | fix (mask) | self-echo: first name alone |
| books | 20.4 | G | drop | generic (jumble-sale prose in the example) |
| stray | 20.3 | S | keep, fix example | **Bungo Stray Dogs** — Gogol is a major character in the anime (verified 1,476 docs); the register that keeps "Nikolai Gogol" alive for a new generation |
| inspector | 20.2 | S | keep | *The Government Inspector* |

Tooltips: `stray`'s example is a Tamil theatre company called "Stray Factory" — flat wrong
register for a chip whose driver is 1,476 anime docs; the worst single example mismatch
alongside sikorsky's `polytechnic`.

Clusters (11): the best cluster set on the site — `anime · bsd` (1,045; Bungo Stray Dogs),
`christmas · book` (903; *The Night Before Christmas*), `russian · overcoat` (840),
`film · story` (737), war-reclamation coverage (277), `inspector · government` (221),
`theatre · theater` (189; family-dup label, merge), `dostoevsky · fyodor` (155),
`music · opera` (137), `viy · film` (121; the *Viy* horror films). Every label names a real
register.

### odesa — "Odesa" vs "Odessa" (gdelt, openalex, reddit, youtube)

UA chips (10 S — the best list on the site):

| term | z | class | verdict | why |
|---|---|---|---|---|
| drones | 16.0 | S | keep | strike-coverage register |
| kyiv | 14.7 | S | keep | UA-form bundle |
| zelenskyy | 11.7 | S | keep | UA name-form bundle (double-y) |
| overnight | 9.2 | S | keep | "overnight strike/explosion" news collocate |
| kiper | 8.9 | S | keep | Oleh Kiper, Odesa governor — precisely pair-specific |
| zaporizhzhia | 8.5 | S | keep | UA toponym bundle |
| ballistic | 7.1 | S | keep | strike coverage |
| serhiy | 6.6 | S | keep | UA name-form bundle |
| nabu | 6.5 | S | keep | Ukrainian institution (anti-corruption bureau) |
| russia-ukraine | 6.4 | S | keep | compound framing term |

RU chips (7 G / 3 A — none about the Ukrainian city):

| term | z | class | verdict | why |
|---|---|---|---|---|
| pussy | 33.9 | A | drop via filter | porn-spam reservoir (the 1,443-text `anal · tits` cluster); example is a sanitized gdelt art review — misleadingly tame |
| girl | 31.7 | G | drop via filter | US-homonym + spam reservoir |
| home | 28.8 | G | drop via filter | Odessa TX/MO real-estate & local prose |
| sprite | 28.2 | A | drop | Reddit markup + racing-animal names ("Magic Sprite") |
| indian | 28.0 | G | drop via filter | spam tags + generic |
| big | 27.2 | G | drop via filter | generic; example is Odessa College (Texas) basketball |
| family | 27.0 | G | drop via filter | US local prose |
| music | 26.7 | G | drop via filter | auto-generated YouTube topic uploads |
| school | 26.4 | G | drop via filter | Odessa TX school-district news |
| sex | 26.4 | A | drop via filter | porn spam; example again sanitized |

This is finding_odesa_us_reservoir rendered as UI: the entire RU chip list describes American
towns, spam, and fiction, not the city. Nothing here is fixable by stopwords — it needs the
tiered referent filter.

Tooltips: `pussy` and `sex` show accidental-euphemism examples (gdelt art reviews) while the
real driver is porn spam — the tooltip actively hides what the chip is; `big`'s Texas
college-basketball example at least reveals the reservoir.

Clusters (13): the referent map is actually useful diagnostics — war coverage in four tiers
(1,772 / 1,243 / 836 / 405) is fine; junk that ships as visible legend text: `anal · tits`
(1,443 — porn label on a policy-facing site), `home · crappie` (1,474 — Texas/Missouri lake
fishing), `looking · host` (433 — hookup-personals spam), `marie · alt` (407 — Gen V/The Boys
fandom: godolkin/homelander; verified 293 docs), `ukraine · background-image` (384 — literal
inline CSS in scraped text), `sprite · bar-` (216 — Reddit markup), `police · crousore` (744 —
Odessa, MO police blotter; the gloss "crime & police news" is right but it is the WRONG
Odessa), `music · youtube` (1,082 — auto-generated uploads). After the referent fix most of
these clusters leave the corpus entirely.

### oleksandr-usyk — "Oleksandr Usyk" vs "Alexander Usyk" (reddit, youtube; RU list empty)

UA chips (6 S / 2 F / 1 G / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| boxing | 69.9 | S | keep | the register |
| fury | 50.9 | S | keep | Tyson Fury — opponent bundle |
| usyk | 50.0 | A | fix (mask) | self-echo: surname through the phrase mask |
| tyson | 44.8 | F | merge → fury | same person split across two chips |
| fight | 38.5 | S | keep | boxing register |
| dubois | 33.5 | S | keep | opponent bundle |
| joshua | 33.2 | S | keep | opponent bundle |
| anthony | 30.4 | F | merge → joshua | same person |
| undisputed | 30.4 | S | keep | his actual achievement — the most pair-specific chip |
| live | 28.8 | G | drop | broadcast boilerplate |

RU side empty ("no distinctive vocabulary") — honest: nothing leans "Alexander Usyk"
consistently in two sources.

Tooltips: all quote gdelt boxing wire although sources are Reddit+YouTube (wrong-source
defect); content itself is on-register.

Clusters: `fight · fury` (12,664, 96.2% UA) and `joshua · fight` (550) — fine; labels honest.

### serhii-korolyov — solo "Sergei Korolev" (RU form dominant) — cleanest list on the site

Solo chips (9 S / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| moon | 17.7 | S | keep | space-race register |
| spacecraft | 17.2 | S | keep | same |
| korolev | 17.1 | A | fix (mask) | self-echo: surname through the phrase mask |
| nasa | 17.1 | S | keep | space-race framing |
| gagarin | 16.9 | S | keep | same |
| rocket | 16.6 | S | keep | same |
| orbit | 16.4 | S | keep | same |
| sputnik | 15.3 | S | keep | same |
| apollo | 15.0 | S | keep | same |
| braun | 14.3 | S | keep | the Von Braun comparison — historiography register |

Tooltips: fine throughout.

Clusters: `space · soviet` (729) + `space · soviet · gagarin` (224) — near-duplicate labels
(the dedupe appended a third term instead of merging; candidates for a single cluster);
`crater · mars` (31 — the Korolev crater, genuine); `composer · youtube` (37 — auto-generated
topic channels for a NAMESAKE musician, amuse.io distribution; filter (c)8). Both small ones
are below a sensible display floor.

### ternopil — "Ternopil" vs "Ternopol" (openalex, reddit, youtube; RU list empty)

UA chips (4 S / 1 F / 4 G / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| ukraine | 38.7 | S | keep (weak) | family head |
| university | 21.1 | S | keep | TNMU — the university register is Ternopil's genuine story |
| medical | 20.9 | S | keep | same |
| mbbs | 19.8 | S | keep | Indian medical students at TNMU — the most pair-specific chip |
| region | 18.9 | G | drop | boilerplate |
| ukrainian | 18.7 | F | merge → ukraine | sibling |
| news | 17.3 | G | drop | boilerplate |
| suspilne | 16.6 | A | drop | outlet name (public broadcaster) — same policy as other outlet tokens; the fact that UA-spelling texts come from Suspilne is provenance, not discourse |
| national | 15.4 | G | drop | generic |
| city | 15.3 | G | drop | generic |

Tooltips: `mbbs` example is exactly right ("Top 5 universities for studying MBBS in
Ukraine"); `city` example (Cedar City, Utah — Yushchenko visit) shows the term is generic.

Clusters (14 — most of any pair, several below floor): good — `ukraine · ukrainian` war
(1,940), `university · medical` (1,350), `ukraine · eurovision` (548, TVORCHI), `ukraine ·
music` (456), `region · development` (168), `sanctions · ukraine` (150), `patients · ukraine`
(121, covid/Lyme). Junk/opaque visible labels: `sprite · bar-` (224 — Reddit markup),
`shearling · ternopolleather` (157 — Etsy leather-shop spam; note the shop itself spells
"Ternopol"), `nameless · live` (361 — actually a real Ternopil rock band "Nameless"; opaque
label, genuine content), `sogiz · church` (149 — SOGIZ = "Sons of God in Zion" ministries, a
real Pentecostal church headquartered in Ternopil; opaque acronym label, genuine content),
`pov · tcc` (119 — mobilization street videos; opaque), `non-English` (120). The two church/
band clusters deserve honest glosses, not deletion.

### varenyky — "Varenyky" vs "Vareniki" (reddit, youtube)

UA chips (8 S / 1 F / 1 G):

| term | z | class | verdict | why |
|---|---|---|---|---|
| ukraine | 35.5 | S | keep (weak) | family head |
| kyiv | 24.6 | S | keep | UA-form bundle |
| ukrainian | 17.4 | F | merge → ukraine | sibling |
| borshch | 9.1 | S | keep | UA culinary bundle — "varenyky" texts also write "borshch"; excellent |
| traditional | 7.7 | S | keep | heritage framing |
| christmas | 6.9 | S | keep, fix example | Sviata Vecheria / Christmas Eve varenyky — genuine; the example is a Saskatoon concert listing, replace |
| dishes | 6.8 | S | keep | culinary register |
| lviv | 6.7 | S | keep | UA toponym bundle |
| cuisine | 6.6 | S | keep | culinary register |
| often | 6.2 | G | drop | function word (STOP gap) |

RU chips (2 S / 1 G / 2 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| pelmeni | 6.2 | S | keep | the RU-cuisine frame: "vareniki" travels with "pelmeni" — the pair's whole story in one chip |
| ich | 3.0 | A | drop (lang gate) | German postcard prose |
| mai | 2.5 | G | drop | "Mai Tais" happy-hour listing — no register |
| cho | 2.2 | A | drop (lang gate) | Vietnamese |
| popkoff | 2.0 | S | keep (weak) | Popkoff's pelmeni brand — RU-cuisine commerce; z at the floor |

Clusters: `dumplings · dough` (1,215) cooking; `ukraine · ukrainian` (318); `phone ·
information` (234) = Saskatchewan church perogy-sale contact boilerplate — genuine diaspora
signal buried under a boilerplate label; relabel ("diaspora church sales") or strip
contact-info lines pre-clustering.

### volodymyr-the-great — "Volodymyr the Great" vs "Vladimir the Great" (reddit, youtube)

UA chips (6 S / 1 F / 1 G / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| ukraine | 5.4 | S | keep (weak) | family head |
| kyiv | 4.7 | S | keep | UA-form bundle |
| trident | 3.6 | S | keep | the tryzub — heraldry register, precisely pair-specific |
| kyivan | 3.5 | S | keep | UA-form bundle (Kyivan Rus) |
| coat | 3.4 | S | keep | coat of arms — same heraldry story |
| volodymyr | 3.3 | A | fix (mask) | self-echo: first name through the phrase mask (also the corvette "Volodymyr the Great" referent) |
| ukrainian | 2.8 | F | merge → ukraine | sibling |
| equipment | 2.1 | G | drop | crowdfunding boilerplate |
| khotyn | 2.0 | S | keep (weak) | castles-of-Ukraine heritage register |

RU chips (4 S / 2 F / 1 G / 3 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| russia | 16.6 | S | keep | the Moscow-claims-him framing — the pair's argument |
| vladimir | 13.2 | A | fix (mask) | self-echo: half of the pair's own RU form |
| great | 12.9 | A | fix (mask) | self-echo: the other half |
| kievan | 12.6 | S | keep | RU-form bundle (Kievan Rus) |
| russian | 11.7 | F | merge → russia | sibling |
| kiev | 11.2 | S | keep | RU-form bundle |
| rus | 11.1 | F | merge → kievan | same phrase split across chips |
| wikipedia | 11.0 | A | drop | platform token |
| war | 10.8 | G | drop | boilerplate |
| history | 10.7 | S | keep | history register |

Tooltips: examples on the RU side are strong (the Moscow-statue story); `vladimir`/`great`
examples show the chips merely quote the pair's own name.

Clusters: the best-glossed set on the site — "Kyivan Rus history" (619), "war & news
coverage" (226), "commemorative coins" (83), "church & baptism" (52; hover label `islam ·
russia` is odd but the underlying Christianity-vs-Islam choice narrative is genuine), "Kyiv
monuments & places" (51). Below-floor sizes on the last three, but every gloss is right.

### volodymyr-zelenskyy — "Volodymyr Zelenskyy" vs "Vladimir Zelensky" (gdelt, reddit, youtube)

UA chips (4 S / 2 F / 3 G / 1 A):

| term | z | class | verdict | why |
|---|---|---|---|---|
| ukraine | 274.5 | S | keep (weak) | family head |
| russia | 205.0 | S | keep (weak) | family head |
| president | 201.7 | G | drop | boilerplate |
| russian | 180.4 | F | merge → russia | sibling |
| ukrainian | 178.2 | F | merge → ukraine | sibling |
| war | 156.3 | G | drop | boilerplate |
| trump | 154.8 | S | keep | the Trump-Zelenskyy news cycles — genuine register |
| zelenskyy | 137.6 | A | fix (mask) | self-echo: surname through the phrase mask |
| putin | 131.3 | S | keep | war-news actor |
| news | 121.7 | G | drop | boilerplate |

RU chips (7 S / 1 A — the best RU list on the site):

| term | z | class | verdict | why |
|---|---|---|---|---|
| kiev | 20.3 | S | keep | RU-form bundle |
| donbass | 5.8 | S | keep | RU-form bundle (doubled s) |
| zaporozhye | 4.8 | S | keep | RU transliteration bundle |
| bezuglaya | 3.0 | S | keep | RU rendering of Bezuhla — name-form bundle |
| avdeevka | 2.4 | S | keep | RU transliteration bundle |
| pyotr | 2.4 | S | keep | **"Pyotr Poroshenko"** — the RU sphere renders even Petro's name through Russian; the single most eloquent chip on the site |
| pristayko | 2.1 | S | keep | RU rendering of Prystaiko |
| ancak | 2.0 | A | drop (lang gate) | Turkish |

This RU list is the model for what every RU side should look like after the filters: the
old spelling travels as a complete Russian-transliteration bundle.

Clusters: single cluster (14,611 — whole corpus, 91% UA). Uninformative panel; same remedy
as donbas.

---

*End of per-pair review. Consolidated deliverables (a)–(e), systemic cluster-label defects,
and systemic tooltip defects are at the top of this file.*
