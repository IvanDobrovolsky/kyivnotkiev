# Referent decisions, 2026-09-11

Three cases of content named AFTER the real referent, decided the same way as
mykola-hohol's Bungo Stray Dogs rows: a name used for a fictional character, a
product or a map is still that name being written in Latin script, which is
what this study measures. Kept and disclosed. A thing that is simply a
DIFFERENT referent is removed.

## Kept

**S.T.A.L.K.E.R. 2: Heart of Chornobyl** (chornobyl). The filter
`heart\s+of\s+cherno?byl` matched only the Russian spelling, so every
Russian-spelled mention of the game title was deleted while 6,818
Ukrainian-spelled rows were kept. In the raw Reddit backfill 23.1% of
game-title mentions use the Russian form — all of them deleted. "The game title
is always spelled Chornobyl" was therefore true by construction. The filter is
removed; both spellings now count, and the title reads roughly 77/23 rather
than 100/0.

Disclosure for the write-up: 55.5% of Ukrainian-form documents are game
context and 62% are dated 2024-25. On OpenAlex the Ukrainian form is 77.0%
disaster and 1.3% game, i.e. MORE disaster-focused than the Russian form — the
game effect is a web-platform finding, not a universal one.

**Fishing Planet** (dnipro-river). The game shipped a "Dnipro River" map in
2024: 336 rows, 100% YouTube and Ukrainian-form, all 2024-25. It is 20.0% of
all 2024-25 YouTube Ukrainian rows and lifts that period's share from 70.9% to
75.2%, so the 2024-25 tail of that series must be read with it named.

**The Team Fortress 2 "Chicken Kiev" cosmetic** (chicken-kyiv). An item named
after the dish, frozen at creation. Same category as the anime character.

## Removed

**Dynamo Chicken Kiev**, a Sussex amateur football club (chicken-kyiv). Not the
dish under another guise — a different referent entirely, the way Dynamo Kyiv
is its own pair. 149 rows, 147 of them Russian-variant.

## Standing rule this establishes

Named-after-the-referent stays, different-referent goes. And any homonym
pattern in a spelling study must match BOTH spellings —
`pipeline.audit.filter_symmetry` now enforces it.

## Kharkiv wargames — kept, as a class (2026-09-11)

kharkiv deleted ARMA 3, Call of Duty, Hell Let Loose, World of Tanks, Gates of
Hell and Victoria 3 while keeping thousands of rows of Battlegroup, Panzer
Corps, Steel Division and Graviteam doing exactly the same thing. All of them
name the 1942-43 Battles of Kharkov, so under the standing rule the class stays
— and a partial deletion is worse than either choice, because which titles got
cut was arbitrary.

Measured: the eight removed patterns would delete 264 rows, 230 Russian-form
against 34 Ukrainian-form, moving the pair 76.93% -> 77.05%. Keeping them costs
0.12 points and buys consistency.

Disclosure the write-up needs: wargame content is 21.8% of Reddit and 21.4% of
YouTube Russian-side character mass (Ukrainian side 2.5% and 0.2%), a 6.8:1
skew, because "Kharkov" is the map name in every title. Its effect on the curve
is era-dependent and near zero where the headline lives — YouTube +4.8 points
in 2022 and +3.8 in 2021, but +0.2 in 2024-25; Reddit +12.7 in 2013 and about
+6 in 2016-18, +0.1-0.4 from 2022 on. It raises the pre-invasion floor and
flattens the curve rather than changing the verdict.

The Russian-form register that survives is coherent and is the finding:
battlegroup, panzer, soviet, tank, stalingrad, manstein, armored — the old
spelling lives in Second World War military history.

## Open: the r/ukraine daily megathread (2026-09-11)

Not a referent question, a token-weighting one, and the largest unfixed defect
the 24-pair review found.

r/ukraine posts "The Sun is Rising Over Kyiv on the Nth Day of the Full-Scale
Invasion" every day. Each edition names several study pairs once — usually in a
photo caption or a city-profile section — and then donates roughly a thousand
tokens of fundraiser and culture boilerplate to whichever side the spelling
lands on. They are always Ukrainian-form.

Measured: 57.3% of kyivan-rus's Ukrainian-side reddit token mass (94.1% of
r/ukraine's), 83.3% of kazymyr-malevych's, 75.4% of varenyky's, and they carry
97-99% of chicken-kyiv's borshch/salo/deruny/syrnyky.

Why nothing so far catches it:
  * dedup keys on the first 300 characters, and the editions share only ~80 —
    each day's subject differs. Digit-normalising the fingerprint was tried and
    does not collapse them (34 distinct either way).
  * MAX_TOKENS_PER_DOC=1000 does not bite: each edition sits at about the cap.
  * MIN_DOC_FREQ does not bite: they are genuinely 66 separate documents.

The principled fix is to score the CONTEXT AROUND THE MATCH rather than the
whole document — the store already carries match_context, and prosody already
works that way. That changes every keyness number, so it is a deliberate
methodology change, not a patch, and it should be made once and re-verified
rather than bolted on mid-review.
