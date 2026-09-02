# Preliminary findings — for the MFA and journalists

*Drafted 2026-09-02, before the full Bayesian analysis. Every number below is
measured on the current corpus (7 sources, 24 pairs, 2010–2025); items marked
PRELIMINARY need the complete collection or the final model before citing.
Revisit and re-verify after the hierarchical model runs.*

## What worked — and the levers that did it

1. **The campaign flipped English before the war did.** Kyiv's YouTube-title
   crossover lands in 2019 — the #KyivNotKiev / style-guide year — not 2022.
   The invasion amplified an already-won switch (2018: 44% → 2019: 52% →
   2022: 63% → 2023–25: ~67%). Book titles flipped in 2019 too; body prose
   followed only in 2022. **Lever: style desks, not individual words.**

2. **Spelling travels as a house style, not word by word.** Documents that
   write "Vladimir Zelensky" also write Kiev, Donbass, Zaporozhye, Avdeevka
   and Ermak; documents that write "Zelenskyy" carry the Ukrainian set. One
   style-guide decision flips every name at once — the AP 2019 precedent is
   the model.

3. **Quality press adopted harder than the free web.** Recovering 87,000
   paywalled/bot-walled articles skewed Ukrainian ~3:1 and moved luhansk
   +7.4pp. The freely crawlable web *understates* adoption.

4. **Academia is the fastest register.** OpenAlex 2015-16 → 2024-25:
   kyiv 57→94%, odesa 17→70%, **Kyivan Rus 18→66%**. Scholarly usage is
   turning even where public usage is frozen.

5. **A person who asserts their own spelling wins.** Oleksandr Usyk: ~99.9%.
   Bakhmut (legal rename 2016 + wartime default): ~99.9%. Athletes, artists
   and officials stating their form is the strongest person-name vector.

6. **Culture moves names politics can't.** Chornobyl's Ukrainian spelling
   entered English almost entirely through the 2024 STALKER-2 game (its
   discourse cluster is 95% Ukrainian against ~10% overall); borscht was
   anchored by the UNESCO listing. Games, food, music are working vectors.

## The next frontier

7. **Romance languages are the largest remaining "Kiev" reservoir.** The
   residual Kiev-distinctive vocabulary in the corpus is Spanish (que, del,
   ucrania, guerra) and French (les, des, dans, sur) — their codified forms
   still transliterate through Russian. Spanish is the biggest single win
   available (largest Romance language). **Target: RAE/Fundéu, AFP style,
   the way AP was targeted in 2019.** English shows codification can flip
   and the ecosystem follows within a year.

8. **The machine-translation layer re-Russifies.** GDELT's MT emits "Kiev"
   even from Ukrainian outlets (38%); translated docs measure the source
   language's convention, not English. Translation infrastructure (MT
   vendors, CMS pipelines) is an unlobbied chokepoint. PRELIMINARY:
   URL slugs lag body text — sites that write Kyiv still ship /kiev/ URLs.

9. **AI chatbots echo the corpus.** 72 models: shown both spellings they
   pick Ukrainian (90%+); writing freely they regress toward the old
   training text. As public sources shift, models follow — another reason
   the style-desk lever compounds.

## The bitter list

10. **Chernobyl is frozen** (~10% overall; 35% even in academia). The
    disaster is a global brand in Soviet-era spelling; only new cultural
    artifacts (the game) move it.

11. **Odessa resists and its namespace is polluted.** ~40% in news, ~20%
    on YouTube — and English "Odessa" content is heavily diluted by
    Odessa, Texas and the actress Odessa A'zion (17% of English YouTube
    "Odessa" material carried Texas markers). Discoverability itself is
    contested.

12. **Kyivan Rus and its figures read as Russian.** Volodymyr the Great's
    English discourse is imperial-encyclopedic (russia, empire, kievan);
    a whole cluster is Putin being nicknamed "Vladimir the Great". Popular
    history has learned the inheritance as Russia's — though journals are
    turning (finding 4).

13. **Person names without a campaign stay at zero.** Korolyov ~0%,
    Sikorsky ~3% (locked by American corporate branding), Malevych ~1%,
    Hohol ~0.2%. There is no transliteration consensus even inside Ukraine
    (KPI's own English name still says "Igor" while its authors switched
    after 2022). A person-name standard + institutional renaming is the
    missing prerequisite.

14. **Cartography lags.** The river is still "Dnieper" in about half of
    usage; atlas and map vendors are a distinct, slow register.

## Method notes for anyone citing this
Counts are body-verified (the spelling must appear in the text, not the
URL); homonyms (Odessa TX etc.) are filtered; machine-translated content is
excluded from English measurement. Full statistical treatment (hierarchical
Bayesian curves, pre-registered event covariates) ships with the paper.
