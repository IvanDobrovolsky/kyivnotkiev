# One form per side, and what that deliberately excludes

Every pair matches exactly ONE surface form on each side. The Ukrainian form is
the official romanisation — Cabinet of Ministers Resolution 55 — verified against
the pair's own Cyrillic with `translitua` (see `pipeline/audit/` checks). The
Russian form is the spelling English actually inherited.

That choice excludes real usage, on purpose. This note records what is left out,
why, and which direction it biases the result.

## What is excluded

English has no settled romanisation of Ukrainian personal names, and the corpus
shows it directly. For Сергій Корольов, measured across all four stores:

| form | produced by | mentions |
|------|-------------|---------:|
| Serhiy Korolyov | no standard — Russian surname pattern (Королёв, ё → yo) | 9 |
| Serhii Koroliov | no standard — ьо read as "io" | 3 |
| Sergiy Koroliov | no standard | 3 |
| Serhiy Korolov | UkrainianFrench | 2 |
| **Serhii Korolov** | **KMU, official — what we match** | **0** |

Four spellings in circulation, none dominant, and the official one unused. The
same pattern appears for places. Запоріжжя:

| form | GDELT 2015-21 | GDELT 2022-25 |
|------|--------------:|--------------:|
| Zaporizhia (one z) | 506 | 3,213 |
| **Zaporizhzhia (official)** | **190** | **60,882** |

Before 2022 the non-official spelling outnumbered the official one 2.7 : 1; after
2022 the ratio inverts to 1 : 19.

## Why we still match one form

The question this study asks is whether English adopts **the Ukrainian standard**
— the form the Ukrainian state publishes, the one the #KyivNotKiev campaign
asked for. Pooling every romanisation would answer a different question: whether
English has moved away from the Russian form by any route. Both are worth asking;
they are not the same question, and one number cannot carry both.

The fragmentation is itself a finding. That four spellings of one engineer's name
circulate with none dominant is evidence that Ukrainian-to-English romanisation of
personal names has no consensus — which is precisely why person-name pairs behave
differently from place-name pairs, where the MFA campaign produced one agreed
target. **Measuring that ambiguity is a separate study** and is out of scope here.

## Which way it biases the result

Single-form matching UNDERSTATES the Ukrainian side wherever romanisation is
unsettled, so every adoption figure for such a pair is a LOWER BOUND. Measured:

* **zaporizhzhia** — counting all Ukrainian-derived spellings moves the pre-2022
  figure by +18 points on GDELT, +22 on Reddit, +37 on YouTube. The published
  curve therefore overstates the size of the 2022 jump, while the post-2022 level
  is accurate to within ~1 point.
* **serhii-korolyov** — the official form scores zero. All Ukrainian renderings
  combined total 17 against 2,201 Russian-form mentions, i.e. ~0.8% rather than
  0%. The conclusion is unchanged: this name has effectively no Ukrainian-form
  usage in English.

Neither correction changes any pair's verdict. Both must be stated wherever the
curve's SHAPE is discussed, because the shape is what they distort.

## Consequence for reading the site

A pair's number answers: *of English texts using one of these two specific
spellings, what share use the official Ukrainian one?* It does not answer: *what
share of English texts render this name in some Ukrainian-derived way?* For
well-standardised places (Kyiv, Kharkiv, Lviv) the two are nearly identical. For
personal names, and for places whose romanisation settled late, they are not.
