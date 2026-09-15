# The chips and the curve are computed on different populations

`analyze_pair` applies an English-only gate to documents of 80+ characters
before keyness and clustering. That is correct for the linguistic layer — you
cannot run English log-odds over French text, and without it "que", "voor" and
"izle" shipped as collocations. But the adoption SERIES deliberately counts
every Latin-script row regardless of language, because a Russian-language video
writing "vareniki" in Latin script is training data exactly like an English one.

So the two layers describe different corpora, and the gate does not fall evenly:
the Russian side of food pairs is far more often non-English.

| pair | keeps UA | keeps RU | series UA% | chip-corpus UA% | shift |
|------|---------:|---------:|-----------:|----------------:|------:|
| varenyky | 82.6% | 45.6% | 38.0 | 52.6 | +14.6 |
| luhansk | 88.6% | 59.4% | 64.6 | 73.2 | +8.5 |
| borscht | 89.1% | 64.2% | 72.8 | 78.8 | +6.0 |
| kharkiv | 89.2% | 70.8% | 76.9 | 80.8 | +3.9 |
| odesa | 84.5% | 74.1% | 33.2 | 36.1 | +3.0 |
| chicken-kyiv | 81.5% | 93.7% | 13.1 | 11.6 | -1.5 |
| kyivan-rus | 93.3% | 93.3% | 18.6 | 18.6 | 0.0 |

Mean absolute shift across 22 measurable pairs: 2.3 points. The direction is not
uniform — chicken-kyiv, volodymyr-the-great, ternopil and mykola-hohol shift the
other way.

## What this means for the write-up

Never state a collocation finding as if it described the same population as the
adoption curve. For varenyky the contrastive vocabulary describes a corpus that
is 52.6% Ukrainian-form while the plotted series is 38.0%.

For varenyky specifically the underlying composition is: 67.1% of the Russian
side is non-English against 27.1% of the Ukrainian side, and 970 Russian-language
YouTube rows (39.4% of that side) carry Cyrillic running text with Latin
"vareniki" as a transliterated keyword. Adoption on that pair reads 38.0% over
all rows, 57.6% English-only, 19.9% non-English-only — a 19.6-point spread, the
widest of the 24 pairs.

Neither number is wrong. They answer different questions, and the paper has to
say which one it is answering each time.
