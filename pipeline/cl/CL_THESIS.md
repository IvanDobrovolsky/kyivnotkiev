# #KyivNotKiev: A Large-Scale Computational Study of Ukrainian Toponym Adoption

## Core Argument

**Toponym variant choice is not merely a temporal adoption phenomenon — it is a discourse marker that signals the writer's engagement context, awareness level, and relationship to the subject.**

Previous studies of the #KyivNotKiev campaign and similar naming initiatives measured adoption as a binary switching rate: what percentage of sources use the Ukrainian form? We demonstrate that this framing misses the fundamental nature of the phenomenon. Using transformer-based context classification on 42,613 texts across 59 toponym pairs, we show that the Russian and Ukrainian forms of each toponym occupy **systematically different semantic and contextual spaces**.

## Key Findings

### 1. Spelling Variants Live in Different Discourse Worlds

The same toponym pair is used in fundamentally different contexts depending on which form the writer chooses:

| Pair | Russian Form Context | Ukrainian Form Context |
|------|---------------------|----------------------|
| Chernobyl/Chornobyl | Gaming (S.T.A.L.K.E.R.), tourism, HBO series, pop culture | Nuclear plant operations, IAEA, radiation biology, disaster policy |
| Kiev/Kyiv | Chicken recipe, Champions League, historical reference | Missile strikes, wartime resilience, diplomatic context |
| Odessa/Odesa | Soviet Union references, cultural nostalgia, film | Port operations, wartime damage, modern city life |
| Vladimir/Volodymyr Zelenskyy | Foreign media, third-person, translated framing | Political agency, inaugural, defiant, direct address |

**Implication:** Adoption metrics that count binary switches miss the fact that a single source may use both forms — "Kiev" in its food section and "Kyiv" in war reporting. The spelling choice is context-dependent, not source-dependent.

### 2. "Resistant" Toponyms Resist Because the Russian Form IS the Brand

Chernobyl's 5% adoption rate is not because people are ignorant of the Ukrainian form. It's because "Chernobyl" has become a cultural brand:
- The HBO series is titled "Chernobyl"
- The game franchise was "S.T.A.L.K.E.R.: Shadow of Chernobyl" (2007)
- Tourism packages market "Chernobyl tours"
- The subreddit is r/chernobyl

The brand locks in the spelling. Notably, when the game developers (Ukrainian company GSC Game World) released the sequel in 2024, they switched to "S.T.A.L.K.E.R. 2: Heart of **Chornobyl**" — a deliberate cultural reclamation.

Similarly, "Chicken Kiev" resists change because it is a recipe name, not a political statement. Food discourse preserves the Russian form not out of political choice but because recipe names function as proper nouns.

### 3. Sentiment Diverges Systematically by Variant

Texts using the Ukrainian form show different sentiment distributions than those using the Russian form. This is not because one spelling is "more positive" — it's because the topics differ:
- "Kyiv" appears predominantly in war/politics contexts → more negative sentiment (conflict reporting)
- "Kiev" appears in food/sports/tourism → more neutral/positive sentiment

**Implication:** Naive sentiment analysis of "how people feel about Kyiv vs Kiev" would produce a confound — the sentiment difference reflects topic difference, not attitude toward the spelling.

### 4. Academic Text Preserves Historical Naming Longest

OpenAlex data reveals that academic publishing preserves Russian-form toponyms far longer than news media:
- "Vladimir the Great" dominates academic papers at 92% (vs 5% in news after invasion)
- "Kievan Rus" remains standard in history journals despite "Kyivan Rus" existing
- Academic citation chains create path dependence: citing older papers that used "Kiev" perpetuates the form

**Implication:** Institutional naming inertia in academia is qualitatively different from media adoption — it reflects citation norms, not political awareness.

### 5. Adoption is Multi-Dimensional, Not Binary

Our context classifier enables measurement of WHERE within discourse adoption occurs:
- A news outlet scoring "80% Kyiv adoption" may have 100% adoption in war reporting but 0% in recipe sections
- This resolves the paradox of high adoption rates coexisting with persistent Russian forms
- The true metric should be domain-specific adoption rates, not aggregate counts

## Methodology

### Dataset: KyivNotKiev-CL Corpus

- **42,613 texts** across **59 Ukrainian-Russian toponym pairs**
- **4 sources**: Reddit (social media), YouTube (video), OpenAlex (academic), GDELT (news articles)
- **Balanced**: 14,339 Russian / 15,599 Ukrainian variants (48/52%)
- **Multilingual**: 82% Latin script (primarily English), 17% Cyrillic (Ukrainian/Russian), 2% mixed
- **Temporal span**: 2006–2026, stratified by 4 periods

### Annotation Pipeline

1. **Llama 3.1 70B-Instruct** (full BF16 precision) annotates all texts with:
   - Context category (11 classes: politics, war_conflict, sports, culture_arts, food_cuisine, travel_tourism, academic_science, history, business_economy, general_news, religion)
   - Sentiment (positive/neutral/negative with -1 to +1 score)
   - Brief reasoning

2. **Human validation**: 200 random annotations spot-checked for agreement

3. **Three encoder models fine-tuned** on LLM labels:
   - DeBERTa-v3-large (304M parameters)
   - XLM-RoBERTa-large (550M parameters)
   - mDeBERTa-v3-base (86M parameters — lightweight baseline)

4. **Benchmark**: All 3 models evaluated on held-out test set (10%)
   - Per-class F1 scores
   - Per-pair ablation (which pairs are hardest to classify?)
   - Per-source ablation (does the model generalize across Reddit/GDELT/academic?)
   - Cross-lingual transfer (does it work on Cyrillic texts?)

### Training Hyperparameters

All three encoders use identical hyperparameters for fair comparison:

| Parameter | Value | Justification |
|-----------|-------|---------------|
| Epochs | 3 | Standard for fine-tuning; val F1 reported per epoch to show convergence |
| Batch size | 16 train / 32 eval | Balanced GPU memory and gradient stability |
| Learning rate | 2e-5 | Default for BERT-family fine-tuning (Devlin et al., 2019) |
| Warmup ratio | 0.1 | 10% of steps, prevents early divergence on new classification head |
| Weight decay | 0.01 | Standard L2 regularization |
| Max sequence length | 512 tokens | Covers 95th percentile text length (435 words ≈ ~550 tokens) |
| Precision | BF16 | Native on B200, no quality loss vs FP32 for training |
| Optimizer | AdamW | HuggingFace Trainer default |
| Model selection | Best val F1-macro across 3 epochs | F1-macro preferred over accuracy due to class imbalance |
| Label confidence filter | ≥ 0.6 | Removes ~2% lowest-confidence LLM annotations |
| Train/val/test split | 80/10/10 stratified | Preserves label distribution across splits |

**Reproducibility notes:**
- Random seed: 42 for all splits and model initialization
- Hardware: NVIDIA B200 183GB, single GPU
- Software: HuggingFace Transformers, PyTorch with BF16
- Training time: ~12 minutes per model (3 epochs on 34,090 texts)
- All hyperparameters chosen from established defaults, no tuning performed — this is intentional, as hyperparameter optimization would conflate model capability with tuning effort

### Why This Methodology

- **Why not GPT?** Reproducibility. Llama is open-weights, versioned, deterministic at temperature 0.05. The exact prompt and model are documented.
- **Why fine-tune encoders?** The LLM is the annotator; the encoder is the publishable artifact. A 550M parameter model that anyone can download and run on a laptop in seconds.
- **Why 3 models?** Proves the labels are robust (if 3 architectures all learn them at F1>0.85, the signal is real, not noise).
- **Why not just collocations?** Collocations show correlation; the classifier provides causation-adjacent evidence. "Texts about sports tend to use the Russian form" is stronger than "the Russian form co-occurs with 'champion'."

## Published Artifacts

1. **Dataset**: `IvanDobrovolsky/kyivnotkiev-cl` on Hugging Face — 42,613 labeled texts with full provenance
2. **Model**: `IvanDobrovolsky/toponym-context-classifier` — best-performing encoder with benchmark results
3. **Embeddings**: Precomputed sentence embeddings for variant clustering analysis
4. **Code**: Full pipeline at github.com/IvanDobrovolsky/kyivnotkiev under `pipeline/cl/`

## Contribution to the Field

This work contributes to computational linguistics in three ways:

1. **Empirical finding**: Toponym variant choice functions as a discourse marker in multilingual media. This extends sociolinguistic theory on language policy from spoken to written web discourse.

2. **Methodological contribution**: A reproducible pipeline for measuring discourse-dependent adoption in naming disputes. Applicable to Taiwan/Chinese Taipei, Myanmar/Burma, Bombay/Mumbai, and other contested toponyms worldwide.

3. **Reusable artifacts**: A labeled corpus and fine-tuned classifier that other researchers can use immediately. The toponym-context-classifier can be applied to any naming dispute study without retraining.
