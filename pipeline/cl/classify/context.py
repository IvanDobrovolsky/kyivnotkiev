"""Context classification using LLM annotation (Llama 70B) or zero-shot.

Classifies each text into context categories: politics, war_conflict,
sports, culture_arts, food_cuisine, travel_tourism, academic_science,
history, business_economy, general_news.

Two modes:
  1. LLM annotation (vast.ai): Rich labels with explanations
  2. Zero-shot fallback: XLM-RoBERTa NLI for quick local testing

Usage:
    python -m pipeline.cl.classify.context [--mode llm|zero-shot] [--batch-size 32]
"""

import argparse
import json
import logging

import pandas as pd

from pipeline.cl.config import (
    CL_BALANCED_DIR, CL_CLASSIFIED_DIR, CONTEXT_LABELS,
    LLM_ANNOTATOR, ensure_cl_dirs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ANNOTATION_PROMPT = """You are an expert annotator for a computational linguistics study on Ukrainian toponym adoption.

Classify the following text into ONE primary context category. The text contains a Ukrainian or Russian form of a place name (e.g., "Kiev" vs "Kyiv").

Categories:
- politics: Government, diplomacy, elections, legislation, political commentary
- war_conflict: Military operations, battles, casualties, refugees, defense
- sports: Athletic events, teams, leagues, matches, players
- culture_arts: Music, literature, film, art, festivals, entertainment
- food_cuisine: Recipes, restaurants, dishes, culinary traditions
- travel_tourism: Travel guides, tourism, hotels, sightseeing
- academic_science: Research, universities, scientific publications
- history: Historical events, historical figures, historical analysis
- business_economy: Markets, trade, companies, economic policy
- general_news: Breaking news, weather, miscellaneous reporting

Respond with ONLY a JSON object:
{{"category": "<category>", "confidence": <0.0-1.0>, "reason": "<brief explanation>"}}

Text: """


def classify_with_llm(texts_df, model_name=LLM_ANNOTATOR, api_url=None):
    """Classify texts using LLM (vLLM server on vast.ai).

    Expects a vLLM-compatible OpenAI API server running at api_url.
    """
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError("pip install openai — needed for vLLM API client")

    if api_url is None:
        raise ValueError("Provide --api-url for the vLLM server (e.g., http://vast-ip:8000/v1)")

    client = OpenAI(base_url=api_url, api_key="dummy")
    results = []

    for idx, row in texts_df.iterrows():
        text = row["text"][:2000]  # Truncate for context window
        prompt = ANNOTATION_PROMPT + text

        try:
            response = client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=150,
                temperature=0.1,
            )
            raw = response.choices[0].message.content.strip()

            # Parse JSON response
            try:
                parsed = json.loads(raw)
                results.append({
                    "idx": idx,
                    "context_label": parsed.get("category", "general_news"),
                    "context_confidence": parsed.get("confidence", 0.0),
                    "context_reason": parsed.get("reason", ""),
                    "raw_response": raw,
                })
            except json.JSONDecodeError:
                # Try to extract category from freeform text
                found = None
                for label in CONTEXT_LABELS:
                    if label in raw.lower():
                        found = label
                        break
                results.append({
                    "idx": idx,
                    "context_label": found or "general_news",
                    "context_confidence": 0.5,
                    "context_reason": raw[:200],
                    "raw_response": raw,
                })

        except Exception as e:
            log.warning(f"  Error on row {idx}: {e}")
            results.append({
                "idx": idx,
                "context_label": "error",
                "context_confidence": 0.0,
                "context_reason": str(e)[:200],
                "raw_response": "",
            })

        if (len(results) % 100) == 0:
            log.info(f"  Classified {len(results)}/{len(texts_df)}")

    return pd.DataFrame(results)


def classify_zero_shot(texts_df, batch_size=32):
    """Fallback: zero-shot classification with transformers pipeline."""
    try:
        from transformers import pipeline
    except ImportError:
        raise ImportError("pip install transformers torch")

    classifier = pipeline(
        "zero-shot-classification",
        model="facebook/bart-large-mnli",
        device=-1,  # CPU
    )

    results = []
    texts = texts_df["text"].str[:512].tolist()

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        outputs = classifier(batch, CONTEXT_LABELS, multi_label=False)

        for j, out in enumerate(outputs):
            results.append({
                "idx": i + j,
                "context_label": out["labels"][0],
                "context_confidence": out["scores"][0],
                "context_reason": "",
                "raw_response": "",
            })

        log.info(f"  Classified {min(i + batch_size, len(texts))}/{len(texts)}")

    return pd.DataFrame(results)


def run_classification(mode="llm", api_url=None, batch_size=32):
    ensure_cl_dirs()

    corpus_path = CL_BALANCED_DIR / "corpus.parquet"
    if not corpus_path.exists():
        raise FileNotFoundError(f"Balanced corpus not found at {corpus_path}. Run balancing first.")

    df = pd.read_parquet(corpus_path)
    log.info(f"Classifying {len(df)} texts (mode={mode})")

    if mode == "llm":
        labels_df = classify_with_llm(df, api_url=api_url)
    else:
        labels_df = classify_zero_shot(df, batch_size=batch_size)

    # Merge labels back into corpus
    df = df.reset_index(drop=True)
    for col in ["context_label", "context_confidence", "context_reason"]:
        df[col] = labels_df[col].values

    out_path = CL_CLASSIFIED_DIR / "corpus_labeled.parquet"
    df.to_parquet(out_path, index=False)
    log.info(f"Saved labeled corpus: {out_path}")

    # Distribution summary
    dist = df["context_label"].value_counts()
    log.info(f"\nContext distribution:")
    for label, count in dist.items():
        pct = count / len(df) * 100
        log.info(f"  {label}: {count} ({pct:.1f}%)")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["llm", "zero-shot"], default="llm")
    parser.add_argument("--api-url", type=str, default=None,
                        help="vLLM API URL (e.g., http://ip:8000/v1)")
    parser.add_argument("--batch-size", type=int, default=32)
    args = parser.parse_args()

    run_classification(mode=args.mode, api_url=args.api_url, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
