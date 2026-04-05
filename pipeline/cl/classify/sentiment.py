"""Sentiment analysis per variant — do texts using 'Kiev' vs 'Kyiv' differ in tone?

Two modes:
  1. LLM annotation: Llama 70B rates sentiment with explanation
  2. Encoder fallback: cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual

Usage:
    python -m pipeline.cl.classify.sentiment [--mode llm|encoder]
"""

import argparse
import json
import logging

import pandas as pd

from pipeline.cl.config import (
    CL_CLASSIFIED_DIR, SENTIMENT_LABELS, LLM_ANNOTATOR, ensure_cl_dirs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

SENTIMENT_PROMPT = """Rate the sentiment of this text as positive, neutral, or negative.
Consider the overall tone, not just the topic. War coverage can be neutral (factual reporting) or negative (emotional).

Respond with ONLY a JSON object:
{{"sentiment": "<positive|neutral|negative>", "score": <-1.0 to 1.0>, "reason": "<brief>"}}

Text: """


def classify_sentiment_llm(texts_df, api_url):
    from openai import OpenAI
    client = OpenAI(base_url=api_url, api_key="dummy")
    results = []

    for idx, row in texts_df.iterrows():
        text = row["text"][:1500]
        try:
            response = client.chat.completions.create(
                model=LLM_ANNOTATOR,
                messages=[{"role": "user", "content": SENTIMENT_PROMPT + text}],
                max_tokens=100,
                temperature=0.1,
            )
            raw = response.choices[0].message.content.strip()
            try:
                parsed = json.loads(raw)
                results.append({
                    "idx": idx,
                    "sentiment_label": parsed.get("sentiment", "neutral"),
                    "sentiment_score": parsed.get("score", 0.0),
                    "sentiment_reason": parsed.get("reason", ""),
                })
            except json.JSONDecodeError:
                label = "neutral"
                for s in SENTIMENT_LABELS:
                    if s in raw.lower():
                        label = s
                        break
                results.append({
                    "idx": idx,
                    "sentiment_label": label,
                    "sentiment_score": 0.0,
                    "sentiment_reason": raw[:200],
                })
        except Exception as e:
            results.append({
                "idx": idx,
                "sentiment_label": "error",
                "sentiment_score": 0.0,
                "sentiment_reason": str(e)[:200],
            })

        if (len(results) % 100) == 0:
            log.info(f"  Sentiment: {len(results)}/{len(texts_df)}")

    return pd.DataFrame(results)


def classify_sentiment_encoder(texts_df, batch_size=32):
    from transformers import pipeline
    classifier = pipeline(
        "sentiment-analysis",
        model="cardiffnlp/twitter-xlm-roberta-base-sentiment-multilingual",
        device=-1,
        top_k=None,
    )

    results = []
    texts = texts_df["text"].str[:512].tolist()

    label_map = {"positive": "positive", "neutral": "neutral", "negative": "negative"}

    for i in range(0, len(texts), batch_size):
        batch = texts[i:i + batch_size]
        outputs = classifier(batch)
        for j, scores in enumerate(outputs):
            # scores is list of {label, score}
            best = max(scores, key=lambda x: x["score"])
            score_val = best["score"] if best["label"] == "positive" else -best["score"] if best["label"] == "negative" else 0.0
            results.append({
                "idx": i + j,
                "sentiment_label": label_map.get(best["label"], best["label"]),
                "sentiment_score": score_val,
                "sentiment_reason": "",
            })

        log.info(f"  Sentiment: {min(i + batch_size, len(texts))}/{len(texts)}")

    return pd.DataFrame(results)


def run_sentiment(mode="llm", api_url=None, batch_size=32):
    ensure_cl_dirs()

    # Load context-labeled corpus
    corpus_path = CL_CLASSIFIED_DIR / "corpus_labeled.parquet"
    if not corpus_path.exists():
        raise FileNotFoundError(f"Labeled corpus not found. Run context classification first.")

    df = pd.read_parquet(corpus_path)
    log.info(f"Running sentiment on {len(df)} texts (mode={mode})")

    if mode == "llm":
        if not api_url:
            raise ValueError("Provide --api-url for LLM sentiment")
        sent_df = classify_sentiment_llm(df, api_url)
    else:
        sent_df = classify_sentiment_encoder(df, batch_size)

    for col in ["sentiment_label", "sentiment_score", "sentiment_reason"]:
        df[col] = sent_df[col].values

    out_path = CL_CLASSIFIED_DIR / "corpus_labeled.parquet"
    df.to_parquet(out_path, index=False)
    log.info(f"Updated labeled corpus with sentiment: {out_path}")

    # Compare sentiment by variant
    for variant in ["russian", "ukrainian"]:
        vdf = df[df["variant"] == variant]
        if not vdf.empty:
            mean_score = vdf["sentiment_score"].mean()
            dist = vdf["sentiment_label"].value_counts(normalize=True)
            log.info(f"\n  {variant.upper()} variant: mean_score={mean_score:.3f}")
            for label, pct in dist.items():
                log.info(f"    {label}: {pct:.1%}")

    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["llm", "encoder"], default="llm")
    parser.add_argument("--api-url", type=str, default=None)
    parser.add_argument("--batch-size", type=int, default=32)
    args = parser.parse_args()
    run_sentiment(mode=args.mode, api_url=args.api_url, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
