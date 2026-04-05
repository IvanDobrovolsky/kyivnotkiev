"""Annotation script to run ON the vast.ai instance.

Reads corpus.parquet, annotates with Llama 70B via local vLLM,
saves corpus_labeled.parquet.

Usage (on vast.ai):
    # Terminal 1: start vLLM server (see setup.sh)
    # Terminal 2:
    python3 annotate.py [--api-url http://localhost:8000/v1] [--batch-size 50]
"""

import argparse
import json
import logging
import time
from pathlib import Path

import pandas as pd
from openai import OpenAI

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

DATA_DIR = Path("data")

CONTEXT_LABELS = [
    "politics", "war_conflict", "sports", "culture_arts", "food_cuisine",
    "travel_tourism", "academic_science", "history", "business_economy", "general_news",
]

CONTEXT_PROMPT = """You are an expert annotator for a computational linguistics study on Ukrainian toponym adoption in global media.

Classify this text into ONE primary context category. The text may be in English, Ukrainian, Russian, or other languages.

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

Also rate the sentiment as positive, neutral, or negative (about the topic, not the spelling).

Respond with ONLY valid JSON, no other text:
{"category": "<category>", "confidence": <0.0-1.0>, "sentiment": "<positive|neutral|negative>", "sentiment_score": <-1.0 to 1.0>, "reason": "<10 words max>"}

Text: """


def annotate_batch(client, model, texts, indices):
    """Annotate a batch of texts."""
    results = []
    for idx, text in zip(indices, texts):
        text_truncated = text[:2000] if text else ""
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": CONTEXT_PROMPT + text_truncated}],
                max_tokens=120,
                temperature=0.05,
            )
            raw = response.choices[0].message.content.strip()

            # Parse JSON
            try:
                # Handle markdown code blocks
                if raw.startswith("```"):
                    raw = raw.split("```")[1]
                    if raw.startswith("json"):
                        raw = raw[4:]
                parsed = json.loads(raw)
                results.append({
                    "idx": idx,
                    "context_label": parsed.get("category", "general_news"),
                    "context_confidence": float(parsed.get("confidence", 0.0)),
                    "sentiment_label": parsed.get("sentiment", "neutral"),
                    "sentiment_score": float(parsed.get("sentiment_score", 0.0)),
                    "context_reason": parsed.get("reason", ""),
                    "annotation_raw": raw[:300],
                    "annotation_status": "ok",
                })
            except (json.JSONDecodeError, ValueError):
                # Try to extract from freeform text
                found_cat = "general_news"
                for label in CONTEXT_LABELS:
                    if label in raw.lower():
                        found_cat = label
                        break
                found_sent = "neutral"
                for s in ["positive", "negative", "neutral"]:
                    if s in raw.lower():
                        found_sent = s
                        break
                results.append({
                    "idx": idx,
                    "context_label": found_cat,
                    "context_confidence": 0.5,
                    "sentiment_label": found_sent,
                    "sentiment_score": 0.0,
                    "context_reason": "",
                    "annotation_raw": raw[:300],
                    "annotation_status": "parse_fallback",
                })

        except Exception as e:
            results.append({
                "idx": idx,
                "context_label": "error",
                "context_confidence": 0.0,
                "sentiment_label": "neutral",
                "sentiment_score": 0.0,
                "context_reason": str(e)[:100],
                "annotation_raw": "",
                "annotation_status": "error",
            })

    return results


def detect_model(client):
    """Auto-detect which model the vLLM server is running."""
    try:
        models = client.models.list()
        model_id = models.data[0].id
        log.info(f"Detected model: {model_id}")
        return model_id
    except Exception:
        return "meta-llama/Llama-3.1-70B-Instruct"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", default="http://localhost:8000/v1")
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint")
    args = parser.parse_args()

    # Load corpus
    corpus_path = DATA_DIR / "corpus.parquet"
    df = pd.read_parquet(corpus_path)
    log.info(f"Loaded corpus: {len(df)} texts")

    # Resume support
    checkpoint_path = DATA_DIR / "annotation_checkpoint.parquet"
    start_idx = 0
    existing_results = []

    if args.resume and checkpoint_path.exists():
        checkpoint_df = pd.read_parquet(checkpoint_path)
        start_idx = len(checkpoint_df)
        existing_results = checkpoint_df.to_dict("records")
        log.info(f"Resuming from checkpoint: {start_idx} already annotated")

    # Connect to vLLM
    client = OpenAI(base_url=args.api_url, api_key="dummy")
    model = detect_model(client)

    # Quick validation on first 3 texts
    log.info("Validation run (3 texts)...")
    test_results = annotate_batch(
        client, model,
        df["text"].iloc[:3].tolist(),
        list(range(3))
    )
    for r in test_results:
        log.info(f"  [{r['annotation_status']}] {r['context_label']} / {r['sentiment_label']} — {r['context_reason']}")

    ok_count = sum(1 for r in test_results if r["annotation_status"] == "ok")
    if ok_count == 0:
        log.error("All 3 validation texts failed. Check model and prompt.")
        return

    log.info(f"Validation passed ({ok_count}/3 OK). Starting full annotation...")

    # Full annotation
    all_results = existing_results.copy()
    texts = df["text"].tolist()
    total = len(texts)
    t0 = time.time()

    for i in range(start_idx, total, args.batch_size):
        batch_end = min(i + args.batch_size, total)
        batch_texts = texts[i:batch_end]
        batch_indices = list(range(i, batch_end))

        batch_results = annotate_batch(client, model, batch_texts, batch_indices)
        all_results.extend(batch_results)

        # Progress
        done = len(all_results)
        elapsed = time.time() - t0
        rate = (done - start_idx) / elapsed if elapsed > 0 else 0
        eta_min = (total - done) / rate / 60 if rate > 0 else 0

        errors = sum(1 for r in all_results if r["annotation_status"] == "error")
        fallbacks = sum(1 for r in all_results if r["annotation_status"] == "parse_fallback")

        log.info(
            f"  {done}/{total} ({done/total*100:.1f}%) "
            f"| {rate:.1f} texts/sec "
            f"| ETA {eta_min:.0f} min "
            f"| errors={errors} fallbacks={fallbacks}"
        )

        # Checkpoint every 500 texts
        if done % 500 < args.batch_size:
            results_df = pd.DataFrame(all_results)
            results_df.to_parquet(checkpoint_path, index=False)

    # Final save
    results_df = pd.DataFrame(all_results)

    # Merge into corpus
    for col in ["context_label", "context_confidence", "sentiment_label",
                 "sentiment_score", "context_reason", "annotation_status"]:
        df[col] = results_df[col].values

    out_path = DATA_DIR / "corpus_labeled.parquet"
    df.to_parquet(out_path, index=False)
    log.info(f"\nSaved labeled corpus: {out_path}")

    # Summary
    log.info("\n" + "=" * 60)
    log.info("ANNOTATION SUMMARY")
    log.info("=" * 60)
    log.info(f"Total: {len(df)} texts")
    log.info(f"Status: {results_df['annotation_status'].value_counts().to_dict()}")
    log.info(f"\nContext distribution:")
    for label, count in df["context_label"].value_counts().items():
        pct = count / len(df) * 100
        log.info(f"  {label:20s}: {count:5d} ({pct:.1f}%)")
    log.info(f"\nSentiment distribution:")
    for label, count in df["sentiment_label"].value_counts().items():
        pct = count / len(df) * 100
        log.info(f"  {label:10s}: {count:5d} ({pct:.1f}%)")

    # Per-variant sentiment comparison
    log.info("\nSentiment by variant:")
    for variant in ["russian", "ukrainian"]:
        vdf = df[df["variant"] == variant]
        if not vdf.empty:
            mean_score = vdf["sentiment_score"].mean()
            log.info(f"  {variant}: mean={mean_score:.3f}, n={len(vdf)}")

    # Cleanup checkpoint
    if checkpoint_path.exists():
        checkpoint_path.unlink()
        log.info("Checkpoint cleaned up")


if __name__ == "__main__":
    main()
