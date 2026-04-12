"""Context classification using Gemma 4 31B via Ollama.

Classifies each text into one of 10 context categories using the
locally-running Gemma 4 model. Designed to run on the GPU box.

Reads from: data/cl/balanced/corpus.parquet
Writes to:  data/cl/classified/corpus_labeled.parquet

Usage (on GPU box):
    python3 annotate_gemma4.py [--batch-size 50] [--resume]
"""

import argparse
import json
import logging
import time
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "gemma4:31b"

CONTEXT_LABELS = [
    "politics", "war_conflict", "sports", "culture_arts",
    "food_cuisine", "travel_tourism", "academic_science",
    "history", "business_economy", "general_news",
]

PROMPT_TEMPLATE = """Classify this text into exactly ONE category. Reply with ONLY the category name, nothing else.

Categories: politics, war_conflict, sports, culture_arts, food_cuisine, travel_tourism, academic_science, history, business_economy, general_news

Text: {text}

Category:"""

SENTIMENT_PROMPT = """What is the sentiment of this text toward Ukraine? Reply with exactly one word: positive, negative, or neutral.

Text: {text}

Sentiment:"""


def query_ollama(prompt, model=MODEL):
    """Query Ollama and return the response text."""
    try:
        resp = requests.post(OLLAMA_URL, json={
            "model": model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1, "num_predict": 20},
        }, timeout=60)
        if resp.status_code != 200:
            return None
        return resp.json().get("response", "").strip()
    except Exception as e:
        log.warning(f"Ollama error: {e}")
        return None


def classify_context(text):
    """Classify a single text into a context category."""
    truncated = str(text)[:1500]
    response = query_ollama(PROMPT_TEMPLATE.format(text=truncated))
    if not response:
        return "general_news", 0.0, ""

    response_lower = response.lower().strip()
    # Match to valid label
    for label in CONTEXT_LABELS:
        if label in response_lower:
            return label, 0.9, response

    # Fuzzy match
    label_map = {
        "war": "war_conflict", "conflict": "war_conflict", "military": "war_conflict",
        "politic": "politics", "government": "politics", "diplomacy": "politics",
        "sport": "sports", "football": "sports", "soccer": "sports",
        "food": "food_cuisine", "cuisine": "food_cuisine", "recipe": "food_cuisine",
        "travel": "travel_tourism", "tourism": "travel_tourism",
        "academic": "academic_science", "science": "academic_science", "research": "academic_science",
        "histor": "history",
        "culture": "culture_arts", "art": "culture_arts", "music": "culture_arts",
        "business": "business_economy", "economy": "business_economy", "economic": "business_economy",
        "news": "general_news", "general": "general_news",
    }
    for key, label in label_map.items():
        if key in response_lower:
            return label, 0.7, response

    return "general_news", 0.3, response


def classify_sentiment(text):
    """Classify sentiment as positive/negative/neutral."""
    truncated = str(text)[:1500]
    response = query_ollama(SENTIMENT_PROMPT.format(text=truncated))
    if not response:
        return "neutral", 0.0

    response_lower = response.lower().strip()
    if "positive" in response_lower:
        return "positive", 0.5
    elif "negative" in response_lower:
        return "negative", -0.5
    return "neutral", 0.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--corpus", type=str, default="corpus.parquet")
    parser.add_argument("--output", type=str, default="corpus_labeled.parquet")
    args = parser.parse_args()

    # Paths — work relative to script location or data dir
    data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data" / "cl"
    corpus_path = data_dir / "balanced" / args.corpus
    output_path = data_dir / "classified" / args.output
    checkpoint_path = data_dir / "classified" / "annotation_checkpoint.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(corpus_path)
    log.info(f"Loaded corpus: {len(df)} texts")

    # Resume from checkpoint
    done_indices = set()
    if args.resume and checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text())
        done_indices = set(checkpoint.get("done_indices", []))
        log.info(f"Resuming: {len(done_indices)} already classified")

    # Initialize columns
    if "context_label" not in df.columns:
        df["context_label"] = ""
        df["context_confidence"] = 0.0
        df["context_reason"] = ""
        df["sentiment_label"] = ""
        df["sentiment_score"] = 0.0
        df["annotation_status"] = ""

    # Load existing results if resuming
    if args.resume and output_path.exists():
        existing = pd.read_parquet(output_path)
        if len(existing) == len(df):
            # Merge existing annotations
            for col in ["context_label", "context_confidence", "context_reason",
                        "sentiment_label", "sentiment_score", "annotation_status"]:
                if col in existing.columns:
                    df[col] = existing[col]

    total = len(df)
    classified = 0
    start_time = time.time()

    for idx in range(total):
        if idx in done_indices:
            continue

        text = df.at[idx, "text"] if "text" in df.columns else ""
        if not text or len(str(text)) < 20:
            df.at[idx, "context_label"] = "general_news"
            df.at[idx, "context_confidence"] = 0.0
            df.at[idx, "annotation_status"] = "skipped_short"
            done_indices.add(idx)
            continue

        # Context classification
        label, conf, reason = classify_context(text)
        df.at[idx, "context_label"] = label
        df.at[idx, "context_confidence"] = conf
        df.at[idx, "context_reason"] = reason[:200] if reason else ""

        # Sentiment
        sent_label, sent_score = classify_sentiment(text)
        df.at[idx, "sentiment_label"] = sent_label
        df.at[idx, "sentiment_score"] = sent_score
        df.at[idx, "annotation_status"] = "classified"

        done_indices.add(idx)
        classified += 1

        # Checkpoint every batch_size
        if classified % args.batch_size == 0:
            elapsed = time.time() - start_time
            rate = classified / elapsed if elapsed > 0 else 0
            eta = (total - len(done_indices)) / rate / 60 if rate > 0 else 0
            log.info(f"  {len(done_indices)}/{total} ({len(done_indices)/total*100:.1f}%) "
                     f"| {rate:.1f} texts/s | ETA {eta:.0f} min")

            # Save checkpoint
            checkpoint_path.write_text(json.dumps({"done_indices": list(done_indices)}))
            df.to_parquet(output_path, index=False)

    # Final save
    df.to_parquet(output_path, index=False)
    checkpoint_path.write_text(json.dumps({"done_indices": list(done_indices)}))

    elapsed = time.time() - start_time
    log.info(f"\nDone: {classified} texts classified in {elapsed/60:.1f} min")
    log.info(f"Context distribution:")
    log.info(df.context_label.value_counts().to_string())
    log.info(f"\nSaved: {output_path}")


if __name__ == "__main__":
    main()
