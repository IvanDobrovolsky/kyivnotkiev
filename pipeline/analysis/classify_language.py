"""Language classification for corpus and Telegram texts.

Uses fasttext lid.176.bin (Facebook's language ID model) for speed and
reproducibility. Falls back to langdetect with fixed seed if fasttext
unavailable.

Outputs:
    data/corpus/language_labels.parquet — per-text language for corpus
    data/audit/telegram_languages.parquet — per-message language for Telegram

Usage:
    python -m pipeline.analysis.classify_language [--corpus] [--telegram] [--both]
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent.parent
CORPUS_PATH = ROOT / "data" / "corpus" / "toponyms-corpus.parquet"
TELEGRAM_PATH = ROOT / "dataset" / "raw_telegram.parquet"
CORPUS_OUT = ROOT / "data" / "corpus" / "language_labels.parquet"
TELEGRAM_OUT = ROOT / "data" / "audit" / "telegram_languages.parquet"


def get_classifier():
    """Return a language classification function. Prefers fasttext, falls back to langdetect."""
    try:
        import fasttext
        import urllib.request
        model_path = ROOT / "models" / "lid.176.bin"
        if not model_path.exists():
            log.info("Downloading fasttext language ID model...")
            model_path.parent.mkdir(parents=True, exist_ok=True)
            urllib.request.urlretrieve(
                "https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin",
                str(model_path),
            )
        model = fasttext.load_model(str(model_path))
        log.info("Using fasttext lid.176.bin")

        def classify(text: str) -> tuple[str, float]:
            if not isinstance(text, str) or len(text.strip()) < 10:
                return "short", 0.0
            clean = text.replace("\n", " ")[:512]
            pred = model.predict(clean)
            lang = pred[0][0].replace("__label__", "")
            conf = float(pred[1][0])
            return lang, conf

        return classify

    except ImportError:
        from langdetect import detect_langs, DetectorFactory
        DetectorFactory.seed = 42
        log.info("Using langdetect (seed=42 for reproducibility)")

        def classify(text: str) -> tuple[str, float]:
            if not isinstance(text, str) or len(text.strip()) < 10:
                return "short", 0.0
            try:
                results = detect_langs(text[:512])
                lang = results[0].lang
                conf = results[0].prob
                return lang, round(conf, 3)
            except Exception:
                return "unknown", 0.0

        return classify


def classify_dataframe(df: pd.DataFrame, text_col: str = "text") -> pd.DataFrame:
    """Add lang and lang_conf columns to a dataframe."""
    classify = get_classifier()
    log.info(f"Classifying {len(df):,} texts...")

    langs = []
    confs = []
    for i, text in enumerate(df[text_col]):
        lang, conf = classify(text)
        langs.append(lang)
        confs.append(conf)
        if (i + 1) % 5000 == 0:
            log.info(f"  {i+1:,}/{len(df):,}")

    df = df.copy()
    df["lang"] = langs
    df["lang_conf"] = confs
    return df


def classify_corpus():
    """Classify all corpus texts by language."""
    log.info("Loading corpus...")
    df = pd.read_parquet(CORPUS_PATH)
    df = classify_dataframe(df)

    # Summary
    log.info(f"\nCorpus language distribution:")
    for lang, n in df["lang"].value_counts().head(10).items():
        print(f"  {lang}: {n:,} ({n/len(df)*100:.1f}%)")

    non_en = df[~df["lang"].isin(["en", "short"])]
    log.info(f"Non-English texts: {len(non_en):,} ({len(non_en)/len(df)*100:.1f}%)")

    out = df[["lang", "lang_conf"]]
    CORPUS_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(CORPUS_OUT, index=False)
    log.info(f"Saved: {CORPUS_OUT}")


def classify_telegram():
    """Classify all Telegram messages by language."""
    log.info("Loading Telegram...")
    df = pd.read_parquet(TELEGRAM_PATH)
    df = classify_dataframe(df)

    log.info(f"\nTelegram language distribution:")
    for lang, n in df["lang"].value_counts().head(10).items():
        print(f"  {lang}: {n:,} ({n/len(df)*100:.1f}%)")

    en = df[df["lang"] == "en"]
    log.info(f"English messages: {len(en):,} ({len(en)/len(df)*100:.1f}%)")
    log.info(f"  By variant: {en['variant'].value_counts().to_dict()}")
    log.info(f"  Top channels:")
    for ch, n in en.groupby("channel_title").size().sort_values(ascending=False).head(5).items():
        log.info(f"    {ch}: {n}")

    out = df[["lang", "lang_conf"]]
    TELEGRAM_OUT.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(TELEGRAM_OUT, index=False)
    log.info(f"Saved: {TELEGRAM_OUT}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--corpus", action="store_true")
    parser.add_argument("--telegram", action="store_true")
    parser.add_argument("--both", action="store_true")
    args = parser.parse_args()

    if args.both or (not args.corpus and not args.telegram):
        args.corpus = True
        args.telegram = True

    if args.telegram:
        classify_telegram()
    if args.corpus:
        classify_corpus()


if __name__ == "__main__":
    main()
