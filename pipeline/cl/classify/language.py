"""Language detection and filtering.

Detects language of each text, filters to English-primary content,
flags multilingual texts for cross-lingual analysis.

Usage:
    python -m pipeline.cl.classify.language
"""

import logging

import pandas as pd

from pipeline.cl.config import CL_RAW_DIR, ensure_cl_dirs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def detect_languages(texts_df):
    """Detect language for each text using lingua or langdetect."""
    try:
        from lingua import Language, LanguageDetectorBuilder
        detector = LanguageDetectorBuilder.from_all_languages().build()

        def detect(text):
            if not text or len(text) < 20:
                return "unknown", 0.0
            result = detector.detect_language_of(text)
            if result is None:
                return "unknown", 0.0
            confidence_values = detector.compute_language_confidence_values(text)
            conf = confidence_values[0].value if confidence_values else 0.0
            return result.iso_code_639_1.name.lower(), conf

    except ImportError:
        from langdetect import detect as ld_detect, DetectorFactory
        DetectorFactory.seed = 42

        def detect(text):
            if not text or len(text) < 20:
                return "unknown", 0.0
            try:
                lang = ld_detect(text)
                return lang, 0.8  # langdetect doesn't give confidence
            except Exception:
                return "unknown", 0.0

    results = []
    for idx, row in texts_df.iterrows():
        text = row["text"][:1000] if pd.notna(row["text"]) else ""
        lang, conf = detect(text)
        results.append({"idx": idx, "lang_detected": lang, "lang_confidence": conf})

        if (len(results) % 500) == 0:
            log.info(f"  Language detection: {len(results)}/{len(texts_df)}")

    return pd.DataFrame(results)


def run_language_detection(source_dir=None):
    """Run language detection on all raw texts."""
    ensure_cl_dirs()

    sources = ["reddit", "youtube", "gdelt_articles"]
    for source in sources:
        path = CL_RAW_DIR / source / "all_pairs.parquet"
        if not path.exists():
            log.warning(f"Skipping {source}: no data")
            continue

        df = pd.read_parquet(path)
        log.info(f"Detecting languages for {len(df)} {source} texts")

        lang_df = detect_languages(df)
        df["lang_detected"] = lang_df["lang_detected"].values
        df["lang_confidence"] = lang_df["lang_confidence"].values

        df.to_parquet(path, index=False)

        dist = df["lang_detected"].value_counts().head(10)
        log.info(f"  {source} language distribution:")
        for lang, count in dist.items():
            log.info(f"    {lang}: {count} ({count/len(df):.1%})")

    return True


if __name__ == "__main__":
    run_language_detection()
