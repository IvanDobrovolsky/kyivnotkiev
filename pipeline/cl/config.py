"""CL pipeline configuration: sample sizes, model names, label schema."""

from pathlib import Path

from pipeline.config import ROOT_DIR

# Directories
CL_DIR = ROOT_DIR / "data" / "cl"
CL_RAW_DIR = CL_DIR / "raw"
CL_BALANCED_DIR = CL_DIR / "balanced"
CL_CLASSIFIED_DIR = CL_DIR / "classified"
CL_EMBEDDINGS_DIR = CL_DIR / "embeddings"
CL_MODEL_DIR = CL_DIR / "model"
CL_EXPORT_DIR = CL_DIR / "export"

# BigQuery
BQ_DATASET = "kyivnotkiev"
BQ_PROJECT = "kyivnotkiev-research"

# Sampling
MAX_PER_VARIANT_PER_SOURCE = 500  # max texts per (pair, variant, source)
MIN_TEXTS_PER_PAIR = 20  # skip pairs with fewer total texts
YEAR_STRATA = [(2010, 2013), (2014, 2017), (2018, 2021), (2022, 2026)]

# GDELT article fetching
GDELT_FETCH_TIMEOUT = 15  # seconds per URL
GDELT_MIN_ARTICLE_WORDS = 100  # discard if shorter
GDELT_MAX_ARTICLE_WORDS = 5000  # truncate if longer
GDELT_FETCH_DELAY = 0.5  # seconds between requests
GDELT_MAX_URLS_PER_PAIR = 2000  # sample from BQ before fetching

# Context classification labels
CONTEXT_LABELS = [
    "politics",
    "war_conflict",
    "sports",
    "culture_arts",
    "food_cuisine",
    "travel_tourism",
    "academic_science",
    "history",
    "business_economy",
    "general_news",
]

# Sentiment labels
SENTIMENT_LABELS = ["positive", "neutral", "negative"]

# Models (Phase 2: LLM annotation)
LLM_ANNOTATOR = "meta-llama/Llama-3.1-70B-Instruct-AWQ"
LLM_ANNOTATOR_SMALL = "meta-llama/Llama-3.1-8B-Instruct"

# Models (Phase 3: encoder fine-tuning benchmark)
ENCODER_MODELS = {
    "deberta-v3-large": "microsoft/deberta-v3-large",
    "xlm-roberta-large": "FacebookAI/xlm-roberta-large",
    "mdeberta-v3-base": "microsoft/mdeberta-v3-base",
}

# Models (Phase 4: embeddings)
EMBEDDING_MODEL = "intfloat/multilingual-e5-large"

# Top 6 pairs for deep CL analysis
TOP_6_PAIR_IDS = [1, 3, 10, 61, 70, 72]

# All 55 pairs — populated at runtime from config
ALL_PAIR_IDS = None  # set by extract scripts


def ensure_cl_dirs():
    """Create all CL pipeline directories."""
    for d in [CL_RAW_DIR, CL_BALANCED_DIR, CL_CLASSIFIED_DIR,
              CL_EMBEDDINGS_DIR, CL_MODEL_DIR, CL_EXPORT_DIR,
              CL_RAW_DIR / "gdelt_articles",
              CL_RAW_DIR / "reddit",
              CL_RAW_DIR / "youtube",
              CL_RAW_DIR / "openalex",
              CL_EXPORT_DIR / "dataset",
              CL_EXPORT_DIR / "model"]:
        d.mkdir(parents=True, exist_ok=True)
