"""NER-based disambiguation for ambiguous toponym pairs.

Uses spaCy NER to classify whether a matched term refers to a
location (GPE/LOC) or a person (PERSON) in context. Applies to
all pairs where the term is both a place name and a common surname
or other entity type.

Usage:
    python -m pipeline.analysis.ner_disambiguate
"""

import logging
import re
from pathlib import Path

import pandas as pd
import spacy

from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# Pairs with known homonym ambiguity (place vs person/thing)
AMBIGUOUS_PAIRS = {
    6: {"term_ru": "Nikolaev", "term_ua": "Mykolaiv", "ambiguity": "surname"},
    3: {"term_ru": "Odessa", "term_ua": "Odesa", "ambiguity": "us_city"},
    9: {"term_ru": "Rovno", "term_ua": "Rivne", "ambiguity": "adverb"},
}

# All CL raw sources that contain article text
CL_SOURCES = [
    "gdelt", "gdelt_articles", "reddit", "youtube", "openalex", "telegram", "wikipedia",
]


def load_texts_for_pair(pair_id: int) -> pd.DataFrame:
    """Load all CL corpus texts for a given pair across all sources."""
    frames = []
    raw_dir = ROOT_DIR / "data" / "cl" / "raw"
    for src in CL_SOURCES:
        src_dir = raw_dir / src
        if not src_dir.exists():
            continue
        for f in src_dir.glob("*.parquet"):
            if "checkpoint" in f.name or "session" in f.name or "fetch" in f.name or "all_posts" in f.name:
                continue
            try:
                df = pd.read_parquet(f)
                if "pair_id" in df.columns and "text" in df.columns:
                    df["pair_id"] = df["pair_id"].astype(int)
                    sub = df[df["pair_id"] == pair_id][["pair_id", "text", "variant"]].copy()
                    sub["source_file"] = f.name
                    if len(sub):
                        frames.append(sub)
            except Exception:
                continue
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates(subset=["text"])


def classify_entity_type(nlp, text: str, term: str) -> str:
    """Use NER to determine if term is used as GPE/LOC or PERSON in text.

    Returns: 'location', 'person', 'other', or 'unknown'
    """
    doc = nlp(text[:5000])  # cap length for speed

    term_lower = term.lower()
    for ent in doc.ents:
        if term_lower in ent.text.lower():
            if ent.label_ in ("GPE", "LOC", "FAC"):
                return "location"
            elif ent.label_ in ("PERSON", "PER"):
                return "person"
            elif ent.label_ in ("ORG",):
                return "organization"
            elif ent.label_ in ("NORP",):
                return "demonym"
            else:
                return "other"

    # Term not found as named entity — check if it appears at all
    if term_lower in text.lower():
        return "unknown"
    return "not_found"


def disambiguate_pair(nlp, pair_id: int, info: dict) -> dict:
    """Run NER disambiguation on all texts for a pair."""
    log.info(f"Pair {pair_id} ({info['term_ru']}/{info['term_ua']})...")

    texts = load_texts_for_pair(pair_id)
    if texts.empty:
        log.info(f"  No texts found")
        return {"pair_id": pair_id, "total": 0}

    log.info(f"  {len(texts):,} texts loaded")

    # Classify each text
    results = {"location": 0, "person": 0, "organization": 0, "demonym": 0, "other": 0, "unknown": 0, "not_found": 0}
    classified = []

    for _, row in texts.iterrows():
        text = str(row.get("text", ""))
        variant = row.get("variant", "")

        # Determine which term to check
        term = info["term_ru"] if variant == "russian" else info["term_ua"]

        etype = classify_entity_type(nlp, text, term)
        results[etype] += 1
        classified.append({
            "pair_id": pair_id,
            "variant": variant,
            "entity_type": etype,
            "text_preview": text[:200],
        })

    total = sum(results.values())
    location_pct = results["location"] / total * 100 if total > 0 else 0
    person_pct = results["person"] / total * 100 if total > 0 else 0

    log.info(f"  Results: {results}")
    log.info(f"  Location: {location_pct:.1f}%, Person: {person_pct:.1f}%")
    log.info(f"  Contamination estimate: {person_pct:.1f}% (person mentions)")

    return {
        "pair_id": pair_id,
        "total": total,
        "results": results,
        "location_pct": round(location_pct, 1),
        "person_pct": round(person_pct, 1),
        "classified": classified,
    }


def main():
    log.info("Loading spaCy model...")
    try:
        nlp = spacy.load("en_core_web_trf")
    except OSError:
        nlp = spacy.load("en_core_web_lg")
    log.info(f"Model loaded: {nlp.meta['name']}")

    all_results = {}
    for pair_id, info in AMBIGUOUS_PAIRS.items():
        result = disambiguate_pair(nlp, pair_id, info)
        all_results[pair_id] = result

    # Also run on ALL pairs to detect unexpected ambiguity
    log.info("\nScanning all pairs for unexpected person-entity contamination...")
    import yaml
    with open(ROOT_DIR / "config" / "pairs.yaml") as f:
        cfg = yaml.safe_load(f)

    for p in cfg["pairs"]:
        if not p.get("enabled") or p.get("is_control"):
            continue
        pid = p["id"]
        if pid in AMBIGUOUS_PAIRS:
            continue  # already done

        texts = load_texts_for_pair(pid)
        if len(texts) < 10:
            continue

        # Sample 50 texts max for speed
        sample = texts.sample(min(50, len(texts)), random_state=42)
        person_count = 0
        total = 0
        for _, row in sample.iterrows():
            text = str(row.get("text", ""))
            variant = row.get("variant", "")
            term = p["russian"] if variant == "russian" else p["ukrainian"]
            etype = classify_entity_type(nlp, text, term)
            total += 1
            if etype == "person":
                person_count += 1

        if person_count > 0:
            pct = person_count / total * 100
            log.info(f"  Pair {pid} ({p['russian']}/{p['ukrainian']}): {pct:.0f}% person ({person_count}/{total})")
            all_results[pid] = {
                "pair_id": pid,
                "total": total,
                "person_pct": round(pct, 1),
                "person_count": person_count,
                "sample_size": total,
            }

    # Save results
    import json
    out_path = ROOT_DIR / "data" / "cl" / "annotation" / "ner_disambiguation.json"
    with open(out_path, "w") as f:
        json.dump({str(k): {kk: vv for kk, vv in v.items() if kk != "classified"}
                   for k, v in all_results.items()}, f, indent=2)
    log.info(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
