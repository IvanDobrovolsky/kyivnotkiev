"""Extract paper titles and abstracts from OpenAlex API for all enabled pairs.

Searches OpenAlex for each pair's russian and ukrainian terms in paper
titles, reconstructs abstracts from inverted indexes, and writes per-pair
parquet files to data/cl/raw/openalex/.

Usage:
    python -m pipeline.cl.extract.openalex_texts [--pair-ids 1,3,10] [--max-per-variant 500]
"""

import argparse
import logging
import time

import pandas as pd
import requests

from pipeline.cl.config import CL_RAW_DIR, ensure_cl_dirs
from pipeline.config import get_enabled_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OPENALEX_API = "https://api.openalex.org/works"
MAILTO = "ivan@kyivnotkiev.org"
REQUEST_DELAY = 0.2  # polite pool allows 10 req/s
PER_PAGE = 200  # OpenAlex max
DEFAULT_MAX_PER_VARIANT = 500

OUT_DIR = CL_RAW_DIR / "openalex"


def reconstruct_abstract(inverted_index: dict | None) -> str:
    """Reconstruct plain text from OpenAlex abstract_inverted_index.

    The field maps each word to a list of integer positions where that word
    appears. We invert this to build the full abstract string.
    """
    if not inverted_index:
        return ""
    word_positions: list[tuple[int, str]] = []
    for word, positions in inverted_index.items():
        for pos in positions:
            word_positions.append((pos, word))
    word_positions.sort(key=lambda x: x[0])
    return " ".join(w for _, w in word_positions)


def fetch_works_for_term(
    term: str,
    max_works: int = DEFAULT_MAX_PER_VARIANT,
) -> list[dict]:
    """Fetch works with term in title using cursor pagination.

    Returns a list of raw work dicts from the OpenAlex API.
    """
    works = []
    cursor = "*"

    while cursor and len(works) < max_works:
        params = {
            "filter": f"title.search:{term}",
            "per_page": min(PER_PAGE, max_works - len(works)),
            "cursor": cursor,
            "mailto": MAILTO,
            "select": "id,title,abstract_inverted_index,publication_year,cited_by_count",
        }
        try:
            resp = requests.get(OPENALEX_API, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, ValueError) as e:
            log.warning(f"  OpenAlex request failed for '{term}': {e}")
            break

        results = data.get("results", [])
        if not results:
            break

        works.extend(results)

        meta = data.get("meta", {})
        cursor = meta.get("next_cursor")

        time.sleep(REQUEST_DELAY)

    return works[:max_works]


def works_to_rows(
    works: list[dict],
    pair_id: int,
    variant: str,
    matched_term: str,
) -> list[dict]:
    """Convert raw OpenAlex work dicts into flat row dicts."""
    rows = []
    for w in works:
        title = (w.get("title") or "").strip()
        abstract = reconstruct_abstract(w.get("abstract_inverted_index"))
        text = title + ("\n\n" + abstract if abstract else "")
        text = text.strip()

        rows.append({
            "pair_id": pair_id,
            "openalex_id": w.get("id", ""),
            "title": title,
            "abstract": abstract,
            "text": text,
            "variant": variant,
            "matched_term": matched_term,
            "year": w.get("publication_year"),
            "cited_by_count": w.get("cited_by_count", 0),
            "source": "openalex",
            "word_count": len(text.split()) if text else 0,
        })
    return rows


def extract_pair(pair: dict, max_per_variant: int) -> list[dict]:
    """Extract OpenAlex works for one pair (both variants)."""
    pid = pair["id"]
    russian = pair["russian"]
    ukrainian = pair["ukrainian"]

    log.info(f"  Pair {pid}: '{russian}' vs '{ukrainian}'")

    rows = []

    # Russian variant
    ru_works = fetch_works_for_term(russian, max_works=max_per_variant)
    ru_rows = works_to_rows(ru_works, pid, "russian", russian)
    rows.extend(ru_rows)

    # Ukrainian variant
    ua_works = fetch_works_for_term(ukrainian, max_works=max_per_variant)
    ua_rows = works_to_rows(ua_works, pid, "ukrainian", ukrainian)
    rows.extend(ua_rows)

    log.info(f"    {len(ru_rows)} Russian, {len(ua_rows)} Ukrainian works")
    return rows


def extract_openalex(
    pair_ids: list[int] | None = None,
    max_per_variant: int = DEFAULT_MAX_PER_VARIANT,
):
    """Extract OpenAlex texts for all enabled pairs."""
    ensure_cl_dirs()
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    pairs = get_enabled_pairs()
    pairs = [p for p in pairs if not p.get("is_control", False)]
    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    log.info(f"Extracting OpenAlex texts for {len(pairs)} pairs (max {max_per_variant}/variant)")

    all_rows: list[dict] = []

    for pair in pairs:
        rows = extract_pair(pair, max_per_variant)
        if rows:
            pdf = pd.DataFrame(rows)
            out_path = OUT_DIR / f"pair_{pair['id']}.parquet"
            pdf.to_parquet(out_path, index=False)
            all_rows.extend(rows)

    if all_rows:
        df = pd.DataFrame(all_rows)
        combined_path = OUT_DIR / "all_pairs.parquet"
        df.to_parquet(combined_path, index=False)

        # Summary
        total = len(df)
        ru = len(df[df["variant"] == "russian"])
        ua = len(df[df["variant"] == "ukrainian"])
        n_pairs = df["pair_id"].nunique()
        log.info(f"Saved {total} OpenAlex texts ({ru} RU, {ua} UA) across {n_pairs} pairs to {OUT_DIR}")
    else:
        log.warning("No OpenAlex texts found for any pair")


def main():
    parser = argparse.ArgumentParser(description="Extract OpenAlex paper titles and abstracts")
    parser.add_argument("--pair-ids", type=str, default=None,
                        help="Comma-separated pair IDs to process (default: all enabled)")
    parser.add_argument("--max-per-variant", type=int, default=DEFAULT_MAX_PER_VARIANT,
                        help=f"Max works per variant per pair (default: {DEFAULT_MAX_PER_VARIANT})")
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    extract_openalex(pair_ids=pair_ids, max_per_variant=args.max_per_variant)


if __name__ == "__main__":
    main()
