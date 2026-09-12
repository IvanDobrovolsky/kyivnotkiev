"""Fetch view counts for YouTube holdout candidates so the table can rank by views.

The census records video_id, title, channel and date — not statistics — so the
holdout table had nothing to sort on and fell back to recency. Only the 33,885
rows of the BigQuery export carry view_count, a fraction of the corpus.

videos.list returns statistics for 50 ids per call at one quota unit each, so
covering every holdout candidate costs a few hundred units of a 10,000/day
budget. Results are cached and merged, and an interrupted run resumes.

    python -m pipeline.repair.youtube_views --api-key "$KEY" [--per-pair 600]
"""

import argparse
import json
import pathlib
import time

import pandas as pd
import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
OUT = ROOT / "data" / "cl" / "raw" / "youtube_views"
CACHE = OUT / "views.jsonl"
API = "https://www.googleapis.com/youtube/v3/videos"
BATCH = 50


def candidate_ids(per_pair: int) -> list:
    """Top-ranked holdout candidates per pair, newest first within the window."""
    ids, seen = [], set()
    store = ROOT / "data" / "store" / "pairs"
    for f in sorted(store.glob("*.parquet")):
        try:
            d = pd.read_parquet(f, columns=["source", "variant", "date", "doc_id"])
        except Exception:                              # noqa: BLE001
            continue
        d = d[(d.source == "youtube") & d.variant.isin(["russian", "both"])
              & (d.date.astype(str) >= "2025-01")]
        for v in d.sort_values("date", ascending=False).doc_id.astype(str).head(per_pair):
            if v and v != "nan" and v not in seen:
                seen.add(v)
                ids.append(v)
    return ids


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", required=True)
    ap.add_argument("--per-pair", type=int, default=600)
    a = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)

    done = set()
    if CACHE.exists():
        for line in CACHE.read_text().splitlines():
            try:
                done.add(json.loads(line)["video_id"])
            except Exception:                          # noqa: BLE001
                continue
    want = candidate_ids(a.per_pair)
    todo = [v for v in want if v not in done]
    print(f"{len(want):,} candidates, {len(done):,} cached, {len(todo):,} to fetch "
          f"({(len(todo) + BATCH - 1) // BATCH} calls = same number of quota units)")

    got = 0
    with CACHE.open("a") as fh:
        for i in range(0, len(todo), BATCH):
            chunk = todo[i:i + BATCH]
            r = None
            for attempt in range(4):
                try:
                    r = requests.get(API, params={"part": "statistics", "id": ",".join(chunk),
                                                  "key": a.api_key, "maxResults": BATCH}, timeout=60)
                    if r.status_code == 200:
                        break
                    if r.status_code == 403 and "quota" in r.text.lower():
                        print("  quota exhausted, stopping cleanly")
                        r = None
                        break
                    time.sleep(2 * (attempt + 1))
                except requests.RequestException:
                    time.sleep(2 * (attempt + 1))
            if r is None:
                break
            for it in r.json().get("items", []):
                st = it.get("statistics", {})
                fh.write(json.dumps({"video_id": it.get("id"),
                                     "view_count": int(st.get("viewCount", 0) or 0),
                                     "like_count": int(st.get("likeCount", 0) or 0)}) + "\n")
                got += 1
            fh.flush()
            if (i // BATCH) % 20 == 0:
                print(f"  {i + len(chunk):,}/{len(todo):,} requested, {got:,} returned", flush=True)
            time.sleep(0.05)

    rows = [json.loads(l) for l in CACHE.read_text().splitlines() if l.strip()]
    pd.DataFrame(rows).drop_duplicates("video_id").to_parquet(
        OUT / "views.parquet", compression="zstd", index=False)
    print(f"done: {len(rows):,} videos with view counts -> {OUT / 'views.parquet'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
