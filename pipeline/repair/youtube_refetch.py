"""Re-fetch YouTube titles and descriptions the phone pattern destroyed.

The census kept titles only, so descriptions had no clean local copy — and
YouTube titles are full of dates ("Adam K LIVE 2009.12.12 Godskitchen"), which
the old phone pattern read as phone numbers. videos.list returns 50 ids per
call at one unit each, so the whole repair costs a few hundred units of a
10,000/day budget.

Writes data/cl/raw/youtube_refetch/refetch.parquet, which store_restore then
treats as another clean upstream copy.

    python -m pipeline.repair.youtube_refetch --api-key "$KEY"
"""

import argparse
import json
import pathlib
import time

import pandas as pd
import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
STORE = ROOT / "data" / "store" / "pairs"
OUTDIR = ROOT / "data" / "cl" / "raw" / "youtube_refetch"
CACHE = OUTDIR / "refetch.jsonl"
API = "https://www.googleapis.com/youtube/v3/videos"
BATCH = 50


def damaged_ids() -> set:
    ids = set()
    for f in sorted(STORE.glob("*.parquet")):
        try:
            d = pd.read_parquet(f, columns=["source", "doc_id", "title", "text", "match_context"])
        except Exception:                              # noqa: BLE001
            continue
        m = d.source.eq("youtube")
        if not m.any():
            continue
        dmg = m & d[["title", "text", "match_context"]].fillna("").astype(str).apply(
            lambda s: s.str.contains(r"\[phone\]", regex=True)).any(axis=1)
        ids |= set(d.loc[dmg, "doc_id"].astype(str))
    return {i for i in ids if i and i != "nan"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", required=True)
    a = ap.parse_args()
    OUTDIR.mkdir(parents=True, exist_ok=True)

    want = damaged_ids()
    done = set()
    if CACHE.exists():
        for line in CACHE.read_text().splitlines():
            try:
                done.add(json.loads(line)["video_id"])
            except Exception:                          # noqa: BLE001
                continue
    todo = sorted(want - done)
    print(f"{len(want):,} damaged video ids, {len(done):,} cached, {len(todo):,} to fetch "
          f"({(len(todo) + BATCH - 1) // BATCH} calls, same number of quota units)")

    got = 0
    with CACHE.open("a") as fh:
        for i in range(0, len(todo), BATCH):
            chunk = todo[i:i + BATCH]
            for attempt in range(4):
                try:
                    r = requests.get(API, params={"part": "snippet", "id": ",".join(chunk),
                                                  "key": a.api_key, "maxResults": BATCH},
                                     timeout=60)
                    if r.status_code == 200:
                        break
                    if r.status_code == 403 and "quota" in r.text.lower():
                        print("  quota exhausted, stopping cleanly")
                        r = None
                        break
                    time.sleep(2 * (attempt + 1))
                except requests.RequestException:
                    time.sleep(2 * (attempt + 1))
            else:
                continue
            if r is None:
                break
            for it in r.json().get("items", []):
                sn = it.get("snippet", {})
                fh.write(json.dumps({"video_id": it.get("id"),
                                     "title": sn.get("title") or "",
                                     "description": sn.get("description") or ""}) + "\n")
                got += 1
            fh.flush()
            if (i // BATCH) % 20 == 0:
                print(f"  {i + len(chunk):,}/{len(todo):,} requested, {got:,} returned", flush=True)
            time.sleep(0.05)

    rows = [json.loads(l) for l in CACHE.read_text().splitlines() if l.strip()]
    pd.DataFrame(rows).drop_duplicates("video_id").to_parquet(
        OUTDIR / "refetch.parquet", compression="zstd", index=False)
    print(f"done: {len(rows):,} records -> {OUTDIR / 'refetch.parquet'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
