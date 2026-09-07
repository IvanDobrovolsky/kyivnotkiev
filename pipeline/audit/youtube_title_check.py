"""Re-verify YouTube holdout videos against their CURRENT titles.

Creators rename titles after collection (Kiev -> Kyiv), so an exhibit row
claiming "still uses the Russian spelling" can open onto a corrected title.
Every video id in the YouTube holdout tables is re-fetched via videos.list
(1 unit each from the 110K pool — not search quota) and the CURRENT title
is stored in a cache the exporter consults: rows whose current title no
longer attests the claimed form are excluded from exhibits. Series are
untouched — collection-time attestation is the measurement; exhibits must
be live-verifiable.

    python -m pipeline.audit.youtube_title_check --api-key "$KEY"
"""

import argparse
import json
import pathlib
import time

import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
HOLDOUTS = ROOT / "site" / "src" / "data" / "holdouts_by_pair.json"
CACHE = ROOT / "data" / "audit" / "youtube_titles.json"
API = "https://www.googleapis.com/youtube/v3/videos"
RECHECK_DAYS = 45


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api-key", required=True)
    a = ap.parse_args()

    h = json.loads(HOLDOUTS.read_text())
    cache = json.loads(CACHE.read_text()) if CACHE.exists() else {}
    now = time.time()

    ids = []
    for src in h.values():
        for e in (src.get("youtube") or []):
            vid = str(e.get("url", "")).split("v=")[-1].split("&")[0]
            ent = cache.get(vid)
            if vid and (not ent or now - ent.get("checked_at", 0) > RECHECK_DAYS * 86400):
                ids.append(vid)
    ids = sorted(set(ids))
    print(f"{len(ids)} video(s) to re-fetch")

    for i in range(0, len(ids), 50):
        batch = ids[i:i + 50]
        r = requests.get(API, params={
            "part": "snippet", "id": ",".join(batch), "key": a.api_key,
        }, timeout=30)
        r.raise_for_status()
        found = {}
        for item in r.json().get("items", []):
            found[item["id"]] = {
                "title": item["snippet"].get("title", ""),
                # collection verifies title OR description — the recheck must
                # look at both, or description-attested videos read as renamed
                "description": item["snippet"].get("description", "")[:800],
                "channel": item["snippet"].get("channelTitle", ""),
                "status": "live",
                "checked_at": now,
            }
        for vid in batch:
            # absent from the response = deleted/private
            cache[vid] = found.get(vid, {"status": "gone", "checked_at": now})
        time.sleep(0.3)

    CACHE.parent.mkdir(parents=True, exist_ok=True)
    CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=1))
    from collections import Counter
    print("cache totals:", dict(Counter(e.get("status", "?") for e in cache.values())))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
