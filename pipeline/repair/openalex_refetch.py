"""Re-fetch OpenAlex records whose text a PII scrub destroyed.

The phone pattern read "1904-1905" as 1|904|-|19|05 and replaced it with
"[phone]", so academic titles lost their date ranges: "the Russian-Japanese
war of [phone]", "the mass uprising of [phone]" (really 2013-2014). 2,757
works were hit. No unscrubbed copy survives locally — openalex_raw was
scrubbed too, and zero of its 41,762 rows still contain a year range — so the
text is recovered from the API it came from.

Fetches in batches of 50, appending each batch to a JSONL cache so an
interrupted run resumes instead of refetching.

    python -m pipeline.repair.openalex_refetch
"""

import json
import pathlib
import time

import requests

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
DAMAGE = ROOT / "data" / "audit" / "phone_damage.json"
CACHE = ROOT / "data" / "audit" / "openalex_refetch.jsonl"
API = "https://api.openalex.org/works"
MAILTO = "dobrovolsky94@gmail.com"
BATCH = 50


def reconstruct(inv: dict | None) -> str:
    """OpenAlex ships abstracts as {word: [positions]}; put them back in order."""
    if not inv:
        return ""
    slots: dict[int, str] = {}
    for word, positions in inv.items():
        for p in positions:
            slots[p] = word
    return " ".join(slots[i] for i in sorted(slots))


def main() -> int:
    want = json.loads(DAMAGE.read_text())["work_ids"]
    done = set()
    if CACHE.exists():
        for line in CACHE.read_text().splitlines():
            try:
                done.add(json.loads(line)["id"])
            except Exception:                          # noqa: BLE001
                continue
    todo = [w for w in want if w not in done]
    print(f"{len(want):,} works damaged, {len(done):,} already cached, {len(todo):,} to fetch")

    got = 0
    with CACHE.open("a") as fh:
        for i in range(0, len(todo), BATCH):
            chunk = todo[i:i + BATCH]
            params = {"filter": "openalex_id:" + "|".join(chunk),
                      "per-page": BATCH, "mailto": MAILTO,
                      "select": "id,title,abstract_inverted_index,publication_year"}
            for attempt in range(4):
                try:
                    r = requests.get(API, params=params, timeout=60)
                    if r.status_code == 200:
                        break
                    time.sleep(2 * (attempt + 1))
                except requests.RequestException:
                    time.sleep(2 * (attempt + 1))
            else:
                print(f"  batch {i//BATCH}: failed after retries, skipping")
                continue
            for w in r.json().get("results", []):
                rec = {"id": str(w.get("id", "")).rsplit("/", 1)[-1],
                       "title": w.get("title") or "",
                       "abstract": reconstruct(w.get("abstract_inverted_index")),
                       "year": w.get("publication_year")}
                fh.write(json.dumps(rec) + "\n")
                got += 1
            fh.flush()
            print(f"  {i + len(chunk):,}/{len(todo):,} requested, {got:,} returned", flush=True)
            time.sleep(0.2)
    print(f"done: {got:,} records written to {CACHE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
