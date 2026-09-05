"""Stream-filter a monthly Reddit dump (.zst) for the study's surface forms.

Reads zstd-decompressed NDJSON on stdin, applies a cheap byte-level prefilter
(combined regex over the raw line) before JSON-parsing candidates, and writes
matching records as NDJSON to stdout. Word-bounded, case-insensitive exact
matching over title+selftext (submissions) or body (comments) — the same
matching class as the PullPush ingest.

    zstd -dc RS_2025-05.zst | python -m pipeline.ingestion.reddit_dump_filter \
        --kind submission > matches.jsonl
"""
import argparse
import json
import re
import sys

import yaml


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--kind", choices=["submission", "comment"], required=True)
    a = ap.parse_args()

    cfg = yaml.safe_load(open("config/pairs.yaml"))
    terms = sorted({t for p in cfg["pairs"] if p.get("enabled", True)
                    for t in (p["ukrainian"], p["russian"])}, key=len, reverse=True)
    rx = re.compile(r"\b(?:" + "|".join(
        re.escape(t).replace(r"\ ", r"\s+") for t in terms) + r")\b", re.I)
    pre = re.compile("|".join(re.escape(t.split()[0]) for t in terms).encode(), re.I)

    n_in = n_out = 0
    out = sys.stdout
    for raw in sys.stdin.buffer:
        n_in += 1
        if not pre.search(raw):
            continue
        try:
            d = json.loads(raw)
        except Exception:                              # noqa: BLE001
            continue
        text = ((d.get("title") or "") + " " + (d.get("selftext") or "")
                if a.kind == "submission" else (d.get("body") or ""))
        if not rx.search(text):
            continue
        keep = {k: d.get(k) for k in (
            "id", "subreddit", "author", "created_utc", "title", "selftext",
            "body", "score", "num_comments", "permalink", "link_id")}
        keep["kind"] = a.kind
        out.write(json.dumps(keep, ensure_ascii=False) + "\n")
        n_out += 1
        if n_out % 2000 == 0:
            print(f"  {n_out:,} matches / {n_in:,} lines", file=sys.stderr, flush=True)
    print(f"DONE {a.kind}: {n_out:,} matches from {n_in:,} lines", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
