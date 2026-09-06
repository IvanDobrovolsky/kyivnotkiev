"""Normalize the dump-backfill JSONLs into store-ready parquets.

Emits one row per (post, pair) with the dataset's exact conventions
(post_id, subreddit, author, title, selftext, score, variant, date,
pair_slug, matched_term, created_utc). A text matching both variants of a
pair takes the variant of the FIRST occurrence — deterministic, and the
dataset's one-variant-per-row shape is preserved. Rows whose
(post_id, pair_slug) already exist in dataset/raw_reddit.parquet are
dropped: the near-post-time capture wins over the dump snapshot.

Submissions land in backfill_submissions.parquet and are picked up by the
store migration; comments land in backfill_comments.parquet with the same
attribution, held as a separate corpus (not yet a series).
"""

import json
import pathlib
import re

import pandas as pd
import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
BF = ROOT / "data" / "raw" / "reddit" / "backfill"


def main() -> int:
    cfg = yaml.safe_load(open(ROOT / "config" / "pairs.yaml"))
    pairs = [(p["slug"], p["ukrainian"], p["russian"])
             for p in cfg["pairs"] if p.get("enabled", True)]
    rx = {slug: (re.compile(r"\b" + re.escape(ua).replace(r"\ ", r"\s+") + r"\b", re.I),
                 re.compile(r"\b" + re.escape(ru).replace(r"\ ", r"\s+") + r"\b", re.I),
                 ua, ru)
          for slug, ua, ru in pairs}

    existing = pd.read_parquet(ROOT / "dataset" / "raw_reddit.parquet",
                               columns=["post_id", "pair_slug"])
    have = set(zip(existing.post_id.astype(str), existing.pair_slug))

    out = {"submission": [], "comment": []}
    for f in sorted(BF.glob("R[SC]_*_matches.jsonl")):
        for line in open(f):
            try:
                d = json.loads(line)
            except Exception:                          # noqa: BLE001
                continue
            kind = d.get("kind", "submission")
            text = ((d.get("title") or "") + " " + (d.get("selftext") or "")
                    if kind == "submission" else (d.get("body") or ""))
            for slug, (rua, rru, tua, tru) in rx.items():
                mu, mr = rua.search(text), rru.search(text)
                if not mu and not mr:
                    continue
                if mu and mr:
                    variant, term = (("ukrainian", tua) if mu.start() <= mr.start()
                                     else ("russian", tru))
                elif mu:
                    variant, term = "ukrainian", tua
                else:
                    variant, term = "russian", tru
                pid = str(d.get("id"))
                if kind == "submission" and (pid, slug) in have:
                    continue
                ts = int(d.get("created_utc") or 0)
                out[kind].append({
                    "post_id": pid,
                    "subreddit": d.get("subreddit"),
                    "author": d.get("author"),
                    "title": d.get("title") if kind == "submission" else None,
                    "selftext": (d.get("selftext") if kind == "submission"
                                 else d.get("body")),
                    "score": int(d.get("score") or 0),
                    "variant": variant,
                    "date": pd.Timestamp(ts, unit="s").strftime("%Y-%m-%d"),
                    "pair_slug": slug,
                    "matched_term": term,
                    "created_utc": ts,
                })

    for kind, rows in out.items():
        df = pd.DataFrame(rows)
        dest = BF / f"backfill_{kind}s.parquet"
        df.to_parquet(dest, index=False)
        vc = df.variant.value_counts().to_dict() if len(df) else {}
        print(f"{kind}s: {len(df):,} pair-rows -> {dest.name}  variants={vc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
