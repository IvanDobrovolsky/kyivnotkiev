"""Fold browser-refetch recoveries into the master text corpus. Idempotent.

Approved 2026-08-31 after the merge audit (merge_audit.csv): 72,960 net-new
texts, homonym-filtered, body-deduped. Rows in article_texts.parquet that
previously carried status 403 / too_short and no text receive the recovered
body and its classification. A pre-merge backup is written once per day.

Re-running after the oaoa overnight pass folds in only the new arrivals.

    python -m pipeline.merge_browser_refetch
"""
from __future__ import annotations

import datetime
import pathlib
import re
import shutil

import pandas as pd
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
MASTER = ROOT / "data" / "raw" / "gdelt" / "texts" / "article_texts.parquet"
PARTS = ROOT / "data" / "raw" / "gdelt" / "texts" / "browser_refetch" / "parts"


def main() -> int:
    parts = sorted(PARTS.glob("part-*.parquet"))
    rec = pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
    rec = rec[rec.text.notna() & (rec.text_len >= 300)].drop_duplicates("url", keep="last")
    print(f"recovered pool: {len(rec):,} texts")

    cfg = yaml.safe_load(open(ROOT / "config" / "pairs.yaml"))
    pats = {p["slug"]: [re.compile(f, re.I) for f in p.get("homonym_filters", [])]
            for p in cfg["pairs"] if p.get("homonym_filters")}
    fp = rec.apply(lambda r: bool(pats.get(r.pair_slug)
                                  and any(x.search(str(r.text)) for x in pats[r.pair_slug])), axis=1)
    rec = rec[~fp]
    print(f"after homonym filter: {len(rec):,} (-{int(fp.sum()):,})")

    m = pd.read_parquet(MASTER)
    known_hash = set(m.text_hash.dropna())
    rec = rec[~rec.text_hash.isin(known_hash)]
    # only fill rows that still have no text — never overwrite an existing body
    empty_urls = set(m.loc[m.text.isna(), "url"])
    rec = rec[rec.url.isin(empty_urls)]
    print(f"net new to merge: {len(rec):,}")
    if not len(rec):
        print("nothing to merge")
        return 0

    stamp = datetime.date.today().isoformat()
    bak = MASTER.with_suffix(f".pre_merge_{stamp}.parquet")
    if not bak.exists():
        shutil.copy2(MASTER, bak)
        print(f"backup: {bak.name}")

    rec_i = rec.set_index("url")
    idx = m.index[m.url.isin(rec_i.index) & m.text.isna()]
    cols = ["text", "text_len", "body_ua", "body_ru", "body_variant", "text_hash"]
    aligned = rec_i.loc[m.loc[idx, "url"], cols]
    for c in cols:
        m.loc[idx, c] = aligned[c].values
    m.loc[idx, "status"] = 200
    m.loc[idx, "error"] = None
    m.loc[idx, "recovered_via"] = "browser_refetch"
    m.to_parquet(MASTER, compression="zstd", index=False)
    print(f"merged {len(idx):,} rows into {MASTER.name}: "
          f"{m.text.notna().sum():,} rows with text ({m.text.notna().mean()*100:.1f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
