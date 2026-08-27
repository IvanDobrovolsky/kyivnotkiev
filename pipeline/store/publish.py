"""Publish data/store/ to HuggingFace, structure preserved.

The repo mirrors the local layout exactly, so a wiped laptop can be rebuilt from the
dataset alone:

    <source>_raw.parquet         everything the provider returned
    <source>_processed.parquet   cleaned + regex-matched
    pairs/<slug>.parquet         all sources stacked for one pair
    _manifest.json               rows, columns and checksum for every artifact

Publishing is by explicit invocation only. Nothing here runs as part of rebuild.

    python -m pipeline.store.publish --dry-run
    python -m pipeline.store.publish
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

STORE = pathlib.Path("data/store")
REPO = "KyivNotKiev/toponym-adoption-data"

CARD = """---
license: cc-by-4.0
language: [en]
tags: [linguistics, toponyms, ukraine, computational-social-science]
---

# Toponym adoption data

English-language adoption of Ukrainian vs Russian toponym transliterations
(Kyiv/Kiev, Chornobyl/Chernobyl, ...), 2010-2026.

## Layout

Four stages, each a function of the previous. Only `raw` is expensive; everything
downstream is a recompute.

| file | what it is |
|---|---|
| `<source>_raw.parquet` | exactly what the provider returned, nothing dropped |
| `<source>_processed.parquet` | cleaned and regex-matched; only records containing a spelling |
| `pairs/<slug>.parquet` | every source's processed rows for one pair, unbalanced |
| `_manifest.json` | rows, columns and a content checksum for every artifact |

`raw` deliberately contains material the study excludes -- videos about Vladimir Putin
rather than Vladimir the Great, taxonomy papers citing the botanist T. Borsch, articles
about Odessa, Texas. They are what the provider returned, and they are the evidence
that the filtering did something. Without them the filtering cannot be audited.

## Columns in `_processed` and `pairs/`

`record_id, pair_slug, source, doc_id, url, date, title, text, ua_hits, ru_hits,
variant, match_span, text_hash`

There is no `verified` column. `ua_hits` and `ru_hits` are word-boundary match counts
and `variant` is derived from them; `match_span` is the surrounding text so the match
can be checked by eye. The evidence is the data.

`pairs/` carries `source` because it is needed at evaluation time -- the first question
about any cluster is whether it merely rediscovered the source. It must not be used as
a training feature.

## Coverage is uneven, on purpose

Not every pair has every source. Wikipedia, Trends and Ngrams produce counts rather
than documents and so appear in no `pairs/` file. YouTube census collection is partial.
The per-pair files show what each pair actually has rather than implying uniformity.
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    files = sorted(STORE.glob("*.parquet")) + sorted((STORE / "pairs").glob("*.parquet"))
    manifest = STORE / "_manifest.json"
    total = sum(f.stat().st_size for f in files) / 1e6
    print(f"{len(files)} parquet file(s), {total:,.0f} MB")
    for f in files:
        rel = f.relative_to(STORE)
        print(f"  {str(rel):<40}{f.stat().st_size/1e6:>8.1f} MB")
    if a.dry_run:
        print("\ndry run — nothing uploaded")
        return 0

    token = None
    for c in (pathlib.Path("/etc/secrets/hf"), pathlib.Path.home() / ".huggingface" / "token"):
        if c.exists():
            token = c.read_text().strip()
            break
    if not token:
        print("no HF token found", file=sys.stderr)
        return 1

    from huggingface_hub import HfApi
    api = HfApi(token=token)
    (STORE / "README.md").write_text(CARD)
    print(f"\nuploading to {REPO} ...")
    api.upload_folder(folder_path=str(STORE), repo_id=REPO, repo_type="dataset",
                      allow_patterns=["*.parquet", "_manifest.json", "README.md"],
                      commit_message="Publish store layout: raw, processed, per-pair")
    remote = sorted(api.list_repo_files(repo_id=REPO, repo_type="dataset"))
    print(f"\nrepo now holds {len(remote)} file(s):")
    for r in remote:
        print("   ", r)
    local = {str(f.relative_to(STORE)) for f in files}
    missing = local - set(remote)
    print("\nall local files present remotely" if not missing else f"MISSING REMOTELY: {sorted(missing)}")
    return 0 if not missing else 1


if __name__ == "__main__":
    raise SystemExit(main())
