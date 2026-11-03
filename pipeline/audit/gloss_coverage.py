"""Every collocation chip on the site must say what the word is doing.

A chip with no gloss is a bare token published as a finding: the reader sees
"fsr" or "isch" and has nothing to check it against. This reports the gap and
exits 1 when any displayed term lacks one, so a deploy chain can gate on it.

    python -m pipeline.audit.gloss_coverage
"""

import json
import pathlib
import sys

import pipeline.config as cfg

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SITE = ROOT / "site" / "src" / "data" / "cl_keyness.json"
STATS = ROOT / "data" / "stats"


def text_reach(slug: str) -> str | None:
    """Latest date in the keyness input, from parquet row-group statistics."""
    p = STATS / slug / "records.parquet"
    if not p.exists():
        return None
    try:
        import pyarrow.parquet as pq
        md = pq.ParquetFile(p).metadata
        i = md.schema.to_arrow_schema().names.index("date")
        return max(str(md.row_group(g).column(i).statistics.max)
                   for g in range(md.num_row_groups)
                   if md.row_group(g).column(i).statistics is not None)
    except (ValueError, OSError):
        return None


def main() -> int:
    if not SITE.exists():
        print("cl_keyness.json not exported yet")
        return 0
    k = json.loads(SITE.read_text())
    missing: dict = {}
    total = glossed = 0
    for slug, p in k.items():
        for side in ("ua", "ru", "solo"):
            for x in (p.get(side) or []):
                total += 1
                if x.get("g"):
                    glossed += 1
                else:
                    missing.setdefault(slug, []).append(x["w"])
    print(f"collocation chips (TEXT window {cfg.STUDY_START}..{cfg.TEXT_END}): "
          f"{total}, glossed {glossed}, missing {total - glossed}")
    for slug, words in sorted(missing.items()):
        print(f"  {slug:22s} {', '.join(words)}")
    # A gloss describes a chip computed over the TEXT window. Chips computed
    # from text past it are a different measurement wearing the same gloss:
    # volodymyr-zelenskyy ru 'regime' survived only on 2026 GDELT rows.
    # green_check gates on this; listed here so the gloss review sees it.
    late = {s: r for s in sorted(k) if (r := text_reach(s)) and r[:7] > cfg.TEXT_END}
    if late:
        print(f"  NOTE {len(late)} pair(s) ship chips computed from text past "
              f"{cfg.TEXT_END}: " + ", ".join(f"{s} ({r[:10]})" for s, r in late.items()))
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
