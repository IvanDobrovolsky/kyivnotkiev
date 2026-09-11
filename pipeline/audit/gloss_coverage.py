"""Every collocation chip on the site must say what the word is doing.

A chip with no gloss is a bare token published as a finding: the reader sees
"fsr" or "isch" and has nothing to check it against. This reports the gap and
exits 1 when any displayed term lacks one, so a deploy chain can gate on it.

    python -m pipeline.audit.gloss_coverage
"""

import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SITE = ROOT / "site" / "src" / "data" / "cl_keyness.json"


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
    print(f"collocation chips: {total}, glossed {glossed}, missing {total - glossed}")
    for slug, words in sorted(missing.items()):
        print(f"  {slug:22s} {', '.join(words)}")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
