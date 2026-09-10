"""Assert the exhibits show nothing that verification rejected.

Matching is on (pair, url) exactly. An earlier throwaway version compared
bare URL tails, which flagged 494 news rows that were never dropped — a
false alarm that cost a debugging cycle. Identifier matching is allowed
only for platform URLs, where the id is unambiguous.

Exit 1 on any real leak, so deploy chains can gate on it.

    python -m pipeline.audit.exhibit_check
"""

import collections
import json
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
CAP = 100


def platform_id(url: str) -> str | None:
    if "youtube.com" in url or "youtu.be" in url:
        return url.rsplit("v=", 1)[-1].split("&")[0].rsplit("/", 1)[-1]
    if "reddit.com" in url or "openalex.org" in url:
        return url.rstrip("/").rsplit("/", 1)[-1]
    return None


def main() -> int:
    h = json.loads((ROOT / "site" / "src" / "data" / "holdouts_by_pair.json").read_text())
    vf = ROOT / "data" / "audit" / "holdout_ai_verification.json"
    drops = json.loads(vf.read_text()).get("drops", []) if vf.exists() else []
    by_pair_url = {(d.get("pair"), str(d.get("url"))) for d in drops}
    by_pair_id = {(d.get("pair"), platform_id(str(d.get("url"))))
                  for d in drops if platform_id(str(d.get("url")))}

    leaks = collections.Counter()
    sizes = collections.Counter()
    short = []
    for slug, srcs in h.items():
        for src, entries in srcs.items():
            if not isinstance(entries, list):
                continue
            sizes[src] += len(entries)
            if len(entries) < CAP:
                short.append((slug, src, len(entries)))
            for e in entries:
                u = str(e.get("url", ""))
                pid = platform_id(u)
                if (slug, u) in by_pair_url or (pid and (slug, pid) in by_pair_id):
                    leaks[src] += 1

    print(f"exhibit entries: {sum(sizes.values()):,} {dict(sizes)}")
    print(f"tables under {CAP}: {len(short)}")
    print("verified-wrong leaks:", dict(leaks) or "none")
    if leaks:
        for slug, srcs in h.items():
            for src, entries in srcs.items():
                if not isinstance(entries, list):
                    continue
                for e in entries[:200]:
                    u = str(e.get("url", ""))
                    if (slug, u) in by_pair_url:
                        print(f"  e.g. {slug}/{src}: {u[:80]}")
                        break
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
