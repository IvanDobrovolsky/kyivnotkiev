"""Verify per-pair Tools claims (Hunspell + AP/BBC stylebook).

For every pair we know:
- Hunspell en_US: does the dictionary accept the Russian form? the Ukrainian
  form? what does it suggest as a correction?
- AP Stylebook: is this pair in AP's documented Ukraine entries?
- BBC News Style Guide: is this pair in BBC's documented entries?

Closed-source tools (Microsoft Word, Grammarly) are intentionally NOT
verified per-pair — there is no honest way to do that without manual
testing of a proprietary service. They are dropped from the per-pair UI.

The first word of each multi-word toponym is checked for hunspell
(e.g. for "Kievan Rus" we check "Kievan"; for "Kiev cake" we check "Kiev").
This matches what a real spellchecker would flag in a sentence.

Output: site/src/data/tools_per_pair.json
"""

import json
import os
from pathlib import Path

# Point pyenchant at the brew-installed enchant lib
os.environ.setdefault(
    "PYENCHANT_LIBRARY_PATH",
    "/opt/homebrew/lib/libenchant-2.dylib",
)

import enchant

ROOT = Path(__file__).resolve().parent.parent.parent
MANIFEST = ROOT / "site" / "src" / "data" / "manifest.json"
OUT_PATH = ROOT / "site" / "src" / "data" / "tools_per_pair.json"


# Documented entries in major style guides. These are the only pairs we
# claim AP/BBC have explicitly switched on. Source: AP Stylebook Ukraine
# entries (2019, expanded 2022) and BBC News Style Guide.
AP_PAIRS = {1, 2, 3, 4, 5}      # Kyiv, Kharkiv, Odesa, Lviv, Mykolaiv
BBC_PAIRS = {1, 2, 3, 4}        # Kyiv, Kharkiv, Odesa, Lviv
# Pairs AP/BBC have NOT documented (still using older form by default).
# Empty for now — anything not in the above lists is "unknown" rather than
# "rejected" because the stylebooks simply don't cover most toponyms.


def first_word(s: str) -> str:
    """Return the first whitespace-separated word of s."""
    return s.strip().split()[0] if s and s.strip() else s


def hunspell_check(d, term: str) -> dict:
    """Run en_US hunspell on the (first word of the) term."""
    word = first_word(term)
    accepted = d.check(word)
    suggestions = [] if accepted else d.suggest(word)[:5]
    return {
        "word_checked": word,
        "accepted": accepted,
        "suggestions": suggestions,
    }


def main():
    with open(MANIFEST) as f:
        manifest = json.load(f)
    pairs = manifest["pairs"]

    d = enchant.Dict("en_US")

    out = {}
    summary = {
        "n_pairs": len(pairs),
        "hunspell_ru_accepted": 0,
        "hunspell_ua_accepted": 0,
        "hunspell_only_ru_accepted": 0,
        "hunspell_only_ua_accepted": 0,
        "hunspell_both_accepted": 0,
        "hunspell_neither_accepted": 0,
        "ap_documented": 0,
        "bbc_documented": 0,
    }

    for p in pairs:
        pid = p["id"]
        ru = p["russian"]
        ua = p["ukrainian"]

        ru_check = hunspell_check(d, ru)
        ua_check = hunspell_check(d, ua)

        if ru_check["accepted"]:
            summary["hunspell_ru_accepted"] += 1
        if ua_check["accepted"]:
            summary["hunspell_ua_accepted"] += 1
        if ru_check["accepted"] and ua_check["accepted"]:
            summary["hunspell_both_accepted"] += 1
        elif ru_check["accepted"] and not ua_check["accepted"]:
            summary["hunspell_only_ru_accepted"] += 1
        elif ua_check["accepted"] and not ru_check["accepted"]:
            summary["hunspell_only_ua_accepted"] += 1
        else:
            summary["hunspell_neither_accepted"] += 1

        ap_documented = pid in AP_PAIRS
        bbc_documented = pid in BBC_PAIRS
        if ap_documented:
            summary["ap_documented"] += 1
        if bbc_documented:
            summary["bbc_documented"] += 1

        out[str(pid)] = {
            "russian": ru,
            "ukrainian": ua,
            "hunspell": {
                "russian": ru_check,
                "ukrainian": ua_check,
            },
            "ap_stylebook": {
                "documented": ap_documented,
                "form": "Ukrainian" if ap_documented else None,
            },
            "bbc_style_guide": {
                "documented": bbc_documented,
                "form": "Ukrainian" if bbc_documented else None,
            },
        }

    payload = {
        "method": (
            "Hunspell en_US dictionary checked via pyenchant on the first "
            "word of each toponym pair. AP Stylebook and BBC News Style "
            "Guide entries are sourced from documented public stylebook "
            "updates (AP 2019/2022, BBC 2019). Closed-source tools "
            "(Microsoft Word, Grammarly) are intentionally NOT verified "
            "per-pair as that would require manual interaction with "
            "proprietary services."
        ),
        "summary": summary,
        "pairs": out,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(payload, f, indent=2)

    print(f"wrote {OUT_PATH}")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
