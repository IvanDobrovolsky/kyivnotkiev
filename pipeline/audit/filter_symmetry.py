"""Find homonym patterns that can only match one of the two spellings.

A pattern naming "kiev" but not "kyiv" deletes a wrong referent from one side
of the comparison and leaves it on the other, which moves the adoption number
directly. Two were found by review on the same day: chicken-kyiv's "Chicken
Kiev speech" (Bush 1991) removed the Russian side and left 23 Ukrainian rows,
and chornobyl's "heart of cherno?byl" deleted every Russian-spelled mention of
the game title while keeping 6,818 Ukrainian-spelled ones.

Exits 1 if any asymmetric pattern is found, with the row counts each side.

    python -m pipeline.audit.filter_symmetry
"""

import pathlib
import re
import sys

import pandas as pd
import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
STORE = ROOT / "data" / "store" / "pairs"

# spelling markers that should always appear as a pair inside a pattern
TWINS = [("kiev", "kyiv"), ("chernobyl", "chornobyl"), ("odessa", "odesa"),
         ("kharkov", "kharkiv"), ("lvov", "lviv"), ("nikolaev", "mykolaiv"),
         ("dnieper", "dnipro"), ("artemovsk", "bakhmut"), ("lugansk", "luhansk"),
         ("donbass", "donbas"), ("rovno", "rivne"), ("ternopol", "ternopil"),
         ("gogol", "hohol"), ("sikorsky", "sikorskyi"), ("borsch", "borshch")]


def main() -> int:
    cfg = yaml.safe_load((ROOT / "config" / "pairs.yaml").read_text())
    findings = []
    for p in cfg["pairs"]:
        slug = p.get("slug")
        for key in ("homonym_filters", "youtube_homonym_filters", "holdout_exclude"):
            for pat in (p.get(key) or []):
                low = str(pat).lower()
                for a, b in TWINS:
                    has_a, has_b = a in low, b in low
                    if has_a == has_b:
                        continue
                    named, missing = (a, b) if has_a else (b, a)
                    findings.append((slug, key, str(pat), named, missing))

    if not findings:
        print("no asymmetric homonym patterns")
        return 0

    print(f"{len(findings)} pattern(s) name one spelling but not its twin:\n")
    for slug, key, pat, named, missing in findings:
        counts = ""
        f = STORE / f"{slug}.parquet"
        if f.exists():
            try:
                d = pd.read_parquet(f, columns=["title", "text", "variant"])
                blob = (d.title.fillna("") + " | " + d.text.fillna("")).astype(str)
                rx_named = re.compile(str(pat), re.I)
                rx_twin = re.compile(re.sub(named, missing, str(pat), flags=re.I), re.I)
                n_named = int(blob.str.contains(rx_named).sum())
                m_twin = blob.str.contains(rx_twin)
                counts = (f"   caught {n_named:,}; the {missing}-spelled twin leaves "
                          f"{int(m_twin.sum()):,} rows "
                          f"({d[m_twin].variant.value_counts().to_dict()})")
            except Exception as e:                     # noqa: BLE001
                counts = f"   (could not measure: {str(e)[:50]})"
        print(f"  {slug}/{key}\n    {pat}\n    names '{named}', missing '{missing}'\n{counts}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
