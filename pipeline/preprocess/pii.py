"""Deterministic PII detection and scrubbing for corpus release.

Regex classes only — auditable, reproducible, no models. `audit` counts
matches per class without modifying anything; `scrub` replaces matches with
bracketed class tokens. Applied to text destined for training or public
release; never to the raw archives.

    python -m pipeline.preprocess.pii --audit
"""
from __future__ import annotations

import argparse
import glob
import json
import pathlib
import re

import pandas as pd

CLASSES = {
    "email": re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"),
    "phone": re.compile(r"(?<!\d)(?:\+?\d{1,3}[\s.-]?)?(?:\(\d{2,4}\)[\s.-]?)?\d{3}[\s.-]\d{2,4}[\s.-]?\d{2,4}(?!\d)"),
    "ipv4": re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b"),
    "handle": re.compile(r"(?<![\w@])@[A-Za-z0-9_]{3,30}\b"),
    "tg_link": re.compile(r"\bt\.me/[A-Za-z0-9_+]{4,}"),
    "url_token": re.compile(r"[?&](?:token|key|api_key|apikey|auth|session|sig)=[^\s&\"']+", re.I),
}
REPLACEMENT = {k: f"[{k}]" for k in CLASSES}


def scrub(text: str) -> tuple[str, dict]:
    counts = {}
    out = str(text)
    for name, rx in CLASSES.items():
        out, n = rx.subn(REPLACEMENT[name], out)
        if n:
            counts[name] = n
    return out, counts


def audit_frame(df: pd.DataFrame, cols: list[str]) -> dict:
    tot: dict = {k: 0 for k in CLASSES}
    docs_hit = 0
    blob = df[cols[0]].fillna("").astype(str)
    for c in cols[1:]:
        if c in df.columns:
            blob = blob + " " + df[c].fillna("").astype(str)
    for t in blob:
        hit = False
        for name, rx in CLASSES.items():
            n = len(rx.findall(t))
            if n:
                tot[name] += n
                hit = True
        docs_hit += hit
    tot["docs_with_pii"] = docs_hit
    tot["docs_total"] = len(df)
    return tot


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit", action="store_true")
    a = ap.parse_args()
    report = {}
    for f in sorted(glob.glob("data/store/pairs/*.parquet")):
        slug = pathlib.Path(f).stem
        df = pd.read_parquet(f)
        cols = [c for c in ("text", "title") if c in df.columns]
        if not cols:
            continue
        report[slug] = audit_frame(df, cols)
        print(f"{slug:24s} docs {report[slug]['docs_total']:7,}  with-PII {report[slug]['docs_with_pii']:6,}  "
              + "  ".join(f"{k}={report[slug][k]:,}" for k in CLASSES if report[slug][k]))
    out = pathlib.Path("data/audit/pii_audit.json")
    out.write_text(json.dumps(report, indent=1))
    agg = {k: sum(r[k] for r in report.values()) for k in CLASSES}
    print("\nTOTAL:", json.dumps(agg))
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
