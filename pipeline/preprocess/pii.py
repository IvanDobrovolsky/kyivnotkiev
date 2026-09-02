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


SCRUB_ALL = ("email", "phone", "ipv4", "url_token")
SCRUB_REDDIT_ONLY = ("handle", "tg_link")
TEXT_COLS = ("text", "title", "description", "match_context")


def scrub_frame(df, source_hint=None):
    """Apply the release policy in place. Returns per-class counts."""
    import pandas as _pd
    counts = {k: 0 for k in CLASSES}
    src = df["source"] if "source" in df.columns else _pd.Series(
        source_hint or "", index=df.index)
    reddit = src.astype(str).str.contains("reddit", case=False)
    for col in TEXT_COLS:
        if col not in df.columns:
            continue
        vals = df[col].astype("object")
        mask = vals.notna()
        def _one(t, is_reddit):
            out = str(t)
            for name in SCRUB_ALL:
                out, n = CLASSES[name].subn(REPLACEMENT[name], out)
                counts[name] += n
            if is_reddit:
                for name in SCRUB_REDDIT_ONLY:
                    out, n = CLASSES[name].subn(REPLACEMENT[name], out)
                    counts[name] += n
            return out
        df.loc[mask, col] = [
            _one(t, r) for t, r in zip(vals[mask], reddit[mask])]
    return counts


def scrub_store() -> dict:
    """Rewrite every store parquet with the release policy applied.
    The store is regenerable from raw via migrate, so this is safe; migrate
    output must be re-scrubbed before any publish (publish.py enforces it).
    """
    report = {}
    for f in sorted(glob.glob("data/store/*.parquet")) + sorted(
            glob.glob("data/store/pairs/*.parquet")):
        df = pd.read_parquet(f)
        if not any(c in df.columns for c in TEXT_COLS):
            continue
        hint = "reddit" if "reddit" in pathlib.Path(f).stem else ""
        c = scrub_frame(df, source_hint=hint)
        if sum(c.values()):
            df.to_parquet(f, compression="zstd", index=False)
            report[pathlib.Path(f).name] = {k: v for k, v in c.items() if v}
            print(f"scrubbed {pathlib.Path(f).name}: "
                  + " ".join(f"{k}={v:,}" for k, v in c.items() if v), flush=True)
    pathlib.Path("data/audit/pii_scrub_report.json").write_text(
        json.dumps(report, indent=1))
    return report


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--audit", action="store_true")
    ap.add_argument("--scrub-store", action="store_true")
    a = ap.parse_args()
    if a.scrub_store:
        scrub_store()
        return 0
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
