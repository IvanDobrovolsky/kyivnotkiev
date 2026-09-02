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
STREAM_OVER_MB = 300


def _scrub_series(sr, classes, counts):
    """Vectorized count-then-replace per class, iterated to a fixpoint:
    non-overlapping replacement can splice two digit runs into a NEW match
    across the seam (verified: 2,968 phone residues after a single pass)."""
    for name in classes:
        rx = CLASSES[name]
        for _ in range(4):
            n = int(sr.str.count(rx.pattern, flags=rx.flags).sum())
            if not n:
                break
            counts[name] = counts.get(name, 0) + n
            sr = sr.str.replace(rx.pattern, REPLACEMENT[name],
                                regex=True, flags=rx.flags)
    return sr


def scrub_frame(df, source_hint=None):
    import pandas as _pd
    counts: dict = {}
    src = df["source"] if "source" in df.columns else _pd.Series(
        source_hint or "", index=df.index)
    is_reddit = src.astype(str).str.contains("reddit", case=False)
    for col in TEXT_COLS:
        if col not in df.columns:
            continue
        mask = df[col].notna()
        if not mask.any():
            continue
        vals = df.loc[mask, col].astype(str)
        r = is_reddit[mask]
        out = vals.copy()
        out.loc[:] = _scrub_series(vals, SCRUB_ALL, counts)
        if r.any():
            out.loc[r] = _scrub_series(out.loc[r], SCRUB_REDDIT_ONLY, counts)
        df.loc[mask, col] = out
    return counts


def _scrub_file(f: str) -> dict:
    import pyarrow as pa
    import pyarrow.parquet as pq
    path = pathlib.Path(f)
    hint = "reddit" if "reddit" in path.stem else ""
    size_mb = path.stat().st_size / 1e6
    if size_mb <= STREAM_OVER_MB:
        df = pd.read_parquet(f)
        c = scrub_frame(df, source_hint=hint)
        if sum(c.values()):
            df.to_parquet(f, compression="zstd", index=False)
        return c
    # Stream row groups so a 2GB file never fully materializes.
    pf = pq.ParquetFile(f)
    tmp = path.with_suffix(".scrub_tmp.parquet")
    writer = None
    counts: dict = {}
    for batch in pf.iter_batches(batch_size=50_000):
        df = batch.to_pandas()
        c = scrub_frame(df, source_hint=hint)
        for k, v in c.items():
            counts[k] = counts.get(k, 0) + v
        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(tmp, table.schema, compression="zstd")
        writer.write_table(table)
    if writer:
        writer.close()
    if sum(counts.values()):
        tmp.replace(path)
    else:
        tmp.unlink(missing_ok=True)
    return counts


def scrub_store() -> dict:
    import pyarrow.parquet as pq
    report = {}
    for f in sorted(glob.glob("data/store/*.parquet")) + sorted(
            glob.glob("data/store/pairs/*.parquet")):
        if ".scrub_tmp" in f or ".pre_merge" in f:
            continue
        names = set(pq.ParquetFile(f).schema_arrow.names)
        if not names & set(TEXT_COLS):
            continue
        c = _scrub_file(f)
        report[pathlib.Path(f).name] = {k: v for k, v in c.items() if v}
        print(f"scrubbed {pathlib.Path(f).name}: "
              + (" ".join(f"{k}={v:,}" for k, v in c.items() if v) or "clean"),
              flush=True)
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
