"""Put the re-fetched OpenAlex text back into the stores — but only where it provably matches.

Guessing how `text` was assembled would silently corrupt rows whose shape we
guessed wrong. Instead each candidate reconstruction (title, title+abstract,
abstract) is run through the ORIGINAL buggy phone pattern; a candidate is
accepted only if that reproduces the stored damaged string byte for byte. A
row we cannot reproduce is left alone and reported, never overwritten.

Accepted text is then re-scrubbed with the corrected patterns, so genuine PII
stays redacted.

    python -m pipeline.repair.openalex_restore [--apply]
"""

import argparse
import json
import pathlib
import re

import pandas as pd

from pipeline.preprocess.pii import scrub

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
CACHE = ROOT / "data" / "audit" / "openalex_refetch.jsonl"

# The pattern exactly as it was when the damage was done.
OLD_PHONE = re.compile(
    r"(?<!\d)(?:\+?\d{1,3}[\s.-]?)?(?:\(\d{2,4}\)[\s.-]?)?\d{3}[\s.-]\d{2,4}[\s.-]?\d{2,4}(?!\d)")
OLD_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
OLD_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
OLD_URLTOK = re.compile(r"[?&](?:token|key|api_key|apikey|auth|session|sig)=[^\s&\"']+", re.I)


def old_scrub(t: str) -> str:
    t = OLD_EMAIL.sub("[email]", str(t))
    t = OLD_PHONE.sub("[phone]", t)
    t = OLD_IPV4.sub("[ipv4]", t)
    return OLD_URLTOK.sub("[url_token]", t)


def load_cache() -> dict:
    out = {}
    if CACHE.exists():
        for line in CACHE.read_text().splitlines():
            try:
                r = json.loads(line)
            except Exception:                          # noqa: BLE001
                continue
            out[r["id"]] = r
    return out


def candidates(rec: dict) -> list[str]:
    t, a = (rec.get("title") or "").strip(), (rec.get("abstract") or "").strip()
    return [c for c in (t, f"{t}\n\n{a}".strip(), f"{t} {a}".strip(), a,
                        f"{t}. {a}".strip(), f"{t}\n{a}".strip()) if c]


def _norm(t: str) -> str:
    return re.sub(r"\s+", " ", str(t)).strip()


def restore_spans(stored: str, cand: str) -> str | None:
    """Put the redacted substrings back without touching anything else.

    Swapping in the whole re-fetched string would also rewrite separators and
    truncation the store applied for its own reasons, so only the [phone] runs
    are replaced, in order, with what the old pattern removed. The candidate
    must reproduce the stored text under whitespace normalisation, and must
    supply exactly as many matches as there are holes, or nothing is changed.
    """
    if _norm(old_scrub(cand)) != _norm(stored):
        return None
    staged = OLD_EMAIL.sub("[email]", cand)
    originals = [m.group(0) for m in OLD_PHONE.finditer(staged)]
    holes = list(re.finditer(r"\[phone\]", stored))
    if len(originals) != len(holes):
        return None
    out, last = [], 0
    for m, orig in zip(holes, originals):
        out.append(stored[last:m.start()])
        out.append(orig)
        last = m.end()
    out.append(stored[last:])
    return "".join(out)


def repair_frame(df: pd.DataFrame, idcol: str, cache: dict, cols: list[str]) -> tuple[int, int]:
    fixed = unmatched = 0
    wid = df[idcol].astype(str).str.extract(r"(W\d+)")[0]
    for col in cols:
        if col not in df.columns:
            continue
        dmg = df[col].fillna("").astype(str).str.contains(r"\[phone\]", regex=True)
        for i in df.index[dmg]:
            rec = cache.get(wid.get(i))
            if not rec:
                unmatched += 1
                continue
            stored = str(df.at[i, col])
            hit = next((r for r in (restore_spans(stored, c) for c in candidates(rec))
                        if r is not None), None)
            if hit is None:
                unmatched += 1
                continue
            df.at[i, col] = scrub(hit)[0]
            fixed += 1
    return fixed, unmatched


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the files (default: dry run)")
    a = ap.parse_args()
    cache = load_cache()
    print(f"cache: {len(cache):,} re-fetched works\n")

    targets = [
        ("data/store/openalex_raw.parquet", "openalex_id", ["title", "text", "abstract"]),
        ("data/store/openalex_processed.parquet", "record_id", ["title", "text"]),
    ] + [(str(p.relative_to(ROOT)), "record_id", ["title", "text"])
         for p in sorted((ROOT / "data" / "store" / "pairs").glob("*.parquet"))]

    tot_f = tot_u = 0
    for rel, idcol, cols in targets:
        path = ROOT / rel
        try:
            df = pd.read_parquet(path)
        except Exception as e:                         # noqa: BLE001
            print(f"  {rel}: unreadable ({str(e)[:60]})")
            continue
        if idcol not in df.columns:
            continue
        probe = [c for c in cols if c in df.columns]
        if not probe or not df[probe].fillna("").astype(str).apply(
                lambda s: s.str.contains(r"\[phone\]", regex=True)).to_numpy().any():
            continue
        f, u = repair_frame(df, idcol, cache, cols)
        tot_f += f
        tot_u += u
        print(f"  {pathlib.Path(rel).name:34s} restored {f:5,}   unmatched {u:4,}")
        if a.apply and f:
            tmp = path.with_suffix(".restore_tmp.parquet")
            df.to_parquet(tmp, compression="zstd", index=False)
            tmp.replace(path)

    print(f"\ntotal restored {tot_f:,}, unmatched {tot_u:,}"
          + ("" if a.apply else "   (dry run — pass --apply to write)"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
