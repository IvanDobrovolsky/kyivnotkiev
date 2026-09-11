"""Undo the phone-pattern damage across the whole store, from local copies.

The scrub's second pass redacted 57,598 rows. It was not phone numbers: it
was dates and date ranges — "Adam K LIVE 2009.12.12 Godskitchen" became
"Adam K LIVE [phone] Godskitchen". The corrected pattern stops new damage but
cannot undo what is already written.

Nothing needs re-fetching. The scrub globs data/store only, so every upstream
copy is intact:

    youtube titles        data/cl/raw/youtube_census      by video_id
    youtube descriptions  data/bq_export/raw_youtube      by video_id
    reddit                data/raw/reddit, cl/raw/reddit* by post_id
    gdelt                 data/cl/corpus/gdelt_verified   by url

A clean string is spliced in only when running it through the ORIGINAL buggy
pattern reproduces text that actually appears in the stored row. That makes
each replacement self-verifying: we never write a string we cannot show the
scrub would have turned into what is there now.

    python -m pipeline.repair.store_restore [--apply] [--pair SLUG]
"""

import argparse
import pathlib
import re

import pandas as pd

from pipeline.preprocess.pii import scrub
from pipeline.repair.openalex_restore import old_scrub

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
STORE = ROOT / "data" / "store" / "pairs"
DAMAGED = re.compile(r"\[phone\]")
FIELDS = ("text", "title", "match_context")


def _collect(paths, idcol, textcols, wanted: set) -> dict:
    """id -> list of clean strings, restricted to the ids we actually need."""
    out: dict = {}
    for f in paths:
        try:
            cols = [idcol] + [c for c in textcols]
            df = pd.read_parquet(f, columns=cols)
        except Exception:                              # noqa: BLE001
            try:
                df = pd.read_parquet(f)
            except Exception:                          # noqa: BLE001
                continue
            if idcol not in df.columns:
                continue
        ids = df[idcol].astype(str)
        keep = df[ids.isin(wanted)]
        if not len(keep):
            continue
        for _, r in keep.iterrows():
            k = str(r[idcol])
            bag = out.setdefault(k, [])
            for c in textcols:
                v = r.get(c)
                if isinstance(v, str) and v.strip():
                    bag.append(v)
    return out


def _ids_needed(source: str) -> dict:
    """pair slug -> set of ids whose rows are damaged, for one source."""
    need: dict = {}
    for f in sorted(STORE.glob("*.parquet")):
        try:
            d = pd.read_parquet(f, columns=["source", "doc_id", "url"] + list(FIELDS))
        except Exception:                              # noqa: BLE001
            continue
        m = d.source.eq(source)
        if not m.any():
            continue
        dmg = m & d[list(FIELDS)].fillna("").astype(str).apply(
            lambda s: s.str.contains(DAMAGED, regex=True)).any(axis=1)
        if dmg.any():
            key = "url" if source == "gdelt" else "doc_id"
            need[f.stem] = set(d.loc[dmg, key].astype(str))
    return need


def splice(stored: str, clean_strings: list) -> str:
    """Replace what the old pattern would have produced with the original."""
    out = str(stored)
    for c in clean_strings:
        sc = old_scrub(c)
        if sc == c or "[phone]" not in sc:
            continue
        if sc in out:
            out = out.replace(sc, c)
            continue
        # The store often keeps a whitespace-normalised copy; try that shape.
        sc_n = re.sub(r"\s+", " ", sc).strip()
        out_n = re.sub(r"\s+", " ", out)
        if sc_n and sc_n in out_n:
            out = out_n.replace(sc_n, re.sub(r"\s+", " ", c).strip())
    return out


SOURCES = {
    "youtube": (["data/cl/raw/youtube_census", "data/bq_export/raw_youtube"],
                "video_id", ["title", "description"], "doc_id"),
    "reddit": (["data/raw/reddit", "data/cl/raw/reddit_full", "data/cl/raw/reddit"],
               "post_id", ["title", "selftext"], "doc_id"),
    "gdelt": (["data/cl/corpus/gdelt_verified"], "url", ["title", "text"], "url"),
}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--pair")
    a = ap.parse_args()

    totals = {}
    for source, (dirs, idcol, textcols, joincol) in SOURCES.items():
        need = _ids_needed(source)
        if a.pair:
            need = {k: v for k, v in need.items() if k == a.pair}
        if not need:
            continue
        wanted = set().union(*need.values())
        paths = [p for d in dirs for p in sorted((ROOT / d).rglob("*.parquet"))]
        print(f"{source}: {len(wanted):,} damaged ids across {len(need)} pair(s), "
              f"scanning {len(paths)} upstream file(s)")
        clean = _collect(paths, idcol, textcols, wanted)
        print(f"  upstream match: {len(clean):,} of {len(wanted):,} ids")
        totals[source] = {"needed": len(wanted), "found": len(clean), "rows": 0}

        for slug, ids in need.items():
            path = STORE / f"{slug}.parquet"
            df = pd.read_parquet(path)
            keys = df[joincol].astype(str)
            fixed = 0
            for i in df.index:
                if df.at[i, "source"] != source:
                    continue
                bag = clean.get(keys.at[i])
                if not bag:
                    continue
                touched = False
                for col in FIELDS:
                    if col not in df.columns:
                        continue
                    v = df.at[i, col]
                    if not isinstance(v, str) or "[phone]" not in v:
                        continue
                    nv = splice(v, bag)
                    if nv != v:
                        df.at[i, col] = scrub(nv)[0]
                        touched = True
                fixed += touched
            totals[source]["rows"] += fixed
            print(f"    {slug:24s} restored {fixed:,} row(s)")
            if a.apply and fixed:
                tmp = path.with_suffix(".restore_tmp.parquet")
                df.to_parquet(tmp, compression="zstd", index=False)
                tmp.replace(path)

    print("\nsummary:")
    for s, t in totals.items():
        print(f"  {s:9s} ids {t['found']:,}/{t['needed']:,}   rows restored {t['rows']:,}")
    if not a.apply:
        print("\n(dry run — pass --apply to write)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
