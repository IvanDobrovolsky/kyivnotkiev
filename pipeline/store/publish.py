"""Publish data/store/ to HuggingFace, structure preserved.

The repo mirrors the local layout exactly, so a wiped laptop can be rebuilt from the
dataset alone:

    <source>_raw.parquet         everything the provider returned
    <source>_processed.parquet   cleaned + regex-matched
    pairs/<slug>.parquet         all sources stacked for one pair
    _manifest.json               rows, columns and checksum for every artifact

Publishing is by explicit invocation only. Nothing here runs as part of rebuild.

    python -m pipeline.store.publish --dry-run
    python -m pipeline.store.publish
    python -m pipeline.store.publish --accept-shrink pairs/kyiv.parquet ...

Two guards run before anything is uploaded, and --dry-run runs them too (it exits 1
when a real publish would be refused):

  * Only ENABLED pairs are uploaded. A retired pair's remote file is left as it was.
  * No file may come out smaller than the archived HF snapshot (--baseline), except
    by rows the study window removes on purpose, or by naming it in --accept-shrink.
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

import pyarrow.parquet as pq

from pipeline.config import SERIES_END, STUDY_START, TEXT_END, last_day, load_pairs

STORE = pathlib.Path("data/store")
REPO = "KyivNotKiev/toponym-adoption-data"
# The last state of the remote that is held locally: HF revision 8b0be97a,
# pulled 2026-09-16. It is the only copy of the 21,428-row borscht besides HF
# itself. Point --baseline at a newer snapshot after each publish, or the guard
# keeps comparing against an old remote.
BASELINE = pathlib.Path.home() / "kyivnotkiev-archive" / "hf-snapshot-2026-09-11"


def _pii_preflight(pair_files: list[pathlib.Path]):
    """Sample-scan the files being published for unscrubbed emails; abort on hits."""
    import pandas as _pd
    from pipeline.preprocess.pii import CLASSES
    for f in sorted(pair_files)[:6]:
        names = pq.read_schema(f).names
        col = next((c for c in ("text", "title") if c in names), None)
        if col is None:
            continue
        sample = _pd.read_parquet(f, columns=[col])[col].dropna().astype(str).head(4000)
        if sample.apply(lambda x: bool(CLASSES["email"].search(x))).any():
            raise SystemExit(f"PII preflight failed on {f}: run "
                             "python -m pipeline.preprocess.pii --scrub-store first")


def publish_set(store: pathlib.Path = STORE) -> tuple[list[pathlib.Path], list[str]]:
    """(files to upload, pair files withheld because their pair is not enabled).

    Retired pairs are withheld, not deleted remotely. borscht is the case this
    exists for: its local pair file is a 1,443-row GDELT-only rebuild, the remote
    holds the 21,428-row multi-source file (72.8% UA) that its retirement rests on,
    and this script used to upload pairs/*.parquet wholesale. A pair with no
    `enabled` key is withheld too: publishing is the step that fails closed.
    """
    on = {p["slug"] for p in load_pairs()["pairs"] if p.get("enabled") is True}
    pairs = sorted((store / "pairs").glob("*.parquet"))
    files = sorted(store.glob("*.parquet")) + [f for f in pairs if f.stem in on]
    withheld = [str(f.relative_to(store)) for f in pairs if f.stem not in on]
    return files, withheld


def _window_for(rel: str) -> str | None:
    """The clamp a tier is built under, so rows past it are an intended loss."""
    if rel.startswith("pairs/"):
        return TEXT_END
    if rel.endswith("_processed.parquet"):
        return SERIES_END
    return None                          # raw keeps whatever the provider returned


def _dates(path: pathlib.Path):
    import pandas as pd
    names = pq.read_schema(path).names
    col = "date" if "date" in names else "year" if "year" in names else None
    return pd.read_parquet(path, columns=[col]) if col else None


def _late_rows(path: pathlib.Path, end: str) -> int:
    """Rows of `path` dated after `end`, by the same rule the store clamp uses."""
    from pipeline.store.migrate import apply_study_scope
    d = _dates(path)
    if d is None:
        return 0
    return len(d) - len(apply_study_scope(d, end, f"window {path.name}"))


def _last_month(path: pathlib.Path, end: str) -> str:
    """The last month `path` holds inside `end`; `end` itself when undatable."""
    import pandas as pd
    from pipeline.store.migrate import apply_study_scope
    d = _dates(path)
    if d is None:
        return end
    d = apply_study_scope(d, end, f"window {path.name}")
    if "date" in d.columns:
        m = pd.to_datetime(d["date"], errors="coerce", format="ISO8601").max()
        return end if pd.isna(m) else min(end, m.strftime("%Y-%m"))
    y = pd.to_numeric(d["year"], errors="coerce").max()
    return end if pd.isna(y) else min(end, f"{int(y)}-12")


def shrink_check(files: list[pathlib.Path], baseline: pathlib.Path,
                 accept: set[str], store: pathlib.Path = STORE) -> tuple[list[str], list[str]]:
    """Compare each file's rows with the baseline snapshot.

    Returns (report lines, refused files). A file is refused when it has fewer rows
    than the baseline holds INSIDE the window that tier is clamped to. Rows the
    window removes are not a loss: the snapshot carries 26,912 GDELT rows dated 2026
    across 21 pair files, the leak the text clamp exists to stop, and refusing on raw
    totals would block every pair file that leak touched, which teaches the operator
    to wave every file through and makes the guard worthless.
    """
    lines, refused = [], []
    for f in files:
        rel = str(f.relative_to(store))
        local = pq.read_metadata(f).num_rows
        base_f = baseline / rel
        if not base_f.exists():
            lines.append(f"  new      {rel:<40}{local:>12,}  (not in baseline)")
            continue
        base = pq.read_metadata(base_f).num_rows
        end = _window_for(rel)
        # Compare the months BOTH files cover: the baseline inside its window,
        # the local file only through the baseline's last month. Comparing
        # totals let new months mask lost ones -- a processed tier now clamped
        # at SERIES_END could lose 2025 rows, gain 2026, and out-number a
        # baseline that stops in 2025.
        late = _late_rows(base_f, end) if end else 0
        inwin = base - late
        if end:
            through = _last_month(base_f, end)
            local_in = local - _late_rows(f, through)
        else:
            through, local_in = None, local
        if local_in >= inwin:
            tag = "ok      " if local >= base and not late else "window  "
            lines.append(f"  {tag} {rel:<40}{local:>12,}  baseline {base:,}"
                         + (f", of which {late:,} after {last_day(end)}" if late else "")
                         + (f"; through {through}: local {local_in:,} vs baseline {inwin:,}"
                            if end else ""))
            continue
        local = local_in
        gap = inwin - local
        why = (f"baseline {base:,}" + (f", in-window {inwin:,}" if late else "")
               + f": {gap:,} row(s) fewer ({gap / inwin:.1%})")
        if rel in accept:
            lines.append(f"  ACCEPTED {rel:<40}{local:>12,}  {why}")
        else:
            lines.append(f"  REFUSED  {rel:<40}{local:>12,}  {why}")
            refused.append(rel)
    return lines, refused


def _configs(names: list[str]) -> str:
    """Declare every parquet as a named config.

    Without this the HF viewer auto-detects and presents the repo as one dataset,
    hiding the raw/processed/per-pair split that the layout exists to express. Each
    file becomes a selectable subset instead.
    """
    lines = ["configs:"]
    for rel in sorted(n for n in names if "/" not in n):
        lines.append(f"- config_name: {pathlib.Path(rel).stem}")
        lines.append(f"  data_files: {rel}")
    for rel in sorted(n for n in names if n.startswith("pairs/")):
        lines.append(f"- config_name: pair_{pathlib.Path(rel).stem.replace('-', '_')}")
        lines.append(f"  data_files: {rel}")
    return "\n".join(lines)


CARD_HEAD = """---
license: cc-by-4.0
language: [en]
tags: [linguistics, toponyms, ukraine, computational-social-science]
{configs}
---
"""

CARD_BODY = """
# Toponym adoption data

English-language adoption of Ukrainian vs Russian toponym transliterations
(Kyiv/Kiev, Chornobyl/Chernobyl, ...), {start_year} to {series_end}.

## Layout

Four stages, each a function of the previous. Only `raw` is expensive; everything
downstream is a recompute.

| file | what it is |
|---|---|
| `<source>_raw.parquet` | exactly what the provider returned, nothing dropped |
| `<source>_processed.parquet` | cleaned and regex-matched; only records containing a spelling |
| `pairs/<slug>.parquet` | every source's processed rows for one pair, unbalanced |
| `_manifest.json` | rows, columns and a content checksum for every artifact |

`raw` deliberately contains material the study excludes -- videos about Vladimir Putin
rather than Vladimir the Great, taxonomy papers citing the botanist T. Borsch, articles
about Odessa, Texas. They are what the provider returned, and they are the evidence
that the filtering did something. Without them the filtering cannot be audited.

## Window

| tier | runs through |
|---|---|
| `<source>_raw` | whatever the provider returned; not clamped |
| `<source>_processed` | {series_end} at the latest. A source ends earlier where its collection does, and {series_year} is a partial year |
| `pairs/` | {text_end}. The text analysis (keyness, collocations, clusters) is frozen there until {text_next} closes |

Dates are clamped, never invented: an undated row is kept rather than dropped.

## Columns in `_processed` and `pairs/`

`record_id, pair_slug, source, doc_id, url, date, title, text, ua_hits, ru_hits,
variant, match_context, text_hash`

There is no `verified` column. `ua_hits` and `ru_hits` are word-boundary match counts
and `variant` is derived from them; `match_context` is the surrounding text so the match
can be checked by eye. The evidence is the data.

`pairs/` carries `source` because it is needed at evaluation time -- the first question
about any cluster is whether it merely rediscovered the source. It must not be used as
a training feature.

## Coverage is uneven, on purpose

Not every pair has every source. Wikipedia, Trends and Ngrams produce counts rather
than documents and so appear in no `pairs/` file. YouTube census collection is partial.
The per-pair files show what each pair actually has rather than implying uniformity.
{retired}"""


def card(published: list[str], retained: list[str], revision: str) -> str:
    """The dataset card, with the window read from pipeline.config.

    The card said "2010-2026" while every processed tier stopped at 2025-12-31.
    """
    retired = ""
    if retained:
        names = ", ".join(f"`{r}`" for r in retained)
        rev = f" (revision `{revision[:8]}`)" if revision else ""
        retired = (f"\n## Retired pairs\n\n{names}: kept as last published{rev} and no "
                   f"longer rebuilt or republished. The study does not use "
                   f"{'it' if len(retained) == 1 else 'them'}.\n")
    return (CARD_HEAD.format(configs=_configs(published + retained))
            + CARD_BODY.format(start_year=STUDY_START[:4], series_end=SERIES_END,
                               series_year=SERIES_END[:4], text_end=TEXT_END,
                               text_next=int(TEXT_END[:4]) + 1, retired=retired))


def staged_manifest(withheld: list[str], baseline: pathlib.Path,
                    store: pathlib.Path = STORE) -> dict:
    """_manifest.json for the remote: the local one, with each withheld file's entry
    replaced by the baseline's, or removed when the baseline has none.

    The local manifest describes pairs/borscht as the degraded 1,443-row file;
    uploading it as-is would describe a remote file that this publish never touches.
    """
    out = json.loads((store / "_manifest.json").read_text())
    base_m = baseline / "_manifest.json"
    base = json.loads(base_m.read_text()) if base_m.exists() else {}
    for w in withheld:
        k = w[:-len(".parquet")]
        if k in base:
            out[k] = base[k]
        else:
            out.pop(k, None)
    return dict(sorted(out.items()))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--baseline", type=pathlib.Path, default=BASELINE,
                    help="local snapshot of the remote to guard against (default %(default)s)")
    ap.add_argument("--accept-shrink", action="append", default=[], metavar="PATH",
                    help="store-relative file allowed to lose in-window rows, e.g. "
                         "pairs/kyiv.parquet; repeat per file")
    a = ap.parse_args()

    files, withheld = publish_set(STORE)
    # A dry run reports every blocker, not just the first: the preflight fails on
    # today's pair files (53-131 email hits in each of the first six samples), and
    # stopping there hid the row-count verdicts the dry run exists to show.
    pii_failed = None
    try:
        _pii_preflight([f for f in files if f.parent.name == "pairs"])
    except SystemExit as e:
        if not a.dry_run:
            raise
        pii_failed = str(e)
        print(f"BLOCKER: {pii_failed}", file=sys.stderr)
    total = sum(f.stat().st_size for f in files) / 1e6
    print(f"{len(files)} parquet file(s), {total:,.0f} MB")
    for f in files:
        rel = f.relative_to(STORE)
        print(f"  {str(rel):<40}{f.stat().st_size/1e6:>8.1f} MB")
    for w in withheld:
        print(f"  WITHHELD {w}: pair not enabled in config/pairs.yaml; remote copy untouched")

    if not a.baseline.is_dir():
        print(f"\nno baseline snapshot at {a.baseline}: cannot prove no file shrinks, "
              f"refusing to publish", file=sys.stderr)
        return 1
    revision = ""
    if (a.baseline / "MANIFEST.json").exists():
        revision = json.loads((a.baseline / "MANIFEST.json").read_text()).get("revision", "")
    print(f"\nrow counts against {a.baseline}" + (f" (HF {revision[:8]})" if revision else ""))
    lines, refused = shrink_check(files, a.baseline, set(a.accept_shrink), STORE)
    print("\n".join(lines))
    unknown = sorted(set(a.accept_shrink) - {str(f.relative_to(STORE)) for f in files})
    if unknown:
        print(f"--accept-shrink names file(s) not being published: {unknown}", file=sys.stderr)
        return 1
    if refused:
        print(f"\nREFUSED: {len(refused)} file(s) would lose rows the remote holds. Check "
              f"each loss, then repeat with --accept-shrink for the ones that are "
              f"intended:\n  " + " ".join(f"--accept-shrink {r}" for r in refused),
              file=sys.stderr)
        if a.dry_run:
            print("\ndry run — nothing uploaded; a real publish would be refused")
        return 1

    published = [str(f.relative_to(STORE)) for f in files]
    retained = [w for w in withheld if (a.baseline / w).exists()]
    if a.dry_run:
        if pii_failed:
            print("\ndry run — nothing uploaded; a real publish would stop at the PII preflight")
            return 1
        print("\ndry run — nothing uploaded")
        return 0

    token = None
    for c in (pathlib.Path("/etc/secrets/hf"), pathlib.Path.home() / ".huggingface" / "token"):
        if c.exists():
            token = c.read_text().strip()
            break
    if not token:
        print("no HF token found", file=sys.stderr)
        return 1

    from huggingface_hub import CommitOperationAdd, HfApi
    api = HfApi(token=token)
    # The shrink guard compares against a LOCAL snapshot. If the remote moved
    # since that snapshot (a publish from elsewhere), the comparison says
    # nothing about what this upload would replace, so refuse until the
    # baseline is re-pulled. The snapshot's MANIFEST.json records its revision.
    head = api.dataset_info(REPO).sha or ""
    if not revision or not head or head != revision:
        print(f"REFUSED: remote {REPO} is at {head[:8] or '?'}, the baseline snapshot is "
              f"{revision[:8] or 'unrecorded'}. Snapshot the remote and pass --baseline.",
              file=sys.stderr)
        return 1
    # One commit with an explicit file list. upload_folder with a "*.parquet"
    # pattern is what made a retired pair's file an upload candidate in the first
    # place; the card and manifest are staged in memory rather than written into
    # data/store, where the manifest must keep describing the local files.
    ops = [CommitOperationAdd(path_in_repo=rel, path_or_fileobj=str(STORE / rel))
           for rel in published]
    ops.append(CommitOperationAdd(
        path_in_repo="_manifest.json",
        path_or_fileobj=json.dumps(staged_manifest(withheld, a.baseline, STORE),
                                   indent=2, sort_keys=True).encode()))
    ops.append(CommitOperationAdd(path_in_repo="README.md",
                                  path_or_fileobj=card(published, retained, revision).encode()))
    print(f"\nuploading {len(published)} file(s) to {REPO} ...")
    api.create_commit(repo_id=REPO, repo_type="dataset", operations=ops,
                      commit_message="Publish store layout: raw, processed, per-pair")
    remote = set(api.list_repo_files(repo_id=REPO, repo_type="dataset"))
    print(f"\nrepo now holds {len(remote)} file(s)")
    missing = sorted(set(published) - remote)
    lost = sorted(set(retained) - remote)
    print("all published files present remotely" if not missing else f"MISSING REMOTELY: {missing}")
    if lost:
        print(f"WITHHELD FILES GONE FROM THE REMOTE: {lost}", file=sys.stderr)
    print("Snapshot the remote and point --baseline at it before the next publish.")
    return 0 if not (missing or lost) else 1


if __name__ == "__main__":
    raise SystemExit(main())
