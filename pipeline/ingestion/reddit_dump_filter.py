"""Stream-filter a monthly Reddit submissions dump (RS_*.zst) for the study's surface forms.

Reads zstd-decompressed NDJSON on stdin, applies a cheap byte-level prefilter
(first word of every term, over the raw line) before JSON-parsing candidates,
and writes matching records as NDJSON to stdout. Word-bounded, case-insensitive
exact matching over title+selftext, the same matching class as the PullPush
ingest: across the 12 multi-word pairs, 0 of 45,512 PullPush rows match only
with a hyphen or punctuation separator, so `\\s+` between words causes no break
at the PullPush->dump seam.

    zstd -dc --long=31 RS_2026-01.zst \\
      | rg --no-config -a -i -F -f prefilter.txt \\
      | python -m pipeline.ingestion.reddit_dump_filter --kind submission \\
          --expect-pairset <sha1> > RS_2026-01_matches.jsonl

pipeline/ingestion/reddit_dump_sweep.py builds that chain, writes prefilter.txt
from `prefilter_tokens()`, and records `pair_set()` in a sidecar so the merge can
refuse output filtered with a different pair set.

Every pair DEFINED in pairs.yaml is searched, enabled or not (`swept_pairs`).
That is the repo's collection rule since 81e4d270 ("the enabled flag decides
what the site shows, never what exists on disk"), and the 2025-09-05 sweep shows
why it matters here: it searched enabled pairs only, and zaporizhzhia was
already defined at that commit (f4a2f92d), merely disabled. Enabled two days
later, it had no Reddit rows for 2025-05-13..12-31, and the only fix is to
re-sweep 146.67 GB. Under this rule, enabling or retiring a pair needs no
re-sweep; only a new pair or a changed term does, and the pair-set hash makes
the merge refuse until that happens.

Submissions only. The 2010-2025 corpus is submissions-only (PullPush
search/submission/), and the 2025 comment matches (265,950 rows) never reached
any series. Ivan decided against comments on 2026-10-15.

The matching rule is unchanged from the 2025-09-05 sweep on purpose: the
re-sweep of 2025-05..12 is the only determinism check 2026 has (there is no
PullPush overlap to validate it against), and it only means something if the
matcher is the one that produced the files it is compared with.
"""
import argparse
import hashlib
import json
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent

# Recorded in every sidecar and checked by the merge. Bump it whenever the
# matching rule below changes, so output filtered under the old rule is refused
# rather than silently mixed with output under the new one.
MATCHER = "wb-ci-ws/v1: \\b(term)\\b, re.I, \\s+ between words, title+' '+selftext"
SCOPE = "defined"          # every pair in pairs.yaml, enabled or not; see the module docstring


def _pairs_yaml(config_path: pathlib.Path | None) -> list[dict]:
    if config_path is None:
        from pipeline.config import load_pairs
        return load_pairs()["pairs"]
    return yaml.safe_load(pathlib.Path(config_path).read_text())["pairs"]


def swept_pairs(config_path: pathlib.Path | None = None) -> list[dict]:
    """The pairs a sweep searches and the merge attributes: all of them.
    pipeline/store/migrate.py drops disabled pairs from the processed tier
    (process_text_source), exactly as it already does for PullPush's raw rows,
    which carry 46 slugs."""
    return list(_pairs_yaml(config_path))


def enabled_pairs(config_path: pathlib.Path | None = None) -> list[dict]:
    """Enabled pairs only. Not what a sweep searches any more; kept because it is
    what the 2025-09-05 sweep searched, which `gate` has to reconstruct."""
    return [p for p in _pairs_yaml(config_path) if p.get("enabled", True)]


def pair_set(pairs: list[dict]) -> dict:
    """What a filter run searched for: slugs, terms, and one hash over both.

    The hash covers (slug, ukrainian, russian) triples, not just slugs: the
    2025-09-05 sweep differs from today's pair set in a term as well as in
    membership (serhii-korolyov's Ukrainian form was 'Serhii Korolyov' until
    2026-09-11, now 'Serhii Korolov'), and a slug-only hash would have passed it.
    The enabled flag is deliberately NOT hashed: flipping it changes nothing a
    sweep searches for.
    """
    triples = sorted([str(p["slug"]), str(p["ukrainian"]), str(p["russian"])] for p in pairs)
    blob = json.dumps(triples, ensure_ascii=False, separators=(",", ":"))
    return {
        "slugs": [t[0] for t in triples],
        "terms": sorted({t for _, ua, ru in triples for t in (ua, ru)}),
        "sha1": hashlib.sha1(blob.encode()).hexdigest(),
    }


def compile_matcher(terms: list[str]) -> re.Pattern:
    """The word-bounded matcher over decoded text. Alternation order does not
    change whether it matches (a failed trailing \\b backtracks into the next
    alternative); the sort is only there so the compiled pattern is identical
    run to run."""
    terms = sorted(set(terms), key=lambda t: (-len(t), t))
    # The byte prefilter can only see a term that appears verbatim in the raw
    # JSON line. JSON encoders escape non-ASCII letters (\\u0457), never ASCII
    # ones, so this holds only while every term is ASCII. All 94 defined terms
    # are (checked 2026-10-19).
    bad = [t for t in terms if not t.isascii()]
    if bad:
        raise ValueError(f"non-ASCII term(s) {bad}: the raw-line prefilter would miss "
                         "\\u-escaped spellings; extend prefilter_tokens before using them")
    return re.compile(r"\b(?:" + "|".join(
        re.escape(t).replace(r"\ ", r"\s+") for t in terms) + r")\b", re.I)


def prefilter_tokens(terms: list[str]) -> list[str]:
    """First word of every term, lowercased. A multi-word term's later words can
    follow a JSON escape ("Babyn\\nYar") rather than a space, so only the first
    word is guaranteed to appear verbatim in the raw line. These are both the rg
    -F -i patterns and the in-process byte prefilter, so rg passes a superset of
    what the filter keeps and the output does not depend on rg."""
    return sorted({t.split()[0].lower() for t in terms})


def text_of(d: dict) -> str:
    return (d.get("title") or "") + " " + (d.get("selftext") or "")


KEEP = ("id", "subreddit", "author", "created_utc", "title", "selftext",
        "body", "score", "num_comments", "permalink", "link_id")


class NotASubmission(ValueError):
    pass


def filter_lines(lines, out, terms: list[str], stats: dict) -> None:
    rx = compile_matcher(terms)
    # The 2025 filter ran the prefilter as one bytes regex, re.I, over the raw
    # line. A bytes pattern folds ASCII case only, and bytes.lower() lowers
    # ASCII only, so `token in raw.lower()` keeps exactly the same lines (same
    # verdict on all 22,607 lines below). The regex was the costliest step per
    # candidate. Measured 2026-10-19 on 22,607 real RS lines: regex 2,929
    # lines/s with the 48 enabled terms and 1,815 with all 94, the substring
    # test 17,002 and 12,780; the whole filter went from 2,039 candidate lines/s
    # (48 terms, regex) to 3,584 (94 terms, substring).
    toks = [t.encode() for t in prefilter_tokens(terms)]
    for raw in lines:
        stats["lines_in"] += 1
        low = raw.lower()
        if not any(t in low for t in toks):
            continue
        stats["candidates"] += 1
        try:
            d = json.loads(raw)
        except Exception:                              # noqa: BLE001
            stats["json_errors"] += 1
            continue
        if "title" not in d:
            # A comment has `body` and no `title`. Matching it on title+selftext
            # would find nothing and exit 0, so an RC file fed here by mistake
            # would read as a month with no matches rather than as an error.
            raise NotASubmission(f"record {d.get('id')!r} has no title (keys: {sorted(d)[:8]})")
        if not rx.search(text_of(d)):
            continue
        keep = {k: d.get(k) for k in KEEP}
        keep["kind"] = "submission"
        out.write(json.dumps(keep, ensure_ascii=False) + "\n")
        stats["matches"] += 1
        if stats["matches"] % 2000 == 0:
            print(f"  {stats['matches']:,} matches / {stats['lines_in']:,} lines",
                  file=sys.stderr, flush=True)


def main() -> int:
    ap = argparse.ArgumentParser()
    # Kept as a flag so the 2025 command line still parses; 'comment' is refused.
    ap.add_argument("--kind", choices=["submission"], default="submission")
    ap.add_argument("--config", type=pathlib.Path, default=None,
                    help="pairs.yaml to read (default: the repo's config/pairs.yaml)")
    ap.add_argument("--enabled-only", action="store_true",
                    help="search enabled pairs only, as the 2025-09-05 sweep did; for "
                         "reconstructing its output, never for a sweep")
    ap.add_argument("--expect-pairset", default=None,
                    help="refuse to run unless the searched pair set hashes to this; the "
                         "sweep passes the hash it wrote into the sidecar and the rg pattern file")
    ap.add_argument("--stats-json", type=pathlib.Path, default=None)
    a = ap.parse_args()

    ps = pair_set((enabled_pairs if a.enabled_only else swept_pairs)(a.config))
    if a.expect_pairset and a.expect_pairset != ps["sha1"]:
        print(f"REFUSED: pairs.yaml now hashes to {ps['sha1']}, the sweep expected "
              f"{a.expect_pairset}. It changed mid-run; re-plan the month.", file=sys.stderr)
        return 2

    stats = {"lines_in": 0, "candidates": 0, "json_errors": 0, "matches": 0,
             "pairset_sha1": ps["sha1"], "matcher": MATCHER}
    out = sys.stdout
    try:
        filter_lines(sys.stdin.buffer, out, ps["terms"], stats)
    except NotASubmission as e:
        print(f"REFUSED: not a submissions dump: {e}", file=sys.stderr)
        return 3
    out.flush()
    if a.stats_json:
        a.stats_json.write_text(json.dumps(stats, indent=1))
    print(f"DONE submission: {stats['matches']:,} matches from {stats['lines_in']:,} lines "
          f"({stats['candidates']:,} prefiltered, {stats['json_errors']} unparseable)",
          file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
