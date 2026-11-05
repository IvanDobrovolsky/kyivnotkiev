"""Count OpenAlex works with each spelling in the title, per publication year.

OpenAlex (openalex.org) indexes 250M+ academic works. Each variant is a
title.search filter grouped by publication_year, 2010 through SERIES_END.

The output is the file the exporter reads, data/raw/openalex/openalex_all_pairs.json,
in its schema: a list of 47 entries in pairs.yaml order, each with
russian_term, ukrainian_term, category, yearly [{year, russian_count,
ukrainian_count, total}], total_russian, total_ukrainian and pair_slug, plus
as_of (fetch time, UTC) and through (the last publication date counted). It is
written to a STAGING path only. The collector this replaces wrote that live path
in a pair_id schema with no pair_slug, so its first successful run would have
blanked OpenAlex on the site; openalex_merge.py diffs and promotes instead.

Counts revise, including closed years: the same query read Kyiv 2025 as 712
Ukrainian titles on 2026-04-03 and 632 on 2026-10-15, and the partial 2026 share
went from 100% to 87.5%. as_of is therefore part of every entry, and a promotion
always comes with a per-year diff.

Three changes from the old query, each measured:
  - per_page 200, and a refusal if groups_count exceeds the groups returned. At
    per_page=50 a group_by query returns only the top 50 years by count: 'Kiev'
    came back with 50 of 114 groups (lowest 27), and any year under the cutoff
    read as 0 papers, which biases the share toward the Ukrainian side.
  - from_publication_date / to_publication_date bound the years in the query,
    so at most 17 groups exist. The upper bound is last_day(SERIES_END): the
    2026 point counts January to September, matching the series window. The
    live file gets it only once SOURCE_SERIES_END['openalex'] reaches it:
    openalex_merge.stage holds later years back.
    Closed years are unaffected: the bounded 'Kiev' query read 2015 = 78 and
    2025 = 45 on 2026-10-19, as the unbounded one did on 2026-10-15.
  - A failed request raises. The old code returned {} on any error, which read
    as "no papers" and made the other side 100%.

Cost. Unkeyed requests share a $0.10/day budget per IP address and cost
$0.001 each (meta.cost_usd, 2026-10-19 probe). The enabled pairs are 48
requests; all 47 defined pairs (--all-defined) are 94, close to the whole day's
budget for every process on this IP. So the default refreshes the enabled pairs
and carries the disabled entries over verbatim from the live file, with as_of
null; metalist-kharkiv, which the live file lacks, is fetched (2 more requests,
50 in all). A key in /etc/secrets/openalex is used if present.

Usage:
    python -m pipeline.ingestion.openalex --dry-run
    python -m pipeline.ingestion.openalex                   # enabled pairs, 50 requests
    python -m pipeline.ingestion.openalex --pairs kyiv,odesa --out /tmp/oa.json
"""

import argparse
import copy
import hashlib
import json
import logging
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path

import requests

from pipeline.config import RAW_DIR, SERIES_END, STUDY_START, last_day, load_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OPENALEX_API = "https://api.openalex.org/works"
OA_RAW_DIR = RAW_DIR / "openalex"
LIVE_PATH = OA_RAW_DIR / "openalex_all_pairs.json"   # the exporter reads this; never written here
STAGING_DIR = OA_RAW_DIR / "staging"
KEY_FILE = Path("/etc/secrets/openalex")
MAILTO = "ivan@kyivnotkiev.org"
REQUEST_DELAY = 0.2
PER_PAGE = 200
RETRIES = 3
START_YEAR = int(STUDY_START[:4])  # pre-2010 data too sparse/noisy for an adoption signal
YM = re.compile(r"\d{4}-(0[1-9]|1[0-2])")
STAGED_FILES = ("stage_report.json", "openalex_all_pairs.staged.json")   # openalex_merge.stage writes these
ENTRY_KEYS = ["russian_term", "ukrainian_term", "category", "yearly",
              "total_russian", "total_ukrainian", "pair_slug", "as_of", "through"]


class Truncated(RuntimeError):
    pass


class Budget:
    def __init__(self, max_calls: int):
        self.max_calls, self.calls, self.cost_usd = max_calls, 0, 0.0

    def spend(self, term: str) -> None:
        if self.calls >= self.max_calls:
            raise RuntimeError(f"call ceiling {self.max_calls} reached before {term!r}")
        self.calls += 1


def _api_key() -> str | None:
    return KEY_FILE.read_text().strip() if KEY_FILE.exists() else None


def count_papers_by_year(term: str, through: str, budget: Budget) -> dict[int, int]:
    """{publication_year: works with `term` in the title}, years START_YEAR..through."""
    if any(c in term for c in ",|"):
        raise ValueError(f"{term!r} would split the OpenAlex filter")
    params = {"filter": f"title.search:{term},from_publication_date:{START_YEAR}-01-01,"
                        f"to_publication_date:{through}",
              "group_by": "publication_year", "per_page": PER_PAGE, "mailto": MAILTO}
    key = _api_key()
    if key:
        params["api_key"] = key
    for attempt in range(1, RETRIES + 1):
        budget.spend(term)
        resp = requests.get(OPENALEX_API, params=params, timeout=30)
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < RETRIES:
            time.sleep(2 ** attempt)
            continue
        resp.raise_for_status()
        break
    data = resp.json()
    meta = data.get("meta", {})
    budget.cost_usd += float(meta.get("cost_usd") or 0)
    groups = data.get("group_by", [])
    if meta.get("groups_count") is not None and int(meta["groups_count"]) > len(groups):
        raise Truncated(f"{term!r}: {len(groups)} of {meta['groups_count']} year groups returned")
    last_year = int(through[:4])
    return {int(g["key"]): int(g["count"]) for g in groups
            if str(g["key"]).isdigit() and START_YEAR <= int(g["key"]) <= last_year}


def entry(pair: dict, ru: dict[int, int], ua: dict[int, int], category, as_of: str,
          through: str) -> dict:
    # Only years with at least one paper, as the live file has them: the exporter
    # smooths by list index, so padding zero years would change its windows.
    rows = [{"year": y, "russian_count": ru.get(y, 0), "ukrainian_count": ua.get(y, 0),
             "total": ru.get(y, 0) + ua.get(y, 0)} for y in sorted(set(ru) | set(ua))]
    rows = [r for r in rows if r["total"] > 0]
    return {"russian_term": pair["russian"], "ukrainian_term": pair["ukrainian"],
            "category": category, "yearly": rows,
            "total_russian": sum(r["russian_count"] for r in rows),
            "total_ukrainian": sum(r["ukrainian_count"] for r in rows),
            "pair_slug": pair["slug"], "as_of": as_of, "through": through}


def to_fetch(pairs: list[dict], refresh: set[str], live: list[dict]) -> set[str]:
    """`refresh`, plus any defined pair the live file lacks: there is nothing to carry.

    The 2026-05-11 file holds 47 entries but not pairs.yaml's 47: it has
    kharkiv-university (removed from pairs.yaml in ecdccc63) and no metalist-kharkiv.
    """
    have = {e["pair_slug"] for e in live}
    return {p["slug"] for p in pairs
            if not (p.get("is_control") and p["russian"] == p["ukrainian"])
            and (p["slug"] in refresh or p["slug"] not in have)}


def collect(pairs: list[dict], refresh: set[str], live: list[dict], through: str,
            budget: Budget, as_of: str) -> list[dict]:
    """One entry per pairs.yaml pair, in its order: fetched now, or carried over verbatim.

    Carried entries keep the live file's values, totals included. Those totals
    disagree with their own years in 34 of 47 live entries (kyiv UA 4,526 against
    4,418 summed), apparently the 2026 count left in after the years were clamped
    to 2025; nothing reads them, and a carried entry is not re-derived.
    """
    by_slug = {e["pair_slug"]: e for e in live}
    fetch = to_fetch(pairs, refresh, live)
    out = []
    for p in pairs:
        slug = p["slug"]
        old = by_slug.get(slug)
        if slug not in fetch:
            # Deep copy: a shallow one shares `yearly` with the live object, so any
            # later edit of the staged entry would silently edit the live one too.
            out.append(copy.deepcopy({k: old.get(k) for k in ENTRY_KEYS}))   # as_of null
            continue
        ru = count_papers_by_year(p["russian"], through, budget)
        time.sleep(REQUEST_DELAY)
        ua = count_papers_by_year(p["ukrainian"], through, budget)
        time.sleep(REQUEST_DELAY)
        e = entry(p, ru, ua, old.get("category") if old else None, as_of, through)
        log.info(f"  {slug:<22} {e['total_ukrainian']:>6} UA {e['total_russian']:>6} RU, "
                 f"years {e['yearly'][0]['year'] if e['yearly'] else '-'}"
                 f"..{e['yearly'][-1]['year'] if e['yearly'] else '-'}")
        out.append(e)
    dropped = sorted(set(by_slug) - {p["slug"] for p in pairs})
    if dropped:
        log.info(f"  dropped live entries pairs.yaml no longer defines: {dropped} "
                 f"(kept in the promote backup)")
    return out


def check_end(end: str) -> None:
    """A closed YYYY-MM month in STUDY_START..SERIES_END; SERIES_END moves by hand only."""
    if not YM.fullmatch(str(end)):
        raise SystemExit(f"--end {end!r} is not YYYY-MM")
    open_month = date.today().strftime("%Y-%m")
    if end >= open_month:
        raise SystemExit(f"--end {end} is not a closed month (today is in {open_month})")
    if end > SERIES_END:
        raise SystemExit(f"--end {end} is past SERIES_END {SERIES_END} (pipeline/config.py)")
    if end < STUDY_START:
        raise SystemExit(f"--end {end} is before STUDY_START {STUDY_START}")


def claim_out_dir(out_dir: Path, replace_staged: bool) -> None:
    """Refuse a staged directory; with replace_staged, unstage it so promote cannot run."""
    if not (out_dir / "stage_report.json").exists():
        return
    if not replace_staged:
        raise SystemExit(f"{out_dir} holds a stage report: a run written here would change files "
                         f"a pending promote checks. Use another --out, or --replace-staged to "
                         f"discard that staging")
    for name in STAGED_FILES:
        (out_dir / name).unlink(missing_ok=True)
    log.warning(f"unstaged {out_dir}: stage it again before promoting")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--pairs", help="comma-separated slugs to refresh (default: enabled pairs)")
    ap.add_argument("--all-defined", action="store_true",
                    help="refresh all 47 defined pairs (94 requests)")
    ap.add_argument("--end", default=SERIES_END, help="last month counted, YYYY-MM (default SERIES_END)")
    ap.add_argument("--live", type=Path, default=LIVE_PATH, help="read-only: carried-over entries")
    ap.add_argument("--out", type=Path, help="staging file")
    ap.add_argument("--max-calls", type=int, default=60)
    ap.add_argument("--replace-staged", action="store_true",
                    help="write into a directory that was already staged, discarding its stage report")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    check_end(args.end)
    through = last_day(args.end)
    base = f"{START_YEAR}_{args.end}"
    if args.pairs and not args.all_defined:
        s_ = sorted(x.strip() for x in args.pairs.split(","))
        base += "." + ("+".join(s_) if len(s_) <= 3 else
                       f"{len(s_)}pairs-{hashlib.sha1(','.join(s_).encode()).hexdigest()[:8]}")
    elif args.all_defined:
        base += ".all-defined"
    out = args.out or STAGING_DIR / base / "openalex_all_pairs.json"
    if out.resolve() == LIVE_PATH.resolve() or out.parent.resolve() == OA_RAW_DIR.resolve():
        raise SystemExit(f"{out} is beside what the exporter reads; stage and promote with "
                         f"openalex_merge.py")

    pairs = load_pairs()["pairs"]
    if args.all_defined:
        refresh = {p["slug"] for p in pairs}
    elif args.pairs:
        refresh = {s.strip() for s in args.pairs.split(",")}
        unknown = sorted(refresh - {p["slug"] for p in pairs})
        if unknown:
            raise SystemExit(f"not defined: {unknown}")
    else:
        refresh = {p["slug"] for p in pairs if p.get("enabled", True)}
    live = json.loads(args.live.read_text()) if args.live.exists() else []
    fetch = to_fetch(pairs, refresh, live)
    n_req = 2 * len(fetch)
    log.info(f"fetch {len(fetch)} of {len(pairs)} pairs"
             + (f" (+{sorted(fetch - refresh)}: absent from the live file)" if fetch - refresh else "")
             + f", {n_req} requests, {START_YEAR}-01-01..{through} -> {out}")
    if n_req > args.max_calls:
        raise SystemExit(f"refusing: {n_req} requests planned, --max-calls {args.max_calls}")
    if args.dry_run:
        return
    claim_out_dir(out.parent, args.replace_staged)

    budget = Budget(args.max_calls)
    started = time.time()
    as_of = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    entries = collect(pairs, refresh, live, through, budget, as_of)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(entries, indent=2))
    run = {"as_of": as_of, "through": through, "refreshed": sorted(fetch), "requests": budget.calls,
           "cost_usd": round(budget.cost_usd, 4), "seconds": round(time.time() - started, 1),
           "entries": len(entries), "per_page": PER_PAGE, "keyed": _api_key() is not None}
    (out.parent / "run.json").write_text(json.dumps(run, indent=1))
    log.info(f"{len(entries)} entries, {budget.calls} requests, ${budget.cost_usd:.3f} -> {out}. "
             f"Next: python -m pipeline.ingestion.openalex_merge stage --run {out.parent}")


if __name__ == "__main__":
    main()
