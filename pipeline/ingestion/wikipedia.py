"""Collect monthly en.wikipedia pageviews for each enabled pair's two titles.

Wikimedia REST per-article pageviews, all-access, agent=user (human readers),
monthly. Until 2026-11-06 the series used all-agents, which counts automated
traffic, and bots do not choose a spelling. On 2026-11-05 Kyiv 2026 read
~83% Ukrainian under all-agents against 87% in 2025, a dip that was entirely
automated traffic: views of the "Kiev" redirect classed as automated rose ~50%
(52.8K to 79.2K a month) while human views fell 21%, and user-only reads 89.6%
(2025) against 89.8% (2026). Every pair and every month is collected under one
agent class, so a series never mixes the two (wikipedia_merge refuses a run whose
agent differs from the live one unless every pair is replaced).

Known limit: Wikimedia classes traffic as "automated" only from 2020-05. Before
that, undetected bots sit inside "user" (spiders were split out from 2015-07), so
pre-2020-05 user views still include some automated traffic. No better consistent
definition exists in the API.

Range is 2015-07 .. SERIES_END. 2015-07 is the first month the pageviews API
serves, and no stored or shipped series has an earlier month: all 19 pairs on
the site start at 2015-07, as do dataset/ and both store tiers. There is no
earlier source to preserve; the legacy pagecount dumps were never used.

The titles and which spelling each measures are declared in wikipedia_titles.py.
Before fetching, one MediaWiki prop=info call records whether each title is an
article, a redirect or missing. A 404 from the pageviews API cannot tell a
missing title from a quiet one, and the difference decides whether a zero is a
measurement.

Output goes to a staging directory and never to dataset/:
    fetched.parquet   new rows in dataset/raw_wikipedia.parquet's schema
    titles.json       per pair and variant: title, status, months, first month
    run.json          as_of, range, agent class, request count
wikipedia_merge.py stages them against the live dataset, writes a diff report,
and is the only step that replaces dataset/raw_wikipedia.parquet. A --pairs run
gets its own default directory, and no run writes into a directory that already
holds a stage report (--replace-staged discards it): promote copies titles.json
from the run, so a recheck written over a staged run would ship its manifest.

Usage:
    python -m pipeline.ingestion.wikipedia --dry-run            # plan only, no requests
    python -m pipeline.ingestion.wikipedia                      # all enabled pairs
    python -m pipeline.ingestion.wikipedia --pairs kyiv,dynamo-kyiv --out-dir /tmp/wk
"""

import argparse
import hashlib
import json
import logging
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from pipeline.config import RAW_DIR, ROOT_DIR, SERIES_END, get_enabled_pairs, last_day
from pipeline.ingestion import wikipedia_titles as wt

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

WIKI_RAW_DIR = RAW_DIR / "wikipedia"
STAGING_DIR = WIKI_RAW_DIR / "staging"
PAGEVIEWS_API = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
INFO_API = "https://en.wikipedia.org/w/api.php"
USER_AGENT = "kyivnotkiev.org research (ivan@kyivnotkiev.org)"
AGENT = "user"           # pageviews agent class; see the module docstring
START_YM = "2015-07"     # the pageviews API's first month, not a study choice
YM = re.compile(r"\d{4}-(0[1-9]|1[0-2])")
STAGED_FILES = ("stage_report.json", "raw_wikipedia.parquet")   # wikipedia_merge.stage writes these
REQUEST_DELAY = 0.5      # the last run did 47 files in 59 s at this delay
RETRIES = 3

# Column order and dtypes of dataset/raw_wikipedia.parquet. `article`, `views`
# and `label` are what the 2026-05 collector wrote; `article` and `views` are
# filled the same way here, `label` was a hand-written caption and stays empty.
SCHEMA_COLUMNS = ["page_title", "variant", "date", "pageviews", "edits",
                  "ingested_at", "article", "views", "label", "pair_slug"]


def _get(url: str, params: dict | None = None) -> requests.Response:
    """GET with retry on 429/5xx. A 404 is returned, not raised."""
    for attempt in range(1, RETRIES + 1):
        r = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=30)
        if r.status_code == 404 or r.ok:
            return r
        if r.status_code in (429, 500, 502, 503, 504) and attempt < RETRIES:
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
    return r


def title_status(titles: list[str]) -> dict[str, dict]:
    """{stored title: {"status": "article" | "redirect" | "missing"}}.

    prop=info WITHOUT redirects=1, the request shape probed on 2026-10-19: each
    asked title comes back as its own page, flagged missing or redirect. A title
    absent from the reply raises rather than defaulting to "article".
    """
    out: dict[str, dict] = {}
    uniq = sorted({wt.display_title(t) for t in titles})
    for i in range(0, len(uniq), 50):                 # prop=info takes 50 titles a call
        chunk = uniq[i:i + 50]
        d = _get(INFO_API, {"action": "query", "format": "json", "formatversion": 2,
                            "prop": "info", "titles": "|".join(chunk)}).json()
        q = d.get("query", {})
        norm = {n["from"]: n["to"] for n in q.get("normalized", [])}
        pages = {p["title"]: p for p in q.get("pages", [])}
        for t in chunk:
            pg = pages.get(norm.get(t, t))
            if pg is None:
                raise RuntimeError(f"prop=info returned nothing for {t!r}")
            out[t] = {"status": "missing" if pg.get("missing") or pg.get("invalid")
                      else "redirect" if pg.get("redirect") else "article"}
    return {t: out[wt.display_title(t)] for t in titles}


def fetch_monthly(title: str, start_ym: str, end_ym: str) -> tuple[int, list[dict]]:
    """(http status, [{"date": "YYYY-MM-01", "views": int}]) for one title.

    The API omits months with no views, so a month absent here is a zero for an
    existing title. Whether the title existed is title_status()'s job.
    """
    url = (f"{PAGEVIEWS_API}/en.wikipedia/all-access/{AGENT}/{wt.api_title(title)}"
           f"/monthly/{start_ym.replace('-', '')}0100/{last_day(end_ym).replace('-', '')}00")
    r = _get(url)
    if r.status_code == 404:
        return 404, []
    rows = []
    for it in r.json().get("items", []):
        ts = it["timestamp"]
        ym = f"{ts[:4]}-{ts[4:6]}"
        if start_ym <= ym <= end_ym:
            rows.append({"date": f"{ym}-01", "views": int(it["views"])})
    return r.status_code, rows


def plan(slugs: list[str]) -> dict:
    titles = sorted({wt.TITLES[s][v] for s in slugs for v in wt.VARIANTS},
                    key=wt.display_title)
    return {"pairs": len(slugs), "titles": len(titles),
            "requests": len(titles) + -(-len(titles) // 50), "title_list": titles}


def collect(slugs: list[str], start_ym: str, end_ym: str) -> tuple[pd.DataFrame, dict, dict]:
    as_of = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    p = plan(slugs)
    status = title_status(p["title_list"])
    time.sleep(REQUEST_DELAY)
    fetched: dict[str, tuple[int, list[dict]]] = {}
    for t in p["title_list"]:
        fetched[t] = fetch_monthly(t, start_ym, end_ym)
        log.info(f"  {t!r}: {status[t]['status']}, http {fetched[t][0]}, "
                 f"{len(fetched[t][1])} months")
        time.sleep(REQUEST_DELAY)

    rows, manifest = [], {}
    for slug in slugs:
        entry = {"shared_article": wt.is_shared(slug)}
        for variant in wt.VARIANTS:
            t = wt.TITLES[slug][variant]
            code, months = fetched[t]
            st = dict(status[t])
            if st["status"] == "missing" and months:
                # Gone now but measured before: the history is real, keep it.
                st["status"] = "missing_now"
            entry[variant] = {"title": t, **st, "http": code, "months": len(months),
                              "first_month": months[0]["date"][:7] if months else None,
                              "last_month": months[-1]["date"][:7] if months else None,
                              "views": sum(m["views"] for m in months)}
            for m in months:
                rows.append({"page_title": t, "variant": variant, "date": m["date"],
                             "pageviews": float(m["views"]), "edits": float("nan"),
                             "ingested_at": as_of, "article": wt.api_title(t),
                             "views": float(m["views"]), "label": None, "pair_slug": slug})
        # A series is measurable only with two distinct titles that both exist.
        entry["measurable"] = (not entry["shared_article"]
                               and all(entry[v]["status"] != "missing" for v in wt.VARIANTS))
        manifest[slug] = entry
    df = pd.DataFrame(rows, columns=SCHEMA_COLUMNS)
    run = {"as_of": as_of.isoformat() + "Z", "start": start_ym, "end": end_ym, "agent": AGENT,
           "pairs": slugs, "titles": p["titles"], "requests": p["requests"],
           "rows": len(df)}
    return df, manifest, run


def check_end(end: str) -> None:
    """A closed YYYY-MM month in START_YM..SERIES_END; SERIES_END moves by hand only."""
    if not YM.fullmatch(str(end)):
        raise SystemExit(f"--end {end!r} is not YYYY-MM")
    open_month = date.today().strftime("%Y-%m")
    if end >= open_month:
        raise SystemExit(f"--end {end} is not a closed month (today is in {open_month})")
    if end > SERIES_END:
        raise SystemExit(f"--end {end} is past SERIES_END {SERIES_END} (pipeline/config.py)")
    if end < START_YM:
        raise SystemExit(f"--end {end} is before {START_YM}, the pageviews API's first month")


def default_out_dir(slugs: list[str], all_enabled: list[str], end: str) -> Path:
    base = f"{START_YM}_{end}"
    if sorted(slugs) == sorted(all_enabled):
        return STAGING_DIR / base
    s = sorted(slugs)
    name = ("+".join(s) if len(s) <= 3 else
            f"{len(s)}pairs-{hashlib.sha1(','.join(s).encode()).hexdigest()[:8]}")
    return STAGING_DIR / f"{base}.{name}"


def claim_out_dir(out_dir: Path, replace_staged: bool) -> None:
    """Refuse a staged directory; with replace_staged, unstage it so promote cannot run."""
    if not (out_dir / "stage_report.json").exists():
        return
    if not replace_staged:
        raise SystemExit(f"{out_dir} holds a stage report: a run written here would change files "
                         f"a pending promote copies. Use another --out-dir, or --replace-staged "
                         f"to discard that staging")
    for name in STAGED_FILES:
        (out_dir / name).unlink(missing_ok=True)
    log.warning(f"unstaged {out_dir}: stage it again before promoting")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--pairs", help="comma-separated slugs (default: every enabled pair)")
    ap.add_argument("--end", default=SERIES_END, help="last month, YYYY-MM (default SERIES_END)")
    ap.add_argument("--out-dir", type=Path, help="staging directory")
    ap.add_argument("--replace-staged", action="store_true",
                    help="write into a directory that was already staged, discarding its stage report")
    ap.add_argument("--dry-run", action="store_true", help="print the plan, make no requests")
    args = ap.parse_args()

    check_end(args.end)

    enabled = get_enabled_pairs()
    problems = wt.check(enabled)
    if problems:
        raise SystemExit("wikipedia_titles.py does not match config/pairs.yaml:\n  "
                         + "\n  ".join(problems))
    slugs = [p["slug"] for p in enabled]
    if args.pairs:
        want = [s.strip() for s in args.pairs.split(",")]
        unknown = sorted(set(want) - set(slugs))
        if unknown:
            raise SystemExit(f"not enabled: {unknown}")
        slugs = [s for s in slugs if s in want]

    p = plan(slugs)
    out_dir = args.out_dir or default_out_dir(slugs, [q["slug"] for q in enabled], args.end)
    if out_dir.resolve() in (WIKI_RAW_DIR.resolve(), (ROOT_DIR / "dataset").resolve()):
        raise SystemExit(f"{out_dir} is a live directory; write to staging and promote with "
                         f"wikipedia_merge.py")
    log.info(f"{p['pairs']} pairs, {p['titles']} titles, {p['requests']} requests, "
             f"{START_YM}..{args.end} -> {out_dir}")
    if args.dry_run:
        return
    claim_out_dir(out_dir, args.replace_staged)

    df, manifest, run = collect(slugs, START_YM, args.end)
    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "fetched.parquet", index=False)
    (out_dir / "titles.json").write_text(json.dumps(manifest, indent=1, ensure_ascii=False))
    (out_dir / "run.json").write_text(json.dumps(run, indent=1))
    for slug, e in manifest.items():
        if not e["measurable"]:
            why = "shared article" if e["shared_article"] else "missing title"
            log.info(f"  {slug}: not measurable ({why})")
    log.info(f"{len(df):,} rows -> {out_dir}. Next: python -m pipeline.ingestion."
             f"wikipedia_merge stage --run {out_dir}")


if __name__ == "__main__":
    main()
