"""GDELT mention counts from the GKG, with provenance preserved.

WHY THIS EXISTS
---------------
The previous GDELT series (`dataset/raw_gdelt.parquet`, 20.1M "mentions") is not a
measure of English-language spelling. It was built by matching
`DocumentIdentifier OR AllNames` and storing only a collapsed `matched_term`, so:

  * 78.9% of rows matched ONLY `AllNames` — a canonicalised NER field, not text the
    outlet wrote. GDELT normalises entity names across languages, so a Ukrainian
    article's "Володимир Великий" surfaces as "Vladimir The Great" and gets counted
    as *Russian* usage. That is a variant flip, not a miss, and it is invisible in totals.
  * 88.7% of matched documents were machine-translated from another language and were
    never English at all.
  * The `url` column was never persisted, so no row could be re-adjudicated. That is
    what forced a full re-query rather than a patch.
  * Terms were matched with `LIKE '%x%'`, so `%borsch%` also matched "borscht" and
    reported 27% Ukrainian where the attested figure is 79.6%.

DESIGN RULES
------------
1. Persist the URL on every row. Never aggregate away provenance.
2. Never collapse the OR — `url_match` and `allnames_match` are separate booleans.
3. Variant comes ONLY from the URL path, which is CMS-authored text. `AllNames` is
   canonicalised and cannot attest a spelling; it is kept as a volume denominator
   and is never summed with the attested series.
4. Word-boundary regex, never `LIKE '%x%'`.
5. Nothing is filtered server-side. `TranslationInfo` is captured raw and filtering
   happens locally, so decisions stay auditable and reversible.
6. This is a PRODUCER: it keeps everything it collected, 2026 included. The study
   window is applied by the consumers (exporter, migrate, analyze_pair) from
   pipeline.config. Nothing here clamps by date.

COST
----
BigQuery bills columns x partitions, NOT matches, so the WHERE clause is free.
A one-pair query and a 24-pair query were measured at byte-for-byte identical cost
(667,884,946,710 bytes = 0.607 TiB). ALWAYS query every pair in ONE pass. Running it
per pair would cost 24 x 0.607 TiB ~= 14.6 TiB ~= $85 for data one pass gets free
inside the 1 TiB/month free tier. There is deliberately no per-pair option.

The source table is partitioned by INGESTION day (`_PARTITIONTIME`, no partitioning
column), so a partition bound is what makes an increment cheap. Measured by dry run
on 2026-10-15: partitions 2026-09-12..09-30 cost 1,937,961,527 bytes, one full 2026
month 3.13-3.66 GB, while the unbounded scan had grown to 672,995,223,973 bytes
(0.612 TiB) -- 61% of a month's free tier for 0.3% new data.

THE TABLE OF RECORD AND INCREMENTS
----------------------------------
`gdelt_v2.mentions_raw` (14,394,987 rows, scanned 2026-09-12, billed 0.609 TiB) is
the table of record. Restoring it costs a 0.612 TiB rescan, so nothing here can
truncate it:

  * The full scan needs `query --full`, and refuses if mentions_raw exists.
  * Every query writes WRITE_EMPTY after a client-side existence check; there is no
    WRITE_TRUNCATE anywhere in this module. The previous version wrote
    WRITE_TRUNCATE to mentions_raw on every run, so a partition-bounded query run
    through it would have replaced 14.4M rows with the delta, and `download` would
    then have rewritten the matched parquet from that delta.
  * An increment goes to its OWN table, gdelt_v2.mentions_<from>_<to>, is downloaded
    to its own parquet, and is appended to the matched parquet locally.

Each increment is bounded twice: by `_PARTITIONTIME` (what is billed) and by the
row's own `DATE` (what the rows are). How the two line up is unmeasured -- a GKG
file stamped 23:45 may land in the next day's partition, and the partition day may
follow a non-UTC clock -- so the partition window runs `--lead-days` before and
`--slack-days` past the DATE window, and the DATE window is expected to START
before the local watermark so that rows ingested after the previous scan are not
lost. The overlap is removed on append by row identity (every selected column,
see NATURAL_KEY), and the increment keeps `partition_time` so the first run
measures the alignment. Selecting it adds no bytes: the 2026-09-12..10-01 window
dry-ran at 2,060,014,170 bytes with and without it; the one slack day costs
122,052,643 of those.

The month's free tier is checked before every paid query: the dry-run bytes and the
month's billed bytes so far (INFORMATION_SCHEMA.JOBS_BY_PROJECT) are printed, and
the query is refused above --max-bytes (default 10 GB) or when billed + planned
would pass 0.9 TiB. September 2026 had already billed 0.609 TiB when the full scan
ran; one more full scan that month would have been paid.

FILES
-----
The BigQuery tables are the source of record; local parquet is derived.
`download` flags rows as it streams, so there is no second identical copy on disk.

    gdelt_mentions_matched.parquet    every GKG row the regex hit + language/attestation flags
                                      (mentions_raw, plus every appended increment)
    gdelt_mentions_final.parquet      the metric: attested, English, deduped
    gdelt_mentions_monthly.parquet    pair x month x variant counts, for the site
    increments/mentions_<a>_<b>.parquet   one downloaded increment, with partition_time
    increments/ledger.jsonl           every query / download / append, with its counts
    backups/                          the previous matched / dataset file, on EVERY replace

USAGE
-----
    python -m pipeline.ingestion.gdelt_mentions query --from 2026-09-12 --to 2026-10-01 --dry-run
    python -m pipeline.ingestion.gdelt_mentions query --from 2026-09-12 --to 2026-10-01
    python -m pipeline.ingestion.gdelt_mentions download --from 2026-09-12 --to 2026-10-01
    python -m pipeline.ingestion.gdelt_mentions append --from 2026-09-12 --to 2026-10-01
    python -m pipeline.ingestion.gdelt_mentions final      # apply metric, dedup, aggregate
    python -m pipeline.ingestion.gdelt_mentions export     # dataset/raw_gdelt.parquet

    python -m pipeline.ingestion.gdelt_mentions query --full ...   # the one-off full scan;
                                                                   # refuses while mentions_raw exists
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shutil
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import yaml

PROJECT = "kyivnotkiev-bq"
DATASET = f"{PROJECT}.gdelt_v2"
DEST = f"{DATASET}.mentions_raw"
SOURCE = "`gdelt-bq.gdeltv2.gkg_partitioned`"
CONFIG = pathlib.Path("config/pairs.yaml")
OUT = pathlib.Path("data/raw/gdelt/mentions_v2")
MATCHED = "gdelt_mentions_matched.parquet"
FINAL = "gdelt_mentions_final.parquet"
MONTHLY = "gdelt_mentions_monthly.parquet"
DATASET_FILE = pathlib.Path("dataset/raw_gdelt.parquet")

# The only write disposition this module uses. WRITE_EMPTY fails rather than
# overwrite, and every query also checks for the destination before it bills.
WRITE_DISPOSITION = "WRITE_EMPTY"

# The identity of a matched row is every column the scan selects. The query does
# not select GKGRECORDID, and (url, gkg_date) is not unique: the base repeats it
# 346 times, and 91 of those rows differ in TranslationInfo, domain or AllNames
# (measured 2026-10-19), so keying on it would discard real records. The other
# 255 repeats are exact GDELT duplicates, which the base keeps; appending
# subtracts multisets, so they survive as they are. Re-recordings of one URL at
# other timestamps are distinct rows here (final dedups those by URL).
NATURAL_KEY = ["url", "domain", "gkg_date", "translation_info", "url_term",
               "name_term", "url_match", "allnames_match"]

# Refuse a FULL scan if a change to pairs.yaml has blown it far past what we measured.
MEASURED_BYTES = 667_884_946_710
BYTES_CEILING = int(MEASURED_BYTES * 1.5)
# An increment of one month measured 3.13-3.66 GB; 10 GB admits about two months
# and stops a mistyped window from turning into a full scan.
MAX_BYTES = 10 * 10 ** 9
# Leave 0.1 TiB of the 1 TiB free tier for re-reads and checks.
MONTH_CAP_BYTES = int(0.9 * 1024 ** 4)
FREE_TIB_PER_MONTH = 1.0
USD_PER_TIB = 6.25

BILLED_SQL = """
SELECT IFNULL(SUM(total_bytes_billed), 0) AS billed, COUNT(*) AS jobs
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_TRUNC(CURRENT_TIMESTAMP(), MONTH)
""".strip()


def load_terms() -> list[tuple[str, str, str, str]]:
    """Return (slug, variant, term, regex_fragment) for EVERY defined pair.

    Deliberately not filtered by `enabled`. The destination table used to be
    written WRITE_TRUNCATE, so keying collection off the enabled flag meant that
    retiring a pair silently destroyed its raw layer on the next scan — and
    that enabling a pair changed the regex for every OTHER pair, because
    REGEXP_EXTRACT takes the leftmost match and a new term can claim URLs that
    previously went elsewhere. Neither is acceptable: the enabled flag decides
    what the SITE SHOWS, never what exists on disk. An increment must also use
    the same 47-pair regex as the base, or its rows would be flagged differently.

    It is also free. BigQuery bills columns x partitions, not matched rows, so
    the WHERE clause costs nothing: a one-pair regex and a 47-pair regex both
    dry-run at 669,552,904,550 bytes, byte for byte. Collecting everything once
    keeps attribution stable no matter which pairs ship.
    """
    doc = yaml.safe_load(CONFIG.read_text())
    pairs = doc["pairs"] if isinstance(doc, dict) and "pairs" in doc else doc
    rows = []
    for p in pairs:
        if not p.get("ukrainian") or not p.get("russian"):
            continue
        for variant in ("ukrainian", "russian"):
            term = str(p[variant]).strip().lower()
            # Escape each word separately. re.escape() escapes spaces too, which would
            # turn "vladimir the great" into a literal "[" — the separator class must
            # be joined in afterwards so URL slugs (kiev-protests, volodymyr_the_great)
            # and AllNames (space-separated) both match.
            frag = "[-_ ]".join(re.escape(w) for w in term.split())
            rows.append((p["slug"], variant, term, frag))
    return rows


def _longest_first(frags) -> list[str]:
    # Longest-first so "borscht" wins over "borsch" and "kievan rus" over "kiev".
    # Ties are broken alphabetically. Sorting a set by length alone left the 19
    # tied lengths in hash-seed order, so the saved SQL differed byte-wise from a
    # rebuild with the same 94 alternates. Ties cannot change a match while every
    # fragment is [a-z0-9] words joined by [-_ ] (all 94 are, 2026-10-19): two
    # fragments matching at one position then stand in a prefix relation, and the
    # longer term has the strictly longer fragment. A term with an escaped
    # character (a literal hyphen, say) would break that, and this comment with it.
    return sorted(set(frags), key=lambda f: (-len(f), f))


def build_regex(rows) -> str:
    return r"\b(" + "|".join(_longest_first(r[3] for r in rows)) + r")\b"


@dataclass(frozen=True)
class Window:
    """One increment: rows whose DATE is in [d_lo, d_hi), read from the
    ingestion partitions [p_lo, p_hi)."""
    d_lo: date
    d_hi: date
    p_lo: date
    p_hi: date

    @classmethod
    def make(cls, d_lo: date, d_hi: date, slack_days: int = 1,
             lead_days: int = 1) -> "Window":
        if d_hi <= d_lo:
            raise ValueError(f"empty window: --to {d_hi} must be after --from {d_lo}")
        if slack_days < 0 or lead_days < 0:
            raise ValueError("--slack-days / --lead-days cannot be negative")
        # The partition window runs past the DATE window at BOTH ends. A file
        # stamped late on the last day can land in the next day's partition; and
        # nothing documents which clock _PARTITIONTIME's day boundary follows. If
        # it is US Pacific (UTC-7/-8), the first 7-8 hours of a UTC DATE day sit
        # in the previous day's partition, and a window starting at d_lo would
        # silently drop them. One lead day costs one partition (~0.12 GB,
        # measured as the slack day's 122,052,643 bytes) and the increment's
        # partition_time column then measures the alignment at both edges.
        return cls(d_lo, d_hi, d_lo - timedelta(days=lead_days),
                   d_hi + timedelta(days=slack_days))

    @property
    def table(self) -> str:
        return f"{DATASET}.mentions_{self.d_lo:%Y%m%d}_{self.d_hi:%Y%m%d}"

    @property
    def stem(self) -> str:
        return self.table.rsplit(".", 1)[1]

    def predicate(self) -> str:
        return (f"_PARTITIONTIME >= TIMESTAMP('{self.p_lo.isoformat()}') "
                f"AND _PARTITIONTIME < TIMESTAMP('{self.p_hi.isoformat()}')\n"
                f"  AND DATE >= {self.d_lo:%Y%m%d}000000 AND DATE < {self.d_hi:%Y%m%d}000000")

    def as_dict(self) -> dict:
        return {k: getattr(self, k).isoformat() for k in ("d_lo", "d_hi", "p_lo", "p_hi")}


def build_sql(rx: str, window: Window | None = None) -> str:
    """The scan. With no window it is the 2026-09-12 full scan, unchanged; with one
    it adds the two bounds and records `_PARTITIONTIME`, which is a pseudo-column
    and adds no billed bytes, so the increment measures DATE/partition alignment."""
    extra = ",\n  _PARTITIONTIME                                       AS partition_time" if window else ""
    match = (f"REGEXP_CONTAINS(LOWER(DocumentIdentifier), r'{rx}')\n"
             f"   OR REGEXP_CONTAINS(LOWER(IFNULL(AllNames,'')), r'{rx}')")
    where = match if window is None else f"{window.predicate()}\n  AND ({match})"
    return f"""
SELECT
  DocumentIdentifier                                   AS url,
  SourceCommonName                                     AS domain,
  DATE                                                 AS gkg_date,
  TranslationInfo                                      AS translation_info,
  REGEXP_EXTRACT(LOWER(DocumentIdentifier), r'{rx}')   AS url_term,
  REGEXP_EXTRACT(LOWER(IFNULL(AllNames,'')), r'{rx}')  AS name_term,
  REGEXP_CONTAINS(LOWER(DocumentIdentifier), r'{rx}')  AS url_match,
  REGEXP_CONTAINS(LOWER(IFNULL(AllNames,'')), r'{rx}') AS allnames_match{extra}
FROM {SOURCE}
WHERE {where}
""".strip()


def flag_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Attach pair/variant/language/attestation flags. Nothing is dropped, only labelled."""
    rows = load_terms()
    lut = {term: (slug, variant) for slug, variant, term, _ in rows}
    norm = lambda s: re.sub(r"[-_]+", " ", s.lower()).strip() if isinstance(s, str) else None
    u = df.url_term.map(norm)
    n = df.name_term.map(norm)
    df["pair_url"] = u.map(lambda t: lut.get(t, (None, None))[0])
    df["var_url"] = u.map(lambda t: lut.get(t, (None, None))[1])
    df["pair_nam"] = n.map(lambda t: lut.get(t, (None, None))[0])
    df["var_nam"] = n.map(lambda t: lut.get(t, (None, None))[1])
    # srclc:<lang>;eng:<engine> when GDELT translated the document; NULL when natively English.
    df["src_lang"] = df.translation_info.str.extract(r"srclc:(\w+)")[0]
    df["native_en"] = df.translation_info.isna() | (df.translation_info == "")
    df["date"] = pd.to_datetime(df.gkg_date.astype(str).str[:8], format="%Y%m%d", errors="coerce")
    df["pair_slug"] = df.pair_url.fillna(df.pair_nam)
    return df


# ── safety: watermark, budget, destinations ──────────────────────────────────

def sql_alternates(sql: str) -> set[str] | None:
    """The set of regex alternates a saved scan SQL searched for, or None if the
    SQL carries no recognisable regex. A set, not the string: tie order inside
    the alternation changed once (see _longest_first) without changing a match."""
    m = re.search(r"r'\\b\((.*?)\)\\b'", sql)
    return set(m.group(1).split("|")) if m else None


def regex_drift(current_sql: str, base_sql_path: pathlib.Path | None = None) -> list[str]:
    """Reasons an increment's regex is not the base scan's.

    The overlap is deduplicated on every selected column, url_term and
    name_term included, and both are REGEXP_EXTRACT outputs. If pairs.yaml
    gained or changed a term after the base scan, the same GKG row comes back
    with a different url_term/name_term, is not recognised as already held, and
    is appended a second time. `final` dedups by URL, so the metric survives,
    but matched would hold the overlap twice and every tier built on matched
    rows (reconcile's matched column, the unattested fetch pool) would count it
    twice. So a drifted regex is refused rather than silently absorbed."""
    base_sql_path = base_sql_path or OUT / "gdelt_mentions_query.sql"
    if not base_sql_path.exists():
        return [f"no base scan SQL at {base_sql_path}: cannot prove the increment uses "
                f"the base scan's regex"]
    base = sql_alternates(base_sql_path.read_text())
    cur = sql_alternates(current_sql)
    if base is None or cur is None:
        return ["could not read the regex out of the base or increment SQL"]
    if base != cur:
        return [f"regex differs from the base scan's ({base_sql_path.name}): "
                f"only in base {sorted(base - cur)[:6]}, only in increment {sorted(cur - base)[:6]}. "
                f"Overlap rows would be appended twice; re-flag deliberately or pass "
                f"--allow-regex-change"]
    return []


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _ledger(event: dict) -> None:
    """Append one line to the increments ledger, so the watermark history and every
    billed byte are auditable without BigQuery."""
    path = OUT / "increments" / "ledger.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    event = {"at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **event}
    with path.open("a") as fh:
        fh.write(json.dumps(event, default=str) + "\n")


def matched_watermark(path: pathlib.Path | None = None) -> int | None:
    """Largest gkg_date in the matched parquet, read from row-group statistics
    (no column scan). 20260913023000 after the 2026-09-12 full scan."""
    import pyarrow.parquet as pq
    path = path or OUT / MATCHED
    if not path.exists():
        return None
    f = pq.ParquetFile(path)
    ci = f.schema_arrow.get_field_index("gkg_date")
    hi = None
    for i in range(f.num_row_groups):
        st = f.metadata.row_group(i).column(ci).statistics
        if st is None or not st.has_min_max:
            # Statistics missing: fall back to reading the one column.
            import pyarrow.compute as pc
            return int(pc.max(pq.read_table(path, columns=["gkg_date"]).column(0)).as_py())
        hi = st.max if hi is None else max(hi, st.max)
    return int(hi) if hi is not None else None


def _gkg_day(gkg_date: int) -> date:
    return datetime.strptime(str(gkg_date)[:8], "%Y%m%d").date()


def check_window_against_watermark(window: Window, watermark: int | None,
                                   allow_gap: bool = False) -> list[str]:
    """Reasons to refuse an increment, given the local matched watermark.

    The DATE window must start on or before the watermark's day. Starting after it
    leaves a hole that no later increment fills, because each one starts from the
    watermark it finds. Starting before it is the intended overlap: the full scan
    ran at 02:31 UTC on 2026-09-13, so rows for 09-12/09-13 ingested after that are
    missing from it and only an overlapping window recovers them.
    """
    if watermark is None:
        return [] if allow_gap else [
            "no local matched parquet, so the watermark is unknown; "
            "download the table of record first (or pass --allow-gap)"]
    wm_day = _gkg_day(watermark)
    if window.d_lo > wm_day and not allow_gap:
        return [f"--from {window.d_lo} is after the local watermark {watermark} ({wm_day}): "
                f"rows dated after {watermark} and before {window.d_lo} would never be "
                f"collected. Start on or before {wm_day} (the overlap is deduplicated on "
                f"append), or pass --allow-gap."]
    return []


def budget_refusals(planned: int, billed: int, max_bytes: int,
                    month_cap: int = MONTH_CAP_BYTES) -> list[str]:
    """Reasons to refuse a scan of `planned` bytes when the month has billed `billed`."""
    out = []
    if planned > max_bytes:
        out.append(f"planned {planned:,} bytes exceeds --max-bytes {max_bytes:,}")
    if billed + planned > month_cap:
        out.append(f"month billed {billed:,} + planned {planned:,} = {billed + planned:,} "
                   f"exceeds the 0.9 TiB cap {month_cap:,}")
    return out


def month_billed_bytes(client) -> tuple[int, int]:
    """(bytes billed, jobs) in kyivnotkiev-bq since the start of the UTC month.

    The UTC month starts 7-8 hours before the Pacific one, so this counts
    slightly more than the billing month, never less. This check itself billed
    20,971,520 bytes when it was first run (2026-10-15). It sees only this
    project; no other project on the billing account has BigQuery jobs (measured
    2026-10-15), so it is the whole free-tier draw today.
    """
    from google.cloud import bigquery
    job = client.query(BILLED_SQL, job_config=bigquery.QueryJobConfig(use_query_cache=False))
    row = list(job.result())[0]
    return int(row["billed"] or 0), int(row["jobs"] or 0)


def destination_for(window: Window | None, full: bool) -> str:
    """The only two shapes a destination can take. An increment never names the
    table of record."""
    if full:
        if window is not None:
            raise ValueError("--full takes no window")
        return DEST
    if window is None:
        raise ValueError("an increment needs --from and --to")
    dest = window.table
    if dest == DEST or not re.fullmatch(rf"{re.escape(DATASET)}\.mentions_\d{{8}}_\d{{8}}", dest):
        raise ValueError(f"refusing increment destination {dest}")
    return dest


def _table_exists(client, table: str) -> bool:
    from google.api_core.exceptions import NotFound
    try:
        client.get_table(table)
        return True
    except NotFound:
        return False


def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _window_from_args(args) -> Window | None:
    if getattr(args, "full", False):
        if args.date_from or args.date_to:
            raise SystemExit("--full scans everything; it takes no --from/--to")
        return None
    if not (args.date_from and args.date_to):
        raise SystemExit("give --from and --to (YYYY-MM-DD) for an increment, or --full")
    return Window.make(_parse_day(args.date_from), _parse_day(args.date_to),
                       getattr(args, "slack_days", 1), getattr(args, "lead_days", 1))


# ── query ─────────────────────────────────────────────────────────────────────

def cmd_query(args) -> int:
    from google.cloud import bigquery

    rows = load_terms()
    window = _window_from_args(args)
    dest = destination_for(window, args.full)
    sql = build_sql(build_regex(rows), window)
    print(f"{len({r[0] for r in rows})} pairs, {len(rows)} terms, one pass")
    if window:
        w = window
        print(f"increment  DATE [{w.d_lo}, {w.d_hi})  partitions [{w.p_lo}, {w.p_hi})")
        wm = matched_watermark()
        print(f"local matched watermark: {wm}")
        refusals = check_window_against_watermark(window, wm, args.allow_gap)
        if refusals:
            print("ABORT: " + "\n       ".join(refusals), file=sys.stderr)
            return 1
        if wm is not None and window.d_hi <= _gkg_day(wm):
            print("note: the whole window ends before the watermark's day; "
                  "only late-ingested rows can be new")
        drift = regex_drift(sql)
        if drift and not args.allow_regex_change:
            print("ABORT: " + "\n       ".join(drift), file=sys.stderr)
            return 1
        if window.d_hi > datetime.now(timezone.utc).date():
            print("note: the window reaches past today; its last days are still being "
                  "ingested and the next increment must overlap them")
    print(f"destination: {dest}  ({WRITE_DISPOSITION})")
    if args.print_sql:
        print(sql)

    client = bigquery.Client(project=PROJECT)
    if _table_exists(client, dest):
        what = "the table of record" if dest == DEST else "this increment"
        print(f"ABORT: {dest} already exists ({what}). Nothing is ever overwritten; "
              f"re-read it with `download`, or choose a new window.", file=sys.stderr)
        return 1

    dry = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    planned = int(dry.total_bytes_processed)
    tib = planned / 1024 ** 4
    print(f"dry run: {planned:,} bytes = {tib:.4f} TiB "
          f"(free tier {FREE_TIB_PER_MONTH:.0f} TiB/month; beyond it ${tib * USD_PER_TIB:.2f})")
    if args.full and planned > BYTES_CEILING:
        print(f"ABORT: scan exceeds ceiling {BYTES_CEILING:,} bytes.", file=sys.stderr)
        return 1

    if args.dry_run and not args.check_billing:
        print("month billed so far: not checked (the INFORMATION_SCHEMA query bills about "
              "10-20 MiB; pass --check-billing). The real run always checks it first.")
        if planned > args.max_bytes:
            print(f"would ABORT: planned {planned:,} bytes exceeds --max-bytes {args.max_bytes:,}",
                  file=sys.stderr)
            return 1
        return 0

    billed, jobs = month_billed_bytes(client)
    print(f"month billed so far: {billed:,} bytes = {billed / 1024 ** 4:.4f} TiB over {jobs} job(s); "
          f"after this scan {(billed + planned) / 1024 ** 4:.4f} TiB of the 0.9 TiB cap")
    refusals = budget_refusals(planned, billed, args.max_bytes)
    if refusals:
        print("ABORT: " + "\n       ".join(refusals), file=sys.stderr)
        return 1
    if args.dry_run:
        return 0

    # BigQuery enforces the same cap server-side: a job that would bill more fails
    # before it is charged.
    cap = min(args.max_bytes, MONTH_CAP_BYTES - billed)
    t0 = time.time()
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        destination=dest, write_disposition=WRITE_DISPOSITION,
        create_disposition="CREATE_IF_NEEDED", use_query_cache=False,
        maximum_bytes_billed=cap))
    result = job.result()
    print(f"done in {time.time() - t0:.0f}s | billed {job.total_bytes_billed:,} bytes "
          f"({job.total_bytes_billed / 1024 ** 4:.4f} TiB) | rows {result.total_rows:,}")
    print(f"-> {dest}   (re-reading this table is cheap; never repeat the scan)")

    sql_dir = OUT if window is None else OUT / "increments"
    sql_dir.mkdir(parents=True, exist_ok=True)
    sql_path = sql_dir / ("gdelt_mentions_query.sql" if window is None else f"{window.stem}.sql")
    sql_path.write_text(sql)
    _ledger({"event": "query", "table": dest, "job_id": job.job_id,
             "window": window.as_dict() if window else None,
             "planned_bytes": planned, "billed_bytes": int(job.total_bytes_billed or 0),
             "month_billed_before": billed, "rows": int(result.total_rows or 0)})
    return 0


# ── download / append ─────────────────────────────────────────────────────────

def _stabilise(df: pd.DataFrame) -> pd.DataFrame:
    """Pin dtypes so every streamed batch yields an identical parquet schema."""
    for c in ("url", "domain", "translation_info", "url_term", "name_term",
              "pair_url", "var_url", "pair_nam", "var_nam", "src_lang", "pair_slug"):
        df[c] = df[c].astype("string")
    return df


def _stream_table(table: str, path: pathlib.Path) -> int:
    """Stream a BigQuery table to `path`, flagging as it goes. Writes to a
    .partial file and renames on success, so an interrupted download never leaves
    a short file at the real name."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import bigquery, bigquery_storage

    client = bigquery.Client(project=PROJECT)
    reader = bigquery_storage.BigQueryReadClient()
    tmp = path.with_name(path.name + ".partial")
    writer, n, t0 = None, 0, time.time()
    for i, batch in enumerate(client.list_rows(table).to_arrow_iterable(bqstorage_client=reader)):
        df = _stabilise(flag_rows(batch.to_pandas()))
        t = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(tmp, t.schema, compression="zstd")
        writer.write_table(t)
        n += len(df)
        if i % 50 == 0:
            print(f"  {n:,} rows ({time.time() - t0:.0f}s)", flush=True)
    if writer is None:
        return 0
    writer.close()
    os.replace(tmp, path)
    return n


def cmd_download(args) -> int:
    """Stream a destination table to disk, flagging as we go.

    Flagging is row-local, so it happens inside the stream. That is deliberate:
    an unflagged dump would be a byte-for-byte duplicate of this file with fewer
    columns, and the BigQuery table is already the source of record.

    An increment downloads to increments/<table>.parquet and never touches the
    matched parquet; `append` does that. `--full` re-downloads the table of
    record, and only to a matched path that does not exist: the matched file
    holds every appended increment, and a whole-table download over it would
    silently drop them.
    """
    OUT.mkdir(parents=True, exist_ok=True)
    if args.full:
        table, path = DEST, OUT / MATCHED
        if path.exists():
            print(f"ABORT: {path} exists and carries every appended increment. Move it "
                  f"aside yourself if you really mean to restore it from {DEST}.", file=sys.stderr)
            return 1
    else:
        window = _window_from_args(args)
        table = destination_for(window, False)
        path = OUT / "increments" / f"{window.stem}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            print(f"ABORT: {path} already exists; delete it first to re-download.", file=sys.stderr)
            return 1
    n = _stream_table(table, path)
    if not n:
        print(f"no rows returned from {table}", file=sys.stderr)
        return 1
    print(f"saved {n:,} matched rows -> {path} ({path.stat().st_size / 1e6:.1f} MB)")
    _ledger({"event": "download", "table": table, "path": str(path), "rows": n})
    return 0


def _backup(path: pathlib.Path) -> pathlib.Path:
    """Keep the file about to be replaced, every time, under a name no later run
    reuses. The previous rule backed up only when no backup existed yet, so after
    the first export every later one overwrote dataset/raw_gdelt.parquet unguarded.
    A hard link costs nothing on disk until the original is replaced."""
    bdir = OUT / "backups"
    bdir.mkdir(parents=True, exist_ok=True)
    dest = bdir / f"{path.stem}.{_stamp()}.bak{path.suffix}"
    if dest.exists():
        raise SystemExit(f"backup {dest} already exists; refusing to overwrite it")
    try:
        os.link(path, dest)
    except OSError:
        shutil.copy2(path, dest)
    return dest


def _row_key(df: pd.DataFrame) -> pd.Series:
    """Exact identity string over NATURAL_KEY. Nulls get their own marker, so a
    NULL TranslationInfo never equals an empty one; no hashing, so no collisions.
    The marker must not contain NUL: pandas' string hashtable stops at "\\x00", and
    a NUL marker merged rows that differed only after it (two AllNames values of
    one URL in one GKG file became one group in the unit test)."""
    parts = [df[c].astype("string").fillna("\x1eNULL\x1e") for c in NATURAL_KEY]
    return parts[0].str.cat(parts[1:], sep="\x1f")


def append_increment(base: pathlib.Path, inc: pathlib.Path, dry_run: bool = False,
                     allow_regex_change: bool = False) -> dict:
    """Append the rows of `inc` that `base` lacks, comparing every selected column.

    Deduplication is a multiset difference: the i-th copy of a row in the
    increment is new only if the base holds fewer than i+1 copies. A row the base
    already has (the intended overlap) is dropped; an exact GDELT duplicate inside
    new territory is kept, the same way the base kept its 255.

    The base is never rewritten in place and never loses or changes a row: the
    merged file is written beside it, its row count verified, the base backed up,
    and only then swapped in, so its 14,394,987 rows remain exactly what
    mentions_raw holds. Only base rows inside the increment's gkg_date range can
    collide, so only those are read; the check stays exact without loading 14.4M
    URLs.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    inc_sql = inc.with_suffix(".sql")
    if not inc_sql.exists():
        raise SystemExit(f"{inc_sql} missing: the increment's regex cannot be checked "
                         f"against the base scan's")
    drift = regex_drift(inc_sql.read_text(), base.parent / "gdelt_mentions_query.sql")
    if drift and not allow_regex_change:
        raise SystemExit("refusing to append: " + "; ".join(drift))
    base_schema = pq.read_schema(base)
    cols = base_schema.names
    d = pd.read_parquet(inc)
    missing = [c for c in cols if c not in d.columns]
    if missing:
        raise SystemExit(f"{inc} lacks matched columns {missing}")
    d = d.reset_index(drop=True)
    st: dict = {"base": str(base), "increment": str(inc), "increment_rows": len(d)}
    base_rows = pq.ParquetFile(base).metadata.num_rows
    watermark = matched_watermark(base)
    st.update(base_rows=base_rows, watermark_before=watermark)

    if "partition_time" in d.columns and len(d):
        # Measured alignment: how many days after its GKG DATE a row was ingested.
        part_day = pd.to_datetime(d.partition_time, utc=True).dt.tz_localize(None).dt.normalize()
        lag = (part_day - d.date.dt.normalize()).dt.days
        st["partition_lag_days"] = {str(k): int(v) for k, v in lag.value_counts().sort_index().items()}

    if len(d):
        lo, hi = int(d.gkg_date.min()), int(d.gkg_date.max())
        seen = pq.read_table(base, columns=NATURAL_KEY,
                             filters=[("gkg_date", ">=", lo), ("gkg_date", "<=", hi)]).to_pandas()
        n_base = _row_key(seen).value_counts()
        k = _row_key(d)
        copy_no = k.groupby(k).cumcount()
        held = k.map(n_base).fillna(0).astype(int)
        is_new = (copy_no >= held).to_numpy()
        st["base_rows_in_range"] = len(seen)
    else:
        is_new, copy_no = pd.Series([], dtype=bool).to_numpy(), pd.Series([], dtype=int)
    new = d[is_new]
    st.update(dup_in_base=int((~is_new).sum()),
              gdelt_repeats_kept=int(((copy_no > 0).to_numpy() & is_new).sum()),
              new_rows=len(new))
    if watermark is not None and len(new):
        # New rows at or before the old watermark were ingested after the previous
        # scan ran: the measured cost of not overlapping.
        st["new_at_or_before_watermark"] = int((new.gkg_date <= watermark).sum())
    if len(new):
        st["new_by_month"] = {k: int(v) for k, v in
                              new.date.dt.to_period("M").astype(str).value_counts().sort_index().items()}
    if dry_run or not len(new):
        st["written"] = False
        return st

    tmp = base.with_name(base.name + ".appending")
    writer = pq.ParquetWriter(tmp, base_schema, compression="zstd")
    try:
        for batch in pq.ParquetFile(base).iter_batches(batch_size=262_144):
            writer.write_batch(batch)
        add = pa.Table.from_pandas(_stabilise(new[cols].copy()), schema=base_schema,
                                   preserve_index=False)
        writer.write_table(add)
    finally:
        writer.close()
    got = pq.ParquetFile(tmp).metadata.num_rows
    if got != base_rows + len(new):
        tmp.unlink()
        raise SystemExit(f"merged file has {got:,} rows, expected {base_rows + len(new):,}; base untouched")
    backup = _backup(base)
    os.replace(tmp, base)
    st.update(written=True, backup=str(backup), rows_after=got, watermark_after=matched_watermark(base))
    return st


def cmd_append(args) -> int:
    if args.increment:
        inc = pathlib.Path(args.increment)
    else:
        window = _window_from_args(args)
        inc = OUT / "increments" / f"{window.stem}.parquet"
    if not inc.exists():
        print(f"missing {inc}; run `download` first", file=sys.stderr)
        return 1
    base = OUT / MATCHED
    st = append_increment(base, inc, dry_run=args.dry_run,
                          allow_regex_change=args.allow_regex_change)
    for k, v in st.items():
        print(f"  {k:<28}{v}")
    if st["written"]:
        print(f"appended {st['new_rows']:,} rows -> {base} ({st['rows_after']:,} rows); "
              f"previous file kept at {st['backup']}")
    elif not st["new_rows"]:
        print("nothing new to append; matched parquet unchanged")
    else:
        print("dry run: nothing written")
    if not args.dry_run:
        _ledger({"event": "append", **st})
    return 0


def cmd_clean(args) -> int:
    """Re-flag in place after config/pairs.yaml changes, without re-downloading."""
    path = OUT / MATCHED
    df = _stabilise(flag_rows(pd.read_parquet(path, columns=["url", "domain", "gkg_date", "translation_info", "url_term", "name_term", "url_match", "allnames_match"])))
    tmp = path.with_name(path.name + ".cleaning")
    df.to_parquet(tmp, compression="zstd", index=False)
    backup = _backup(path)
    os.replace(tmp, path)
    usable = df[df.native_en & df.url_match]
    print(f"re-flagged {len(df):,} rows -> {path} (previous file kept at {backup})")
    print(f"natively English : {df.native_en.sum():,} ({df.native_en.mean() * 100:.1f}%)")
    print(f"URL-attested     : {df.url_match.sum():,} ({df.url_match.mean() * 100:.1f}%)")
    print(f"usable metric    : {len(usable):,} ({len(usable) / len(df) * 100:.1f}%)")
    return 0


def cmd_final(args) -> int:
    """The deliverable: one row per attested English-language article.

    Two filters define the metric, and both are load-bearing:
      * native_en  -- TranslationInfo is NULL. 88.7% of GKG matches are machine
        translations of foreign-language articles and say nothing about English usage.
      * url_match  -- the spelling appears in the CMS-authored URL path. AllNames is
        canonicalised NER and cannot attest a spelling.

    Note this filters on the LANGUAGE OF THE DOCUMENT, never on the domain. Ukrainian
    and Russian outlets are kept when they publish in English -- english.nv.ua,
    en.24tv.ua, english.pravda.ru all survive, as do kyivpost.com and unian.info. A
    TLD blocklist would discard Ukraine's English-language press, the population most
    worth measuring.

    GDELT re-records some articles across many timestamps (one URL appears 2,683
    times), so rows are deduped by URL keeping the earliest date -- publication, not
    re-observation. No URL was found carrying conflicting variants, so this is lossless.
    The same rule makes appended increments safe here: a 2026-09 re-recording of a
    2019 article keeps its 2019 date.
    """
    # Only the metric's rows and columns: the matched file is 14.4M rows and
    # reading it whole held every AllNames-only and translated row in memory
    # for a filter that discards 88.7% of them.
    g = pd.read_parquet(OUT / MATCHED,
                        columns=["url", "domain", "date", "url_term", "pair_slug", "var_url",
                                 "native_en", "url_match"],
                        filters=[("native_en", "==", True), ("url_match", "==", True)])
    before = len(g)

    # Collection covers every defined pair so that toggling one cannot destroy
    # its data. ATTRIBUTION, though, must run over the ENABLED set only: the
    # leftmost-match rule otherwise hands articles to pairs the site does not
    # publish, and the enabled pair loses them. Measured on the 47-pair scan,
    # that silently moved 530 rows from chornobyl to chornobyl-exclusion-zone,
    # 252 from kyiv to kyiv-pechersk-lavra and 393 from volodymyr-zelenskyy to
    # dnipro — all three destinations invisible. Re-derive from the URL, which
    # the matched frame keeps, using only enabled terms.
    doc = yaml.safe_load(CONFIG.read_text())
    pairs = doc["pairs"] if isinstance(doc, dict) and "pairs" in doc else doc
    enabled = {p.get("slug") for p in pairs if p.get("enabled")}
    term_map, frags = {}, []
    for slug, variant, term, frag in load_terms():
        if slug not in enabled:
            continue
        term_map[term] = (slug, variant)
        frags.append(frag)
    if frags:
        # Longest-first, same rule as build_regex: "kyivan rus" must win over
        # "kyiv" and "chicken kiev" over "kiev" when both start at one position.
        rx = re.compile("(" + "|".join(_longest_first(frags)) + ")", re.I)
        found = g.url.astype(str).str.lower().str.extract(rx, expand=False)
        hit = found.notna()
        # The regex fragment joins words with [-_ ], so a URL yields the SLUG form
        # ("volodymyr-zelenskyy") while term_map is keyed by the pairs.yaml form
        # ("volodymyr zelenskyy"). flag_rows() normalises before its lookup; this
        # path re-derives attribution and has to do the same. Without it every
        # multi-word pair resolved to ("", ""): the 2026-09-12 run lost 11 of 24
        # enabled pairs and left 17,103 rows with a blank pair_slug, while the 13
        # single-word pairs were untouched because they carry no separator.
        _norm = lambda t: re.sub(r"[-_]+", " ", t).strip() if isinstance(t, str) else t
        key = found.map(_norm)
        moved = int((g.pair_slug != key.map(lambda t: term_map.get(t, ("", ""))[0])).sum())
        g = g[hit].copy()
        g["pair_slug"] = key[hit].map(lambda t: term_map.get(t, ("", ""))[0])
        g["var_url"] = key[hit].map(lambda t: term_map.get(t, ("", ""))[1])
        unresolved = int((g.pair_slug == "").sum())
        if unresolved:
            raise SystemExit(f"attribution failed for {unresolved:,} rows "
                             f"(terms: {sorted(key[hit][g.pair_slug == ''].unique())[:8]})")
        print(f"attributed over {len(enabled)} enabled pairs "
              f"({len(frags)} terms); {moved:,} row(s) re-attributed, "
              f"{before - len(g):,} dropped as belonging only to disabled pairs")
        before = len(g)
    g = g.sort_values("date").drop_duplicates("url", keep="first")
    g = g.rename(columns={"var_url": "variant"})[
        ["pair_slug", "variant", "date", "domain", "url", "url_term"]]

    path = OUT / FINAL
    g.to_parquet(path, compression="zstd", index=False)

    g["month"] = g.date.dt.to_period("M").astype(str)
    monthly = (g.groupby(["pair_slug", "month", "variant"]).size()
                 .reset_index(name="count").sort_values(["pair_slug", "month", "variant"]))
    mpath = OUT / MONTHLY
    monthly.to_parquet(mpath, compression="zstd", index=False)

    print(f"deduped {before:,} -> {len(g):,} rows ({before - len(g):,} repeat records dropped)")
    print(f"saved {len(g):,} rows -> {path} ({path.stat().st_size / 1e6:.1f} MB)")
    print(f"saved {len(monthly):,} rows -> {mpath} ({mpath.stat().st_size / 1e6:.2f} MB)")
    return 0


def cmd_export(args) -> int:
    """Write dataset/raw_gdelt.parquet in the legacy schema the site pipeline reads.

    Downstream (`export_site_data._load`) expects exactly:
        pair_slug, date ("YYYY-MM-01" string), variant, matched_term, source_domain, count
    Emitting that shape means every consumer -- charts, per-source stats, the world
    map, holdouts -- picks up the rebuilt numbers with no downstream change.

    Every month collected is written, 2026 included; the exporter applies the window.
    The file being replaced is backed up first, every time.
    """
    src = OUT / FINAL
    if not src.exists():
        print(f"missing {src}; run `final` first", file=sys.stderr)
        return 1
    df = pd.read_parquet(src)
    df["date"] = df.date.dt.to_period("M").dt.to_timestamp().dt.strftime("%Y-%m-01")
    out = (df.groupby(["pair_slug", "date", "variant", "url_term", "domain"])
             .size().reset_index(name="count")
             .rename(columns={"url_term": "matched_term", "domain": "source_domain"}))
    out["count"] = out["count"].astype("Int64")
    out = out[["pair_slug", "date", "variant", "matched_term", "source_domain", "count"]]

    dest = DATASET_FILE
    if dest.exists():
        print(f"backed up previous file -> {_backup(dest)}")
    tmp = dest.with_name(dest.name + ".writing")
    out.to_parquet(tmp, compression="zstd", index=False)
    os.replace(tmp, dest)
    print(f"wrote {len(out):,} rows -> {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    print(f"  articles: {int(out['count'].sum()):,} | pairs: {out.pair_slug.nunique()} | domains: {out.source_domain.nunique():,}")
    return 0


def _add_window_args(p, with_full: bool = True) -> None:
    if with_full:
        p.add_argument("--full", action="store_true",
                       help="the whole table of record; refuses while it exists")
    p.add_argument("--from", dest="date_from", metavar="YYYY-MM-DD",
                   help="first GKG DATE day of the increment (inclusive)")
    p.add_argument("--to", dest="date_to", metavar="YYYY-MM-DD",
                   help="end of the increment's DATE window (exclusive)")
    p.add_argument("--slack-days", type=int, default=1,
                   help="ingestion partitions scanned past --to (default 1)")
    p.add_argument("--lead-days", type=int, default=1,
                   help="ingestion partitions scanned before --from (default 1)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    q = sub.add_parser("query", help="a paid scan into its own destination table")
    _add_window_args(q)
    q.add_argument("--dry-run", action="store_true", help="report bytes and stop (free)")
    q.add_argument("--check-billing", action="store_true",
                   help="with --dry-run, also read the month's billed bytes (bills ~10-20 MiB)")
    q.add_argument("--max-bytes", type=int, default=MAX_BYTES,
                   help=f"refuse a scan above this many bytes (default {MAX_BYTES:,})")
    q.add_argument("--allow-gap", action="store_true",
                   help="allow --from after the local watermark (leaves rows uncollected)")
    q.add_argument("--allow-regex-change", action="store_true",
                   help="allow an increment whose regex differs from the base scan's")
    q.add_argument("--print-sql", action="store_true")
    q.set_defaults(func=cmd_query)

    d = sub.add_parser("download", help="destination table -> parquet (increment, or --full)")
    _add_window_args(d)
    d.set_defaults(func=cmd_download)

    a = sub.add_parser("append", help="append a downloaded increment to the matched parquet")
    _add_window_args(a, with_full=False)
    a.add_argument("--increment", help="path of a downloaded increment parquet")
    a.add_argument("--dry-run", action="store_true", help="report what would be appended")
    a.add_argument("--allow-regex-change", action="store_true",
                   help="append although the increment's regex differs from the base scan's")
    a.set_defaults(func=cmd_append)

    sub.add_parser("clean", help="re-flag in place after a pairs.yaml change").set_defaults(func=cmd_clean)
    sub.add_parser("final", help="apply the metric filters, dedup, write the deliverable").set_defaults(func=cmd_final)
    sub.add_parser("export", help="write dataset/raw_gdelt.parquet for the site pipeline").set_defaults(func=cmd_export)
    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
