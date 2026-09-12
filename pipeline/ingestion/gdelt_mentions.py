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

COST
----
BigQuery bills columns x partitions, NOT matches, so the WHERE clause is free.
A one-pair query and a 24-pair query were measured at byte-for-byte identical cost
(667,884,946,710 bytes = 0.607 TiB). ALWAYS query every pair in ONE pass. Running it
per pair would cost 24 x 0.607 TiB ~= 14.6 TiB ~= $85 for data one pass gets free
inside the 1 TiB/month free tier.

Results land in a destination table so the expensive scan is never repeated; every
later question is answered by re-reading that table cheaply.

FILES
-----
The BigQuery destination table is the source of record; local parquet is derived.
`download` flags rows as it streams, so there is no second identical copy on disk.

    gdelt_mentions_matched.parquet    every GKG row the regex hit + language/attestation flags
    gdelt_mentions_final.parquet      the metric: attested, English, deduped
    gdelt_mentions_monthly.parquet    pair x month x variant counts, for the site

USAGE
-----
    python -m pipeline.ingestion.gdelt_mentions query      # the one paid scan
    python -m pipeline.ingestion.gdelt_mentions download   # table -> matched parquet
    python -m pipeline.ingestion.gdelt_mentions final      # apply metric, dedup, aggregate
"""
from __future__ import annotations

import argparse
import pathlib
import re
import sys
import time

import pandas as pd
import yaml

PROJECT = "kyivnotkiev-bq"
DEST = f"{PROJECT}.gdelt_v2.mentions_raw"
SOURCE = "`gdelt-bq.gdeltv2.gkg_partitioned`"
CONFIG = pathlib.Path("config/pairs.yaml")
OUT = pathlib.Path("data/raw/gdelt/mentions_v2")
MATCHED = "gdelt_mentions_matched.parquet"
FINAL = "gdelt_mentions_final.parquet"
MONTHLY = "gdelt_mentions_monthly.parquet"

# Refuse to run if a change to pairs.yaml has blown the scan far past what we measured.
MEASURED_BYTES = 667_884_946_710
BYTES_CEILING = int(MEASURED_BYTES * 1.5)
FREE_TIB_PER_MONTH = 1.0
USD_PER_TIB = 6.25


def load_terms() -> list[tuple[str, str, str, str]]:
    """Return (slug, variant, term, regex_fragment) for every enabled pair."""
    doc = yaml.safe_load(CONFIG.read_text())
    pairs = doc["pairs"] if isinstance(doc, dict) and "pairs" in doc else doc
    rows = []
    for p in pairs:
        if not p.get("enabled"):
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


def build_regex(rows) -> str:
    # Longest-first so "borscht" wins over "borsch" and "kievan rus" over "kiev".
    frags = sorted({r[3] for r in rows}, key=len, reverse=True)
    return r"\b(" + "|".join(frags) + r")\b"


def build_sql(rx: str) -> str:
    return f"""
SELECT
  DocumentIdentifier                                   AS url,
  SourceCommonName                                     AS domain,
  DATE                                                 AS gkg_date,
  TranslationInfo                                      AS translation_info,
  REGEXP_EXTRACT(LOWER(DocumentIdentifier), r'{rx}')   AS url_term,
  REGEXP_EXTRACT(LOWER(IFNULL(AllNames,'')), r'{rx}')  AS name_term,
  REGEXP_CONTAINS(LOWER(DocumentIdentifier), r'{rx}')  AS url_match,
  REGEXP_CONTAINS(LOWER(IFNULL(AllNames,'')), r'{rx}') AS allnames_match
FROM {SOURCE}
WHERE REGEXP_CONTAINS(LOWER(DocumentIdentifier), r'{rx}')
   OR REGEXP_CONTAINS(LOWER(IFNULL(AllNames,'')), r'{rx}')
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


def cmd_query(args) -> int:
    from google.cloud import bigquery

    rows = load_terms()
    sql = build_sql(build_regex(rows))
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "gdelt_mentions_query.sql").write_text(sql)
    print(f"{len({r[0] for r in rows})} pairs, {len(rows)} terms")

    client = bigquery.Client(project=PROJECT)
    dry = client.query(sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False))
    n = dry.total_bytes_processed
    tib = n / 1024 ** 4
    print(f"dry run: {n:,} bytes = {tib:.3f} TiB")
    print(f"free tier covers {FREE_TIB_PER_MONTH:.0f} TiB/month; beyond that ${tib * USD_PER_TIB:.2f}")
    if n > BYTES_CEILING:
        print(f"ABORT: scan exceeds ceiling {BYTES_CEILING:,} bytes.", file=sys.stderr)
        return 1
    if args.dry_run:
        return 0

    t0 = time.time()
    job = client.query(sql, job_config=bigquery.QueryJobConfig(
        destination=DEST, write_disposition="WRITE_TRUNCATE", use_query_cache=False))
    result = job.result()
    print(f"done in {time.time() - t0:.0f}s | billed {job.total_bytes_billed:,} bytes "
          f"({job.total_bytes_billed / 1024 ** 4:.3f} TiB) | rows {result.total_rows:,}")
    print(f"-> {DEST}   (re-reading this table is cheap; never repeat the scan)")
    return 0


def _stabilise(df: pd.DataFrame) -> pd.DataFrame:
    """Pin dtypes so every streamed batch yields an identical parquet schema."""
    for c in ("url", "domain", "translation_info", "url_term", "name_term",
              "pair_url", "var_url", "pair_nam", "var_nam", "src_lang", "pair_slug"):
        df[c] = df[c].astype("string")
    return df


def cmd_download(args) -> int:
    """Stream the destination table to disk, flagging as we go.

    Flagging is row-local, so it happens inside the stream. That is deliberate:
    an unflagged dump would be a byte-for-byte duplicate of this file with fewer
    columns, and the BigQuery table is already the source of record.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from google.cloud import bigquery, bigquery_storage

    OUT.mkdir(parents=True, exist_ok=True)
    client = bigquery.Client(project=PROJECT)
    reader = bigquery_storage.BigQueryReadClient()
    path = OUT / MATCHED
    writer, n, t0 = None, 0, time.time()
    for i, batch in enumerate(client.list_rows(DEST).to_arrow_iterable(bqstorage_client=reader)):
        df = _stabilise(flag_rows(batch.to_pandas()))
        table = pa.Table.from_pandas(df, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(path, table.schema, compression="zstd")
        writer.write_table(table)
        n += len(df)
        if i % 50 == 0:
            print(f"  {n:,} rows ({time.time() - t0:.0f}s)", flush=True)
    if writer is None:
        print("no rows returned", file=sys.stderr)
        return 1
    writer.close()
    print(f"saved {n:,} matched rows -> {path} ({path.stat().st_size / 1e6:.1f} MB)")
    return 0


def cmd_clean(args) -> int:
    """Re-flag in place after config/pairs.yaml changes, without re-downloading."""
    path = OUT / MATCHED
    df = _stabilise(flag_rows(pd.read_parquet(path, columns=["url", "domain", "gkg_date", "translation_info", "url_term", "name_term", "url_match", "allnames_match"])))
    df.to_parquet(path, compression="zstd", index=False)
    usable = df[df.native_en & df.url_match]
    print(f"re-flagged {len(df):,} rows -> {path}")
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
    """
    df = pd.read_parquet(OUT / MATCHED)
    g = df[df.native_en & df.url_match].copy()
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

    The previous file at this path counted canonicalised NER hits on
    machine-translated articles. It is backed up rather than deleted.
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

    dest = pathlib.Path("dataset/raw_gdelt.parquet")
    if dest.exists():
        backup = dest.with_suffix(".v1_allnames.bak.parquet")
        if not backup.exists():
            dest.rename(backup)
            print(f"backed up previous file -> {backup}")
    out.to_parquet(dest, compression="zstd", index=False)
    print(f"wrote {len(out):,} rows -> {dest} ({dest.stat().st_size / 1e6:.1f} MB)")
    print(f"  articles: {int(out['count'].sum()):,} | pairs: {out.pair_slug.nunique()} | domains: {out.source_domain.nunique():,}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    q = sub.add_parser("query", help="the one paid scan; writes to the destination table")
    q.add_argument("--dry-run", action="store_true", help="report bytes and stop")
    q.set_defaults(func=cmd_query)
    sub.add_parser("download", help="destination table -> matched parquet").set_defaults(func=cmd_download)
    sub.add_parser("clean", help="re-flag in place after a pairs.yaml change").set_defaults(func=cmd_clean)
    sub.add_parser("final", help="apply the metric filters, dedup, write the deliverable").set_defaults(func=cmd_final)
    sub.add_parser("export", help="write dataset/raw_gdelt.parquet for the site pipeline").set_defaults(func=cmd_export)
    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
