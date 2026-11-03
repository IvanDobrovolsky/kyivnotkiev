"""Green-pair consistency audit: is every pair marked ready actually whole?

One deterministic pass over the real artifacts — never over assumptions:

  census      every target of the TEXT window (STUDY_START..TEXT_END, both
              variants: 32 for 2010-2025), each with every due month recorded
              and a real week grid
  enriched    the enriched parquet exists and is non-trivial
  corpus      the verified news corpus exists (or the pair is documented
              below-threshold) and its row count matches the stats input
  stats       analysis.json exists and its input_sha1 matches the sha1 of the
              CURRENT corpus file — stale stats are flagged, not trusted — and
              the keyness input stops at TEXT_END
  clusters    the site cluster export carries the pair
  site        timeseries has a YouTube series, keyness entry present,
              data_ready set; capped site months == unresolved census months
              inside YouTube's series window

Green is TEXT-window readiness: the collection the collocations and clusters
were computed from is complete and the analysis is current. It says nothing
about the series running on into a partial year.

Exit code 1 when any green pair fails, so chains can gate on it.

    python -m pipeline.audit.green_check [--pair slug]
"""

import argparse
import hashlib
import json
import pathlib
import sys

import yaml

import pipeline.config as cfg

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
CK = ROOT / "data" / "cl" / "raw" / "youtube_census" / ".checkpoints"
CENSUS = ROOT / "data" / "cl" / "raw" / "youtube_census"
VERIFIED = ROOT / "data" / "cl" / "corpus" / "gdelt_verified"
STATS = ROOT / "data" / "stats"
SITE = ROOT / "site" / "src" / "data"

# Pairs whose news corpus is legitimately below the 30-record chart threshold;
# absence of stats/collocations there is documented, not a defect.
BELOW_THRESHOLD = {"volodymyr-the-great", "feodosiia"}


def sha1(path: pathlib.Path) -> str:
    h = hashlib.sha1()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def months_due(year: int, end_ym: str) -> list[str]:
    """Months a complete `year` must hold, ending at `end_ym`: 12, or 1..N in a
    final partial year, none outside the window. The census's own rule
    (pipeline.ingestion.youtube_window.expected_months), which the exporter
    uses too, so this audit cannot drift from what it audits."""
    from pipeline.ingestion.youtube_window import WindowError, expected_months
    try:
        return expected_months(int(year), end_ym)
    except WindowError:
        return []


def window_floor(n_months: int) -> int:
    """The >=40-window grid floor, pro rata for a partial year. Jan-Sep inside
    the 768 enabled full-year checkpoints never falls below 51 windows
    (measured 2026-10-15), so 30 for nine months still rejects a hollow
    month-grid year, which resolves 12 months in ~12-16 windows."""
    return -(-40 * n_months // 12)


def check_pair(slug: str, ts: dict, key: dict, clu: dict, meta: list) -> list[str]:
    issues: list[str] = []

    # census, over the TEXT window. A literal 32 (16 years x 2 variants) both
    # passed a pair with no 2026 at all and would have failed every pair the
    # day a 2026 checkpoint counted towards it.
    years = range(int(cfg.STUDY_START[:4]), int(cfg.TEXT_END[:4]) + 1)
    need = {(v, y) for v in ("russian", "ukrainian") for y in years}
    yt_end = cfg.series_end("youtube")
    # The years the site plots: the exporter's own rule (every due month of the
    # series window recorded on both variants, depth as declared). A year the
    # census is still collecting -- 2026 while it runs pair by pair -- is not
    # plotted, so its capped months must not be expected on the site.
    from pipeline.export_site_data import _stale_youtube_years
    plotted_stale = _stale_youtube_years()
    ok, unresolved_months = set(), set()
    for f in CK.glob(f"{slug}_*.json"):
        parts = f.stem.rsplit("_", 2)
        if parts[0] != slug:
            continue
        try:
            j = json.loads(f.read_text())
        except Exception:                              # noqa: BLE001
            issues.append(f"census: unreadable checkpoint {f.name}")
            continue
        months = j.get("months", {})
        w = len(j.get("done_windows", [])) + len(j.get("split_windows", []))
        if len(parts) == 3 and parts[2].isdigit():
            due = months_due(int(parts[2]), cfg.TEXT_END)
            if due and set(due) <= set(months) and w >= window_floor(len(due)):
                ok.add((parts[1], int(parts[2])))
        # Only months the site can plot can be compared with the site: a
        # capped month past YouTube's series end is clamped away by design, and
        # so is every month of a year not fully collected for the window.
        if len(parts) == 3 and (slug, parts[2]) not in plotted_stale:
            unresolved_months |= {k for k, m in months.items() if not m.get("resolved")
                                  and cfg.STUDY_START <= k <= yt_end}
    if not need <= ok:
        miss = sorted(f"{v[:2]}{y}" for v, y in need - ok)
        issues.append(f"census: {len(need & ok)}/{len(need)} complete targets "
                      f"for {years[0]}-{years[-1]} (missing {', '.join(miss[:6])}"
                      f"{'...' if len(miss) > 6 else ''})")

    # enriched
    en = CENSUS / f"{slug}_enriched.parquet"
    if not en.exists():
        issues.append("enriched: parquet missing")
    elif en.stat().st_size < 100_000:
        issues.append(f"enriched: suspiciously small ({en.stat().st_size//1024}K)")
    else:
        # Metadata, not just rows. A failed videos.list pass leaves every row
        # with a blank description and channel, and the homonym filters read
        # both, so the pair's series is filtered on titles alone. zaporizhzhia
        # reads 100% blank channel_title and 100% blank description; every
        # other enabled pair is <= 0.17% blank channel and <= 38% blank
        # description (measured 2026-11-02).
        try:
            import pandas as _pd
            _e = _pd.read_parquet(en, columns=["description", "channel_title"])
            _bd = (_e.description.fillna("").astype(str).str.strip() == "").mean()
            _bc = (_e.channel_title.fillna("").astype(str).str.strip() == "").mean()
            if len(_e) and _bd >= 0.999:
                issues.append(f"enriched: no video has a description ({len(_e):,} rows)")
            if len(_e) and _bc > 0.05:
                issues.append(f"enriched: {_bc:.1%} of rows have no channel_title")
        except Exception as e:                         # noqa: BLE001
            issues.append(f"enriched: metadata unreadable ({e})")

    # corpus + stats freshness
    vp = VERIFIED / f"{slug}.parquet"
    an = STATS / slug / "analysis.json"
    if not vp.exists():
        if slug not in BELOW_THRESHOLD:
            issues.append("corpus: verified parquet missing")
    elif slug not in BELOW_THRESHOLD:
        if not an.exists():
            issues.append("stats: analysis.json missing")
        else:
            try:
                a = json.loads(an.read_text())
                # An analysis records the text window it read since the clamp;
                # one computed for another TEXT_END is stale whatever its input.
                tw = a.get("text_window")
                if isinstance(tw, dict) and tw.get("end") != cfg.last_day(cfg.TEXT_END):
                    issues.append(f"stats: computed for text window ending "
                                  f"{tw.get('end')}, TEXT_END is {cfg.TEXT_END}")
                # Freshness against the RECORDED input (the pairs store), not
                # the verified corpus. pipeline.rebuild rewrites parquets after
                # stats with identical content but different bytes, so sha1
                # inequality alone is not staleness — row count is the
                # content-level signal (dedup and refetch change rows).
                ip = ROOT / a.get("input", "")
                if not ip.exists():
                    issues.append(f"stats: recorded input missing ({a.get('input')})")
                elif (rec_sha := a.get("input_sha1")) and \
                        sha1(ip)[:len(rec_sha)] == rec_sha:
                    pass                               # byte-identical: fresh
                else:
                    import pyarrow.parquet as _pq
                    rows = _pq.read_metadata(ip).num_rows
                    # Prefer the pre-filter store count when recorded: pairs
                    # with reader-side homonym filters (odesa) legitimately
                    # analyse fewer rows than the store holds.
                    recorded = a.get("input_rows_store", a.get("input_rows"))
                    if recorded != rows:
                        issues.append(
                            f"stats: STALE — input had {recorded:,} rows, "
                            f"store now has {rows:,}")
            except Exception as e:                     # noqa: BLE001
                issues.append(f"stats: unreadable ({e})")
        # The keyness input must stop at TEXT_END. analyze_pair substitutes the
        # verified news corpus straight off disk, which put 27,499 GDELT rows
        # dated 2026 into keyness while the pair files and clusters were
        # clamped -- and the row-count check above cannot see it, because the
        # pair file never changed. Row-group statistics, so no data is read.
        rp = STATS / slug / "records.parquet"
        if rp.exists():
            try:
                import pyarrow.parquet as _pq
                md = _pq.ParquetFile(rp).metadata
                i = md.schema.to_arrow_schema().names.index("date")
                hi = max(str(md.row_group(g).column(i).statistics.max)
                         for g in range(md.num_row_groups)
                         if md.row_group(g).column(i).statistics is not None)
                if hi[:7] > cfg.TEXT_END:
                    issues.append(f"stats: keyness input runs to {hi[:10]}, past "
                                  f"TEXT_END {cfg.TEXT_END}")
            except (ValueError, OSError) as e:
                issues.append(f"stats: records.parquet date range unreadable ({e})")

    # site layers
    yt = ts.get(slug, {}).get("youtube") or []
    if len(yt) < 40:
        issues.append(f"site: youtube series has {len(yt)} points")
    site_capped = {x["date"] for x in yt if x.get("capped")}
    if site_capped != unresolved_months:
        issues.append(f"site: capped months {sorted(site_capped)} != census unresolved {sorted(unresolved_months)}")
    e = key.get(slug, {})
    if slug not in BELOW_THRESHOLD and not (e.get("ua") or e.get("ru") or e.get("solo")):
        issues.append("site: no keyness entry")
    if not clu.get(slug, {}).get("clusters") and slug not in BELOW_THRESHOLD:
        issues.append("site: no clusters")
    m = next((p for p in meta if p.get("slug") == slug), {})
    if not m.get("data_ready"):
        issues.append("site: data_ready is false")
    return issues


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pair")
    a = ap.parse_args()

    pcfg = yaml.safe_load(open(ROOT / "config" / "pairs.yaml"))
    ts = json.loads((SITE / "timeseries.json").read_text())
    key = json.loads((SITE / "cl_keyness.json").read_text())
    clu = json.loads((SITE / "cl_clusters.json").read_text())
    meta_raw = json.loads((SITE / "pairs_meta.json").read_text())
    meta = meta_raw if isinstance(meta_raw, list) else meta_raw.get("pairs", [])

    greens = [p["slug"] for p in pcfg["pairs"] if p.get("enabled", True)
              and next((m for m in meta if m.get("slug") == p["slug"]), {}).get("data_ready")]
    if a.pair:
        greens = [a.pair]

    failed = 0
    for slug in greens:
        issues = check_pair(slug, ts, key, clu, meta)
        mark = "PASS" if not issues else "FAIL"
        print(f"{mark}  {slug}")
        for i in issues:
            print(f"      - {i}")
        failed += bool(issues)
    print(f"\n{len(greens) - failed}/{len(greens)} green pair(s) pass")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
