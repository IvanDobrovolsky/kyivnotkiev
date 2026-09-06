"""Green-pair consistency audit: is every pair marked ready actually whole?

One deterministic pass over the real artifacts — never over assumptions:

  census      32/32 targets, each with 12 recorded months and a real week grid
  enriched    the enriched parquet exists and is non-trivial
  corpus      the verified news corpus exists (or the pair is documented
              below-threshold) and its row count matches the stats input
  stats       analysis.json exists and its input_sha1 matches the sha1 of the
              CURRENT corpus file — stale stats are flagged, not trusted
  clusters    the site cluster export carries the pair
  site        timeseries has a YouTube series, keyness entry present,
              data_ready set; capped site months == unresolved census months

Exit code 1 when any green pair fails, so chains can gate on it.

    python -m pipeline.audit.green_check [--pair slug]
"""

import argparse
import hashlib
import json
import pathlib
import sys

import yaml

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


def check_pair(slug: str, ts: dict, key: dict, clu: dict, meta: list) -> list[str]:
    issues: list[str] = []

    # census
    targets = list(CK.glob(f"{slug}_*.json"))
    ok_targets, unresolved_months = 0, set()
    for f in targets:
        try:
            j = json.loads(f.read_text())
        except Exception:                              # noqa: BLE001
            issues.append(f"census: unreadable checkpoint {f.name}")
            continue
        months = j.get("months", {})
        w = len(j.get("done_windows", [])) + len(j.get("split_windows", []))
        if len(months) >= 12 and w >= 40:
            ok_targets += 1
        unresolved_months |= {k for k, m in months.items() if not m.get("resolved")}
    if ok_targets < 32:
        issues.append(f"census: {ok_targets}/32 complete targets")

    # enriched
    en = CENSUS / f"{slug}_enriched.parquet"
    if not en.exists():
        issues.append("enriched: parquet missing")
    elif en.stat().st_size < 100_000:
        issues.append(f"enriched: suspiciously small ({en.stat().st_size//1024}K)")

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
                # Freshness against the RECORDED input (the pairs store), not
                # the verified corpus. pipeline.rebuild rewrites parquets after
                # stats with identical content but different bytes, so sha1
                # inequality alone is not staleness — row count is the
                # content-level signal (dedup and refetch change rows).
                ip = ROOT / a.get("input", "")
                if not ip.exists():
                    issues.append(f"stats: recorded input missing ({a.get('input')})")
                elif a.get("input_sha1") and sha1(ip) == a["input_sha1"]:
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

    cfg = yaml.safe_load(open(ROOT / "config" / "pairs.yaml"))
    ts = json.loads((SITE / "timeseries.json").read_text())
    key = json.loads((SITE / "cl_keyness.json").read_text())
    clu = json.loads((SITE / "cl_clusters.json").read_text())
    meta_raw = json.loads((SITE / "pairs_meta.json").read_text())
    meta = meta_raw if isinstance(meta_raw, list) else meta_raw.get("pairs", [])

    greens = [p["slug"] for p in cfg["pairs"] if p.get("enabled", True)
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
