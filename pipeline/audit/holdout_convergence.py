"""Is every holdout table converged? Per pair, per source, with the pool measured.

Converged means one of two things, and the difference matters:
  * the table holds HOLDOUT_CAP entries, or
  * it holds fewer because the candidate pool is genuinely spent.

A table that is short because a cap bound early, or because candidates were
never checked, is NOT converged — it just looks the same from outside. This
reports which it is, so "fewer than 100" is a measurement rather than a claim.

Reddit carries an extra condition: every shipped entry must be probed live AND
unarchived, because the table exists to be clicked through.

Pool sizes come from the exporter's own ledger (data/audit/holdout_accounting.json),
not from the raw store. The store counts rows the exporter is right to drop —
verified-drop lists, umbrella exclusions, syndication dedup, non-English titles —
so a store-derived pool reports headroom that cannot be filled. The store count
is still shown, as the outer bound, but only the ledger decides convergence.

Exit 1 while any table can still be improved.

    python -m pipeline.audit.holdout_convergence
"""

import json
import pathlib
import sys

import pandas as pd

from pipeline.export_site_data import (HOLDOUT_CAP, HOLDOUT_SINCE,
                                       HOLDOUT_VARIANTS, OPENALEX_COLLISIONS,
                                       OPENALEX_SINCE_YEAR)

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SITE = ROOT / "site" / "src" / "data"
SINCE_M = HOLDOUT_SINCE[:7]


def store_pool(slug: str, source: str) -> int:
    """Rows the table could draw on: holdout window, holdout variants."""
    f = ROOT / "data" / "store" / "pairs" / f"{slug}.parquet"
    if not f.exists():
        return 0
    try:
        d = pd.read_parquet(f, columns=["source", "variant", "date"])
    except Exception:                                  # noqa: BLE001
        return 0
    return int(((d.source == source)
                & d.variant.isin(HOLDOUT_VARIANTS)
                & (d.date.astype(str) >= SINCE_M)).sum())


def openalex_pools() -> dict:
    """Distinct works per pair in the academic holdout window."""
    f = ROOT / "data" / "cl" / "raw" / "openalex" / "all_pairs.parquet"
    if not f.exists():
        return {}
    import yaml
    cfg = yaml.safe_load((ROOT / "config" / "pairs.yaml").read_text())
    lut = {}
    for p in cfg["pairs"]:
        for v in ("ukrainian", "russian"):
            lut[str(p[v]).strip().lower()] = (p["slug"], v)
    d = pd.read_parquet(f)
    m = d["matched_term"].astype(str).str.strip().str.lower().map(
        lambda t: lut.get(t, (None, None)))
    d = d.assign(slug=[x[0] for x in m], var=[x[1] for x in m])
    d = d[d.slug.notna() & d["var"].isin(HOLDOUT_VARIANTS)
          & (d.year >= OPENALEX_SINCE_YEAR) & ~d.slug.isin(OPENALEX_COLLISIONS)
          & d.openalex_id.notna() & d.title.notna()]
    return d.drop_duplicates("openalex_id").groupby("slug").size().to_dict()


def main() -> int:
    h = json.loads((SITE / "holdouts_by_pair.json").read_text())
    acct_p = ROOT / "data" / "audit" / "holdout_accounting.json"
    acct = json.loads(acct_p.read_text()) if acct_p.exists() else {}
    if not acct:
        print("NOTE: no holdout_accounting.json — run the exporter first; "
              "falling back to raw-store pools, which overstate headroom.\n")
    # Check the flag; do not trust the file to be pre-filtered. export_site_data
    # builds _meta over all 47 pairs at :2144 and only prune_site_data drops the
    # disabled ones at the end of the same main(). If that prune is ever skipped
    # or fails, an unfiltered read here would audit 47 pairs and report 25 — the
    # trap CLAUDE.md warns about, in a different file.
    enabled = [p["slug"] for p in json.loads((SITE / "pairs_meta.json").read_text())
               if p.get("enabled", True)]
    oa = openalex_pools()
    cand_p = ROOT / "data" / "audit" / "reddit_holdout_candidates.json"
    cand = json.loads(cand_p.read_text()) if cand_p.exists() else {}

    problems, full, exhausted = [], 0, 0
    print(f"{'pair':22s} {'source':14s} {'ships':>6s} {'pool':>7s}  state")
    for slug in enabled:
        for key, src in (("reddit", "reddit"), ("youtube", "youtube"),
                         ("news_articles", "gdelt"), ("openalex", None)):
            rows = h.get(slug, {}).get(key) or []
            ships = len(rows)
            led = (acct.get(slug) or {}).get(key) or {}
            # What the exporter actually had to choose from, after its filters.
            pool = led.get("after_filters", led.get("exhibitable",
                   led.get("live_unarchived")))
            if pool is None:
                pool = oa.get(slug, 0) if key == "openalex" else store_pool(slug, src)

            if key == "reddit":
                bad = sum(1 for e in rows if e.get("live") is not True)
                if bad:
                    problems.append(f"{slug}/{key}: {bad} entr(ies) not live-and-unarchived")
                    print(f"  {slug:22s} {key:14s} {ships:>6d} {pool:>7d}  FAIL {bad} not live")
                    continue
                left = len(cand.get(slug, []))
                if ships < HOLDOUT_CAP and left:
                    problems.append(f"{slug}/{key}: {ships}/{HOLDOUT_CAP}, {left} unprobed")
                    print(f"  {slug:22s} {key:14s} {ships:>6d} {pool:>7d}  short, {left} unprobed")
                    continue

            if ships >= HOLDOUT_CAP:
                full += 1
            elif ships >= pool:
                exhausted += 1
            else:
                cap = (led.get("per_domain_cap") or led.get("per_year_cap")
                       or led.get("per_channel_cap"))
                why = f" (cap settled at {cap})" if cap and cap < 10_000 else ""
                problems.append(
                    f"{slug}/{key}: {ships} of {HOLDOUT_CAP}, {pool - ships} available{why}")
                print(f"  {slug:22s} {key:14s} {ships:>6d} {pool:>7d}  "
                      f"headroom {pool - ships}{why}")

    print(f"\nfull tables: {full}   short but pool-exhausted: {exhausted}   "
          f"improvable: {len(problems)}")
    lv_p = ROOT / "data" / "audit" / "reddit_liveness.json"
    if lv_p.exists():
        lv = json.loads(lv_p.read_text())
        cur = [v for v in lv.values() if v.get("schema") == 2]
        live = [v for v in cur if v.get("status") == "live"]
        print(f"reddit: {len(cur):,} probed, {len(live):,} live "
              f"({100 * len(live) / max(len(cur), 1):.0f}%), "
              f"{sum(1 for v in live if v.get('archived')):,} of those archived, "
              f"{sum(len(v) for v in cand.values()):,} unprobed")
    print("CONVERGED" if not problems else f"NOT converged: {len(problems)} table(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
