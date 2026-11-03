"""Assert what a reader sees, per pair, before anything ships.

Every failure this checks was found by the user on the live site after I had
reported the work done. Each one is visible by opening a single pair, and each
one was already measurable at export time — so it belongs in a gate, not in a
report I write afterwards.

    python -m pipeline.audit.site_invariants

Exits 1 listing every violation.
"""

import json
import pathlib
import sys

import pipeline.config as cfg

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
SITE = ROOT / "site" / "src" / "data"
STATS = ROOT / "data" / "stats"
CAP = 100


def window_violations(man: dict, ts: dict, hold: dict) -> list:
    """What the reader sees must sit inside the window pipeline.config defines.

    The window moves by hand, one closed month at a time. A stale export keeps
    the old axis and labels while the config says otherwise, and nothing in the
    build or the pre-push smoke can tell: both pass on any parseable JSON."""
    bad = []
    sw = man.get("study_window") or {}
    want = {"start": cfg.STUDY_START, "end": cfg.SERIES_END,
            "text_end": cfg.TEXT_END, "headline_year": cfg.HEADLINE_YEAR,
            "sources": {k: cfg.series_end(k) for k in sorted(cfg.SOURCE_SERIES_END)}}
    for k, v in want.items():
        if sw.get(k) != v:
            bad.append(f"manifest.study_window.{k} is {sw.get(k)!r}, config says {v!r} "
                       f"— re-run pipeline.export_site_data")
    hy = cfg.HEADLINE_YEAR
    if (man.get("headline") or {}).get("year") != hy:
        bad.append(f"manifest.headline.year is not HEADLINE_YEAR {hy}")

    # Every plotted point inside its source's window. A yearly point is dated
    # YYYY-01 and stands for its year: a year still open in the source's window
    # may ship only flagged provisional, and never in volume mode (the site
    # filters on the flag).
    for slug, srcs in ts.items():
        if not isinstance(srcs, dict):
            continue
        for src, ser in srcs.items():
            if not isinstance(ser, list) or not ser:
                continue
            end = cfg.series_end(src)
            for d in ser:
                dt = str(d.get("date", ""))
                if dt < cfg.STUDY_START:
                    bad.append(f"{slug}/{src}: point {dt} before STUDY_START")
                elif d.get("provisional"):
                    if src != "openalex" or f"{dt[:4]}-12" <= end or dt[:4] > end[:4]:
                        bad.append(f"{slug}/{src}: provisional point {dt} with window end {end}")
                elif dt > end or (src in ("openalex", "ngrams") and f"{dt[:4]}-12" > end):
                    bad.append(f"{slug}/{src}: point {dt} past its window end {end}")

    # Every holdout inside the headline year: the subtitles say "in <year>".
    for slug, srcs in hold.items():
        for key in ("reddit", "youtube", "news_articles"):
            for e in (srcs.get(key) or []):
                if str(e.get("month", ""))[:4] != str(hy):
                    bad.append(f"{slug}: {key} holdout dated {e.get('month')} outside {hy}")
        for e in (srcs.get("openalex") or []):
            if e.get("year") != hy:
                bad.append(f"{slug}: openalex holdout from {e.get('year')} outside {hy}")
    return bad


def computed_cluster_violations(enabled: set) -> list:
    """Every COMPUTED cluster resolves to a curated name and description.

    The shipped set is checked below, but the display merge folds clusters that
    share a gloss into larger cards, so a computed cluster with no curated entry
    can be invisible today and surface after any rebuild moves the merge
    (chornobyl 'radiation · thyroid' and kyivan-rus 'ukraine · armor' were two
    such). Same lookup as the exporter, by label or by term identity."""
    from pipeline.export_site_data import written_entry

    def book(name):
        p = ROOT / "data" / "audit" / name
        try:
            return {k: {kk.lower(): vv for kk, vv in v.items()}
                    for k, v in json.loads(p.read_text()).items()}
        except (OSError, ValueError):
            return {}
    names, glosses = book("cluster_names.json"), book("cluster_glosses.json")
    pairs = {p["slug"]: p for p in cfg.load_pairs()["pairs"]}
    bad = []
    for summ in sorted(STATS.glob("*/clusters/summary.json")):
        slug = summ.parent.parent.name
        if slug not in enabled:
            continue
        try:
            cs = json.loads(summ.read_text()).get("clusters", [])
        except ValueError:
            bad.append(f"{slug}: computed clusters unreadable")
            continue
        pw = {w.lower() for v in ("russian", "ukrainian")
              for w in str(pairs.get(slug, {}).get(v, "")).split()}
        for c in cs:
            top = c.get("top_terms", [])
            terms = [t for t in top if t.lower() not in pw and len(t) > 2]
            label = ("non-English" if c.get("english_ratio", 1.0) < 0.08
                     else " · ".join(terms[:2]) or f"cluster {c.get('cluster')}")
            miss = [what for what, b in (("name", names), ("description", glosses))
                    if not str(written_entry(b, slug, label, top) or "").strip()]
            if miss:
                bad.append(f"{slug}: computed cluster '{label}' (size {c.get('size')}) "
                           f"has no curated {' or '.join(miss)}")
    return bad


def main() -> int:
    key = json.loads((SITE / "cl_keyness.json").read_text())
    clus = json.loads((SITE / "cl_clusters.json").read_text())
    hold = json.loads((SITE / "holdouts_by_pair.json").read_text())
    man = json.loads((SITE / "manifest.json").read_text())
    ts = json.loads((SITE / "timeseries.json").read_text())
    enabled = {p["slug"] for p in cfg.load_pairs()["pairs"] if p.get("enabled", True)}
    cand_p = ROOT / "data" / "audit" / "reddit_holdout_candidates.json"
    cand = json.loads(cand_p.read_text()) if cand_p.exists() else {}

    bad: list = []

    # 1. Every displayed chip carries a gloss. A bare token is not a finding.
    for slug, p in key.items():
        for side in ("ua", "ru", "solo"):
            for x in (p.get(side) or []):
                if not (x.get("g") or "").strip():
                    bad.append(f"{slug}: chip '{x.get('w')}' has no gloss")

    # 2. Every displayed chip quotes an example that contains it.
    for slug, p in key.items():
        for side in ("ua", "ru", "solo"):
            for x in (p.get(side) or []):
                ex = (x.get("ex") or {}).get("t") if isinstance(x.get("ex"), dict) else None
                if ex and str(x.get("w", "")).lower() not in str(ex).lower():
                    bad.append(f"{slug}: chip '{x.get('w')}' quotes a text not containing it")

    # 3. Every cluster has a description, and it is not just the label again.
    for slug, p in clus.items():
        cs = p.get("clusters") if isinstance(p, dict) else p
        if isinstance(cs, dict):
            cs = list(cs.values())
        for c in (cs or []):
            if not isinstance(c, dict):
                continue
            desc = (c.get("desc") or "").strip()
            label = str(c.get("gloss") or c.get("label") or "").strip()
            # `name` was unguarded, which is how an exact-key lookup once
            # orphaned 62 of 129 clusters unnoticed: a missing name renders as
            # the raw c-TF-IDF label, so it reads as poor labelling rather than
            # a lookup miss. Checking desc alone does not catch it — the two
            # live in separate curated books and can go missing independently.
            if not (c.get("name") or "").strip():
                bad.append(f"{slug}: cluster '{label}' has no legend name")
            if not desc:
                bad.append(f"{slug}: cluster '{label}' has no description")
            elif desc.lower() == label.lower():
                bad.append(f"{slug}: cluster '{label}' description repeats the label")

    # 4. Every reddit holdout was probed and found live. No padding.
    for slug, srcs in hold.items():
        for e in (srcs.get("reddit") or []):
            if e.get("live") is not True:
                bad.append(f"{slug}: reddit holdout {e.get('url')} is not probed-live")

    # 5. A short table is only acceptable when its pool is exhausted.
    for slug, srcs in hold.items():
        n = len(srcs.get("reddit") or [])
        left = len(cand.get(slug, []))
        if n < CAP and left:
            bad.append(f"{slug}: reddit table has {n} of {CAP} with {left} candidates unprobed")

    # 6. A pair that shows nothing must really have nothing — if a solo profile
    #    could be computed, "no distinctive vocabulary" is wrong.
    for slug, p in key.items():
        if not (p.get("ua") or p.get("ru") or p.get("solo")):
            bad.append(f"{slug}: ships no vocabulary at all and no solo profile")

    # 7. The window: config, manifest, every plotted point and every holdout agree.
    bad += window_violations(man, ts, hold)

    # 8. Every computed cluster, shipped or folded, has its curated entry.
    bad += computed_cluster_violations(enabled)

    if not bad:
        print("site invariants: all pass")
        return 0
    print(f"site invariants: {len(bad)} violation(s)\n")
    for b in bad[:80]:
        print(f"  {b}")
    if len(bad) > 80:
        print(f"  ... and {len(bad) - 80} more")
    return 1


if __name__ == "__main__":
    sys.exit(main())
