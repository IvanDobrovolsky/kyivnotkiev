"""Collect Google Trends interest for toponym pairs, via SerpApi.

WHY SOLO CALLS
--------------
Trends normalises every request to the joint maximum across all terms in it.
Ask for both variants at once and the low-volume one is compressed into the
0-1 band and integer-rounded away.  Measured on chornobyl, same 192 months,
same term, the only difference being the query mode:

    joint  ->  2 distinct values,   1/192 months non-zero, range 0-1
    solo   -> 21 distinct values, 120/192 months non-zero, range 0-100

The Russian column is identical either way -- it is the high-volume side and
sets the scale regardless -- so the joint query costs nothing on RU and
destroys UA.  Hence: one solo call per variant.

Windowing was tried and made things WORSE, which is worth recording because it
is counter-intuitive.  Narrow windows return weekly buckets, and a weekly
bucket for a rare term often falls under Google's reporting threshold and comes
back as zero.  On a common monthly basis:

    single 16-year query   120/192 months non-zero  (62 %)
    4 x 4-year windows     102/193 months non-zero  (53 %)

So the widest possible span is the right call for solo queries, and no
stitching or chain-rescaling is needed at all.

RECOVERING THE SCALE
--------------------
Solo calls each renormalise to their own peak, so both variants come back
topping out at 100 and the cross-term scale is gone.  One joint call restores
it, placed in the window where the Ukrainian variant is strongest and
quantisation therefore bites least.  Validated on chornobyl: this path puts
Nov-2024 at 15.3 % against 15.5 % measured by direct joint query -- two
different routes to the same number.

Note Trends is non-deterministic: repeated identical calls differ by a point
or two (2019-05 came back 54 and 56 on two calls).  Responses are cached to
disk and reused rather than re-fetched, so a rebuild is reproducible.

THE SPAN MOVES, SO HISTORY MOVES
--------------------------------
The span is STUDY_START..SERIES_END from pipeline.config. Because each solo call
renormalises to its own peak, extending the span cannot append months: it
refetches every month on a new scale. The calibrated share is invariant to that
rescaling in exact arithmetic (k carries sum(ru)/sum(ua) over the window) and
moves only through integer quantisation, sampling noise and a calibration window
that can itself move. trends_merge.py measures that movement per pair-month
against what ships before anything replaces dataset/raw_trends.parquet.

Cache tags carry the span ('solo_Kyiv_2010-01_2026-09'). The old tag
'solo_Kyiv_16y' carried none, so moving the end date returned the cached
2010-2025 series and the run "succeeded" without changing a value. The 47
'_16y' files stay on disk, unread: they are the provenance of what ships. All
9,216 rows built from them (trends_world_monthly.parquet) sit value-identical in
dataset/raw_trends.parquet, so the diff gate compares against that dataset, the
file the exporter actually reads. One enabled pair is the exception:
zaporizhzhia's 384 world rows there have no cache file and no calibration row,
and interest equals interest_calibrated on every one (RU peaks at 16, UA at
100), i.e. a single joint query from outside the tree. Its first solo run will
move it more than calibration noise would, and the gate will say so.

COST
----
The SerpApi account is on the Free Plan: 250 searches a month, 250 an hour
(account.json, 2026-10-15). A full refetch is 2 solo calls per pair plus 1-3
joint calls (64 calls took 5 min 47 s on 2026-08-29), 72-120 for 24 pairs. So
every run plans its calls first, refuses if the plan exceeds plan_searches_left,
and stops hard at --max-calls. Cached responses cost nothing, so a stopped run
resumes where it stopped.

Output goes to a staging directory, never to data/raw/trends/ directly: the
exporter reads calibration_report.csv there, and a half-finished run must not
change the site. trends_merge.py stages, diffs and promotes. A --pairs run gets
its own default directory: sharing the --all one let a one-pair recheck replace
calibration_report.csv under a staged 24-pair dataset, and promote would have
shipped a k = 0 set of one pair, drawing the four pinned pairs' 635 defined
months as 0.0% (2026-10-19 review). A run also refuses a directory that already
holds a stage report unless --replace-staged, which deletes that report first.

Usage:
    python -m pipeline.ingestion.trends --all --plan-only
    python -m pipeline.ingestion.trends --all --max-calls 120
    python -m pipeline.ingestion.trends --pairs chornobyl --max-calls 5
"""

import argparse
import hashlib
import json
import logging
import pathlib
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timezone

import pandas as pd

from pipeline.config import (ROOT_DIR, SERIES_END, STUDY_START, get_enabled_pairs,
                             get_pair_by_slug, last_day)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CACHE = ROOT_DIR / "data" / "raw" / "trends_serp"
LIVE_DIR = ROOT_DIR / "data" / "raw" / "trends"     # the exporter reads calibration_report.csv here
STAGING_DIR = LIVE_DIR / "staging"
KEY_FILE = pathlib.Path("/etc/secrets/serpapi")
SEARCH_URL = "https://serpapi.com/search"
ACCOUNT_URL = "https://serpapi.com/account.json"   # free: costs no search credit

CAL_MONTHS = 24            # width of the calibration window
CAL_CANDIDATES = 3         # joint calls tried per pair before giving up
# Fewer non-zero solo months than this share of the span is quantisation noise.
# Set as 10 of 192 when the span was fixed at 2010-2025; the four pinned pairs
# had 0-2. Stated as a fraction so the bar holds as the span grows (10 of 201).
BELOW_FLOOR_NONZERO_FRAC = 10 / 192
REQUEST_DELAY = 2.0
# SerpApi marks the still-open period; the exact key has not been observed (every
# cached span ends in a closed month), so every spelling seen in Trends clients is
# honoured. The calendar guard in timeline() holds even if none of them appears.
PARTIAL_KEYS = ("partial_data", "isPartial", "is_partial", "partial")


@dataclass(frozen=True)
class Span:
    start: str      # 'YYYY-MM'
    end: str        # 'YYYY-MM', a closed month

    @property
    def serp(self) -> str:
        return f"{self.start}-01 {last_day(self.end)}"

    @property
    def tag(self) -> str:
        return f"{self.start}_{self.end}"

    @property
    def months(self) -> int:
        (y0, m0), (y1, m1) = (map(int, self.start.split("-")), map(int, self.end.split("-")))
        return (y1 - y0) * 12 + (m1 - m0) + 1

    @property
    def floor_nonzero(self) -> int:
        return int(BELOW_FLOOR_NONZERO_FRAC * self.months)


YM = re.compile(r"\d{4}-(0[1-9]|1[0-2])")


def make_span(end: str = SERIES_END) -> Span:
    """STUDY_START..end, refusing anything but a closed month inside the window.

    The format is checked because a string compare accepts '2025-9', which made a
    SerpApi date of '2025-9-30' and a new cache tag. The SERIES_END bound is
    checked because SERIES_END is advanced by hand, after review, and a closed
    month past it is still not one the study has admitted.
    """
    if not YM.fullmatch(str(end)):
        raise SystemExit(f"span end {end!r} is not YYYY-MM")
    open_month = date.today().strftime("%Y-%m")
    if end >= open_month:
        raise SystemExit(f"span end {end} is not a closed month (today is in {open_month}); "
                         f"Trends does not flag a partial month reliably, so it is never fetched")
    if end > SERIES_END:
        raise SystemExit(f"span end {end} is past SERIES_END {SERIES_END} (pipeline/config.py)")
    if end < STUDY_START:
        raise SystemExit(f"span end {end} is before STUDY_START {STUDY_START}")
    return Span(STUDY_START, end)


def solo_tag(term: str, span: Span) -> str:
    return f"solo_{term}_{span.tag}"


class BudgetExhausted(RuntimeError):
    pass


class Budget:
    """Counts network calls; refuses the one that would pass the ceiling.

    The module-level default allows none, so importing this module and calling
    collect_pair() can never spend credits unless main() set a ceiling.
    """

    def __init__(self, max_calls: int = 0):
        self.max_calls, self.calls, self.cache_hits = max_calls, 0, 0

    def spend(self, tag: str) -> None:
        if self.calls >= self.max_calls:
            raise BudgetExhausted(f"call ceiling {self.max_calls} reached before {tag}")
        self.calls += 1


_budget = Budget(0)


def _key() -> str:
    return KEY_FILE.read_text().strip()


def _get_json(url: str, params: dict) -> dict:
    with urllib.request.urlopen(url + "?" + urllib.parse.urlencode(params), timeout=120) as r:
        return json.loads(r.read())


def account() -> dict:
    """SerpApi account state. Free; the key is never logged."""
    d = _get_json(ACCOUNT_URL, {"api_key": _key()})
    return {k: d.get(k) for k in ("plan_name", "searches_per_month", "plan_searches_left",
                                  "total_searches_left", "this_month_usage",
                                  "this_hour_searches", "account_rate_limit_per_hour")}


def call(q: str, date_range: str, tag: str, cache: pathlib.Path = CACHE) -> dict:
    """One SerpApi request, cached on disk by tag.  Cache hit costs nothing."""
    f = cache / f"{tag}.json"
    if f.exists():
        _budget.cache_hits += 1
        return json.loads(f.read_text())
    _budget.spend(tag)
    d = _get_json(SEARCH_URL, {"engine": "google_trends", "q": q, "data_type": "TIMESERIES",
                               "date": date_range, "api_key": _key()})
    if "error" in d:
        # "hasn't returned any results" means the term is below Trends' reporting
        # threshold for the whole span. That is a measurement — the spelling has no
        # detectable English search volume — not a failure, and it must not be
        # confused with a fetch error. Cache it so it is not re-queried.
        if "any results" in str(d["error"]).lower():
            d = {"below_floor": True, "interest_over_time": {"timeline_data": []}}
        else:
            raise RuntimeError(f"SerpApi: {d['error']}")
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(d))          # save on arrival
    log.info(f"    fetched {tag}")
    time.sleep(REQUEST_DELAY)
    return d


def timeline(d: dict, last_month: str | None = None) -> tuple[pd.DataFrame, int]:
    """timeline_data -> (DataFrame indexed by timestamp, one column per query, n dropped).

    Drops any point flagged partial, and any point in the open month or after
    `last_month`, whether flagged or not.
    """
    open_month = date.today().strftime("%Y-%m")
    rows: dict[str, dict] = {}
    dropped = 0
    for x in d.get("interest_over_time", {}).get("timeline_data", []):
        ts = x.get("timestamp")
        if not ts:
            continue
        t = pd.to_datetime(int(ts), unit="s")
        ym = t.strftime("%Y-%m")
        if (any(x.get(k) for k in PARTIAL_KEYS) or ym >= open_month
                or (last_month is not None and ym > last_month)):
            dropped += 1
            continue
        for v in x.get("values", []):
            rows.setdefault(v.get("query"), {})[t] = v.get("extracted_value", 0)
    return pd.DataFrame(rows).sort_index(), dropped


def frame(d: dict) -> pd.DataFrame:
    return timeline(d)[0]


def calibration_windows(ru: pd.Series, uk: pd.Series, span: Span,
                        n: int = CAL_CANDIDATES) -> list[tuple[str, str]]:
    """Candidate calibration spans, best first.

    A joint call resolves both variants only where BOTH are strong, so windows are
    scored by the WEAKER side after normalising each series to its own peak — not
    by the Ukrainian peak alone. Scoring on UA broke feodosiia and kazymyr-malevych,
    where the Ukrainian spelling dominates and the joint call therefore quantised the
    RUSSIAN side to zero, leaving no scale factor at all.

    Several candidates are returned because quantisation is only observable after
    spending the call.
    """
    whole = [(f"{span.start}-01", last_day(span.end))]
    a = ru.resample("MS").mean()
    b = uk.resample("MS").mean()
    idx = a.index.union(b.index)
    a, b = a.reindex(idx).fillna(0), b.reindex(idx).fillna(0)
    if not len(idx) or a.max() <= 0 or b.max() <= 0:
        return whole
    ra = (a / a.max()).rolling(CAL_MONTHS, min_periods=1).sum()
    rb = (b / b.max()).rolling(CAL_MONTHS, min_periods=1).sum()
    score = pd.concat([ra, rb], axis=1).min(axis=1)      # the weaker side governs
    out, used = [], []
    for end in score.sort_values(ascending=False).index:
        if any(abs((end - u).days) < 30 * CAL_MONTHS // 2 for u in used):
            continue                                     # keep candidates distinct
        used.append(end)
        start = max(idx[0], end - pd.DateOffset(months=CAL_MONTHS - 1))
        out.append((str(start.date()), str((end + pd.offsets.MonthEnd(1)).date())))
        if len(out) >= n:
            break
    return out or whole


def _step_days(s: pd.Series) -> float | None:
    """Median spacing of a solo series: ~31 for monthly buckets, 7 for weekly."""
    if len(s) < 2:
        return None
    return float(pd.Series(s.index).diff().dt.days.median())


def collect_pair(pair: dict, span: Span,
                 cache: pathlib.Path = CACHE) -> tuple[pd.DataFrame | None, dict]:
    slug, ru, uk = pair["slug"], pair["russian"], pair["ukrainian"]
    base = {"slug": slug, "span": span.tag, "ru_term": ru, "ua_term": uk}

    ru_s, dropped = timeline(call(ru, span.serp, solo_tag(ru, span), cache), span.end)
    if ru_s.empty:
        return None, {**base, "ok": False, "reason": "no_ru_series"}
    ru_s = ru_s.iloc[:, 0]
    base["solo_step_days"] = _step_days(ru_s)

    if pair.get("is_control") and ru == uk:
        return pd.DataFrame({ru: ru_s}), {**base, "ok": True, "control": True,
                                          "partial_dropped": dropped}

    uk_raw, d_uk = timeline(call(uk, span.serp, solo_tag(uk, span), cache), span.end)
    dropped += d_uk
    base["partial_dropped"] = dropped
    if uk_raw.empty:
        # Below Trends' reporting threshold for the entire span. That is a result:
        # the spelling has no detectable English search volume. Carry it as an
        # explicit zero series so the pair is present and reads as ~0 adoption,
        # rather than dropping the pair or leaving a stale artifact in its place.
        uk_s = pd.Series(0.0, index=ru_s.index)
        frame_out = pd.DataFrame({ru: ru_s, uk: uk_s, f"{uk}__cal": uk_s})
        return frame_out, {**base, "ok": True, "control": False,
                           "below_floor": True, "k": 0.0,
                           "note": f"UA below Trends reporting threshold for {span.start}..{span.end}"}
    uk_s = uk_raw.iloc[:, 0]

    # Quantisation is only visible after the call, so try candidate windows in order.
    tried = []
    k = None
    for lo, hi in calibration_windows(ru_s, uk_s, span):
        jd = call(f"{ru},{uk}", f"{lo} {hi}", f"joint_{slug}_{lo[:7]}_{hi[:7]}", cache)
        j, _ = timeline(jd, hi[:7])
        if jd.get("below_floor"):
            # An empty joint response is the floor, not a malformed reply; the
            # 2026-09-05 audit found feodosiia's 2016-08 window mislabelled.
            tried.append(f"{lo[:7]}:below_floor")
            continue
        if ru not in j.columns or uk not in j.columns:
            tried.append(f"{lo[:7]}:missing_cols")
            continue
        j_ru, j_uk = j[ru].sum(), j[uk].sum()
        s_ru, s_uk = ru_s[lo:hi].sum(), uk_s[lo:hi].sum()
        if j_ru <= 0 or j_uk <= 0:
            tried.append(f"{lo[:7]}:zero(ru={j_ru:.0f},ua={j_uk:.0f})")
            continue
        if s_ru <= 0 or s_uk <= 0:
            tried.append(f"{lo[:7]}:solo_zero")
            continue
        k = (j_uk / j_ru) * s_ru / s_uk
        break
    if k is None:
        # No window resolves both variants. The weaker one is then treated as below
        # the instrument floor when its solo series is a handful of non-zero months:
        # the same quantisation noise that gave ihor-sikorsky a single month at 100
        # and a fake 79% adoption peak. Carry it as an explicit zero instead of
        # dropping the pair; `tried` records what each joint window actually showed
        # (for feodosiia one window zeroed RU, not UA).
        nz_ru = int((ru_s.resample("MS").mean() > 0).sum())
        nz_uk = int((uk_s.resample("MS").mean() > 0).sum())
        weak, strong = (uk, ru) if nz_uk <= nz_ru else (ru, uk)
        weak_nz, strong_nz = (nz_uk, nz_ru) if weak == uk else (nz_ru, nz_uk)
        if weak_nz <= span.floor_nonzero < strong_nz:
            zero = pd.Series(0.0, index=ru_s.index if weak == uk else uk_s.index)
            keep = ru_s if weak == uk else uk_s
            frame_out = (pd.DataFrame({ru: keep, uk: zero, f"{uk}__cal": zero})
                         if weak == uk else
                         pd.DataFrame({ru: zero, uk: keep, f"{uk}__cal": keep}))
            return frame_out, {**base, "ok": True, "control": False,
                               "below_floor": True, "k": 0.0,
                               "note": f"{weak} below floor: {weak_nz}/{span.months} non-zero "
                                       f"months; no joint window resolved both variants",
                               "tried": "; ".join(tried)}
        # Both sides carry real volume but no window resolves them together; an
        # intermediate anchor term would be required.
        return None, {**base, "ok": False, "reason": "joint_quantised_to_zero",
                      "tried": "; ".join(tried)}
    # Two columns per variant, deliberately. `interest` is what the solo call
    # returned, each variant normalised to its OWN peak -- that is the series with
    # full dynamic range, and the one worth plotting: both variants reach 100 at
    # their own high points and their individual shapes stay legible.
    # `interest_calibrated` puts the Ukrainian variant on the Russian variant's
    # scale, which is required for the adoption share and useless for display,
    # because at a 111x gap it draws as a flat line on the axis.
    frame_out = pd.DataFrame({ru: ru_s, uk: uk_s,
                              f"{uk}__cal": uk_s * k}).dropna()
    return frame_out, {**base, "ok": True, "control": False,
                       "cal_window": f"{lo}..{hi}", "k": round(float(k), 6),
                       "joint_ratio": round(float(j_uk / j_ru), 5)}


def to_records(frame_out: pd.DataFrame, pair: dict) -> pd.DataFrame:
    ru, uk = pair["russian"], pair["ukrainian"]
    control = pair.get("is_control") and ru == uk
    m = frame_out.resample("MS").mean()
    cal_col = f"{uk}__cal"
    rows = []
    for ts, r in m.iterrows():
        rows.append({"date": str(ts.date()), "term": ru, "variant": "russian",
                     "interest": float(r[ru]),
                     "interest_calibrated": float(r[ru]), "geo": "",
                     "pair_slug": pair["slug"], "source": "trends"})
        if not control:
            rows.append({"date": str(ts.date()), "term": uk, "variant": "ukrainian",
                         "interest": float(r[uk]),
                         "interest_calibrated": float(r[cal_col]) if cal_col in m.columns
                                                else float(r[uk]),
                         "geo": "", "pair_slug": pair["slug"], "source": "trends"})
    return pd.DataFrame(rows)


def plan(pairs: list[dict], span: Span, cache: pathlib.Path = CACHE) -> list[dict]:
    """Per pair, the calls a run would make: solo misses exactly, joint as bounds.

    Joint windows depend on the solo series, so they are known only when both solo
    responses are cached; otherwise the bound is 1..CAL_CANDIDATES.
    """
    out = []
    for p in pairs:
        ru, uk = p["russian"], p["ukrainian"]
        control = p.get("is_control") and ru == uk
        tags = [solo_tag(ru, span)] + ([] if control else [solo_tag(uk, span)])
        solo_miss = sum(not (cache / f"{t}.json").exists() for t in tags)
        lo, hi = (0, 0) if control else (1, CAL_CANDIDATES)
        if not control and solo_miss == 0:
            ru_s = frame(json.loads((cache / f"{tags[0]}.json").read_text()))
            uk_s = frame(json.loads((cache / f"{tags[1]}.json").read_text()))
            if ru_s.empty or uk_s.empty:
                lo = hi = 0
            else:
                wins = calibration_windows(ru_s.iloc[:, 0], uk_s.iloc[:, 0], span)
                miss = [not (cache / f"joint_{p['slug']}_{a[:7]}_{b[:7]}.json").exists()
                        for a, b in wins]
                # Windows are tried in order and the first success stops the loop.
                lo = 1 if miss[0] else 0
                hi = sum(miss)
        out.append({"slug": p["slug"], "solo": solo_miss, "joint_min": lo, "joint_max": hi})
    return out


def default_out_dir(pairs: list[dict], span: Span, all_enabled: list[str]) -> pathlib.Path:
    """STAGING_DIR/<span> for the enabled set, a directory of its own for any subset."""
    slugs = sorted(p["slug"] for p in pairs)
    if slugs == sorted(all_enabled):
        return STAGING_DIR / span.tag
    name = ("+".join(slugs) if len(slugs) <= 3 else
            f"{len(slugs)}pairs-{hashlib.sha1(','.join(slugs).encode()).hexdigest()[:8]}")
    return STAGING_DIR / f"{span.tag}.{name}"


STAGED_FILES = ("stage_report.json", "raw_trends.parquet")   # trends_merge.stage writes these


def claim_out_dir(out_dir: pathlib.Path, replace_staged: bool) -> None:
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


def write_outputs(out_dir: pathlib.Path, frames: list[pd.DataFrame], report: list[dict],
                  run: dict) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(report).to_csv(out_dir / "calibration_report.csv", index=False)
    world = out_dir / "trends_world_monthly.parquet"
    if frames:
        pd.concat(frames, ignore_index=True).to_parquet(world, index=False)
    else:
        world.unlink(missing_ok=True)      # never leave an earlier run's rows beside this report
    (out_dir / "run.json").write_text(json.dumps(run, indent=1, default=str))


def main() -> None:
    global _budget
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--pairs")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--end", default=SERIES_END, help="last month, YYYY-MM (default SERIES_END)")
    ap.add_argument("--max-calls", type=int,
                    help="hard ceiling on SerpApi searches this run (required if any call is planned)")
    ap.add_argument("--plan-only", action="store_true", help="print the plan and the account, call nothing")
    ap.add_argument("--out-dir", type=pathlib.Path, help="staging directory")
    ap.add_argument("--replace-staged", action="store_true",
                    help="write into a directory that was already staged, discarding its stage report")
    ap.add_argument("--cache-dir", type=pathlib.Path, default=CACHE)
    args = ap.parse_args()

    enabled = get_enabled_pairs()
    if args.all:
        pairs = enabled
    elif args.pairs:
        want = [s.strip() for s in args.pairs.split(",") if s.strip()]
        unknown = [s for s in want if get_pair_by_slug(s) is None]
        if unknown:
            raise SystemExit(f"not enabled in config/pairs.yaml: {unknown}")
        pairs = [p for p in enabled if p["slug"] in want]
    else:
        ap.error("pass --pairs or --all")

    span = make_span(args.end)
    out_dir = args.out_dir or default_out_dir(pairs, span, [p["slug"] for p in enabled])
    if out_dir.resolve() in (LIVE_DIR.resolve(), (ROOT_DIR / "dataset").resolve()):
        raise SystemExit(f"{out_dir} is what the exporter reads; write to staging and promote "
                         f"with trends_merge.py")

    pl = plan(pairs, span, args.cache_dir)
    lo = sum(p["solo"] + p["joint_min"] for p in pl)
    hi = sum(p["solo"] + p["joint_max"] for p in pl)
    log.info(f"{len(pairs)} pair(s), span {span.serp} ({span.months} months): "
             f"planned SerpApi searches {lo}..{hi} (cached responses are free)")
    acct = None
    if hi > 0:
        try:
            acct = account()
        except Exception as exc:                       # noqa: BLE001
            raise SystemExit(f"cannot read SerpApi account.json, so the budget is unverified: {exc}")
        left = acct.get("plan_searches_left")
        log.info(f"SerpApi {acct.get('plan_name')}: plan_searches_left={left}, "
                 f"total_searches_left={acct.get('total_searches_left')}, "
                 f"this_hour={acct.get('this_hour_searches')}/"
                 f"{acct.get('account_rate_limit_per_hour')}")
        if left is None or hi > int(left):
            raise SystemExit(f"refusing: up to {hi} searches planned, plan_searches_left={left}")
        rate, used = acct.get("account_rate_limit_per_hour"), acct.get("this_hour_searches") or 0
        if rate is not None and hi > int(rate) - int(used):
            raise SystemExit(f"refusing: up to {hi} searches planned, {int(rate) - int(used)} "
                             f"left in this hour's rate limit")
    if args.plan_only:
        for p in pl:
            log.info(f"  {p['slug']:<22} solo {p['solo']}  joint {p['joint_min']}..{p['joint_max']}")
        return
    if hi > 0 and args.max_calls is None:
        raise SystemExit(f"pass --max-calls (planned {lo}..{hi})")
    claim_out_dir(out_dir, args.replace_staged)
    ceiling = 0 if hi == 0 else min(args.max_calls, int(acct["plan_searches_left"]))
    if hi > ceiling:
        log.warning(f"the plan's upper bound {hi} exceeds the ceiling {ceiling}; the run may stop "
                    f"early, and a rerun resumes from the cache")
    _budget = Budget(ceiling)

    args.cache_dir.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    frames, report, complete, stop = [], [], True, None
    for i, pair in enumerate(pairs, 1):
        log.info(f"[{i}/{len(pairs)}] {pair['slug']}")
        try:
            f, meta = collect_pair(pair, span, args.cache_dir)
        except BudgetExhausted as exc:
            complete, stop = False, str(exc)
            log.error(f"    STOP: {exc}")
            break
        except Exception as exc:                       # noqa: BLE001
            f, meta = None, {"slug": pair["slug"], "span": span.tag, "ok": False,
                             "reason": str(exc)[:120]}
        report.append(meta)
        if f is None:
            complete = False
            log.warning(f"    SKIP {pair['slug']}: {meta.get('reason')}")
            continue
        frames.append(to_records(f, pair))
        log.info(f"    ok  k={meta.get('k')}  window={meta.get('cal_window')}")

    run = {"as_of": started.replace(microsecond=0).isoformat(), "span": span.tag,
           "start": span.start, "end": span.end, "pairs": [p["slug"] for p in pairs],
           "collected": [r["slug"] for r in report if r.get("ok")],
           "complete": complete, "stopped": stop,
           "searches": _budget.calls, "cache_hits": _budget.cache_hits,
           "planned": [lo, hi], "account_before": acct,
           "partial_dropped": int(sum(r.get("partial_dropped", 0) or 0 for r in report))}
    write_outputs(out_dir, frames, report, run)
    log.info(f"{len(run['collected'])}/{len(pairs)} pairs, {_budget.calls} searches, "
             f"{_budget.cache_hits} cache hits -> {out_dir}")
    if not complete:
        raise SystemExit(2)
    log.info(f"Next: python -m pipeline.ingestion.trends_merge stage --run {out_dir}")


if __name__ == "__main__":
    main()
