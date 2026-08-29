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

Usage:
    python -m pipeline.ingestion.trends --pairs chornobyl
    python -m pipeline.ingestion.trends --all
"""

import argparse
import json
import logging
import pathlib
import time
import urllib.parse
import urllib.request

import pandas as pd

from pipeline.config import ROOT_DIR, get_enabled_pairs, get_pair_by_slug

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

CACHE = ROOT_DIR / "data" / "raw" / "trends_serp"
OUT_DIR = ROOT_DIR / "data" / "raw" / "trends"
KEY_FILE = pathlib.Path("/etc/secrets/serpapi")

STUDY_START, STUDY_END = "2010-01-01", "2025-12-31"
CAL_MONTHS = 24            # width of the calibration window
REQUEST_DELAY = 2.0


def _key() -> str:
    return KEY_FILE.read_text().strip()


def call(q: str, date: str, tag: str) -> dict:
    """One SerpApi request, cached on disk by tag.  Cache hit costs nothing."""
    f = CACHE / f"{tag}.json"
    if f.exists():
        return json.loads(f.read_text())
    params = {"engine": "google_trends", "q": q, "data_type": "TIMESERIES",
              "date": date, "api_key": _key()}
    url = "https://serpapi.com/search?" + urllib.parse.urlencode(params)
    with urllib.request.urlopen(url, timeout=120) as r:
        d = json.loads(r.read())
    if "error" in d:
        raise RuntimeError(f"SerpApi: {d['error']}")
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(d))          # save on arrival
    log.info(f"    fetched {tag}")
    time.sleep(REQUEST_DELAY)
    return d


def frame(d: dict) -> pd.DataFrame:
    """timeline_data -> DataFrame indexed by timestamp, one column per query."""
    rows: dict[str, dict] = {}
    for x in d.get("interest_over_time", {}).get("timeline_data", []):
        ts = x.get("timestamp")
        if not ts:
            continue
        t = pd.to_datetime(int(ts), unit="s")
        for v in x.get("values", []):
            rows.setdefault(v.get("query"), {})[t] = v.get("extracted_value", 0)
    return pd.DataFrame(rows).sort_index()


def calibration_window(uk: pd.Series) -> tuple[str, str]:
    """The CAL_MONTHS-wide span holding the most Ukrainian-variant interest."""
    m = uk.resample("MS").mean()
    if not len(m):
        return STUDY_START, STUDY_END
    roll = m.rolling(CAL_MONTHS, min_periods=1).sum()
    end = roll.idxmax()
    start = max(m.index[0], end - pd.DateOffset(months=CAL_MONTHS - 1))
    return str(start.date()), str((end + pd.offsets.MonthEnd(1)).date())


def collect_pair(pair: dict) -> tuple[pd.DataFrame | None, dict]:
    slug, ru, uk = pair["slug"], pair["russian"], pair["ukrainian"]
    span = f"{STUDY_START} {STUDY_END}"

    ru_s = frame(call(ru, span, f"solo_{ru}_16y"))
    if ru_s.empty:
        return None, {"slug": slug, "ok": False, "reason": "no_ru_series"}
    ru_s = ru_s.iloc[:, 0]

    if pair.get("is_control") and ru == uk:
        return pd.DataFrame({ru: ru_s}), {"slug": slug, "ok": True, "control": True}

    uk_s = frame(call(uk, span, f"solo_{uk}_16y"))
    if uk_s.empty:
        return None, {"slug": slug, "ok": False, "reason": "no_uk_series"}
    uk_s = uk_s.iloc[:, 0]

    lo, hi = calibration_window(uk_s)
    j = frame(call(f"{ru},{uk}", f"{lo} {hi}", f"joint_{slug}_{lo[:7]}_{hi[:7]}"))
    if ru not in j.columns or uk not in j.columns:
        return None, {"slug": slug, "ok": False, "reason": "joint_missing_columns"}

    j_ru, j_uk = j[ru].sum(), j[uk].sum()
    if j_ru <= 0 or j_uk <= 0:
        # The variants are never close enough in volume for a joint call to
        # resolve both; an intermediate anchor term would be needed.
        return None, {"slug": slug, "ok": False, "reason": "joint_quantised_to_zero",
                      "window": f"{lo}..{hi}"}

    s_ru, s_uk = ru_s[lo:hi].sum(), uk_s[lo:hi].sum()
    if s_ru <= 0 or s_uk <= 0:
        return None, {"slug": slug, "ok": False, "reason": "solo_zero_in_window"}

    k = (j_uk / j_ru) * s_ru / s_uk
    # Two columns per variant, deliberately. `interest` is what the solo call
    # returned, each variant normalised to its OWN peak -- that is the series with
    # full dynamic range, and the one worth plotting: both variants reach 100 at
    # their own high points and their individual shapes stay legible.
    # `interest_calibrated` puts the Ukrainian variant on the Russian variant's
    # scale, which is required for the adoption share and useless for display,
    # because at a 111x gap it draws as a flat line on the axis.
    frame_out = pd.DataFrame({ru: ru_s, uk: uk_s,
                              f"{uk}__cal": uk_s * k}).dropna()
    return frame_out, {"slug": slug, "ok": True, "control": False,
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs")
    ap.add_argument("--all", action="store_true")
    args = ap.parse_args()

    if args.all:
        pairs = get_enabled_pairs()
    elif args.pairs:
        pairs = [p for p in (get_pair_by_slug(s.strip())
                             for s in args.pairs.split(",")) if p]
    else:
        ap.error("pass --pairs or --all")

    CACHE.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    log.info(f"{len(pairs)} pair(s) x 3 calls = {len(pairs) * 3} SerpApi requests (cached ones are free)")

    out, report = [], []
    for i, pair in enumerate(pairs, 1):
        log.info(f"[{i}/{len(pairs)}] {pair['slug']}")
        try:
            f, meta = collect_pair(pair)
        except Exception as exc:
            f, meta = None, {"slug": pair["slug"], "ok": False, "reason": str(exc)[:120]}
        report.append(meta)
        if f is None:
            log.warning(f"    SKIP {pair['slug']}: {meta.get('reason')}")
            continue
        out.append(to_records(f, pair))
        log.info(f"    ok  k={meta.get('k')}  window={meta.get('cal_window')}")

    pd.DataFrame(report).to_csv(OUT_DIR / "calibration_report.csv", index=False)
    if not out:
        log.error("nothing collected")
        return
    df = pd.concat(out, ignore_index=True)
    df.to_parquet(OUT_DIR / "trends_world_monthly.parquet", index=False)
    log.info(f"{sum(1 for r in report if r.get('ok'))}/{len(report)} pairs, "
             f"{len(df):,} rows -> {OUT_DIR/'trends_world_monthly.parquet'}")


if __name__ == "__main__":
    main()
