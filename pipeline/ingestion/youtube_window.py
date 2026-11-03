"""Which months the YouTube census may collect, and which make a year complete.

The window itself is defined once, in pipeline/config.py (STUDY_START and
SERIES_END). This module turns it into the two month-level rules that the
census, its orchestrator, youtube_saturate and deploy_youtube.sh all need, so
none of them re-derives the rule on its own.

  collectable_months   what one census run may query: months of the year up to
                       min(--through, the last CLOSED month at run time)
  expected_months      what a complete year must hold: 12 months before the
                       final year, January..SERIES_END's month in it

The rule used to be `len(months) >= 12` in six places. For 2026 that is wrong in
both directions. The census looped months 1..12 with no date guard and froze
every visited month forever (it skips a recorded month on resume). A run on
2026-10-15 would have recorded October at a partial count. November-December
would have been recorded at whatever a future window returns: an empty page or
an abandoned HTTP error both end a window as done. Under `>= 12` the year would
then have read as complete.

A month is CLOSED once its last second (23:59:59 UTC) has passed. Every census
window is bounded in UTC (publishedBefore ...Z), so the clock is UTC too.
"""
from __future__ import annotations

import calendar
import re
from datetime import datetime, timedelta, timezone

from pipeline.config import SERIES_END, STUDY_START

_YM = re.compile(r"^(\d{4})-(0[1-9]|1[0-2])$")


class WindowError(ValueError):
    """A request to collect outside the study window, or a month not yet closed."""


def utcnow() -> datetime:
    """The census clock. Tests replace this one function to pin the date."""
    return datetime.now(timezone.utc)


def check_ym(s: str) -> str:
    if not isinstance(s, str) or not _YM.match(s):
        raise WindowError(f"not a YYYY-MM month: {s!r}")
    return s


def month_end(ym: str) -> datetime:
    """Last second of the month, UTC: the publishedBefore bound of its window."""
    y, m = int(ym[:4]), int(ym[5:7])
    return datetime(y, m, calendar.monthrange(y, m)[1], 23, 59, 59, tzinfo=timezone.utc)


def last_closed_month(now: datetime | None = None) -> str:
    now = now or utcnow()
    first_of_month = now.astimezone(timezone.utc).replace(day=1, hour=0, minute=0,
                                                          second=0, microsecond=0)
    return (first_of_month - timedelta(days=1)).strftime("%Y-%m")


def expected_months(year: int, through: str = SERIES_END) -> list[str]:
    """Month keys a COMPLETE `year` must hold, ending at `through` ('YYYY-MM')."""
    check_ym(through)
    y0, y1 = int(STUDY_START[:4]), int(through[:4])
    if not y0 <= year <= y1:
        raise WindowError(f"{year} is outside the study window {STUDY_START}..{through}")
    first = int(STUDY_START[5:7]) if year == y0 else 1
    last = int(through[5:7]) if year == y1 else 12
    return [f"{year}-{m:02d}" for m in range(first, last + 1)]


def is_complete_months(months: dict, year: int, through: str = SERIES_END) -> bool:
    """Every expected month is recorded. A month is recorded only once every
    window inside it was visited, so this is "fully explored", not "resolved":
    an hour-capped month is a flagged lower bound, not missing work."""
    return all(k in months for k in expected_months(year, through))


def validate_through(through: str, now: datetime | None = None) -> str:
    """--through must be a closed month inside the window. Raise otherwise."""
    check_ym(through)
    closed = last_closed_month(now)
    if through > closed:
        raise WindowError(
            f"--through {through} has not closed: the last closed month at "
            f"{(now or utcnow()).strftime('%Y-%m-%dT%H:%M:%SZ')} is {closed}. A month "
            f"recorded while open is frozen at a partial count, because the census "
            f"never revisits a recorded month.")
    if through > SERIES_END:
        raise WindowError(
            f"--through {through} is past SERIES_END {SERIES_END}. Advance SERIES_END "
            f"in pipeline/config.py first; it is the one definition of the window.")
    if through < STUDY_START:
        raise WindowError(f"--through {through} is before STUDY_START {STUDY_START}")
    return through


def parse_months(spec: str, year: int) -> list[str]:
    """'01..07', '2026-07', '03,05,09' or '2026-01..2026-07' -> month keys of `year`."""
    def one(tok: str) -> int:
        tok = tok.strip()
        if re.fullmatch(r"0?[1-9]|1[0-2]", tok):
            return int(tok)
        check_ym(tok)
        if int(tok[:4]) != year:
            raise WindowError(f"--months {tok} is not in --year {year}")
        return int(tok[5:7])

    out: set[int] = set()
    for part in spec.split(","):
        if not part.strip():
            continue
        if ".." in part:
            a, b = part.split("..", 1)
            lo, hi = one(a), one(b)
            if lo > hi:
                raise WindowError(f"--months range {part} runs backwards")
            out.update(range(lo, hi + 1))
        else:
            out.add(one(part))
    if not out:
        raise WindowError(f"--months {spec!r} names no month")
    return [f"{year}-{m:02d}" for m in sorted(out)]


def collectable_months(year: int, through: str = SERIES_END, now: datetime | None = None,
                       only: str | None = None) -> list[str]:
    """Months one census run may query, in calendar order. Raise rather than
    return a month that has not closed, or an empty schedule."""
    validate_through(through, now)
    if year > int(through[:4]):
        raise WindowError(f"no closed month of {year} is inside the window (through {through})")
    allowed = expected_months(year, through)
    if only is None:
        return allowed
    wanted = parse_months(only, year)
    outside = [m for m in wanted if m not in allowed]
    if outside:
        raise WindowError(f"--months {','.join(outside)} past --through {through} or "
                          f"before STUDY_START {STUDY_START}; refusing to collect them")
    return wanted
