"""Publisher holiday calendars for the daily data clock (updater/health.py _business_age_days).

A daily source's newest observation is aged in BUSINESS days: Saturday and Sunday do not count, for every
`cadence: daily` source (health.py, since 2026-07-13). A source may also DECLARE its publisher's holiday
calendar in the registry, `business_calendar: <name>`; that calendar's holidays then do not count either.
No declaration means weekends only, exactly as before. An unknown name also means weekends only (the strict
direction: a typo can never make the gate more lenient), and registry.validate refuses it.

WHY (2026-11-27). Thursday 2026-11-26 was Thanksgiving. ofr held the Office of Financial Research's own
newest observation (2026-11-24), but the 06Z gate on Friday 2026-11-27 counted the holiday as a business day:
obs 11-24 at 09:30Z scored 3.4 business days, over the 3-day daily clock, so ofr read RED-DATA. The same false
red comes back on Mon 2026-12-28 (Christmas, Fri 12-25) and Mon 2027-01-04 (New Year, Fri 01-01).

ONE CALENDAR, `us_federal_reserve`: the weekdays on which the Federal Reserve Banks are closed.
  * the eleven legal public holidays of 5 U.S.C. 6103(a): New Year's Day (Jan 1), Birthday of Martin Luther
    King Jr. (3rd Monday of January), Washington's Birthday (3rd Monday of February), Memorial Day (last
    Monday of May), Juneteenth (Jun 19, from 2021), Independence Day (Jul 4), Labor Day (1st Monday of
    September), Columbus Day (2nd Monday of October), Veterans Day (Nov 11), Thanksgiving Day (4th Thursday
    of November), Christmas Day (Dec 25);
  * a holiday on a SUNDAY is closed on the following Monday;
  * a holiday on a SATURDAY moves NOWHERE. This is NOT the rule for federal employees (they get the Friday
    before). The Reserve Banks stay open that Friday, and the publishers' data say so: see MEASURED below.
  * not in it: the day after Thanksgiving, Dec 24, Dec 31, Good Friday, Inauguration Day, days off given by
    executive order, national days of mourning. The data show a value on every one of those days in the window
    measured (and the last three are not rules, so no calendar can carry them).

MEASURED 2026-11-27 on the Office of Financial Research API dumps of that morning (data.financialresearch.gov
/v1/series/dataset, datasets fnyr and repo, 194 daily series), every weekday 2016-03-01..2026-11-24:
  * the union of all fnyr + repo series has NO value on exactly the 104 weekdays this rule names, and a value
    on every other weekday: 0 rule days with data, 0 data-less weekdays the rule misses;
  * the same check with the federal-employee Saturday->Friday shift turned on finds 7 Fridays WITH data
    (2017-11-10, 2020-07-03, 2021-06-18, 2021-12-24, 2021-12-31, 2023-11-10, 2026-07-03): on those days the
    effective fed funds and overnight bank funding rates are published even where the repo series are not;
  * the fnyr SOFR / OBFR / TGCR series alone (the New York Fed rates the nyfed fetcher reads from FRED),
    2018-04-03..2026-11-24: 85 rule days, 0 with data, 0 data-less weekdays missed; Saturday->Friday on: 6
    Fridays with data.
tests/fixtures/business_calendar/ofr_weekday_closures.json records those dates (written from the dumps by
tools/measure_ofr_weekday_closures.py, which applies no rule; its header says how to re-measure);
tests/test_health_business_calendar.py pins this rule to them in both directions, and pins 2026-2028 date by date.

WHAT IT DOES NOT COVER. An unscheduled closure (a national day of mourning on which the publisher really does
close) is not in any rule: on such a day the gate keeps its old behaviour and can read a false red for a day.
That is the existing behaviour, not a new risk. A future change to 5 U.S.C. 6103(a) needs a line here.
"""
from __future__ import annotations

from datetime import date, timedelta
from functools import lru_cache

# The registry values `business_calendar` may take. registry.validate refuses any other value.
US_FEDERAL_RESERVE = "us_federal_reserve"


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The n-th `weekday` (Mon=0) of `month`."""
    d = date(year, month, 1)
    d += timedelta(days=(weekday - d.weekday()) % 7)
    return d + timedelta(weeks=n - 1)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """The last `weekday` (Mon=0) of `month`."""
    nxt = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    d = nxt - timedelta(days=1)
    return d - timedelta(days=(d.weekday() - weekday) % 7)


def federal_holidays(year: int) -> dict:
    """{date: name} of the legal public holidays of 5 U.S.C. 6103(a) in `year`, on their ACTUAL dates (a
    weekend date stays on the weekend; the observed day is the calendar's business). Juneteenth from 2021."""
    h = {
        date(year, 1, 1): "New Year's Day",
        _nth_weekday(year, 1, 0, 3): "Birthday of Martin Luther King, Jr.",
        _nth_weekday(year, 2, 0, 3): "Washington's Birthday",
        _last_weekday(year, 5, 0): "Memorial Day",
        date(year, 7, 4): "Independence Day",
        _nth_weekday(year, 9, 0, 1): "Labor Day",
        _nth_weekday(year, 10, 0, 2): "Columbus Day",
        date(year, 11, 11): "Veterans Day",
        _nth_weekday(year, 11, 3, 4): "Thanksgiving Day",
        date(year, 12, 25): "Christmas Day",
    }
    if year >= 2021:
        h[date(year, 6, 19)] = "Juneteenth National Independence Day"
    return h


@lru_cache(maxsize=None)
def us_federal_reserve(year: int) -> frozenset:
    """The WEEKDAYS of `year` on which the Federal Reserve Banks are closed (see the module docstring)."""
    out = set()
    for d in federal_holidays(year):
        if d.weekday() == 6:            # Sunday -> the following Monday (never crosses a year: no Dec 31 holiday)
            out.add(d + timedelta(days=1))
        elif d.weekday() < 5:           # a weekday: closed that day
            out.add(d)
        # Saturday: no weekday closure (the Reserve Banks are open the preceding Friday; MEASURED above)
    return frozenset(out)


CALENDARS = {US_FEDERAL_RESERVE: us_federal_reserve}
BUSINESS_CALENDARS = frozenset(CALENDARS)


def holiday_test(name):
    """A function date -> bool (True on a holiday of calendar `name`), or None for no calendar.

    None also for an unknown or non-string name: the caller then counts weekends only, which is the strict
    direction (an unrecognised declaration cannot make the gate more lenient). registry.validate refuses
    such a name, so this is the fallback, not the guard."""
    rule = CALENDARS.get(name) if isinstance(name, str) else None
    if rule is None:
        return None
    return lambda d: d in rule(d.year)
