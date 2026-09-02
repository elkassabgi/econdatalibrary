"""parse_cbs_period, pinned against CBS's OWN Perioden titles (fetched 2026-09-01, R573).

Three defects made (series_key, obs_date) non-unique across 74 of the 374 publish-set cbs_nl
files: HJ01 and HJ02 both dated to 1 July; 'W127' read as week 12 (CBS week codes are W<k><nn>);
and 'X000' — 15 meanings across 55 tables — dated as a two-school-year span, two years late
(kept, counted, R589). Titles from https://opendata.cbs.nl/ODataApi/odata/<table>/Perioden:

    70895ned  1971W101 "1971 week 1" .. 2026W132 "2026 week 32";  1971X000 "1971 week 0 (3 dagen)"
    86156NED  2025HJ01 "1e halfjaar 2025", 2025HJ02 "2e halfjaar 2025", 2025JJ00 "2025"
    82242NED  1981MM01 "1981 januari", 1981KW01 "1981 1e kwartaal", 1981JJ00 "1981"
    37456     1998W401 "1998 week 01 - 04" .. 1998W413 "week 49 - 52", 1998W417 "week 01 - 52 (gemiddelde)"

Quarters and months still share first-of-month dates (1981KW01 and 1981MM01 both -> 1981-01-01):
a frequency token in the series_key is the fix, and it changes series identities, so it is
Ahmed's decision (not made here). The tests pin those collisions as KNOWN, not as accepted.
"""
import datetime as dt
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs import ingest_cbs_nl as _mod  # noqa: E402

p = _mod.parse_cbs_period


def test_half_years_are_two_distinct_dates():
    assert p("2025HJ01") == dt.date(2025, 1, 1)
    assert p("2025HJ02") == dt.date(2025, 7, 1)
    assert p("2025HJ03") is None


def test_weeks_are_W_k_nn_calendar_clipped_and_never_collapse():
    # single weeks (70895ned): week 1 starts on 1 January when the ISO Monday is in December
    assert p("1971W101") == dt.date(1971, 1, 4)          # ISO week 1 of 1971 starts Mon 4 Jan; 1-3 Jan is X000 "week 0 (3 dagen)"
    assert p("1974W101") == dt.date(1974, 1, 1)          # "1974 week 1 (6 dagen)": ISO Monday is 1973-12-31
    assert p("2026W132") == dt.date.fromisocalendar(2026, 32, 1)
    assert p("2026W127") == dt.date.fromisocalendar(2026, 27, 1)   # was read as week 12
    weeks = {p(f"2026W1{w:02d}") for w in range(1, 33)}
    assert len(weeks) == 32
    # week 53 of a 52-ISO-week year: "1973 week 53 (1 dag)" = the Monday after ISO week 52
    assert p("1973W153") == dt.date(1973, 12, 31)
    assert p("2026W154") is None
    # four-week periods (37456/72006): 'W401' = weeks 1-4, 'W402' = weeks 5-8, 'W417' = annual average
    assert p("1998W401") == dt.date(1998, 1, 1)
    assert p("1998W402") == dt.date.fromisocalendar(1998, 5, 1)
    assert p("1998W413") == dt.date.fromisocalendar(1998, 49, 1)
    assert p("1998W417") == dt.date(1998, 12, 31)        # "week 01 - 52 (gemiddelde)" in a 17-code year
    assert p("2000W415") == dt.date(2000, 12, 31)        # the same average in a 15-code year (37456, 72006ned)
    # 53-ISO-week years (2004, 2009) carry variants: W414 "week 49 - 53 (5 weken)" -> week 53's
    # Monday (distinct from W413), W415 "01 - 52 (gemiddelde)" -> 30 Dec, W417 "01 - 53 (gemiddelde)" -> 31 Dec
    assert p("2004W413") == dt.date.fromisocalendar(2004, 49, 1)
    assert p("2004W414") == dt.date.fromisocalendar(2004, 53, 1)
    assert p("2004W415") == dt.date(2004, 12, 30)
    assert p("2004W417") == dt.date(2004, 12, 31)
    assert len({p(f"2004W4{n:02d}") for n in (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 17)}) == 16
    assert len({p(f"1998W4{n:02d}") for n in range(1, 15)}) == 14  # 13 four-week periods + week 53 (1998 has 53 ISO weeks)


def test_week_one_never_collides_with_the_previous_annual_code():
    """R588 #3: an unclipped ISO week 1 landed on 31 December = the previous year's JJ00."""
    for y in (1974, 1979, 1985, 1990, 1996, 2002, 2013, 2019, 2024, 2030):
        assert p(f"{y}W101") != p(f"{y-1}JJ00")
        assert p(f"{y}W101").year == y


def test_X000_is_legacy_dated_and_counted():
    """R588 #4 / R589: X000 means 15 different things across 55 tables and is the ONLY code family of
    8 served tables; no global date is right, None would empty those tables - the legacy dating
    is kept and every occurrence counted."""
    PERIOD_DISCARDS = _mod.PERIOD_DISCARDS
    before = PERIOD_DISCARDS.get("X000-legacy-dated", 0)
    assert p("1971X000") == dt.date(1973, 7, 31)
    assert PERIOD_DISCARDS.get("X000-legacy-dated", 0) == before + 1
    assert p("2003X001") == dt.date(2005, 7, 31)   # the school-year span keeps its meaning


def test_annual_month_quarter_conventions_unchanged():
    assert p("1981JJ00") == dt.date(1981, 12, 31)
    assert p("1981MM01") == dt.date(1981, 1, 1)
    assert p("2026MM07") == dt.date(2026, 7, 1)
    assert p("1981KW01") == dt.date(1981, 1, 1)
    assert p("2026KW02") == dt.date(2026, 4, 1)
    assert p("2000SJ00") == dt.date(2001, 7, 31)
    assert p("19990924") == dt.date(1999, 9, 24)


def test_known_quarter_month_collision_is_pinned_not_hidden():
    """Still true after this fix; changing it re-keys series and is a reserved design call."""
    assert p("1981KW01") == p("1981MM01")


def test_known_week_collisions_are_pinned_not_hidden():
    """R592: period-start dates cannot separate these codes; a frequency/variant token in the
    key would (Ahmed's decision). Pinned so the class is measured, never narrated."""
    assert p("1973W153") == p("1973JJ00")            # "1973 week 53 (1 dag)" vs the annual code
    assert p("1993X000") == p("1995W131")            # legacy-dated "1993 week 0" vs 1995 week 31
    assert p("2021X000") == p("2023W131")
