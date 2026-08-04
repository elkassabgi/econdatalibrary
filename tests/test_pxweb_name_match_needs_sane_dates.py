"""resolve_time_dim step 3: a NAME match must not outrank the absence of any real date.

The branch existed to handle date-less tables and its comment promised they "will legitimately
yield 0 rows". That was only true if the source's parse_fn rejected the codes — and none of them
do. Every PxWeb source's parse_date turns any 4-digit token into a year, so a zero-padded
counter becomes a calendar:

    parse_date('0001') -> 0001-12-31    parse_date('0114') -> 0114-12-31
    parse_date('9999') -> 9999-12-31

The names that match are genuinely time-ish while their VALUES are not years: `vecka` is Swedish
for week, `manudur` Icelandic for month, `leto` Slovenian for year on an index-coded axis.
Measured on the live stores 2026-08-04, this produced ~637,000 served observations:

    stat_slovenia 05W   506,605 rows, one key holding years 1,2,3...6152, all at 12-31
    scb BE / HE          71,368 rows below year 1500  (DodaVeckaRegionCKM = deaths by WEEK)
    statfin tyonv        32,013 rows at 9999-12-31
    hagstofa Umhverfi     1,120 rows at 3005-12-31

Step 2 already refused every one of them (date_parse_rate is 0.0 inside the sane window); step 3
handed the same axis back on its name.
"""
import datetime as dt

import pytest

from core.pxweb import TIME_CODES, date_parse_rate, resolve_time_dim


def parse_year(s):
    """A faithful stand-in for the source parsers: any 4-digit token becomes a year."""
    s = (s or "").strip()
    if len(s) == 4 and s.isdigit():
        try:
            return dt.date(int(s), 12, 31)
        except ValueError:
            return None
    return None


PADDED_COUNTER = [f"{i:04d}" for i in range(1, 200)]     # 0001..0199 — stat_slovenia's shape
REAL_YEARS = [str(y) for y in range(2000, 2026)]


def test_the_parser_really_does_turn_a_padded_counter_into_years():
    """The premise. If this stops being true the guard becomes a no-op, not a bug."""
    assert parse_year("0001") == dt.date(1, 12, 31)
    assert parse_year("9999") == dt.date(9999, 12, 31)
    assert parse_year("1") is None, "a BARE counter is rejected; only the padded form bites"


def test_step2_already_refused_these():
    """Establishes that the sanity check existed and step 3 was walking around it."""
    assert date_parse_rate(PADDED_COUNTER, parse_year, sane_lo=1500, sane_hi=2100) == 0.0
    assert date_parse_rate(REAL_YEARS, parse_year, sane_lo=1500, sane_hi=2100) == 1.0


@pytest.mark.parametrize("time_name", ["leto", "vecka", "manudur", "time"])
def test_a_time_NAMED_axis_of_counters_is_NOT_the_time_axis(time_name):
    """THE regression. Before the fix this returned index 1 and every row got a fake date."""
    assert time_name in TIME_CODES, "guard: the name really is one step 3 matches"
    idx = resolve_time_dim([time_name, "region"], [PADDED_COUNTER, ["SI", "AT"]],
                           parse_fn=parse_year)
    assert idx is None, f"{time_name!r} has no sane dates; the table is date-less"


def test_a_time_named_axis_with_REAL_years_still_resolves():
    """The fix must not break the working case it exists to serve."""
    idx = resolve_time_dim(["leto", "region"], [REAL_YEARS, ["SI", "AT"]], parse_fn=parse_year)
    assert idx == 0


def test_a_mostly_odd_axis_holding_SOME_real_years_still_resolves():
    """Threshold is 'any sane date', not min_rate — a real axis with messy codes must survive."""
    mixed = PADDED_COUNTER[:50] + ["2023", "2024"]
    assert date_parse_rate(mixed, parse_year, sane_lo=1500, sane_hi=2100) < 0.6
    idx = resolve_time_dim(["leto", "region"], [mixed, ["SI"]], parse_fn=parse_year)
    assert idx == 0, "some real dates present, so it IS the time axis"


def test_step2_still_wins_when_a_better_axis_exists():
    """A value-driven match must keep outranking a name match — unchanged precedence."""
    idx = resolve_time_dim(["leto", "ar"], [PADDED_COUNTER, REAL_YEARS], parse_fn=parse_year)
    assert idx == 1, "the axis whose values are real dates wins over the time-NAMED one"


def test_authoritative_metadata_is_still_obeyed():
    """Step 1 is untouched: when upstream declares the axis we take it."""
    idx = resolve_time_dim(["region", "leto"], [["SI"], REAL_YEARS],
                           meta_time_code="leto", parse_fn=parse_year)
    assert idx == 1


def test_a_genuinely_dateless_table_returns_None_as_the_docstring_always_promised():
    idx = resolve_time_dim(["region", "sex"], [["SI", "AT"], ["M", "F"]], parse_fn=parse_year)
    assert idx is None
