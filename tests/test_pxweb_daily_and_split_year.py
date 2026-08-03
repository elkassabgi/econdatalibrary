"""PxWeb daily (TLIST(D1)) and split/academic-year period codes must parse.

WHAT WAS LOST. cso reported "60/60 sub-unit(s) transient-failed; will retry" — the shape of an
upstream outage. It was not. Probed live 2026-08-03, three of the matrices it named return HTTP 200
with real bodies and parsed to NOTHING, because the period grammar stopped at monthly:

    EDA21   time codes "2003-2004" (academic year)   ->      0 rows, now      22
    MTD05   time codes "2010M01D01" (daily)          ->      0 rows, now  72,284
    MTH05   time codes "2010M01D01" (daily)          ->      0 rows, now 1,734,965

1,807,271 rows from three matrices, dropped silently and filed as a network failure. The fetcher's
message for an unparseable body and for a dead endpoint was the same sentence, so nothing
distinguished "CSO is down" from "we cannot read what CSO sent".

THE NEGATIVE CONTROL IS THE POINT OF THE SPLIT-YEAR RULE. parse_date/parse_period double as the
DETECTOR that picks the time dimension (is_time_dim's value-driven fallback, date_parse_rate).
A permissive "^\\d{4}-\\d{4}$" would make an ordinary range label — "1990-2000", a cohort or a
footnoted span — parse as a date, and could promote a classification axis to the time axis. That is
precisely the swapped-axis defect that cost 290 matrices and 754,780 rows to repair (R288). So the
second year must be the first + 1, and test_range_label_is_not_a_date pins it.

Both parsers are checked: core/pxweb.parse_period is the shared grammar for the PxWeb family, and
jobs/ingest_cso_ireland.parse_date is cso's own drifted copy — which is the one that actually runs
for cso, so fixing only the shared one would have changed nothing here.
"""
from __future__ import annotations
import datetime as dt
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pxweb import parse_period                                # noqa: E402
from jobs.ingest_cso_ireland import parse_date                     # noqa: E402

PARSERS = pytest.mark.parametrize("fn", [parse_period, parse_date],
                                  ids=["shared", "cso"])


@PARSERS
def test_daily_codes_parse(fn):
    assert fn("2010M01D01") == dt.date(2010, 1, 1)
    assert fn("2010M12D31") == dt.date(2010, 12, 31)
    assert fn("2026M06D30") == dt.date(2026, 6, 30)


@PARSERS
def test_impossible_daily_codes_do_not_parse(fn):
    assert fn("2010M13D01") is None, "month 13"
    assert fn("2010M00D01") is None, "month 0"
    assert fn("2010M01D32") is None, "day 32"
    assert fn("2010M02D30") is None, "30 February must not be invented"


@PARSERS
def test_split_year_parses_to_its_starting_year(fn):
    """Consistent with bare YYYY -> that year's 31 Dec."""
    assert fn("2003-2004") == dt.date(2003, 12, 31)
    assert fn("2024-2025") == dt.date(2024, 12, 31)


@PARSERS
def test_range_label_is_not_a_date(fn):
    """The load-bearing constraint: only consecutive years are a period.

    These parsers also DETECT the time axis, so a permissive range rule could hand a
    classification axis to the resolver as time (R288)."""
    assert fn("1990-2000") is None
    assert fn("2003-2003") is None, "same year is not a span"
    assert fn("2004-2003") is None, "backwards is not a span"
    assert fn("2003-2005") is None, "two-year gap is a range label, not a period"


@PARSERS
def test_existing_grammar_is_untouched(fn):
    assert fn("2022") == dt.date(2022, 12, 31)
    assert fn("2022M03") == dt.date(2022, 3, 1)
    assert fn("2022Q3") == dt.date(2022, 7, 1)
    assert fn("2022-05-17") == dt.date(2022, 5, 17)


@PARSERS
def test_non_time_codes_still_rejected(fn):
    """Municipality ids, positional indices and sentinels must not become dates."""
    for junk in ("", "abc", "9999-9999", "TOTAL", "-"):
        assert fn(junk) is None, junk


@PARSERS
def test_four_digit_category_codes_parse_but_are_out_of_sane_range(fn):
    """`0111` DOES parse, to year 111 — and that is the documented design, not a defect.

    I first wrote this test asserting `parse("0111") is None` and it failed. The code is right and
    the assumption was wrong: parse_period is a pure grammar, and the sanity bound lives one layer
    up in date_parse_rate(sane_lo=1500, sane_hi=2100), deliberately, so that a genuine historical
    axis (Statistics Iceland's population series starts 1703) still scores as time while 4-digit
    category codes are too sparse inside that window to win.

    Worth pinning because 0111 is not a hypothetical: it is a CSO crop code, and 0111-12-31 is
    exactly the kind of date found in the swapped-axis matrices (R288). The parser producing it is
    fine; what must never happen is such an axis being CHOSEN as time — which is what the sane
    range below enforces."""
    d = fn("0111")
    assert d == dt.date(111, 12, 31)
    assert not (1500 <= d.year <= 2100), "must fall outside the detector's sane window"


def test_detector_scores_a_category_axis_zero_and_a_year_axis_one():
    """The layer that actually protects axis choice, exercised directly."""
    from core.pxweb import date_parse_rate
    crop_codes = ["0111", "0112", "0113", "0121", "0122", "0123"]
    year_codes = ["1949", "1950", "1951", "1952"]
    assert date_parse_rate(crop_codes) == 0.0
    assert date_parse_rate(year_codes) == 1.0


def test_detector_accepts_the_two_new_grammars():
    """A daily axis and an academic-year axis must now score as time, or the fix is inert."""
    from core.pxweb import date_parse_rate
    assert date_parse_rate(["2010M01D01", "2010M01D02", "2010M01D03"]) == 1.0
    assert date_parse_rate(["2003-2004", "2004-2005", "2005-2006"]) == 1.0
