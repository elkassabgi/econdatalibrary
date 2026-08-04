"""ONS V4 time CODE grammars — the class that made 10 of 12 datasets parse to zero rows.

parse_ons_period() knew '2022', '2022 Q1', 'Dec 2022' and ISO dates, and NONE of the formats
ONS actually ships in its V4 code columns. Every row of cpih01, retail-sales-index,
gdp-to-four-decimal-places, index-private-housing-rental-prices, wellbeing-local-authority and
life-expectancy-by-local-authority was therefore dropped for want of a parseable date, the
fetcher recorded "real body, zero parseable rows", and — because a zero-row dataset
deliberately does not advance its vintage — those datasets sat at the head of the work queue
re-downloading themselves forever.

The two things these tests are really pinning down:

  1. THE COLUMN NAME IS THE DISCRIMINATOR, because the values collide. '2011-12' is financial
     year 2011/12, the two-year interval 2001-2003, or ISO December 2011, depending ONLY on
     which column it sits in. Anything that sniffs the value is guessing.
  2. THE CENTURY IS NOT A CONSTANT. cpih01 carries 'Jan-88' and 'Jan-26' in one column and both
     are real. Verified against live data: under the sliding window the dataset yields exactly
     457 distinct months running 1988-01..2026-01 with no gaps, and 457 is exactly the number
     of months in that span — no other assignment is contiguous.
"""
import datetime as dt

import pytest

from jobs.ingest_ons_uk import parse_ons_period, parse_ons_time_code


NOW = 2026          # pin the sliding window so these tests do not drift with the wall clock


def test_mmm_yy_spans_the_century_boundary():
    """The exact pair that no fixed pivot can serve: both are real, 98 years apart."""
    assert parse_ons_time_code("Jan-26", "mmm-yy", NOW) == dt.date(2026, 1, 1)
    assert parse_ons_time_code("Jan-88", "mmm-yy", NOW) == dt.date(1988, 1, 1)


def test_mmm_yy_never_returns_a_future_year():
    """A two-digit year just past the window belongs to the previous century, not the next."""
    assert parse_ons_time_code("Dec-27", "mmm-yy", NOW) == dt.date(1927, 12, 1)
    assert parse_ons_time_code("Dec-26", "mmm-yy", NOW) == dt.date(2026, 12, 1)


def test_cpih01_month_run_is_contiguous_under_the_sliding_window():
    """The property that PROVES the mapping rather than merely asserting it.

    Jan-1988..Jan-2026 inclusive is 457 months, and live cpih01 has exactly 457 distinct
    period codes. Reconstruct them from the two-digit codes and require an unbroken run —
    a wrong century assignment puts a ~100-year hole in the middle.
    """
    codes = []
    y, m = 1988, 1
    while (y, m) <= (2026, 1):
        codes.append(f"{['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][m-1]}-{y % 100:02d}")
        m += 1
        if m == 13:
            y, m = y + 1, 1
    assert len(codes) == 457
    got = [parse_ons_time_code(c, "mmm-yy", NOW) for c in codes]
    assert all(g is not None for g in got)
    assert got == sorted(got), "century wrap would break monotonicity"
    assert got[0] == dt.date(1988, 1, 1) and got[-1] == dt.date(2026, 1, 1)


def test_the_same_string_means_different_dates_in_different_columns():
    """'2011-12' — the collision that makes value-sniffing impossible."""
    assert parse_ons_time_code("2011-12", "yyyy-yy", NOW) == dt.date(2012, 3, 31)   # FY 2011/12 ends 31 Mar
    assert parse_ons_time_code("2011-12", "yyyy-mm", NOW) == dt.date(2011, 12, 1)   # ISO December 2011
    # ...and under two-year-intervals it is not even well formed (2011+2 != 13), so we bail
    # rather than invent a reading.
    assert parse_ons_time_code("2011-12", "two-year-intervals", NOW) is None


def test_two_year_interval_ends_at_the_interval_end():
    assert parse_ons_time_code("2001-03", "two-year-intervals", NOW) == dt.date(2003, 12, 31)
    assert parse_ons_time_code("2017-19", "two-year-intervals", NOW) == dt.date(2019, 12, 31)


def test_financial_year_second_half_must_follow_the_first():
    """Guards against reading an arbitrary 'YYYY-NN' as a financial year."""
    assert parse_ons_time_code("2022-23", "yyyy-yy", NOW) == dt.date(2023, 3, 31)
    assert parse_ons_time_code("1999-00", "yyyy-yy", NOW) == dt.date(2000, 3, 31)  # mod-100 rollover
    assert parse_ons_time_code("2022-25", "yyyy-yy", NOW) is None


def test_cumulative_span_ends_at_its_final_financial_year():
    assert parse_ons_time_code("1978-to-2020-21", "yyyy-to-yyyy-yy", NOW) == dt.date(2021, 3, 31)


def test_quarters_map_to_the_quarters_FIRST_month():
    """Pinned against the STORE, not taste.

    An earlier draft used q*3 — the quarter's LAST month. It disagreed with every one of
    regional-gdp-by-quarter's 31,992 on-disk rows while producing an IDENTICAL key set, so a
    check that compared series ids would have passed it. The approved re-key holds
    2012-01-01 / 2012-04-01 / 2012-07-01 / 2012-10-01, and parse_ons_period has always used
    (q-1)*3+1 for 'YYYY Qn' — the new grammar must not contradict the old one.
    """
    assert parse_ons_time_code("2012-q1", "yyyy-qq", NOW) == dt.date(2012, 1, 1)
    assert parse_ons_time_code("2012-q2", "yyyy-qq", NOW) == dt.date(2012, 4, 1)
    assert parse_ons_time_code("2012-q3", "yyyy-qq", NOW) == dt.date(2012, 7, 1)
    assert parse_ons_time_code("2012-q4", "yyyy-qq", NOW) == dt.date(2012, 10, 1)
    assert parse_ons_time_code("2020 Q3", "yyyy-qq", NOW) == parse_ons_period("2020 Q3")


def test_rolling_three_month_window_takes_its_FIRST_month():
    """labour-market ships 'apr-jun-2019' three-month averages. Decided from the store:
    under first-month all 31,968 (key, date) pairs reproduce the on-disk table exactly;
    under last-month 1,728 disagree."""
    assert parse_ons_time_code("apr-jun-2019", "mmm-mmm-yyyy", NOW) == dt.date(2019, 4, 1)
    assert parse_ons_time_code("sep-nov-2024", "mmm-mmm-yyyy", NOW) == dt.date(2024, 9, 1)
    assert parse_ons_time_code("feb-apr-2025", "mmm-mmm-yyyy", NOW) == dt.date(2025, 2, 1)
    assert parse_ons_time_code("xxx-jun-2019", "mmm-mmm-yyyy", NOW) is None


def test_unknown_column_falls_back_to_the_old_grammar_unchanged():
    """Strictly additive: calendar-years and friends still parse exactly as before."""
    for s in ("2019", "2022 Q1", "Dec 2022", "2022-06-30"):
        assert parse_ons_time_code(s, "calendar-years", NOW) == parse_ons_period(s)
    assert parse_ons_time_code("2019", "calendar-years", NOW) == dt.date(2019, 12, 31)


@pytest.mark.parametrize("bad", ["", "   ", "notadate", "Xyz-26", "26-Jan"])
def test_malformed_values_return_none_rather_than_a_wrong_date(bad):
    assert parse_ons_time_code(bad, "mmm-yy", NOW) is None
