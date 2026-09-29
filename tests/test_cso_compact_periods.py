"""cso: the COMPACT two-year period code ('20112012', label '2011/12') is dated by the ingester's existing
split/academic-year rule (R288: '2011-2012' -> 2011-12-31), and a compact multi-year WINDOW ('20182021') is named
span_time - the convention not yet chosen - rather than `unparsed` (our bug, retried every run).
Found 2026-09-29: HSPAE136 and EIIA15 among the matrices the daily gate reported as 'parsed 0 obs - OURS'."""
import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import cso as C  # noqa: E402

ING = C._ingester()


def _jsonstat(codes, values):
    return {"id": ["STATISTIC", "TLIST(A1)"], "size": [1, len(codes)], "role": {"time": ["TLIST(A1)"]},
            "dimension": {"STATISTIC": {"category": {"index": ["S1"], "label": {"S1": "x"}}},
                          "TLIST(A1)": {"category": {"index": codes, "label": {c: c for c in codes}}}},
            "value": values}


def test_a_compact_academic_year_is_dated_like_its_dashed_form():
    assert ING.parse_date("20112012") == dt.date(2011, 12, 31) == ING.parse_date("2011-2012")
    assert ING.parse_date("20242025") == dt.date(2024, 12, 31)


def test_a_compact_window_is_not_dated():
    assert ING.parse_date("20182021") is None                    # a window: no convention yet
    assert ING.parse_date("2018-2021") is None


def test_codes_that_are_not_two_plausible_years_are_untouched():
    assert ING.parse_date("20190101") is None                    # not a year pair; no daily compact grammar
    assert ING.parse_date("18991900") is None                    # implausible first year
    assert ING.parse_date("197511") == dt.date(1975, 11, 1)      # the existing YYYYMM form still works
    assert ING.parse_date("2022") == dt.date(2022, 12, 31)


def test_a_compact_academic_table_parses_and_a_compact_window_is_span_time():
    rows = ING.parse_jsonstat2(_jsonstat(["20112012", "20122013"], [1.5, 2.5]), "CSO:T1")
    assert sorted(d for _, d, _ in rows) == [dt.date(2011, 12, 31), dt.date(2012, 12, 31)], rows
    window = _jsonstat(["20182021"], [3.0])
    assert ING.parse_jsonstat2(window, "CSO:T2") == []
    assert ING._why_unparsed(window) == "span_time"
    assert ING._why_unparsed(_jsonstat(["2018-2021"], [3.0])) == "span_time"  # the dashed form still is
