"""Impossible dates must AGGREGATE, not just print (ledger R320).

merge's guard has always printed one line per affected file and returned a count the caller
threw away. So the only record was scattered lines in a run log, and 273,980 rows across six
sources sat published with dates between 2999-12-31 and 9999-12-31 while it printed on every
affected run. They were found by finally running a standalone audit, not by anyone reading a
log. A warning that never blocks AND never aggregates is indistinguishable from silence.

These tests pin the accumulator the orchestrator reports from — and, deliberately, that the
guard still does NOT block a publish: dropping rows on a heuristic would be data loss, and that
judgement is unchanged.
"""
import datetime as dt

import pyarrow as pa
import pytest

from updater import merge


def _tbl(dates):
    return pa.table({
        "series_key": pa.array([f"k{i}" for i in range(len(dates))], pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array([1.0] * len(dates), pa.float64()),
    })


def test_clean_data_leaves_the_accumulator_at_zero():
    merge.impossible_reset()
    merge._report_impossible_dates(_tbl([dt.date(2024, 1, 1), dt.date(1999, 6, 30)]), "a.parquet")
    assert merge.impossible_report() == {"rows": 0, "files": 0, "worst": None}


def test_bad_rows_are_counted_and_the_worst_is_kept():
    merge.impossible_reset()
    merge._report_impossible_dates(
        _tbl([dt.date(2024, 1, 1), dt.date(6152, 12, 31), dt.date(3005, 12, 31)]), "b.parquet")
    rep = merge.impossible_report()
    assert rep["rows"] == 2
    assert rep["files"] == 1
    assert rep["worst"][1] == dt.date(6152, 12, 31)


def test_it_accumulates_ACROSS_files_which_is_the_whole_point():
    """One line per file was already printed; what was missing is the total for the source."""
    merge.impossible_reset()
    merge._report_impossible_dates(_tbl([dt.date(9999, 12, 31)] * 3), "one.parquet")
    merge._report_impossible_dates(_tbl([dt.date(2999, 12, 31)] * 5), "two.parquet")
    merge._report_impossible_dates(_tbl([dt.date(2024, 1, 1)]), "clean.parquet")
    rep = merge.impossible_report()
    assert rep["rows"] == 8, "3 + 5, and the clean file must not contribute"
    assert rep["files"] == 2, "the clean file is not an affected file"
    assert rep["worst"][1] == dt.date(9999, 12, 31)


def test_reset_is_per_unit_so_one_source_cannot_inherit_anothers_count():
    merge.impossible_reset()
    merge._report_impossible_dates(_tbl([dt.date(9999, 12, 31)]), "prev_source.parquet")
    assert merge.impossible_report()["rows"] == 1
    merge.impossible_reset()
    assert merge.impossible_report() == {"rows": 0, "files": 0, "worst": None}


def test_the_guard_still_does_not_block_a_publish():
    """Deliberate: dropping rows on a heuristic is data loss. It reports; it does not refuse."""
    merge.impossible_reset()
    n = merge._report_impossible_dates(_tbl([dt.date(9999, 12, 31)]), "c.parquet")
    assert n == 1                      # it returns a count
    # ...and raises nothing, which is what lets the caller publish anyway.


def test_a_table_without_obs_date_is_ignored_rather_than_raising():
    merge.impossible_reset()
    t = pa.table({"series_key": pa.array(["x"], pa.string()),
                  "value": pa.array([1.0], pa.float64())})
    assert merge._report_impossible_dates(t, "d.parquet") == 0
    assert merge.impossible_report()["rows"] == 0


def test_the_orchestrator_can_actually_reach_these():
    """Regression on a real slip: orchestrate.py used merge.* without importing merge, so the
    reporting line would have raised NameError on the first affected run."""
    from updater import orchestrate
    assert orchestrate.merge is merge
