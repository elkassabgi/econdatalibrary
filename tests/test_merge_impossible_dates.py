"""Regression gate: the write path must announce observations dated beyond any possible horizon.

WHY THIS CHECK EXISTS AT ALL. Every instrument in this system measures RECENCY — is the newest
observation old? A fabricated FUTURE date passes that trivially; it makes a source look
maximally fresh. health.py even filters forward-dated periods out of its recency signal, and
correctly so, because real projections exist (ABS to 2046 and 2071, UN WPP to 2101, IMF WEO to
2031) and would otherwise cry wolf every day. The consequence is that the one mechanism
protecting real projections also conceals fabricated dates. Nothing anywhere asked whether a
value was POSSIBLE.

Measured 2026-08-03: cso had been SERVING 434,408 rows (0.887% of 48,960,271, across 11 files)
dated beyond 2100 — 272,445 of them in Census 2016 at 9998-12-31 — because a classification
dimension whose codes are CSO sentinels (3001/9998/9999) was being read as the time axis. It
reached users, and it was found only by reading the store by hand.

The check lives in merge_and_write because that is the single choke point every fetcher writes
through: one place, whole fleet, permanently.

WHAT THESE TESTS PIN, and the second matters as much as the first:
  * a fabricated date is COUNTED and announced;
  * a genuine long projection is NOT — a bound that flagged UN WPP would be turned off within
    a week and the check would be worth nothing.
"""
from __future__ import annotations
import datetime as dt
import os
import sys

import pyarrow as pa

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater import merge   # noqa: E402


def _tbl(dates):
    return pa.table({
        "series_key": pa.array([f"K{i}" for i in range(len(dates))], pa.string()),
        "obs_date": pa.array(dates, pa.date32()),
        "value": pa.array([1.0] * len(dates), pa.float64()),
    })


def test_counts_fabricated_dates():
    n = merge._report_impossible_dates(
        _tbl([dt.date(2019, 1, 1), dt.date(9998, 12, 31), dt.date(3001, 12, 31)]), "x")
    assert n == 2, f"expected both fabricated dates counted, got {n}"


def test_real_projections_are_not_flagged():
    # The exact horizons this fleet actually carries. If any of these tripped the check, the
    # check would be noise and would be disabled — which is how a guard stops protecting
    # anything.
    n = merge._report_impossible_dates(
        _tbl([dt.date(2101, 7, 1),     # un_wpp
              dt.date(2100, 12, 31),   # gapminder / owid
              dt.date(2071, 12, 31),   # abs
              dt.date(2075, 12, 31),   # bfs
              dt.date(2031, 12, 31)]), "x")
    assert n == 0, f"a genuine projection horizon was flagged ({n} row(s))"


def test_clean_table_is_silent():
    assert merge._report_impossible_dates(
        _tbl([dt.date(2024, 1, 1), dt.date(2025, 6, 1)]), "x") == 0


def test_missing_or_odd_input_never_breaks_a_publish():
    # This runs on the path EVERY fetcher takes; it must not be able to fail a good publish.
    assert merge._report_impossible_dates(pa.table({"a": pa.array([1])}), "x") == 0
    assert merge._report_impossible_dates(_tbl([]), "x") == 0
    assert merge._report_impossible_dates(
        pa.table({"obs_date": pa.array(["not-a-date"], pa.string())}), "x") == 0


def test_it_reports_rather_than_dropping(tmp_path):
    """Dropping would be data loss decided by a heuristic, on the whole fleet's write path.
    merge_and_write must still publish every row it was given."""
    out = str(tmp_path / "s.parquet")
    rows = [dt.date(2019, 1, 1), dt.date(9998, 12, 31)]
    n, _ = merge.merge_and_write(out, _tbl(rows), mode="overwrite")
    assert n == 2, f"rows were dropped: {n} of 2 survived"
