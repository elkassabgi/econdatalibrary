"""Regression gate: _max_by_key returns ISO STRINGS, and each caller must convert or not
convert according to what ITS OWN consumer needs.

WHAT HAPPENED. `_common._max_by_key` ends with `{k: d.isoformat() ...}` — the values are
strings. Five fetchers call it and three got the type wrong, in two different ways:

    boc       .isoformat() on a str   -> AttributeError; boc's last recorded run is a
                                         transient_fail and it has never reported a success
    tcmb      .isoformat() on a str   -> AttributeError
    riksbank  isinstance(v, dt.date)  -> no string satisfies it, so the map came back EMPTY on
                                         every run. No crash, no log line: every series then
                                         re-fetched from EARLIEST, no cursors reached the §5.7
                                         coherence check, the run was demoted to `partial`, and
                                         a partial never sets last_success_utc (R231).

bcrp and scb work only because ISO strings sort and compare exactly like dates.

THE FIX IS NOT THE SAME FOR ALL THREE, which is the part worth pinning. What each function owes
its caller differs:
    boc._stored_maxes       -> ISO STRINGS. They are interpolated into a URL
                               (`start_date={start}`) and reduced with min().
    tcmb._per_series_cursors-> ISO STRINGS. They are cursors, which are stored as strings.
    riksbank._stored_max    -> dt.date. update() compares `cat_max <= smax` against a date and
                               passes smax to revision_since(). Passing strings through here
                               would have swapped a silent empty for a TypeError — which is
                               exactly what I did first, before reading the consumer.

So each test below asserts the type that FUNCTION's consumer requires, not one global answer.
"""
from __future__ import annotations
import datetime as dt
import os
import sys

import pyarrow as pa

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater.strategies.fetchers._common import _max_by_key   # noqa: E402

EXPECT = {"A": "2025-06-01", "B": "2023-03-01"}


def _tbl():
    return pa.table({
        "series_key": pa.array(["A", "A", "B"], pa.string()),
        "obs_date": pa.array([dt.date(2024, 1, 1), dt.date(2025, 6, 1), dt.date(2023, 3, 1)],
                             pa.date32()),
        "value": pa.array([1.0, 2.0, 3.0], pa.float64()),
    })


def test_max_by_key_values_are_iso_strings():
    m = _max_by_key(_tbl())
    assert m == EXPECT, m
    assert all(isinstance(v, str) for v in m.values()), (
        "three of five callers got this wrong while it was undocumented")


def test_iso_strings_order_like_dates():
    """bcrp and scb depend on this without saying so; boc reduces with min() over them. If the
    format ever stops being ISO, all three break silently."""
    assert "2023-03-01" < "2024-01-01" < "2025-06-01"


def _patch_store(monkeypatch, mod):
    monkeypatch.setattr(mod.blob, "exists", lambda p: True)
    monkeypatch.setattr(mod.blob, "read_table", lambda p, columns=None: _tbl())


def test_boc_returns_iso_strings_for_its_url(monkeypatch):
    import updater.strategies.fetchers.boc as boc
    _patch_store(monkeypatch, boc)
    out = boc._stored_maxes("x.parquet")
    assert out == EXPECT, out
    assert all(isinstance(v, str) for v in out.values()), (
        "boc interpolates these into start_date={} and reduces with min(); dates would break "
        "the URL")


def test_tcmb_returns_iso_strings_as_cursors(monkeypatch):
    import updater.strategies.fetchers.tcmb as tcmb
    _patch_store(monkeypatch, tcmb)
    out = tcmb._per_series_cursors("x.parquet")
    assert out == EXPECT, out
    assert all(isinstance(v, str) for v in out.values())


def test_riksbank_returns_DATES_because_its_caller_compares_against_one(monkeypatch):
    """The regression that mattered most, and the one whose fix is the opposite of the others.
    It must be non-empty (the silent failure) AND made of dt.date (update() does
    `cat_max <= smax` against a date and calls revision_since(smax, unit))."""
    import updater.strategies.fetchers.riksbank as rb
    _patch_store(monkeypatch, rb)
    out = rb._stored_max("x.parquet")
    assert out, "riksbank returned an EMPTY map — the silent failure this pins"
    assert set(out) == {"A", "B"}, out
    assert all(isinstance(v, dt.date) for v in out.values()), (
        f"riksbank must yield dt.date, got {[type(v).__name__ for v in out.values()]}")
    assert out["A"] == dt.date(2025, 6, 1) and out["B"] == dt.date(2023, 3, 1), out


def test_riksbank_values_survive_the_comparison_its_caller_performs():
    """Directly exercises the expression that would have raised TypeError had _stored_max
    passed the ISO strings through: `cat_max <= smax` with cat_max a dt.date."""
    import updater.strategies.fetchers.riksbank as rb
    smax = dt.date(2025, 6, 1)
    cat_max = rb._pdate("2025-06-02")
    assert cat_max is not None and (cat_max <= smax) is False
