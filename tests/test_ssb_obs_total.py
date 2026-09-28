"""ssb: obs (served as obs_count) is the store total on every budget-stopped pass (review AR-126).

Distinct row counts per group (Aa 1, Bb 2, Cc 4), so any double count or omission shows. The review
found two mutants - "skip done groups" and "count only unvisited groups" - that the earlier test
could not see; the pass-2 case below catches both.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.test_ssb_cycle import _wire  # noqa: E402
from updater.strategies.fetchers import ssb  # noqa: E402

SIZES = {"Aa": 1, "Bb": 2, "Cc": 4}
STORE = sum(SIZES.values())


def _seed(tmp_path):
    for g, n in SIZES.items():
        pq.write_table(pa.table({"series_key": [f"k{i}" for i in range(n)],
                                 "obs_date": pa.array([dt.date(2026, 1, 1)] * n),
                                 "value": [1.0] * n}), tmp_path / f"grp_{g}.parquet")


def test_a_cut_inside_a_group_then_a_stop(monkeypatch, tmp_path):
    _seed(tmp_path)
    _wire(monkeypatch, tmp_path, allow=2, tables_per_group=2)
    res = ssb.update(None, None)
    assert res.status == "partial" and res.obs == STORE, res.obs


def test_done_groups_after_the_stop_point_are_counted(monkeypatch, tmp_path):
    _seed(tmp_path)
    _wire(monkeypatch, tmp_path, allow=2)                          # pass 1: Aa visited, stop at Bb
    assert ssb.update(None, None).obs == STORE
    _wire(monkeypatch, tmp_path, allow=1, tables_per_group=1)       # pass 2: Bb, Cc, Aa(done); cut Bb
    res = ssb.update(None, None)
    assert res.status == "partial" and res.obs == STORE, res.obs


def test_a_stop_before_the_first_group(monkeypatch, tmp_path):
    _seed(tmp_path)
    _wire(monkeypatch, tmp_path, allow=0)
    res = ssb.update(None, None)
    assert res.status == "partial" and res.obs == STORE, res.obs


def test_negative_control_no_stop_counts_each_group_once(monkeypatch, tmp_path):
    _seed(tmp_path)
    _wire(monkeypatch, tmp_path, allow=99)
    assert ssb.update(None, None).obs == STORE
