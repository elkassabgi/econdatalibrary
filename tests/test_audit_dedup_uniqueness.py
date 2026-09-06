"""`audit_file` streams now, and its verdict must not change (R806).

The old implementation read the whole table and called
`t.group_by(list(key_cols)).aggregate([]).num_rows`. pyarrow's hash aggregate FAST-FAILS on a
large table — exit 0xC0000409, no exception, no traceback, and with stdout buffered, no output at
all. Measured 2026-09-06: it survives imf's largest file at 6,300,194 rows and dies on cso's at
29,760,740 and vdem's at 77,371,121. Seventeen of 379 stores were unmeasurable for that reason in
the first fleet sweep, and a crash with no output is indistinguishable from a tool that printed
nothing.

This file did not exist. The tool has been the fleet's answer to "can this store be tailed
incrementally" with nothing pinning it at all, which is how a rewrite could have silently changed
every verdict. The test that matters most is
`test_a_duplicate_SPLIT_ACROSS_ROW_GROUPS_is_still_found`: a per-batch aggregate would pass every
other test here and miss exactly that.
"""
from __future__ import annotations
import datetime as dt
import importlib.util
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

_spec = importlib.util.spec_from_file_location(
    "audit_dedup_uniqueness", os.path.join(ROOT, "tools", "audit_dedup_uniqueness.py"))
aud = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(aud)

KEY = ("series_key", "obs_date")
D1, D2 = dt.date(2026, 9, 5), dt.date(2026, 9, 6)


def _write(path, keys, dates, values=None, row_group_size=None):
    t = pa.table({"series_key": pa.array(keys, pa.string()),
                  "obs_date": pa.array(dates, pa.date32()),
                  "value": pa.array(values if values is not None
                                    else [float(i) for i in range(len(keys))], pa.float64())})
    pq.write_table(t, path, row_group_size=row_group_size or max(len(keys), 1))
    return path


def test_a_uniquely_keyed_file_reports_pairs_equal_to_rows(tmp_path):
    p = _write(str(tmp_path / "clean.parquet"), ["a", "a", "b"], [D1, D2, D1])
    r = aud.audit_file(p, KEY)
    assert (r.rows, r.keys, r.pairs, r.capped) == (3, 2, 3, False)


def test_an_under_keyed_file_reports_fewer_pairs_than_rows(tmp_path):
    p = _write(str(tmp_path / "bad.parquet"), ["a", "a", "a"], [D1, D1, D2])
    r = aud.audit_file(p, KEY)
    assert (r.rows, r.keys, r.pairs) == (3, 1, 2)
    assert r.pairs < r.rows


def test_a_duplicate_SPLIT_ACROSS_ROW_GROUPS_is_still_found(tmp_path):
    """THE test for a streaming rewrite. Both copies of ('a', D1) are real duplicates, but they
    land in different row groups, so anything that aggregates per batch and adds the results sees
    two groups of one and reports the file clean."""
    p = _write(str(tmp_path / "split.parquet"),
               ["a", "b", "a", "c"], [D1, D1, D1, D1], row_group_size=2)
    assert pq.read_metadata(p).num_row_groups == 2, "the fixture must actually be split"
    r = aud.audit_file(p, KEY)
    assert (r.rows, r.pairs) == (4, 3), (
        f"rows={r.rows} pairs={r.pairs}: a cross-batch duplicate was missed")


def test_the_same_KEY_in_two_row_groups_counts_once(tmp_path):
    p = _write(str(tmp_path / "k.parquet"), ["a", "a"], [D1, D2], row_group_size=1)
    r = aud.audit_file(p, KEY)
    assert r.keys == 1 and r.pairs == 2


def test_a_file_without_the_key_columns_returns_None(tmp_path):
    p = str(tmp_path / "other.parquet")
    pq.write_table(pa.table({"a": pa.array([1]), "b": pa.array([2])}), p)
    assert aud.audit_file(p, KEY) is None


def test_an_empty_file_is_zero_not_a_crash(tmp_path):
    p = _write(str(tmp_path / "empty.parquet"), [], [])
    r = aud.audit_file(p, KEY)
    assert (r.rows, r.keys, r.pairs, r.capped) == (0, 0, 0, False)


def test_a_three_column_key_is_honoured(tmp_path):
    """`--key` and several fetchers use keys that are not the default pair; the packing must be
    generic over the number of columns, not hardwired to two."""
    p = str(tmp_path / "three.parquet")
    pq.write_table(pa.table({"series_key": pa.array(["a", "a"]),
                             "obs_date": pa.array([D1, D1], pa.date32()),
                             "period": pa.array(["Q1", "Q2"])}), p)
    r = aud.audit_file(p, ("series_key", "obs_date", "period"))
    assert (r.rows, r.pairs) == (2, 2), "the third column must separate the rows"
    r2 = aud.audit_file(p, KEY)
    assert (r2.rows, r2.pairs) == (2, 1), "and without it they collide"


def test_NULLS_do_not_collapse_into_each_other_or_vanish(tmp_path):
    p = str(tmp_path / "nulls.parquet")
    pq.write_table(pa.table({"series_key": pa.array(["a", None, None]),
                             "obs_date": pa.array([D1, D1, D1], pa.date32())}), p)
    r = aud.audit_file(p, KEY)
    assert r.rows == 3 and r.pairs == 2, (r.rows, r.pairs)


def test_past_the_cap_the_file_is_UNMEASURED_not_reported_clean(tmp_path, monkeypatch):
    """Memory scales with the DISTINCT count, so there has to be a ceiling. Hitting it must
    produce "unknown", never a verdict — being killed silently is what this replaced."""
    monkeypatch.setattr(aud, "MAX_EXACT_PAIRS", 2)
    p = _write(str(tmp_path / "many.parquet"),
               [f"k{i}" for i in range(10)], [D1] * 10, row_group_size=10)
    r = aud.audit_file(p, KEY)
    assert r.capped is True
    assert r.rows == 10, "the row count is still honest"
    assert r.pairs == -1, "and the pair count is explicitly absent, not zero"


def test_the_cap_does_not_fire_below_it(tmp_path, monkeypatch):
    monkeypatch.setattr(aud, "MAX_EXACT_PAIRS", 1000)
    p = _write(str(tmp_path / "few.parquet"), ["a", "b"], [D1, D1])
    assert aud.audit_file(p, KEY).capped is False
