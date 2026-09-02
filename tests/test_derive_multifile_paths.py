"""One reading of `Resolution.parquet_path`, for all three of its documented shapes.

The field is documented as "absolute native file OR directory", `_resolve.py:1852` already
branches on `isinstance(res.parquet_path, str)` for a LIST, and no resolver has ever returned
one - so the multi-file path was aspiration, and two consumers would have gone wrong quietly:

  * `sorted_csv_gz` interpolated `str(res.parquet_path)` into DuckDB's `read_parquet('...')`.
    A list stringifies as `['a', 'b']` with Python quoting: not a path, not a glob, and a
    parse error a long way from its cause.
  * `_series_csv_bytes` gated the MAX_ROWS ceiling on `isinstance(..., str)`, so a list or a
    directory SKIPPED the size check. A ceiling that stops applying at exactly the shape which
    means MORE data is R658 F3's defect, and it fails toward serving something undeliverable.

These pin both, plus the fact that a split file and its original read back identically - which
is the assumption the whole freeze-and-forward layout rests on.
"""
from __future__ import annotations

import os
import sys

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as pads
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.derive_csv import _duck_source, resolved_paths  # noqa: E402


class Res:
    """The only field these two helpers read."""

    def __init__(self, p):
        self.parquet_path = p


def test_a_single_path_is_a_one_element_list():
    assert resolved_paths(Res("a/b.parquet")) == ["a/b.parquet"]


def test_a_list_survives_verbatim():
    got = resolved_paths(Res(["a/b.parquet", "a/c.parquet"]))
    assert got == ["a/b.parquet", "a/c.parquet"]


def test_a_tuple_is_accepted_too():
    assert resolved_paths(Res(("x.parquet", "y.parquet"))) == ["x.parquet", "y.parquet"]


def test_a_directory_expands_to_its_parquet_files(tmp_path):
    (tmp_path / "sub").mkdir()
    for rel in ("one.parquet", "sub/two.parquet"):
        p = tmp_path / rel
        pq.write_table(pa.table({"a": [1]}), p)
    (tmp_path / "notes.txt").write_text("not parquet", encoding="utf-8")
    got = resolved_paths(Res(str(tmp_path)))
    assert len(got) == 2, got
    assert all(g.endswith(".parquet") for g in got)
    assert got == sorted(got), "the order must be stable or the derive is not reproducible"


def test_the_duckdb_argument_is_a_quoted_path_for_one_file():
    assert _duck_source(Res(r"E:\x\y.parquet")) == "'E:/x/y.parquet'"


def test_the_duckdb_argument_is_a_LIST_for_several():
    """`str(['a','b'])` is not valid SQL. read_parquet takes a real list."""
    got = _duck_source(Res([r"E:\x\y.parquet", r"E:\x\z.parquet"]))
    assert got == "['E:/x/y.parquet', 'E:/x/z.parquet']"
    assert not got.startswith("'["), "the list was stringified instead of built"


def test_a_single_quote_in_a_path_is_escaped():
    """A path with an apostrophe would otherwise close the SQL string early."""
    assert _duck_source(Res("/tmp/o'brien.parquet")) == "'/tmp/o''brien.parquet'"


def test_the_MAX_ROWS_ceiling_sums_every_part(tmp_path):
    """R658 F3's shape: the ceiling used to skip anything that was not a `str`, so a
    partitioned series - the case that means MORE rows - bypassed it entirely."""
    import core.derive_csv as dc
    a, b = tmp_path / "a.parquet", tmp_path / "b.parquet"
    pq.write_table(pa.table({"n": list(range(60))}), a)
    pq.write_table(pa.table({"n": list(range(70))}), b)

    # PATCH THE FUNCTION, NOT THE MODULE. Substituting a stand-in object for
    # `econdl._resolve` broke `_bundle.py`'s own `from ._resolve import ResolveError`
    # several imports away - a fake wide enough to reach code the test is not about.
    from econdl import _resolve as real_resolve
    saved_max, saved_fn = dc._MAX_ROWS, real_resolve.resolve
    try:
        dc._MAX_ROWS = 100                      # 60 + 70 = 130 is over; either half alone is not
        real_resolve.resolve = lambda _sid: Res([str(a), str(b)])
        with pytest.raises(dc.TooLarge) as e:
            dc._series_csv_bytes("x:y")
        assert "130" in str(e.value), str(e.value)
    finally:
        dc._MAX_ROWS = saved_max
        real_resolve.resolve = saved_fn


def test_a_split_series_reads_back_identical_to_the_unsplit_one(tmp_path):
    """The assumption freeze-and-forward rests on. Both halves are non-empty by construction:
    a first version of this experiment cut at a date after the series ended, so the live half
    was empty and it proved only that one file plus nothing reads like one file (R632)."""
    dates = pa.array([f"20{y:02d}-01-01" for y in range(10, 30)]).cast(pa.string())
    whole = pa.table({"series_key": ["k"] * 20,
                      "obs_date": dates,
                      "value": [float(i) for i in range(20)]})
    frozen = whole.filter(pc.field("obs_date") < "2020-01-01")
    live = whole.filter(pc.field("obs_date") >= "2020-01-01")
    assert frozen.num_rows and live.num_rows, "degenerate split proves nothing"
    one = tmp_path / "one.parquet"
    fp, lp = tmp_path / "frozen.parquet", tmp_path / "live.parquet"
    pq.write_table(whole, one)
    pq.write_table(frozen, fp)
    pq.write_table(live, lp)
    pred = pc.field("series_key") == "k"
    a = pads.dataset(str(one)).to_table(filter=pred).sort_by([("obs_date", "ascending")])
    b = pads.dataset([str(fp), str(lp)]).to_table(filter=pred).sort_by(
        [("obs_date", "ascending")])
    assert a.num_rows == b.num_rows == 20
    assert a.equals(b), "the union of the two halves is not the original series"
