"""The file-grain stream CARRIES insee_melodi's flow assertion instead of refusing it.

`_resolve_file_grain` returns `flow == <native id>` for a store whose files carry a `flow` column
(insee_melodi). Until this change `_series_csv_to_file_sorted` refused every predicate except
is_valid(key) (review R1131: dropping a predicate serves the wrong rows), so a flow-grain declaration
for insee_melodi would have sent every flow over FLOW_INMEM_MAX_ROWS to the desktop debt. The stream
now translates exactly that Expression into SQL. These tests hold the three things that must stay true:
the bytes equal the in-memory path, the filter is APPLIED (rows of another flow in the same file are
excluded by both paths), and any other predicate is still refused.
"""
import gzip
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))


def _store(tmp_path, monkeypatch, flows):
    """A one-file insee_melodi store holding rows for every flow code in `flows` (in that order)."""
    d = tmp_path / "insee_melodi"
    d.mkdir(parents=True, exist_ok=True)
    vals = [1.0, 0.1, 1e-05, 123456789.123, 2.5e16, -0.0, 1691.908732, 3.0, 7e-07, 42.0]
    rows = {"flow": [], "series_key": [], "obs_date": [], "value": []}
    for f in flows:
        for i, v in enumerate(vals):
            rows["flow"].append(f)
            rows["series_key"].append(f"GEO=2026-COM-{i % 3:05d}:SEX=_T:{f}")
            rows["obs_date"].append(f"20{10 + i}-12-31")
            rows["value"].append(v)
    tbl = pa.table({"flow": rows["flow"], "series_key": rows["series_key"],
                    "obs_date": pa.array(rows["obs_date"]).cast(pa.date32()),
                    "value": rows["value"]})
    pq.write_table(tbl, str(d / "DS_X.parquet"))
    monkeypatch.setenv("ECONDL_DATA", str(tmp_path))
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", str(tmp_path / "duck"))


def _streamed(tmp_path, sid):
    import core.derive_csv as dc
    out = tmp_path / "out.csv.gz"
    dc._series_csv_to_file_sorted(sid, str(out))
    with gzip.open(out, "rb") as fh:
        return fh.read()


def test_the_stream_carries_the_flow_predicate_and_matches_the_in_memory_bytes(tmp_path, monkeypatch):
    import core.derive_csv as dc
    _store(tmp_path, monkeypatch, ["DS_X"])
    sid = "insee_melodi:DS_X"
    assert _streamed(tmp_path, sid) == dc._series_csv_bytes(sid)


def test_the_flow_filter_is_applied_not_dropped(tmp_path, monkeypatch):
    """Rows of ANOTHER flow inside the same file must not reach the CSV. If the stream dropped the
    predicate (the R1131 hazard) it would emit 20 data lines here instead of 10."""
    import core.derive_csv as dc
    _store(tmp_path, monkeypatch, ["DS_X", "DS_OTHER"])
    sid = "insee_melodi:DS_X"
    body = _streamed(tmp_path, sid)
    assert body == dc._series_csv_bytes(sid)
    lines = body.decode().splitlines()
    assert len(lines) == 1 + 10
    assert all("DS_OTHER" not in ln for ln in lines)


def test_an_empty_series_key_is_written_and_sorted_as_the_in_memory_path_does(tmp_path, monkeypatch):
    """Three real flows (DD_CNA_AGREGATS, DD_CNA_PATRIMOINE, DD_CNA_PATRIMOINE_BRANCHES) hold a series
    whose series_key is ''. Python's csv writes it as an empty field and sorts it first; DuckDB's COPY
    quoted it ("") - the only difference found across all 139 catalogued flows."""
    import core.derive_csv as dc
    d = tmp_path / "insee_melodi"
    d.mkdir(parents=True)
    tbl = pa.table({"flow": ["DS_X"] * 4,
                    "series_key": ["B=1", "", "A=1", ""],
                    "obs_date": pa.array(["2024-12-31", "2025-12-31", "2024-12-31", "2024-12-31"]).cast(pa.date32()),
                    "value": [2.0, 12005.8, 1.5, 3903.3]})
    pq.write_table(tbl, str(d / "DS_X.parquet"))
    monkeypatch.setenv("ECONDL_DATA", str(tmp_path))
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", str(tmp_path / "duck"))
    sid = "insee_melodi:DS_X"
    body = _streamed(tmp_path, sid)
    assert body == dc._series_csv_bytes(sid)
    assert body.decode().splitlines()[1].startswith(",2024-12-31,")      # empty key, first, unquoted


def _one_file(tmp_path, monkeypatch, flow_values, name="DS_X", flow_col=True):
    d = tmp_path / "insee_melodi"
    d.mkdir(parents=True, exist_ok=True)
    cols = {"series_key": [f"K={i}" for i in range(len(flow_values))],
            "obs_date": pa.array(["2024-12-31"] * len(flow_values)).cast(pa.date32()),
            "value": [float(i) for i in range(len(flow_values))]}
    if flow_col:
        cols = {"flow": list(flow_values), **cols}
    pq.write_table(pa.table(cols), str(d / f"{name}.parquet"))
    monkeypatch.setenv("ECONDL_DATA", str(tmp_path))
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", str(tmp_path / "duck"))


def test_a_misnamed_file_is_refused_not_served_header_only(tmp_path, monkeypatch):
    """DS_X.parquet whose rows all say flow='DS_Y': the in-memory path raises 'zero rows matched'.
    The stream must refuse too. COPY ... HEADER writes a header even for zero rows, so a size test
    alone let a header-only CSV through to the PUT (review F1)."""
    import core.derive_csv as dc
    from econdl import _resolve
    _one_file(tmp_path, monkeypatch, ["DS_Y", "DS_Y"])
    sid = "insee_melodi:DS_X"
    with pytest.raises(Exception, match="zero rows"):
        dc._series_csv_bytes(sid)                                     # the oracle refuses
    with pytest.raises(dc._ResolveZero):
        _streamed(tmp_path, sid)


def test_the_flow_comparison_is_case_sensitive(tmp_path, monkeypatch):
    """flow='ds_x' does not satisfy the resolver's `flow == "DS_X"`; the stream must not match it
    case-insensitively (a lower() comparison would serve these rows)."""
    import core.derive_csv as dc
    _one_file(tmp_path, monkeypatch, ["ds_x"])
    with pytest.raises(dc._ResolveZero):
        _streamed(tmp_path, "insee_melodi:DS_X")


def test_the_whole_file_branch_writes_an_empty_key_as_the_in_memory_path_does(tmp_path, monkeypatch):
    """No flow column: the resolver returns is_valid(series_key) - the eurostat-shaped branch. It had the
    same '' vs \"\" divergence; both branches now write and sort the empty key like the in-memory path."""
    import core.derive_csv as dc
    d = tmp_path / "insee_melodi"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["B=1", "", "A=1"],
                             "obs_date": pa.array(["2024-12-31"] * 3).cast(pa.date32()),
                             "value": [2.0, 1.0, 3.0]}), str(d / "DS_W.parquet"))
    monkeypatch.setenv("ECONDL_DATA", str(tmp_path))
    monkeypatch.setenv("ECONDL_DUCKDB_TMP", str(tmp_path / "duck"))
    sid = "insee_melodi:DS_W"
    body = _streamed(tmp_path, sid)
    assert body == dc._series_csv_bytes(sid)
    assert body.decode().splitlines()[1] == ",2024-12-31,1.0"


def test_negative_control_a_flow_predicate_for_a_different_id_is_refused(monkeypatch):
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import core.derive_csv as dc
    from econdl import _resolve
    other = _resolve.Resolution("insee_melodi:DS_X", "insee_melodi", "x.parquet", "series_key",
                                pc.equal(ds.field("flow"), "DS_OTHER"))
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: other)
    with pytest.raises(ValueError, match="selects a SUBSET"):
        dc._series_csv_to_file_sorted("insee_melodi:DS_X", "out.csv.gz")


def test_negative_control_an_id_that_is_not_a_plain_code_never_reaches_the_sql(monkeypatch):
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import core.derive_csv as dc
    from econdl import _resolve
    bad = "DS_X' OR '1'='1"
    res = _resolve.Resolution(f"insee_melodi:{bad}", "insee_melodi", "x.parquet", "series_key",
                              pc.equal(ds.field("flow"), bad))
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: res)
    with pytest.raises(ValueError, match="selects a SUBSET"):
        dc._series_csv_to_file_sorted(f"insee_melodi:{bad}", "out.csv.gz")


def test_negative_control_an_equality_on_another_column_is_still_refused(monkeypatch):
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import core.derive_csv as dc
    from econdl import _resolve
    res = _resolve.Resolution("insee_melodi:DS_X", "insee_melodi", "x.parquet", "series_key",
                              pc.equal(ds.field("series_key"), "DS_X"))
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: res)
    with pytest.raises(ValueError, match="selects a SUBSET"):
        dc._series_csv_to_file_sorted("insee_melodi:DS_X", "out.csv.gz")
