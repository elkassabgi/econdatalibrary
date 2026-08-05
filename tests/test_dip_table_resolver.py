"""imf_dip_direct's mid-key table resolver: position-exact, immune to window misalignment.

Same trap class as imf_pip_direct, one position over: counterpart-country codes (position 1)
share the COUNTRY vocabulary (position 2), and the key has exactly 5 parts with NO trailing
dim after INDICATOR. The fixture plants USA-as-counterpart in the country window and asserts
only the true-alignment rows resolve — including a key where a match would need a 6th part.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))


def _store(tmp_path):
    d = tmp_path / "imf_dip_direct"
    d.mkdir()
    keys = [
        "DIP:ABW.USA.SCC.A.IDI_TOT",      # IN table USA.A.IDI_TOT
        "DIP:AFG.USA.SNC.A.IDI_TOT",      # in
        "DIP:USA.ABW.SCC.A.IDI_TOT",      # USA as COUNTERPART, table is ABW — out
        "DIP:ABW.USA.SCC.Q.IDI_TOT",      # wrong freq — out
        "DIP:ABW.USA.SCC.A.ODI_TOT",      # wrong indicator — out
    ]
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array([dt.date(2023, 12, 31)] * len(keys), pa.date32()),
        "value": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], pa.float64()),
    })
    pq.write_table(tbl, str(d / "imf_dip_direct.parquet"))
    return tmp_path


def test_mid_key_predicate_rejects_counterpart_misalignment(tmp_path):
    from econdl import _resolve as R
    root = _store(tmp_path)
    res = R.resolve("imf_dip_direct:DIP:USA.A.IDI_TOT", root=str(root))
    t = R.read_native(res)
    got = sorted(t.column("series_key").to_pylist())
    assert got == ["DIP:ABW.USA.SCC.A.IDI_TOT",
                   "DIP:AFG.USA.SNC.A.IDI_TOT"], got
    assert sorted(t.column("value").to_pylist()) == [pytest.approx(1.0), pytest.approx(2.0)]


def test_indicator_is_terminal_no_sixth_part(tmp_path):
    """The regex must END at INDICATOR — a 6-part key whose first five parts align
    would be a DIFFERENT dataset shape and must not resolve into the table."""
    from econdl import _resolve as R
    root = _store(tmp_path)
    d = root / "imf_dip_direct"
    extra = pa.table({
        "series_key": pa.array(["DIP:ABW.USA.SCC.A.IDI_TOT.S1"], pa.string()),
        "obs_date": pa.array([dt.date(2023, 12, 31)], pa.date32()),
        "value": pa.array([99.0], pa.float64()),
    })
    base = pq.read_table(str(d / "imf_dip_direct.parquet"))
    pq.write_table(pa.concat_tables([base, extra]), str(d / "imf_dip_direct.parquet"))
    res = R.resolve("imf_dip_direct:DIP:USA.A.IDI_TOT", root=str(root))
    t = R.read_native(res)
    assert 99.0 not in t.column("value").to_pylist(), \
        "6-part key leaked through — the predicate is not anchored at the key end"


def test_missing_store_refuses(tmp_path):
    from econdl import _resolve as R
    (tmp_path / "imf_dip_direct").mkdir()
    with pytest.raises(R.ResolveError):
        R.resolve("imf_dip_direct:DIP:USA.A.IDI_TOT", root=str(tmp_path))
