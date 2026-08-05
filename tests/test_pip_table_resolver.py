"""imf_pip_direct's mid-key table resolver: position-exact, immune to window misalignment.

The trap this exists for: counterpart-country codes (position 2) share the COUNTRY vocabulary
(position 4). A substring match on `.USA.A.<IND>.` could align USA-as-counterpart into the
country slot. The fixture plants exactly that shape and asserts only the true-alignment rows
resolve.
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
    d = tmp_path / "imf_pip_direct"
    d.mkdir()
    keys = [
        "PIP:A.ABW.S1.USA.A.P_TOT_USD.S121",    # IN table USA.A.P_TOT_USD
        "PIP:L.AFG.S1.USA.A.P_TOT_USD.S1",      # in
        "PIP:A.USA.S1.ABW.A.P_TOT_USD.S121",    # USA as COUNTERPART, table is ABW — out
        "PIP:A.ABW.S1.USA.Q.P_TOT_USD.S121",    # wrong freq — out
        "PIP:A.ABW.S1.USA.A.P_EQ_USD.S121",     # wrong indicator — out
    ]
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array([dt.date(2023, 12, 31)] * len(keys), pa.date32()),
        "value": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], pa.float64()),
    })
    pq.write_table(tbl, str(d / "imf_pip_direct.parquet"))
    return tmp_path


def test_mid_key_predicate_rejects_counterpart_misalignment(tmp_path):
    from econdl import _resolve as R
    root = _store(tmp_path)
    res = R.resolve("imf_pip_direct:PIP:USA.A.P_TOT_USD", root=str(root))
    t = R.read_native(res)
    got = sorted(t.column("series_key").to_pylist())
    assert got == ["PIP:A.ABW.S1.USA.A.P_TOT_USD.S121",
                   "PIP:L.AFG.S1.USA.A.P_TOT_USD.S1"], got
    assert sorted(t.column("value").to_pylist()) == [pytest.approx(1.0), pytest.approx(2.0)]


def test_missing_store_refuses(tmp_path):
    from econdl import _resolve as R
    (tmp_path / "imf_pip_direct").mkdir()
    with pytest.raises(R.ResolveError):
        R.resolve("imf_pip_direct:PIP:USA.A.P_TOT_USD", root=str(tmp_path))
