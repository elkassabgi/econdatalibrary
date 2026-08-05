"""imf_imts_direct's table-grain resolver: exact membership, byte-contract round trip.

The predicate is prefix + suffix. The failure mode that matters: a table id matching series
from the WRONG table (e.g. USA.M.MG_CIF_USD pulling USA.M.MG_FOB_USD rows via a sloppy
substring, or matching a COUNTERPART whose code happens to equal the country). Fixture builds
exactly those traps and asserts the resolver takes only its own rows.
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
    d = tmp_path / "imf_imts_direct"
    d.mkdir()
    keys = [
        "IMTS:USA.ABW.M.MG_CIF_USD.IMTS",     # in table USA.M.MG_CIF_USD
        "IMTS:USA.AFG.M.MG_CIF_USD.IMTS",     # in
        "IMTS:USA.ABW.M.MG_FOB_USD.IMTS",     # WRONG indicator — out
        "IMTS:USA.ABW.A.MG_CIF_USD.IMTS",     # WRONG freq — out
        "IMTS:ABW.USA.M.MG_CIF_USD.IMTS",     # WRONG country (USA as counterpart) — out
    ]
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array([dt.date(2024, 1, 31)] * len(keys), pa.date32()),
        "value": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], pa.float64()),
    })
    pq.write_table(tbl, str(d / "imf_imts_direct.parquet"))
    return tmp_path


def test_table_predicate_takes_only_its_own_rows(tmp_path):
    from econdl import _resolve as R
    root = _store(tmp_path)
    res = R.resolve("imf_imts_direct:IMTS:USA.M.MG_CIF_USD", root=str(root))
    t = R.read_native(res)
    got = sorted(t.column("series_key").to_pylist())
    assert got == ["IMTS:USA.ABW.M.MG_CIF_USD.IMTS", "IMTS:USA.AFG.M.MG_CIF_USD.IMTS"], got
    assert sorted(t.column("value").to_pylist()) == [pytest.approx(1.0), pytest.approx(2.0)]


def test_missing_store_refuses_loudly(tmp_path):
    from econdl import _resolve as R
    (tmp_path / "imf_imts_direct").mkdir()
    with pytest.raises(R.ResolveError):
        R.resolve("imf_imts_direct:IMTS:USA.M.MG_CIF_USD", root=str(tmp_path))
