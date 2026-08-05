'''imf_qgfs_direct: COUNTRY.FREQ mid-key at positions 2-3 of 7, part count pinned.'''
from __future__ import annotations

import datetime as dt
import os
import sys

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))


def _store(tmp_path):
    d = tmp_path / "imf_qgfs_direct"
    d.mkdir()
    keys = [
        "QGFS:SSUC.USA.Q.true.G26_TCB_XDC.GFSM01.S1311B",    # IN table USA.Q
        "QGFS:BS.USA.Q.true.EO_TCB_XDC.GFSM01.S1321",        # in (other ACCOUNTS)
        "QGFS:USA.ALB.Q.true.G26_TCB_XDC.GFSM01.S1311B",     # USA at pos 1, table ALB - out
        "QGFS:SSUC.USA.A.true.G26_TCB_XDC.GFSM01.S1311B",    # wrong freq - out
        "QGFS:SSUC.USA.Q.true.G26_TCB_XDC.GFSM01.S1311B.X",  # 8 parts - out
    ]
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array([dt.date(2023, 12, 31)] * len(keys), pa.date32()),
        "value": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], pa.float64()),
    })
    pq.write_table(tbl, str(d / "imf_qgfs_direct.parquet"))
    return tmp_path


def test_mid_key_and_part_count(tmp_path):
    from econdl import _resolve as R
    root = _store(tmp_path)
    res = R.resolve("imf_qgfs_direct:QGFS:USA.Q", root=str(root))
    t = R.read_native(res)
    got = sorted(t.column("series_key").to_pylist())
    assert got == ["QGFS:BS.USA.Q.true.EO_TCB_XDC.GFSM01.S1321",
                   "QGFS:SSUC.USA.Q.true.G26_TCB_XDC.GFSM01.S1311B"], got
