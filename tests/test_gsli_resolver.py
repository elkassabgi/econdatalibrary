'''imf_gsli_direct: COUNTRY.FREQ mid-key at positions 3-4 of 11, part count pinned.'''
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
    d = tmp_path / "imf_gsli_direct"
    d.mkdir()
    keys = [
        "GS_LI:Y15T64._T.USA.A._T._Z._Z._Z._Z._T.EMP_RT",   # IN table USA.A
        "GS_LI:Y15T64._T.USA.A._T._Z._Z._Z._Z._T.LF_RT",    # in
        "GS_LI:USA.A.ALB.A._T._Z._Z._Z._Z._T.EMP_RT",       # USA at pos 1, table ALB - out
        "GS_LI:Y15T64._T.USA.Q._T._Z._Z._Z._Z._T.EMP_RT",   # wrong freq - out
        "GS_LI:Y15T64._T.USA.A._T._Z._Z._Z._Z._T.EMP_RT.X", # 12 parts - out
    ]
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array([dt.date(2023, 12, 31)] * len(keys), pa.date32()),
        "value": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], pa.float64()),
    })
    pq.write_table(tbl, str(d / "imf_gsli_direct.parquet"))
    return tmp_path


def test_mid_key_and_part_count(tmp_path):
    from econdl import _resolve as R
    root = _store(tmp_path)
    res = R.resolve("imf_gsli_direct:GS_LI:USA.A", root=str(root))
    t = R.read_native(res)
    got = sorted(t.column("series_key").to_pylist())
    assert got == ["GS_LI:Y15T64._T.USA.A._T._Z._Z._Z._Z._T.EMP_RT",
                   "GS_LI:Y15T64._T.USA.A._T._Z._Z._Z._Z._T.LF_RT"], got
