"""imf_mfs*_direct family resolver: prefix-exact COUNTRY.FREQ tables.

The traps this exists for: (1) a one-letter FREQ code must not prefix-match a longer code
in the same slot (A vs AB) — the trailing dot in the predicate stops it; (2) a COUNTRY code
must anchor at the string start so a counterpart-style code deeper in the key can never
align (the DIP/PIP lesson, though this family keeps its table dims in front).
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
    d = tmp_path / "imf_mfsdc_direct"
    d.mkdir()
    keys = [
        "MFS_DC:AFG.A.true.DCORP_A_ACO_NRES.XDC",   # IN table AFG.A
        "MFS_DC:AFG.A.true.DCORP_L_BM.XDC",         # in
        "MFS_DC:AFG.AB.true.DCORP_A_ACO_NRES.XDC",  # freq 'AB' — the A-prefix trap, out
        "MFS_DC:AFG.M.true.DCORP_A_ACO_NRES.XDC",   # wrong freq — out
        "MFS_DC:ALB.A.true.DCORP_A_ACO_NRES.XDC",   # wrong country — out
    ]
    tbl = pa.table({
        "series_key": pa.array(keys, pa.string()),
        "obs_date": pa.array([dt.date(2020, 12, 31)] * len(keys), pa.date32()),
        "value": pa.array([1.0, 2.0, 3.0, 4.0, 5.0], pa.float64()),
    })
    pq.write_table(tbl, str(d / "imf_mfsdc_direct.parquet"))
    return tmp_path


def test_prefix_predicate_is_position_exact(tmp_path):
    from econdl import _resolve as R
    root = _store(tmp_path)
    res = R.resolve("imf_mfsdc_direct:MFS_DC:AFG.A", root=str(root))
    t = R.read_native(res)
    got = sorted(t.column("series_key").to_pylist())
    assert got == ["MFS_DC:AFG.A.true.DCORP_A_ACO_NRES.XDC",
                   "MFS_DC:AFG.A.true.DCORP_L_BM.XDC"], got
    assert sorted(t.column("value").to_pylist()) == [pytest.approx(1.0), pytest.approx(2.0)]


def test_missing_store_refuses(tmp_path):
    from econdl import _resolve as R
    (tmp_path / "imf_mfsdc_direct").mkdir()
    with pytest.raises(R.ResolveError):
        R.resolve("imf_mfsdc_direct:MFS_DC:AFG.A", root=str(tmp_path))
