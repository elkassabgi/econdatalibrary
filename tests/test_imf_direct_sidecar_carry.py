"""The dims sidecar must survive the staging boundary (the _staging_ strand class).

The defect: jobs/ingest_imf_direct.py records the key order at <out_path>.dims.json, where
out_path is the STAGING parquet the strategy hands it. The strategy publishes rows via
merge into the canonical path and deletes the staging parquet — the sidecar stayed under
its _staging_ name, unreadable to every title builder (imf_sdg_direct exposed it; DIP,
IMTS, PIP and NA_MAIN all carried the same strand on R2).
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def test_carry_moves_staged_sidecar(monkeypatch):
    from updater.strategies.fetchers import _imf_direct as M
    from updater import blob
    store = {"/out/_staging_x.parquet.dims.json": b'{"key_dims": ["A"]}'}
    written = {}
    monkeypatch.setattr(blob, "read_bytes", lambda p: store.get(p.replace("\\", "/")))
    monkeypatch.setattr(blob, "write_bytes_atomic",
                        lambda p, d: written.__setitem__(p.replace("\\", "/"), d))
    assert M._carry_dims_sidecar("/out/_staging_x.parquet", "/out/x.parquet") is True
    assert written == {"/out/x.parquet.dims.json": b'{"key_dims": ["A"]}'}


def test_carry_absent_sidecar_is_quiet(monkeypatch):
    from updater.strategies.fetchers import _imf_direct as M
    from updater import blob
    monkeypatch.setattr(blob, "read_bytes", lambda p: None)
    called = []
    monkeypatch.setattr(blob, "write_bytes_atomic", lambda p, d: called.append(p))
    assert M._carry_dims_sidecar("/out/_staging_x.parquet", "/out/x.parquet") is False
    assert not called
