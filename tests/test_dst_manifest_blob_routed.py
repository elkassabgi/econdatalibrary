"""dst's vintage manifest must live where the store lives — behind blob, never raw I/O.

The defect this pins (R355): _load_manifest used os.path.exists + open(), so under
backend=r2 every CI run saw "no manifest", cold-started, adopted the catalog's CURRENT
timestamps as baseline, and marked nothing due — the served store froze for four months
behind daily green no_change while the fetcher converged only on the workstation.

The property tested is the ROUTING itself: load and save must go through blob.read_bytes /
blob.write_bytes_atomic. On the old code both tests fail (blob is never called).
"""
from __future__ import annotations

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def test_load_reads_through_blob(monkeypatch, tmp_path):
    from updater.strategies.fetchers import dst as D
    from updater import blob, config
    monkeypatch.setattr(config, "source_dir", lambda s: str(tmp_path))
    calls = []
    payload = {"tables": {"AUS09": "2026-07-30T08:00:00"}, "catalog_token": "t"}

    def fake_read(path):
        calls.append(path)
        return json.dumps(payload).encode("utf-8")

    monkeypatch.setattr(blob, "read_bytes", fake_read)
    man = D._load_manifest()
    assert calls and calls[0].endswith(D.MANIFEST_NAME), \
        "manifest load did not go through blob.read_bytes"
    assert man["tables"] == payload["tables"]


def test_load_absent_manifest_is_cold_start(monkeypatch, tmp_path):
    from updater.strategies.fetchers import dst as D
    from updater import blob, config
    monkeypatch.setattr(config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(blob, "read_bytes", lambda path: None)
    assert D._load_manifest() == {"tables": {}}


def test_save_writes_through_blob(monkeypatch, tmp_path):
    from updater.strategies.fetchers import dst as D
    from updater import blob, config
    monkeypatch.setattr(config, "source_dir", lambda s: str(tmp_path))
    written = {}

    def fake_write(path, data):
        written[path] = data

    monkeypatch.setattr(blob, "write_bytes_atomic", fake_write)
    D._save_manifest({"tables": {"X": "1"}})
    assert written, "manifest save did not go through blob.write_bytes_atomic"
    (path, data), = written.items()
    assert path.endswith(D.MANIFEST_NAME)
    assert json.loads(data.decode("utf-8")) == {"tables": {"X": "1"}}
