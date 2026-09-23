"""ssb on the CI runner: the changed set derives from the one group file the run wrote (2026-09-23).

The runner holds ONLY the group files written that run. Before this change the orchestrator derived
every table of every visited group from the seeded cursors, and a table whose group was not written
failed "zero rows matched". Real ssb.update(), real merge, real orchestrate._derive_changed_csvs and
real derive.derive_and_put; only PxWeb and the R2 PUT are faked. From the review of this branch.
"""
from __future__ import annotations

import datetime as dt
import gzip
import os
import shutil
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import orchestrate  # noqa: E402
from updater.strategies.fetchers import ssb  # noqa: E402


class _Blob:
    def __init__(self):
        self.puts = {}

    def put_atomic(self, key, data):
        self.puts[key] = data
        return True


def _store(tmp_path):
    d = tmp_path / "data" / "clean_full" / "ssb"
    d.mkdir(parents=True)
    pq.write_table(pa.table({"series_key": ["SSB:AaOne:x=1", "SSB:AaTwo:x=1"],
                             "obs_date": pa.array([dt.date(2026, 1, 1)] * 2),
                             "value": [1.0, 5.0]}), d / "grp_Aa.parquet")
    pq.write_table(pa.table({"series_key": ["SSB:BbOne:x=1"],
                             "obs_date": pa.array([dt.date(2026, 1, 1)]), "value": [1.0]}),
                   d / "grp_Bb.parquet")
    return d


def _wire(monkeypatch, d, rows_for):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(ssb.config, "source_dir", lambda source: str(d))
    monkeypatch.setattr(ssb, "_load_catalog", lambda out_dir: [{"id": t} for t in ("AaOne", "AaTwo", "BbOne")])
    monkeypatch.setattr(ssb, "RATE", 0)
    meta = {"variables": [{"code": "Tid", "time": True, "values": ["2026M08"]},
                          {"code": "x", "values": ["1"]}]}
    monkeypatch.setattr(ssb, "_get_meta", lambda sess, tid: meta if tid in rows_for else None)
    monkeypatch.setattr(ssb, "_time_var", lambda variables: ("Tid", ["2026M08"]))
    monkeypatch.setattr(ssb, "_newer_codes", lambda vals, floor: ["2026M08"])
    monkeypatch.setattr(ssb, "_build_query", lambda variables, tc, newer: [{"code": "Tid"}])
    monkeypatch.setattr(ssb, "_post_data", lambda sess, tid, body: {"tid": tid})
    monkeypatch.setattr(ssb, "parse_jsonstat2",
                        lambda resp, tid, tc: [(f"SSB:{tid}:x=1", dt.date(2026, 8, 1), 2.0)])


def _catalog(tmp_path, monkeypatch):
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)",
                    [(f"ssb:SSB:{t}", "ssb") for t in ("AaOne", "AaTwo", "BbOne")])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))


def _runner(tmp_path, d, written):
    r = tmp_path / "runner" / "ssb"
    r.mkdir(parents=True)
    for fn in written:
        shutil.copy(d / fn, r / fn)
    return r.parent


def _derive(monkeypatch, runner_root, res):
    monkeypatch.setenv("ECONDL_DATA", str(runner_root))
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    b = _Blob()
    unit = types.SimpleNamespace(key="ssb/_all", source_id="ssb", unit_id="_all")
    return orchestrate._derive_changed_csvs(unit, res, b, store=None), b


def test_a_runner_holding_only_the_written_group_derives_the_changed_table(tmp_path, monkeypatch):
    d = _store(tmp_path)
    _wire(monkeypatch, d, rows_for={"AaOne"})
    _catalog(tmp_path, monkeypatch)
    res = ssb.update(None, None)
    assert res.changed_keys == {"SSB:AaOne": "2026-08-01"}
    (failed, note, deferred, _r), b = _derive(monkeypatch, _runner(tmp_path, d, ["grp_Aa.parquet"]), res)
    assert failed == [] and deferred == [] and not (note or "").startswith("csv_derive failed"), (failed, note)
    keys = list(b.puts)
    assert len(keys) == 1 and "AaOne" in keys[0], keys
    body = b.puts[keys[0]]
    try:
        body = gzip.decompress(body)
    except OSError:
        pass
    txt = body.decode("utf-8")
    assert "2026-01-01" in txt and "2026-08-01" in txt, txt   # the whole table, old row and new


def test_negative_control_the_cursor_path_fails_on_the_same_runner(tmp_path, monkeypatch):
    """The reading before this change (changed_keys None -> the seeded cursors) on the same runner
    must reproduce 'zero rows matched' for the visited group that was not written - or the test
    above could not fail."""
    d = _store(tmp_path)
    _wire(monkeypatch, d, rows_for={"AaOne"})
    _catalog(tmp_path, monkeypatch)
    res = ssb.update(None, None)
    res.changed_keys = None
    (failed, note, _d, reasons), _b = _derive(monkeypatch, _runner(tmp_path, d, ["grp_Aa.parquet"]), res)
    assert failed == ["ssb:SSB:BbOne"], (failed, note)
    assert "zero rows matched" in reasons["ssb:SSB:BbOne"], reasons
