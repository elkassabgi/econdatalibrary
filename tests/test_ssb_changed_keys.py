"""ssb reports the MERGE-MEASURED changed set at table grain (2026-09-23).

Its seeded cursors named every table of every group it visited, so in CI the orchestrator tried to
derive tables whose group file was never written to the runner: 246 of 947 failed "zero rows matched
in 13 files", 2,088 ids queued, and ssb read partial on every run. The real update() and the real
merge run; the PxWeb calls are faked.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import orchestrate  # noqa: E402
from updater.strategies.fetchers import ssb  # noqa: E402


def _wire(monkeypatch, tmp_path, rows_for):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(ssb.config, "source_dir", lambda source: str(tmp_path))
    cat = [{"id": t} for t in ("AaOne", "AaTwo", "BbOne")]
    monkeypatch.setattr(ssb, "_load_catalog", lambda out_dir: cat)
    monkeypatch.setattr(ssb, "_group_of", lambda tid: tid[:2])
    for g in ("Aa", "Bb"):
        pq.write_table(pa.table({"series_key": [f"SSB:{g}One:x=1"],
                                 "obs_date": pa.array([dt.date(2026, 1, 1)]), "value": [1.0]}),
                       tmp_path / f"grp_{g}.parquet")
    monkeypatch.setattr(ssb, "_per_table_max", lambda path: {})
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


def test_only_tables_whose_served_values_changed_are_reported(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, rows_for={"AaOne"})
    res = ssb.update(None, None)
    assert res.changed_keys == {"SSB:AaOne": "2026-08-01"}, res.changed_keys


def test_the_table_grain_key_maps_to_the_catalogued_id(tmp_path, monkeypatch):
    import sqlite3
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [("ssb:SSB:AaOne", "ssb"), ("ssb:SSB:BbOne", "ssb")])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    ids, unmapped = orchestrate._catalog_ids_for("ssb", ["SSB:AaOne"])
    assert sorted(ids) == ["ssb:SSB:AaOne"] and not unmapped, (ids, unmapped)


def test_a_quiet_run_reports_nothing_changed(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, rows_for=set())
    res = ssb.update(None, None)
    assert res.changed_keys == {}, "{} is the honest 'nothing to derive', not None (the legacy path)"
