"""abs: merge-measured changed keys in the catalogue's '<FLOW>:<key>' form, and `catalog_scope: subset`
(2026-09-23).

abs stores bare keys per flow file ('1.10001.10.50.Q' in CPI.parquet); its 18 catalogue ids carry the
flow ('abs:CPI:1.10001.10.50.Q'). The bare cursor keys could never map, so every run read "csv coherence
unmet: 50000 changed series_keys have no catalog mapping" and demoted to partial - and with a
cap-saturated (truncated) cursor set, the subset exception was refused too. The real update(), merge and
orchestrator mapping run; ABS is faked.
"""
from __future__ import annotations

import datetime as dt
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tests.test_abs_cycle import _wire  # noqa: E402
from updater import orchestrate  # noqa: E402
from updater.strategies.fetchers import abs as A  # noqa: E402


def test_changed_keys_are_merge_measured_in_the_catalogue_form(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=99)
    res = A.update(None, None)
    assert res.changed_keys == {"AAA:AAA.k": "2026-08-01", "BBB:BBB.k": "2026-08-01",
                                "CCC:CCC.k": "2026-08-01"}, res.changed_keys


def test_an_identical_refetch_reports_nothing_changed(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=99)
    A.update(None, None)                                  # lands the 2026-08-01 rows, closes the cycle
    _wire(monkeypatch, tmp_path, allow=99)
    res = A.update(None, None)                            # same rows again
    assert res.changed_keys == {}, "{} is the honest 'nothing to derive', not None"


def test_a_pass_over_the_changed_keys_cap_reports_none_and_says_so(monkeypatch, tmp_path, capsys):
    """Review R1115: the run-wide dict was unbounded; census flows re-fetch up to 24.4M rows."""
    monkeypatch.setattr(A.merge, "CHANGED_KEYS_CAP", 2)
    _wire(monkeypatch, tmp_path, allow=99)
    res = A.update(None, None)                            # 3 flows, one changed key each
    assert res.changed_keys is None, res.changed_keys
    assert "falls back to the cursor path" in capsys.readouterr().out
    assert res.series_cursors, "the cursor path still has its cursors"


def test_negative_control_at_the_cap_the_set_is_still_complete(monkeypatch, tmp_path):
    monkeypatch.setattr(A.merge, "CHANGED_KEYS_CAP", 3)
    _wire(monkeypatch, tmp_path, allow=99)
    assert len(A.update(None, None).changed_keys) == 3


def _catalog(tmp_path, monkeypatch, ids):
    # abs runs in the CLOUD: under the r2 backend _catalog_ids_for returns (exact, unmapped); the
    # local backend's derive-all-small-sources fallback never runs there.
    monkeypatch.setattr(orchestrate.config, "BACKEND", "r2")
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(i, "abs") for i in ids])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))


def test_the_flow_form_maps_to_the_catalogued_id(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, ["abs:CPI:1.10001.10.50.Q"])
    ids, unmapped = orchestrate._catalog_ids_for("abs", ["CPI:1.10001.10.50.Q", "XYZ:1.2.3"])
    assert ids == ["abs:CPI:1.10001.10.50.Q"] and list(unmapped) == ["XYZ:1.2.3"], (ids, unmapped)
    ids, unmapped = orchestrate._catalog_ids_for("abs", ["1.10001.10.50.Q"])
    assert ids == [], "negative control: the bare store key - what the cursors carried - never maps"


def test_the_registry_declares_abs_a_catalogue_subset():
    orchestrate._REG_ENTRIES = None
    assert orchestrate._catalog_scope("abs") == "subset"


def test_a_pass_moving_only_uncatalogued_flows_is_coverage_not_a_demotion(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, ["abs:CPI:1.10001.10.50.Q"])
    orchestrate._REG_ENTRIES = None
    unit = types.SimpleNamespace(key="abs/_all", source_id="abs", unit_id="_all")
    res = types.SimpleNamespace(obs=10, series_cursors={f"k{i}": "2026-08-01" for i in range(60000)},
                                changed_keys={f"C21_G01:k{i}": "2026-08-01" for i in range(60000)},
                                new_vintage="v", status="ok")
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, None, store=None)
    assert failed == [] and note.startswith("csv coverage note:"), note


def test_a_pass_moving_a_catalogued_and_uncatalogued_flows_derives_the_catalogued_id(tmp_path, monkeypatch):
    """Review R1115 gap: catalogued and uncatalogued keys changing in one pass."""
    _catalog(tmp_path, monkeypatch, ["abs:CPI:1.10001.10.50.Q"])
    orchestrate._REG_ENTRIES = None
    asked = []
    from updater import derive
    monkeypatch.setattr(derive, "derive_and_put", lambda ids, blob, **k: asked.extend(ids) or {})
    unit = types.SimpleNamespace(key="abs/_all", source_id="abs", unit_id="_all")
    res = types.SimpleNamespace(obs=10, series_cursors={}, new_vintage="v", status="ok",
                                changed_keys={"CPI:1.10001.10.50.Q": "2026-08-01",
                                              **{f"C21_G01:k{i}": "2026-08-01" for i in range(20)}})
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, object(), store=None)
    assert asked == ["abs:CPI:1.10001.10.50.Q"] and failed == [], (asked, failed)
    assert note is None or note.startswith("csv coverage note:"), note


def test_negative_control_the_same_pass_demotes_without_the_declaration(tmp_path, monkeypatch):
    _catalog(tmp_path, monkeypatch, ["abs:CPI:1.10001.10.50.Q"])
    monkeypatch.setattr(orchestrate, "_catalog_scope", lambda source_id: "full")
    unit = types.SimpleNamespace(key="abs/_all", source_id="abs", unit_id="_all")
    res = types.SimpleNamespace(obs=10, series_cursors={}, new_vintage="v", status="ok",
                                changed_keys={f"C21_G01:k{i}": "2026-08-01" for i in range(10)})
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, None, store=None)
    assert note.startswith("csv coherence unmet:"), note
