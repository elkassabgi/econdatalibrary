"""stat_estonia: the CSV phase's changed set is the tables this pass MERGED, not every stored table (2026-09-23).

stat_estonia seeds a cursor for every stored table of every subject it visits, so a frozen table cannot
hide behind the subject-level max. The orchestrator mapped all of those cursors to catalogue ids, and
under the r2 backend a runner holds only the subject files the pass WROTE - every other table's derive
failed with ResolveError ("csv_derive failed 173/911 series", 2026-09-18; 2,089 stat_estonia ids stuck
in csv_retry_queue). R151 recorded the mechanism; orchestrate._catalog_ids_for assumes a mapped id's file
was written this run. The real update(), merge and orchestrator mapping run; PxWeb is faked.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate  # noqa: E402
from updater.strategies.fetchers import stat_estonia as S  # noqa: E402

D0, D1 = dt.date(2024, 12, 31), dt.date(2025, 12, 31)
TABLES = ["majandus/palk/PA001.PX", "majandus/palk/PA002.PX", "rahvastik/RV01.PX"]


def _store(tmp_path):
    """Two subjects on disk; each table has a 2024 value."""
    by_subj = {}
    for p in TABLES:
        by_subj.setdefault(p.split("/")[0], []).append(p)
    for subj, paths in by_subj.items():
        keys = [f"{S._table_prefix(p)}:Sugu=1" for p in paths]
        pq.write_table(pa.table({"series_key": keys, "obs_date": pa.array([D0] * len(keys)),
                                 "value": [1.0] * len(keys)}), str(tmp_path / f"{subj}.parquet"))
    (tmp_path / "_catalog.json").write_text(json.dumps([{"path": p} for p in TABLES]), encoding="utf-8")


def _run(tmp_path, monkeypatch, new_for):
    """new_for: the table paths PxWeb has a 2025 value for this pass."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(S.config, "source_dir", lambda s: str(tmp_path))
    monkeypatch.setattr(S.time, "sleep", lambda s: None)
    monkeypatch.setattr(S, "_ingester", lambda: types.SimpleNamespace(
        BASE="https://x", parse_jsonstat2=lambda resp, prefix, tcode: resp["rows"]))
    monkeypatch.setattr(S, "_get_meta", lambda sess, url: {"variables": [{"code": "Aasta"}]})
    monkeypatch.setattr(S, "_build_query", lambda ing, variables, stored_max: ([{"q": 1}], "Aasta", 1))

    def _post(sess, url, body):
        path = url.split("https://x/", 1)[1]
        rows = [(f"{S._table_prefix(path)}:Sugu=1", D1, 2.0)] if path in new_for else []
        if path in new_for and isinstance(new_for, dict):
            rows += [(f"{S._table_prefix(path)}:Sugu=2", d, 3.0) for d in new_for[path]]
        return {"rows": rows}
    monkeypatch.setattr(S, "_post_data", _post)
    return S.update(types.SimpleNamespace(config={}, key="stat_estonia/_all"), None)


def test_the_changed_set_is_the_merged_tables_only(tmp_path, monkeypatch):
    _store(tmp_path)
    res = _run(tmp_path, monkeypatch, {"majandus/palk/PA002.PX"})
    assert res.changed_keys == {"EE:majandus:palk:PA002.PX": "2025-12-31"}, res.changed_keys
    assert len(res.series_cursors) == 3, "every stored table still reports its freshness cursor"


def test_tables_in_two_subjects_are_both_reported_and_the_quiet_one_is_not(tmp_path, monkeypatch):
    _store(tmp_path)
    res = _run(tmp_path, monkeypatch, {"majandus/palk/PA001.PX", "rahvastik/RV01.PX"})
    assert res.changed_keys == {"EE:majandus:palk:PA001.PX": "2025-12-31",
                                "EE:rahvastik:RV01.PX": "2025-12-31"}, res.changed_keys


def test_a_table_reports_its_newest_changed_date(tmp_path, monkeypatch):
    _store(tmp_path)
    res = _run(tmp_path, monkeypatch, {"rahvastik/RV01.PX": [dt.date(2026, 6, 1)]})
    assert res.changed_keys == {"EE:rahvastik:RV01.PX": "2026-06-01"}, res.changed_keys


def _catalog(tmp_path, monkeypatch):
    monkeypatch.setattr(orchestrate.config, "BACKEND", "r2")
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)",
                    [(f"stat_estonia:{S._table_prefix(t)}", "stat_estonia") for t in TABLES])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))


def test_a_pass_changing_only_an_uncatalogued_table_is_coverage_not_a_demotion(tmp_path, monkeypatch):
    """R1129: 1,538 of PxWeb's 4,978 tables are uncatalogued (no stored rows yet). A pass that lands
    rows only there maps to zero ids; with catalog_scope: subset that is a coverage note."""
    _store(tmp_path)
    res = _run(tmp_path, monkeypatch, {"rahvastik/RV01.PX"})
    monkeypatch.setattr(orchestrate.config, "BACKEND", "r2")
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)",           # RV01 NOT catalogued
                    [(f"stat_estonia:{S._table_prefix(t)}", "stat_estonia") for t in TABLES[:2]])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    orchestrate._REG_ENTRIES = None
    unit = types.SimpleNamespace(key="stat_estonia/_all", source_id="stat_estonia", unit_id="_all")
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, object(), store=None)
    assert failed == [] and note.startswith("csv coverage note:"), note


def test_a_key_without_a_px_leaf_is_kept_whole_not_dropped(tmp_path, monkeypatch):
    _store(tmp_path)
    monkeypatch.setattr(S.merge, "merge_and_write",
                        lambda path, tbl, **kw: (1, "2025-12-31", {"EE:odd:key": "2025-12-31"}))
    res = _run(tmp_path, monkeypatch, {"rahvastik/RV01.PX"})
    assert res.changed_keys == {"EE:odd:key": "2025-12-31"}, res.changed_keys


def test_the_registry_declares_stat_estonia_a_catalogue_subset():
    orchestrate._REG_ENTRIES = None
    assert orchestrate._catalog_scope("stat_estonia") == "subset"


def test_the_csv_phase_derives_only_the_changed_table(tmp_path, monkeypatch):
    """Planted positive AND the negative control: with the seeded cursors (the old reading) all three
    ids would be asked for, two of them from files this pass never wrote."""
    _store(tmp_path)
    res = _run(tmp_path, monkeypatch, {"majandus/palk/PA002.PX"})
    _catalog(tmp_path, monkeypatch)
    orchestrate._REG_ENTRIES = None
    asked = []
    from updater import derive
    monkeypatch.setattr(derive, "derive_and_put", lambda ids, blob, **k: asked.extend(ids) or {})
    unit = types.SimpleNamespace(key="stat_estonia/_all", source_id="stat_estonia", unit_id="_all")
    orchestrate._derive_changed_csvs(unit, res, object(), store=None)
    assert asked == ["stat_estonia:EE:majandus:palk:PA002.PX"], asked
    asked.clear()
    old = types.SimpleNamespace(**{**vars(res), "changed_keys": None})
    orchestrate._derive_changed_csvs(unit, old, object(), store=None)
    assert len(asked) == 3, "the old reading: every seeded cursor, rahvastik included"
