"""ilostat: the CSV phase's changed set at the catalogue's grain, and misses booked as debts (review R1131).

ilostat never recorded a success: every pass read "csv coherence unmet: 50000 changed series_keys have no
catalog mapping for ilostat" - its series cursors (store keys, capped at 50,000) cannot map to catalogue
ids that are at INDICATOR grain ('ilostat:<stem>', 'ilostat:<stem>#<part>', 80 legacy
'ilostat:<flow>:<c1>:<geo>'). The fetcher rewrites a whole indicator file, so the changed set is the
published stems. Hermetic: the ILO downloader is faked; a tmp store under the LOCAL backend.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import derive, orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.base import Result  # noqa: E402
from updater.strategies.fetchers import ilostat as I  # noqa: E402


def _wire(monkeypatch, tmp_path, toc, broken=()):
    """toc: [(id, last.update, n.records)]; `broken` ids parse to 0 rows although records exist."""
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(I.config, "source_dir", lambda s: str(tmp_path))
    rows = [{"id": i, "last.update": u, "n.records": n} for i, u, n in toc]
    monkeypatch.setattr(I.ig, "download_toc", lambda: None)
    monkeypatch.setattr(I.ig, "read_toc", lambda: rows)

    def _process(ds):
        iid = ds["id"]
        if iid in broken:
            return iid, 0, 0, 5, "empty", None, None, 0, 0
        pq.write_table(pa.table({"series_key": [f"{iid}|x"], "obs_date": pa.array(["2025-12-31"]).cast(pa.date32()),
                                 "value": [1.0]}), str(tmp_path / f"{iid}.parquet"))
        return iid, 1, 1, 1, "ok", "2025-12-31", "2025-12-31", 10, 0.1
    monkeypatch.setattr(I.ig, "process_one", _process)


def test_the_changed_set_is_the_published_indicators(tmp_path, monkeypatch):
    _wire(monkeypatch, tmp_path, [("EMP_A", "01/09/2026 10:00:00", 5), ("UNE_Q", "01/09/2026 10:00:00", 5)])
    res = I.update(None, None)
    assert res.changed_keys == {"EMP_A": "2025-12-31", "UNE_Q": "2025-12-31"}, res.changed_keys
    assert res.series_cursors, "freshness cursors stay for health"
    res = I.update(None, None)                                      # nothing moved upstream
    assert res.changed_keys == {}, res.changed_keys


def test_a_structural_indicator_does_not_lose_the_published_ones(tmp_path, monkeypatch):
    """R1131: finalize raised, the orchestrator skipped the CSV phase, and the published indicators'
    sidecar stamps had already advanced - their CSVs stayed stale for good."""
    _wire(monkeypatch, tmp_path, [("EMP_A", "u1", 5), ("BAD_A", "u1", 5)], broken={"BAD_A"})
    res = I.update(None, None)
    assert res.status == "partial" and res.changed_keys == {"EMP_A": "2025-12-31"}, (res.status, res.changed_keys)
    assert "BAD_A" in res.error
    assert res.series_cursors, "health's freshness cursors survive the structural catch too (R1137)"


def test_negative_control_nothing_published_still_raises(tmp_path, monkeypatch):
    _wire(monkeypatch, tmp_path, [("BAD_A", "u1", 5)], broken={"BAD_A"})
    with pytest.raises(I.DefinitiveError):
        I.update(None, None)


def test_the_split_map_is_brought_onto_the_machine_and_its_absence_is_loud(tmp_path, monkeypatch, capsys):
    _wire(monkeypatch, tmp_path, [("EMP_A", "u1", 5)])
    I.update(None, None)
    assert "_split_map.json is absent" in capsys.readouterr().out
    (tmp_path / I.SPLIT_MAP).write_text('{"EMP_TEMP": {"col": "classif1"}}', encoding="utf-8")
    monkeypatch.setattr(I.blob, "read_bytes", lambda p: b'{"X": 1}' if p.endswith(I.SPLIT_MAP) else None)
    monkeypatch.setattr(I.blob, "write_bytes_atomic", lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("the map must never be PUT back from a runner (R1137 RV2)")))
    I._fetch_split_map(str(tmp_path))
    assert (tmp_path / I.SPLIT_MAP).read_bytes() == b'{"X": 1}', "the store's copy, written locally"


# ---- the orchestrator's map ------------------------------------------------------------------------
IDS = ["ilostat:EMP_A", "ilostat:EMP_TEMP_Q#p1", "ilostat:EMP_TEMP_Q#p2",
       "ilostat:EMP:ECO_TOTAL:USA", "ilostat:EMP:ECO_TOTAL:FRA", "ilostat:EMPX:ECO:DEU",
       "ilostat:EMP:ECO_TOTAL"]                                     # 2 colons: not a legacy id


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    monkeypatch.setattr(orchestrate.config, "BACKEND", "r2")
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(i, "ilostat") for i in IDS])
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    orchestrate._REG_ENTRIES = None
    return p


def test_a_stem_maps_to_itself_its_parts_and_its_legacy_ids(catalog):
    ids, unmapped = orchestrate._catalog_ids_for("ilostat", ["EMP_A", "EMP_TEMP_Q", "NOPE_A"])
    assert sorted(ids) == sorted(["ilostat:EMP_A", "ilostat:EMP:ECO_TOTAL:USA", "ilostat:EMP:ECO_TOTAL:FRA",
                                  "ilostat:EMP_TEMP_Q#p1", "ilostat:EMP_TEMP_Q#p2"]), ids
    assert list(unmapped) == ["NOPE_A"]


def test_negative_control_a_quarterly_stem_claims_no_legacy_id(catalog):
    ids, _ = orchestrate._catalog_ids_for("ilostat", ["EMP_Q"])
    assert not any(i.count(":") == 3 for i in ids), ids


# ---- misses become debts ---------------------------------------------------------------------------
def _fake_derive(deferred=()):
    def f(ids, blob, **kw):
        d = [s for s in ids if s in set(deferred)]
        return {"put": len(ids) - len(d), "failed": list(d), "deferred": len(d), "deferred_ids": d,
                "failed_reasons": {}, "skipped_identical": 0, "deferred_large": {}}
    return f


def test_budget_deferred_ids_are_booked_as_desktop_debt_not_queued(tmp_path, catalog, monkeypatch):
    monkeypatch.setattr(derive, "derive_and_put", _fake_derive(deferred=["ilostat:EMP_A"]))
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    st = StateStore(path=str(tmp_path / "state.db"))
    unit = types.SimpleNamespace(key="ilostat/_all", source_id="ilostat", unit_id="_all")
    res = Result(status="partial", obs=10, changed_keys={"EMP_A": "2025-12-31"})
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, object(), st)
    assert failed == [] and deferred == [], (failed, deferred)
    rows = st.csv_desktop_owed("ilostat")
    assert [r["series_id"] for r in rows] == ["ilostat:EMP_A"] and "csv_misses" in rows[0]["reason"]


def test_the_registry_declares_ilostat_csv_misses_and_validates_the_value(monkeypatch):
    orchestrate._REG_ENTRIES = None
    assert orchestrate._csv_misses("ilostat") == "desktop_owed"
    assert orchestrate._csv_misses("abs") == ""
    assert orchestrate._catalog_scope("ilostat") == "subset", "declared explicitly (R1137)"
    bad = {"sources": [{"source_id": "zz", "strategy": "extend_by_date", "cadence": "daily",
                        "csv_misses": "desktop"}]}
    problems = registry.validate(bad) if hasattr(registry, "validate") else None
    if problems is not None:
        assert any("csv_misses" in p for p in problems), problems


# ---- the desktop map reaches the store ---------------------------------------------------------------
class _FakeR2:
    def __init__(self, have=None):
        self.objs = {} if have is None else {"clean_full/ilostat/_split_map.json": have}
        self.puts = 0

    def get(self, key):
        return self.objs.get(key)

    def put_atomic(self, key, body):
        self.puts += 1
        self.objs[key] = body


def _publish(tmp_path, monkeypatch, have, *args, local_map=b'{"EMP_TEMP_Q": {"col": "classif1"}}',
             part_stems=("EMP_TEMP_Q",)):
    from updater import blob
    sys.path.insert(0, os.path.join(ROOT, "tools"))
    import publish_ilostat_split_map as T
    local = tmp_path / "data" / "clean_full" / "ilostat" / "_split_map.json"
    local.parent.mkdir(parents=True)
    local.write_bytes(local_map)
    cat = tmp_path / "catalog.db"
    con = sqlite3.connect(cat)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(f"ilostat:{s}#p1", "ilostat") for s in part_stems]
                    + [("ilostat:EMP_A", "ilostat")])
    con.commit()
    con.close()
    fake = _FakeR2(have)
    monkeypatch.setattr(blob, "_r2_routed", lambda: fake)
    monkeypatch.setenv("AQUEDUCT_BACKEND", "local")
    return T.main(["--local", str(local), "--catalog", str(cat), *args]), fake


def test_a_map_missing_a_catalogued_part_stem_is_refused(tmp_path, monkeypatch):
    rc, fake = _publish(tmp_path, monkeypatch, None, "--apply", part_stems=("EMP_TEMP_Q", "UNE_X_Q"))
    assert rc == 2 and fake.puts == 0


def test_a_map_dropping_a_stem_r2_holds_is_refused_even_with_replace(tmp_path, monkeypatch):
    have = b'{"EMP_TEMP_Q": {"col": "classif1"}, "OLD_Q": {"col": "sex"}}'
    rc, fake = _publish(tmp_path, monkeypatch, have, "--apply", "--replace")
    assert rc == 2 and fake.puts == 0


def test_the_split_map_is_published_when_absent_and_read_back(tmp_path, monkeypatch):
    rc, fake = _publish(tmp_path, monkeypatch, None)
    assert rc == 0 and fake.puts == 0, "dry run by default"
    rc, fake = _publish(tmp_path / "b", monkeypatch, None, "--apply")
    assert rc == 0 and fake.puts == 1 and fake.objs["clean_full/ilostat/_split_map.json"].startswith(b'{"EMP_TEMP_Q"')


def test_a_different_map_on_the_store_is_not_overwritten_without_replace(tmp_path, monkeypatch):
    older = b'{"EMP_TEMP_Q": {"col": "sex"}}'                       # same stems, different cut
    rc, fake = _publish(tmp_path, monkeypatch, older, "--apply")
    assert rc == 2 and fake.puts == 0
    rc, fake = _publish(tmp_path / "b", monkeypatch, older, "--apply", "--replace")
    assert rc == 0 and fake.puts == 1


# ---- the file-grain stream refuses a subset predicate ------------------------------------------------
def test_the_file_grain_stream_refuses_a_resolver_that_selects_a_subset(monkeypatch):
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import core.derive_csv as dc
    from econdl import _resolve
    sub = _resolve.Resolution("ilostat:X#p1", "ilostat", "x.parquet", "series_key",
                              pc.equal(ds.field("classif1"), "A"))
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: sub)
    with pytest.raises(ValueError, match="selects a SUBSET"):
        dc._series_csv_to_file_sorted("ilostat:X#p1", "out.csv.gz")


def test_negative_control_the_whole_file_predicate_passes_the_guard(monkeypatch, tmp_path):
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import core.derive_csv as dc
    from econdl import _resolve
    p = tmp_path / "w.parquet"
    pq.write_table(pa.table({"series_key": ["k"], "obs_date": pa.array(["2024-01-01"]).cast(pa.date32()),
                             "value": [1.0]}), str(p))
    whole = _resolve.Resolution("eurostat:w", "eurostat", str(p), "series_key", pc.is_valid(ds.field("series_key")))
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: whole)
    assert dc._series_csv_to_file_sorted("eurostat:w", str(tmp_path / "w.csv.gz")) > 0


def test_a_whole_id_predicate_on_other_columns_is_refused_too(monkeypatch):
    """R1137 RV1: the guard must refuse ANY predicate but the whole-file one, not only '==' subsets. A
    whole ilostat/istat/statcan id resolves with is_valid(value) and is_valid(obs_date); the stream
    would serve its null-value rows."""
    import pyarrow.compute as pc
    import pyarrow.dataset as ds
    import core.derive_csv as dc
    from econdl import _resolve
    pred = pc.is_valid(ds.field("value")) & pc.is_valid(ds.field("obs_date"))
    whole_ish = _resolve.Resolution("ilostat:EMP_A", "ilostat", "x.parquet", "series_key", pred)
    monkeypatch.setattr(_resolve, "resolve", lambda sid, root=None: whole_ish)
    with pytest.raises(ValueError, match="selects a SUBSET"):
        dc._series_csv_to_file_sorted("ilostat:EMP_A", "out.csv.gz")


# ---- review R1137: failures and fence trips are per-id debts, and health names their reason ------
def test_failed_ids_are_booked_as_debts_not_queued_and_still_demote(tmp_path, catalog, monkeypatch):
    def _fake(ids, blob, **kw):
        return {"put": 0, "failed": ["ilostat:EMP_A"], "deferred": 0, "deferred_ids": [],
                "failed_reasons": {"ilostat:EMP_A": "ResolveError: never split"},
                "skipped_identical": 0, "deferred_large": {}}
    monkeypatch.setattr(derive, "derive_and_put", _fake)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    st = StateStore(path=str(tmp_path / "state.db"))
    unit = types.SimpleNamespace(key="ilostat/_all", source_id="ilostat", unit_id="_all")
    res = Result(status="partial", obs=10, changed_keys={"EMP_A": "2025-12-31"})
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(unit, res, object(), st)
    assert failed == [] and note and not note.startswith("csv coverage note:"), (failed, note)
    rows = st.csv_desktop_owed("ilostat")
    assert [r["series_id"] for r in rows] == ["ilostat:EMP_A"] and "never split" in rows[0]["reason"]
    # the second run: nothing changed upstream, the debt is still there for health to read
    res2 = Result(status="no_change", obs=10, changed_keys={})
    assert orchestrate._derive_changed_csvs(unit, res2, object(), st)[1] is None
    assert [r["series_id"] for r in st.csv_desktop_owed("ilostat")] == ["ilostat:EMP_A"]


def test_negative_control_another_source_still_queues_its_failures(tmp_path, catalog, monkeypatch):
    def _fake(ids, blob, **kw):
        return {"put": 0, "failed": list(ids), "deferred": 0, "deferred_ids": [], "failed_reasons": {},
                "skipped_identical": 0, "deferred_large": {}}
    monkeypatch.setattr(derive, "derive_and_put", _fake)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    monkeypatch.setattr(orchestrate, "_csv_misses", lambda s: "")
    st = StateStore(path=str(tmp_path / "state.db"))
    unit = types.SimpleNamespace(key="ilostat/_all", source_id="ilostat", unit_id="_all")
    res = Result(status="partial", obs=10, changed_keys={"EMP_A": "2025-12-31"})
    failed, _n, _d, _r = orchestrate._derive_changed_csvs(unit, res, object(), st)
    assert "ilostat:EMP_A" in failed and st.csv_desktop_owed("ilostat") == [], failed


def test_a_fence_trip_books_each_mapped_changed_id(tmp_path, catalog):
    st = StateStore(path=str(tmp_path / "state.db"))
    unit = types.SimpleNamespace(key="ilostat/_all", source_id="ilostat", unit_id="_all")
    res = Result(status="partial", obs=10, changed_keys={"EMP_A": "2025-12-31"})
    note = orchestrate._book_fence_trip(unit, res, st, 12.0)
    assert note.startswith("csv coverage note:") and "12-min fence" in note and "; " not in note, note
    got = sorted(r["series_id"] for r in st.csv_desktop_owed("ilostat"))
    assert got == sorted(["ilostat:EMP_A", "ilostat:EMP:ECO_TOTAL:USA", "ilostat:EMP:ECO_TOTAL:FRA"]), got
    assert "fence" in st.csv_desktop_owed("ilostat")[0]["reason"]
    assert st.full_rederives_owed() == [], "never the bulk-tool debt (R1137)"


def test_a_fence_trip_that_cannot_book_is_a_failure_and_other_sources_keep_their_note(tmp_path, catalog):
    unit = types.SimpleNamespace(key="ilostat/_all", source_id="ilostat", unit_id="_all")
    res = Result(status="partial", obs=10, changed_keys={"EMP_A": "2025-12-31"})
    note = orchestrate._book_fence_trip(unit, res, None, 12.0)
    assert "could NOT be booked" in note and not note.startswith("csv coverage note:"), note
    other = types.SimpleNamespace(key="abs/_all", source_id="abs", unit_id="_all")
    assert orchestrate._book_fence_trip(other, res, None, 12.0) is None


def test_health_names_the_stored_reason_of_a_desktop_debt(tmp_path, monkeypatch):
    from updater import health
    st = StateStore(path=str(tmp_path / "state.db"))
    st.note_csv_desktop_owed("ilostat", [("ilostat:EMP_A", None, "derive failed for an id; more")])
    rows = health.assess(st)["sources"]
    row = next(r for r in rows if r["source"] == "ilostat")
    text = " ".join(row.get("attention") or [])
    assert "1 derive failed for an id" in text and "too large" not in text, text


def test_run_once_books_a_fence_trip_through_the_helper():
    """The fence handler sits inside run_once, which no unit test can trip cheaply; pin the call site so the
    helper cannot be bypassed silently (R1137: the booking had no test at all)."""
    import ast
    import inspect
    tree = ast.parse(inspect.getsource(orchestrate.run_once))
    handlers = [h for h in ast.walk(tree) if isinstance(h, ast.ExceptHandler)
                and isinstance(h.type, ast.Name) and h.type.id == "UnitTimeout"]
    calls = [c for h in handlers for c in ast.walk(h) if isinstance(c, ast.Call)
             and getattr(c.func, "id", None) == "_book_fence_trip"]
    assert calls, "run_once's UnitTimeout handler must call _book_fence_trip"
    uses = [n for h in handlers for n in ast.walk(h) if isinstance(n, ast.Name) and n.id == "_fence_note"]
    assert len(uses) >= 2, "and must use its note"
