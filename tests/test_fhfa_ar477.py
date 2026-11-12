"""fhfa diff-measured changed set: the review AR-477 findings (F1, F2) and changes (C1-C5), each pinned.

  P1  (the reviewer's probe, F1) a csv-fence trip on a reported change leaves it in a durable home
  P2  (the reviewer's probe, F2, assertion per C2) a crash between build() and the sidecar makes the next
      pass FALL BACK - it never diffs new against new
  C1  csv_misses/csv_owed_repay/csv_ids declared and validated; an owed id is repaid first at the next rebuild
      that changes its cube and B3 clears it; a rebuild that changes nothing pays nothing; health names
      fhfa's own payer, never ilostat's tool or fed_board's Z1 tool, and never the bulk tool alone
  C2  the store-side PENDING marker: written before build(), cleared after the sidecar; set or unreadable -> fallback
  C3  a cube that is not unique on (series_key, obs_date), on either side, falls back
  C5  a dropped value column and a retyped one each report every key of that cube (kills "no schema guard")
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
pytest.importorskip("duckdb")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tests.test_fhfa_diff_changed_keys import D, F, Rig, _keys, _master, rig  # noqa: E402,F401
from updater import blob as blob_mod, config, derive, health, orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402

IDS = [f"fhfa:hpi_master:K{i}" for i in range(1, 6)]


def _run(tmp_path, monkeypatch, res, derive_fn, st=None, catalogued=IDS):
    """The REAL run_once over a fake strategy returning `res`, with `derive_fn` as derive_and_put."""
    for k in list(os.environ):
        if k.startswith("GITHUB_") or k.startswith("AQUEDUCT_"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_cutover_flag"))
    cat = tmp_path / "catalog.db"
    if not cat.exists():
        con = sqlite3.connect(cat)
        con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        con.executemany("INSERT INTO series VALUES (?,?)", [(i, "fhfa") for i in catalogued])
        con.commit()
        con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    data = tmp_path / "data"
    monkeypatch.setattr(config, "DATA_ROOT", str(data / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(data / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(data / "_aqueduct" / "state.db"))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    orchestrate._REG_ENTRIES = None
    monkeypatch.setattr(derive, "derive_and_put", derive_fn)

    class _S:
        def is_due(self, unit, us, **k):
            return True

        def detect_change(self, unit, us):
            return "v2"

        def run(self, unit, since=None):
            return res
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: _S())
    st = st or StateStore(path=str(tmp_path / "state.db"))
    orchestrate.run_once(sources=["fhfa"], store=st, blob=blob_mod.R2Blob())
    return st


def _putting(asked):
    def f(ids, blob, **kw):
        ids = list(dict.fromkeys(ids))
        asked.append(list(ids))
        return {"put": len(ids), "put_ids": ids, "failed": [], "deferred": 0, "deferred_ids": [],
                "failed_reasons": {}, "skipped_identical": 0, "deferred_large": {}}
    return f


def _homes(st):
    return ([r for r in st.full_rederives_owed() if r["source_id"] == "fhfa"],
            list(st.csv_retries("fhfa")), list(st.csv_desktop_owed("fhfa")))


# ------------------------------------------------------------------------------------- P1 (reviewer, F1)
def test_p1_fence_trip_then_forced_rebuild_leaves_the_changed_id_in_a_home(rig, monkeypatch, tmp_path):
    rig.rebuild({"hpi_master": _master()})
    res1 = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert _keys(res1) == ["hpi_master:K1"]

    def trip(ids, blob, **kw):
        raise orchestrate.UnitTimeout("csv phase fence")
    st = _run(tmp_path, monkeypatch, res1, trip)
    o, q, d = _homes(st)
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "state.db"))   # run 1's state WAS kept (pushed)
    res2 = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})     # forced rebuild, same data
    assert res2.changed_keys == {}, "the signal really is used up"
    assert [r["series_id"] for r in d] == ["fhfa:hpi_master:K1"], (o, q, d)
    assert "fence" in d[0]["reason"]


def test_p1b_negative_control_without_csv_misses_the_trip_books_nothing(rig, monkeypatch, tmp_path):
    """The probe's control: the same pass with routing OFF leaves the id in no home (the F1 defect)."""
    rig.rebuild({"hpi_master": _master()})
    res1 = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})

    def trip(ids, blob, **kw):
        raise orchestrate.UnitTimeout("csv phase fence")
    monkeypatch.setattr(orchestrate, "_csv_misses", lambda s: "")
    st = _run(tmp_path, monkeypatch, res1, trip)
    assert _homes(st) == ([], [], [])


# ------------------------------------------------------------------------------------- P2 (reviewer, F2)
def test_p2_a_crash_after_build_makes_the_next_pass_fall_back(rig, monkeypatch, capsys):
    """Reviewer's probe, assertion per C2: 'report K1, or book the debt'. The fallback hands the orchestrator
    the cursor set (which holds K1 here, and books the debt when capped - see P2b)."""
    rig.rebuild({"hpi_master": _master()})
    rig.cubes = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}
    rig.n += 1
    monkeypatch.setattr(F, "_validators", lambda sess, n=rig.n: {"hpi_master.csv": f"v{n}"})
    with monkeypatch.context() as m:
        m.setattr(F.blob, "publish_file", lambda p: (_ for _ in ()).throw(OSError("killed")))
        with pytest.raises(OSError):
            F.update(None, None)
    capsys.readouterr()
    res2 = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert res2.changed_keys is None, "a crash between build and sidecar must not read as 'nothing changed'"
    assert "hpi_master:K1" in (res2.series_cursors or {})
    assert "did not finish" in capsys.readouterr().out
    # the completed fallback pass cleared the marker: the next rebuild diffs again
    res3 = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert res3.changed_keys == {}


def test_p2b_capped_the_fallback_books_the_debt(rig, monkeypatch, tmp_path):
    rig.rebuild({"hpi_master": _master()})
    rig.cubes = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}
    rig.n += 1
    monkeypatch.setattr(F, "_validators", lambda sess, n=rig.n: {"hpi_master.csv": f"v{n}"})
    with monkeypatch.context() as m:
        m.setattr(F.blob, "publish_file", lambda p: (_ for _ in ()).throw(OSError("killed")))
        with pytest.raises(OSError):
            F.update(None, None)
    monkeypatch.setattr(F, "CURSOR_CAP", 3)
    res2 = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert res2.changed_keys is None and res2.cursor_cap_hit is True
    st = _run(tmp_path, monkeypatch, res2, _putting([]))
    o, _q, _d = _homes(st)
    assert len(o) == 1 and "cursor cap hit" in o[0]["note"], o


# ------------------------------------------------------------------------------------- C2
def test_c2_the_marker_is_written_before_build_and_cleared_after_the_sidecar(rig, monkeypatch):
    writes = []
    real = F.blob.write_bytes_atomic

    def rec(path, data):
        writes.append((os.path.basename(path), json.loads(data.decode("utf-8")) if path.endswith(".json") else None))
        rig.events.append(("write", os.path.basename(path)))
        return real(path, data)
    monkeypatch.setattr(F.blob, "write_bytes_atomic", rec)
    rig.rebuild({"hpi_master": _master()})
    names = [e for e in rig.events if e[0] in ("write", "build")]
    assert names == [("write", F.PENDING), ("build",), ("write", F.SIDECAR), ("write", F.PENDING)], names
    assert writes[0][1]["pending"] is True and writes[-1][1]["pending"] is False


@pytest.mark.parametrize("raw", [b'{"pending": true, "since": "2026-11-11T00:00:00+00:00"}', b"not json", b"[1]"])
def test_c2_a_set_or_unreadable_marker_falls_back(rig, raw):
    rig.rebuild({"hpi_master": _master()})
    (rig.out / F.PENDING).write_bytes(raw)
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert res.changed_keys is None


def test_c2_a_marker_that_cannot_be_written_falls_back(rig, monkeypatch):
    rig.rebuild({"hpi_master": _master()})
    real = F.blob.write_bytes_atomic

    def no_marker(path, data):
        if path.endswith(F.PENDING) and b'"pending": true' in data:
            raise OSError("R2 PUT failed")
        return real(path, data)
    monkeypatch.setattr(F.blob, "write_bytes_atomic", no_marker)
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert res.changed_keys is None


def test_c2_negative_control_a_cleared_marker_lets_the_diff_run(rig):
    rig.rebuild({"hpi_master": _master()})
    assert json.loads((rig.out / F.PENDING).read_text())["pending"] is False
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert _keys(res) == ["hpi_master:K1"]


# ------------------------------------------------------------------------------------- C3
@pytest.mark.parametrize("side", ["new", "old"])
def test_c3_a_cube_not_unique_on_its_key_falls_back(rig, side):
    base = _master()
    dup = pa.concat_tables([base, base.slice(0, 1)])
    if side == "old":
        rig.rebuild({"hpi_master": dup})
        res = rig.rebuild({"hpi_master": base})
    else:
        rig.rebuild({"hpi_master": base})
        res = rig.rebuild({"hpi_master": dup})
    assert res.changed_keys is None, res.changed_keys


# ------------------------------------------------------------------------------------- C5
def test_c5_a_dropped_value_column_reports_every_key_of_the_cube(rig):
    rig.rebuild({"hpi_master": _master()})
    res = rig.rebuild({"hpi_master": _master().drop_columns(["rstderr"])})
    assert _keys(res) == [f"hpi_master:K{i}" for i in range(1, 6)], res.changed_keys


def test_c5_a_retyped_value_column_reports_every_key_of_the_cube(rig):
    rig.rebuild({"hpi_master": _master()})
    # rstderr is all NULL, so a value-only comparison sees NO difference - only the schema guard can report it
    t = _master()
    i = t.schema.get_field_index("rstderr")
    t = t.set_column(i, "rstderr", t.column("rstderr").cast(pa.float32()))
    res = rig.rebuild({"hpi_master": t})
    assert _keys(res) == [f"hpi_master:K{i}" for i in range(1, 6)], res.changed_keys


# ------------------------------------------------------------------------------------- C1
def test_c1_the_registry_declares_fhfas_routing():
    e = {x["source_id"]: x for x in registry.load()["sources"]}["fhfa"]
    assert (e.get("csv_misses"), e.get("csv_owed_repay"), e.get("csv_ids")) == (
        "desktop_owed", "fresh", "shard_qualified")
    assert "csv_bulk" not in e and "csv_misses_from" not in e


@pytest.mark.parametrize("extra,ok", [
    ({"csv_misses": "desktop_owed", "csv_owed_repay": "fresh", "csv_ids": "shard_qualified"}, True),
    ({"csv_misses": "desktop_owed", "csv_owed_repay": "fresh"}, False),
    ({"csv_owed_repay": "fresh", "csv_ids": "shard_qualified"}, False),
    ({"csv_misses": "desktop_owed", "csv_ids": "shard"}, False),
    ({"csv_misses": "desktop_owed", "csv_owed_repay": "fresh", "csv_ids": "shard_qualified",
      "csv_bulk": "qualify_with_shard"}, False),
])
def test_c1_the_validator(extra, ok):
    reg = {"sources": [{"source_id": "zz", "strategy": "bulk_snapshot_if_changed", "cadence": "weekly",
                        "live": True, **extra}]}
    probs = [p for p in registry.validate(reg) if "csv_" in p]
    assert (not probs) == ok, probs


def test_c1_an_owed_id_is_repaid_first_when_its_cube_changes_and_b3_clears_it(rig, monkeypatch, tmp_path):
    st = StateStore(path=str(tmp_path / "state.db"))
    st.note_csv_desktop_owed("fhfa", [("fhfa:hpi_master:K2", None, "csv phase fence trip (test)")])
    rig.rebuild({"hpi_master": _master()})
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    asked = []
    st = _run(tmp_path, monkeypatch, res, _putting(asked), st=st)
    assert asked and asked[0][:2] == ["fhfa:hpi_master:K2", "fhfa:hpi_master:K1"], asked
    assert list(st.csv_desktop_owed("fhfa")) == []


def test_c1_a_rebuild_that_changes_nothing_pays_nothing(rig, monkeypatch, tmp_path):
    """The R1844 payer, stated: an id whose cube does not change stays owed (health shows it)."""
    st = StateStore(path=str(tmp_path / "state.db"))
    st.note_csv_desktop_owed("fhfa", [("fhfa:hpi_master:K2", None, "csv phase fence trip (test)")])
    rig.rebuild({"hpi_master": _master()})
    res = rig.rebuild({"hpi_master": _master()})
    asked = []
    st = _run(tmp_path, monkeypatch, res, _putting(asked), st=st)
    assert asked == [] and [r["series_id"] for r in st.csv_desktop_owed("fhfa")] == ["fhfa:hpi_master:K2"]


def _health_row(monkeypatch, st):
    e = {x["source_id"]: x for x in registry.load()["sources"]}["fhfa"]
    monkeypatch.setattr(health.registry, "load", lambda *a, **k: {"sources": [e]})
    return next(r for r in health.assess(store=st)["sources"] if r["source"] == "fhfa")


def test_c1_health_names_fhfas_own_payer_for_the_fallback_debt(monkeypatch):
    st = StateStore(path=":memory:")
    st.note_full_rederive_owed("fhfa", "date-tail", "cursor cap hit - changed-set evidence truncated (50,000 keys)")
    text = next(a for a in _health_row(monkeypatch, st)["attention"] if a.startswith("full re-derive OWED"))
    assert "core.derive_csv" in text and "--after-desktop-derive" in text and "exits 2" in text, text
    assert "derive_ilostat_indicators" not in text and "qualify-with-shard" not in text, text


def test_c1_health_names_fhfas_own_payer_for_a_desktop_debt(monkeypatch):
    st = StateStore(path=":memory:")
    st.note_csv_desktop_owed("fhfa", [("fhfa:annual_zip5:12345", None, "derive budget spent - deferred")])
    text = next(a for a in _health_row(monkeypatch, st)["attention"] if "OWED to the desktop derive" in a)
    assert "clear_csv_desktop_owed.py --source fhfa" in text and "CHANGES each id's cube" in text, text
    assert "clear_fed_board_z1_retries" not in text, text
