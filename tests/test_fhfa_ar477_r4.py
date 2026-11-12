"""AR-477 round 4.

F1 (MEDIUM): when the csv_desktop_owed booking of a `csv_misses: desktop_owed` source FAILS, the changed id goes
only to csv_retry_queue (it cannot drain on r2, and health does not read it). run_once recorded the build receipt
all the same, so fhfa's next pass trusted the build and skipped: the change signal was spent and nothing named
the id again. run_once now records no receipt when such an id is left in csv_failed or csv_deferred, so the next
pass falls back. R4A is the reviewer's probe (tests/test_zz_ar477_r4_probe.py in the review clone) as a test.

The negative controls pin the receipt's generic contract (the reviewer's surviving mutants Z4, Z5, Z7): a dry
run, a transient_fail and a no_change write no receipt row, and a receipt is per (source, unit)."""
from __future__ import annotations

import os
import sqlite3

import pytest

pytest.importorskip("pyarrow"); pytest.importorskip("duckdb")
from tests.test_fhfa_ar477 import IDS  # noqa: E402
from tests.test_fhfa_diff_changed_keys import D, _master, rig  # noqa: E402,F401
from tests.test_fhfa_ar477_r2 import _deferring, _putting  # noqa: E402
from tests.test_fhfa_ar477_r3 import K1, _derive, _pass, _sidecar_build  # noqa: E402
from tests.test_fhfa_ar477_r3_probe import V1, V2, _failing_k1  # noqa: E402
from updater import blob as blob_mod, config, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.base import Result, Strategy  # noqa: E402


def _boom(self, *a, **k):
    raise RuntimeError("booking write failed")


def _assert_falls_back(rc0, build1, r1, us1, rc1, q1, r2, o2, d2, asked2, out1):
    assert us1[0] == "partial", us1
    assert K1 in [r["series_id"] for r in q1], "the failed booking did not queue K1"
    # pass 1's build must not be receipted: the stored receipt stays pass 0's
    assert rc1 == rc0 and rc1 != build1, "the receipt was recorded although K1 is only in csv_retry_queue"
    assert "build receipt NOT recorded" in out1
    # the next pass does not trust the build: it rebuilds, and K1 is derived again or in a health-read home
    assert r2, "the next pass skipped with K1 only in csv_retry_queue"
    assert any(K1 in c for c in (asked2 or [])) or o2 or K1 in [r["series_id"] for r in d2], \
        "K1 was neither derived nor booked in a health-read home"


def test_r4a_failed_desktop_booking_of_a_failed_id_records_no_receipt(rig, monkeypatch, tmp_path, capsys):
    a = tmp_path / "A.db"
    _r0, _us0, rc0, *_ = _pass(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _derive(_putting), V1,
                               capsys)
    assert rc0
    changed = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}
    monkeypatch.setattr(StateStore, "note_csv_desktop_owed", _boom)      # stays patched for pass 2 too
    r1, us1, rc1, (o1, q1, d1), out1, _ = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(_failing_k1), V2,
                                                capsys)
    build1 = _sidecar_build(rig)
    r2, us2, rc2, (o2, q2, d2), out2, asked2 = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(_putting),
                                                     V2, capsys)
    with capsys.disabled():
        print(f"\nR4A pass1 status={us1[0]} receipt={'pass1 build' if rc1 == build1 else 'pass0 only'} owed={len(o1)} "
              f"queue={[r['series_id'] for r in q1]} desktop={[r['series_id'] for r in d1]}; "
              f"pass2 rebuilt={r2} status={us2[0]} queue={[r['series_id'] for r in q2]} "
              f"desktop={[r['series_id'] for r in d2]} owed={len(o2)}")
    _assert_falls_back(rc0, build1, r1, us1, rc1, q1, r2, o2, d2, asked2, out1)


def test_r4b_failed_desktop_booking_of_a_budget_deferred_id_records_no_receipt(rig, monkeypatch, tmp_path,
                                                                              capsys):
    """The budget-deferral fallback: _deferring defers every id, the booking fails, the ids are queued."""
    a = tmp_path / "A.db"
    _r0, _us0, rc0, *_ = _pass(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _derive(_putting), V1,
                               capsys)
    assert rc0
    changed = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}
    monkeypatch.setattr(StateStore, "note_csv_desktop_owed", _boom)
    r1, us1, rc1, (o1, q1, d1), out1, _ = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(_deferring), V2,
                                                capsys)
    assert K1 in [r["series_id"] for r in q1], "the deferred id was not queued"
    assert rc1 == rc0 and rc1 != _sidecar_build(rig), \
        "the receipt was recorded although the deferred ids are only in csv_retry_queue"
    r2, _us2, _rc2, (o2, _q2, d2), _o2, asked2 = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(_putting),
                                                       V2, capsys)
    assert r2, "the next pass skipped with the deferred ids only in csv_retry_queue"
    assert any(K1 in c for c in (asked2 or [])) or o2 or K1 in [r["series_id"] for r in d2]


def test_r4c_control_a_booked_failure_still_records_the_receipt(rig, monkeypatch, tmp_path, capsys):
    """Positive control for R4A: the same partial with a WORKING booking keeps its receipt (AR-477 r3 F4)."""
    a = tmp_path / "A.db"
    _pass(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _derive(_putting), V1, capsys)
    changed = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}
    r1, us1, rc1, (o1, q1, d1), out1, _ = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(_failing_k1), V2,
                                                capsys)
    assert us1[0] == "partial" and [r["series_id"] for r in d1] == [K1] and not q1
    assert rc1 == _sidecar_build(rig) and "build receipt NOT recorded" not in out1


# ------------------------------------------------------------------ negative controls (generic receipt field)
class _Stub(Strategy):
    """A strategy whose run returns a fixed Result carrying a build receipt."""
    name = "stub"

    def __init__(self, result):
        self.result = result
        self.ran = 0

    def is_due(self, unit, us, **k):
        return True

    def detect_change(self, unit, us):
        return "stub-vintage-new"

    def run(self, unit, since=None):
        self.ran += 1
        return self.result


def _run_stub(tmp_path, monkeypatch, result, dry=False):
    for k in list(os.environ):
        if k.startswith("GITHUB_") or (k.startswith("AQUEDUCT_") and k != "AQUEDUCT_BACKEND"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.delenv("ECONDL_DATA", raising=False)
    from core import cutover
    monkeypatch.setattr(cutover, "FLAG_PATH", str(tmp_path / "no_cutover_flag"))
    cat = tmp_path / "catalog.db"
    if not cat.exists():
        con = sqlite3.connect(cat)
        con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
        con.executemany("INSERT INTO series VALUES (?,?)", [(i, "fhfa") for i in IDS])
        con.commit(); con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    state = tmp_path / "S.db"
    monkeypatch.setattr(config, "DATA_ROOT", str(tmp_path / "data" / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(state))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    orchestrate._REG_ENTRIES = None
    stub = _Stub(result)
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: stub)
    st = StateStore(path=str(state))
    orchestrate.run_once(sources=["fhfa"], store=st, blob=blob_mod.R2Blob(), dry=dry)
    rows = st.db.execute("SELECT source_id, unit_id, receipt FROM build_receipt").fetchall()
    st.db.close()
    return stub, [tuple(r) for r in rows]


def test_r4n_control_an_ok_result_with_a_receipt_writes_its_row(tmp_path, monkeypatch):
    """The positive control the negatives below are read against: the stub's receipt does reach the store."""
    stub, rows = _run_stub(tmp_path, monkeypatch,
                           Result(status="ok", new_vintage="stub-vintage-new", changed_keys={},
                                  build_receipt="B-ok"))
    assert stub.ran == 1
    assert rows == [("fhfa", "_all", "B-ok")], rows


def test_r4n_a_dry_run_writes_no_receipt_row(tmp_path, monkeypatch):
    stub, rows = _run_stub(tmp_path, monkeypatch,
                           Result(status="ok", new_vintage="stub-vintage-new", changed_keys={},
                                  build_receipt="B-dry"), dry=True)
    assert rows == [], rows


@pytest.mark.parametrize("status", ["transient_fail", "no_change"])
def test_r4n_a_transient_fail_or_no_change_writes_no_receipt_row(tmp_path, monkeypatch, status):
    stub, rows = _run_stub(tmp_path, monkeypatch,
                           Result(status=status, new_vintage="stub-vintage-new", changed_keys={},
                                  build_receipt=f"B-{status}"))
    assert stub.ran == 1, "the stub did not run, so the control proves nothing"
    assert rows == [], rows


def test_r4n_a_receipt_is_per_unit(tmp_path, monkeypatch):
    """Two units in one store: unit A's receipt does not confirm unit B, in the store and in fhfa's read."""
    state = tmp_path / "U.db"
    st = StateStore(path=str(state))
    st.note_build_receipt("fhfa", "unitA", "B-A")
    assert st.build_receipt("fhfa", "unitA") == "B-A"
    assert st.build_receipt("fhfa", "unitB") is None
    assert st.build_receipt("fhfa", "_all") is None
    st.db.commit(); st.db.close()
    from updater.strategies.fetchers import fhfa
    monkeypatch.setattr(config, "STATE_DB", str(state))
    ok, _why = fhfa._build_confirmed("B-A")
    assert not ok, "fhfa trusted a receipt recorded for another unit"
    st = StateStore(path=str(state))
    st.note_build_receipt("fhfa", "_all", "B-A")
    st.db.commit(); st.db.close()
    assert fhfa._build_confirmed("B-A")[0], "control: fhfa's own unit '_all' must confirm"
