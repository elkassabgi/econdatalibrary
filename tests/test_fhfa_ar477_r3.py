"""AR-477 round 3, finding F4: a KEPT partial is not a LOST state.

C6 trusted the store's last build only when the pulled state's upstream_vintage named it, and a partial
never advances upstream_vintage. So one persistently failing CSV id made every later due pass rebuild fhfa
(the reviewer's probe, tests/test_fhfa_ar477_r3_probe.py::test_r3c). run_once now records the fetcher's
Result.build_receipt in state.db after the pass's CSV bookings, on an ok or partial pass, and fhfa accepts
either record. These tests drive the REAL strategy, run_once and fhfa.update() (the probe's harness)."""
from __future__ import annotations

import shutil
import sqlite3

import pytest

pytest.importorskip("pyarrow"); pytest.importorskip("duckdb")
from tests.test_fhfa_ar477 import _homes  # noqa: E402
from tests.test_fhfa_diff_changed_keys import D, _master, rig  # noqa: E402,F401
from tests.test_fhfa_ar477_r2 import _putting  # noqa: E402
from tests.test_fhfa_ar477_r3_probe import V1, V2, _failing_k1, _orch  # noqa: E402

K1 = "fhfa:hpi_master:K1"


def _pass(tmp_path, monkeypatch, rig, db, cubes, derive_fn, vals, capsys):
    capsys.readouterr()
    asked = getattr(derive_fn, "asked", None)
    g, st, us, fro = _orch(tmp_path, monkeypatch, rig, db, cubes, derive_fn, vals)
    out = capsys.readouterr().out
    rebuilt = "unchanged" not in out
    receipt = st.build_receipt("fhfa", "_all")
    homes = _homes(st)
    st.db.commit(); st.db.close()
    return rebuilt, us, receipt, homes, out, asked


def _derive(fn_factory):
    asked = []
    f = fn_factory(asked)
    f.asked = asked
    return f


def _sidecar_build(rig):
    import json
    import os
    from updater import config
    from updater.strategies.fetchers import fhfa
    p = os.path.join(config.source_dir("fhfa"), fhfa.SIDECAR)
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)["build_id"]


def test_f4a_a_kept_partial_is_confirmed_and_its_failed_id_stays_owed(rig, monkeypatch, tmp_path, capsys):
    a = tmp_path / "A.db"
    r1, us1, rc1, *_ = _pass(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _derive(_putting), V1,
                             capsys)
    assert us1[0] == "ok" and rc1 == us1[1]
    changed = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}
    r2, us2, rc2, (o2, q2, d2), _o, asked2 = _pass(tmp_path, monkeypatch, rig, a, changed,
                                                   _derive(_failing_k1), V2, capsys)
    assert r2 and us2[0] == "partial"
    assert us2[1] == us1[1], "a partial advanced upstream_vintage"
    assert rc2 == _sidecar_build(rig) and rc2 != rc1, "the partial's build receipt was not recorded"
    # the failed id's durable home: csv_desktop_owed (fhfa declares csv_misses: desktop_owed), not the queue
    assert [r["series_id"] for r in d2] == [K1] and not q2 and not o2
    for n in (3, 4, 5):
        r, us, rc, (o, q, d), out, asked = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(_failing_k1),
                                                 V2, capsys)
        assert not r, f"pass {n} rebuilt after a kept partial:\n{out}"
        assert rc == rc2
        assert [x["series_id"] for x in d] == [K1], f"pass {n}: K1 left its home"
        assert not o, f"pass {n} booked the full re-derive debt"


def test_f4b_a_partial_whose_state_is_thrown_away_still_falls_back(rig, monkeypatch, tmp_path, capsys):
    a = tmp_path / "A.db"
    _pass(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _derive(_putting), V1, capsys)
    a1 = tmp_path / "A1.db"; shutil.copyfile(a, a1)
    changed = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}
    r1, us1, *_ = _pass(tmp_path, monkeypatch, rig, a1, changed, _derive(_failing_k1), V2, capsys)
    assert r1 and us1[0] == "partial"
    a2 = tmp_path / "A2.db"; shutil.copyfile(a, a2)          # the partial's state is never pushed
    f = _derive(_putting)
    r2, us2, rc2, homes, out, asked = _pass(tmp_path, monkeypatch, rig, a2, changed, f, V2, capsys)
    assert r2, "an unkept partial was skipped over"
    assert "not confirmed by the pulled state" in out
    assert any(K1 in c for c in asked), "K1 was neither derived nor booked"


def _drop_receipts(db):
    con = sqlite3.connect(db)
    con.execute("DROP TABLE build_receipt")
    con.commit(); con.close()


def test_f4c_a_state_written_before_receipts_is_read_by_its_vintage(rig, monkeypatch, tmp_path, capsys):
    """A pulled state from code without build_receipt: an ok vintage that names the build confirms it; one
    that does not, falls back. A missing table is never an error that hides the vintage."""
    a = tmp_path / "A.db"
    _pass(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _derive(_putting), V1, capsys)
    from updater.strategies.fetchers import fhfa
    from updater import config
    monkeypatch.setattr(config, "STATE_DB", str(a))
    _drop_receipts(a)
    build = _sidecar_build(rig)
    assert fhfa._build_confirmed(build) == (True, "recorded")
    ok, why = fhfa._build_confirmed(build + "-other")
    assert not ok and "state records" in why


def test_f4d_a_csv_phase_crash_leaves_k1_in_a_home_or_the_next_pass_rebuilds(rig, monkeypatch, tmp_path,
                                                                             capsys):
    a = tmp_path / "A.db"
    _pass(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _derive(_putting), V1, capsys)
    changed = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}

    def crash(asked):
        def f(ids, blob, **kw):
            asked.append(list(ids))
            raise RuntimeError("derive crashed")
        return f
    _r, _us, rc, (o, q, d), _out, _a = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(crash), V2, capsys)
    in_home = bool(o) or any(r["series_id"] == K1 for r in q) or any(r["series_id"] == K1 for r in d)
    r2, *_ = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(_putting), V2, capsys)
    with capsys.disabled():
        print(f"\nF4D status={_us[0]} receipt={'set' if rc else 'none'} owed={len(o)} queue={len(q)} "
              f"desktop={[r['series_id'] for r in d]} next_rebuilt={r2}")
    assert in_home or r2, "the crashed pass left K1 in no home and the next pass skipped"
