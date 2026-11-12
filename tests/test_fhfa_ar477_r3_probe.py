"""AR-477 round 3 reviewer probe (copied in for F4). REAL BulkSnapshotIfChanged strategy (only is_due forced),
real run_once, real fhfa.update(); only the HTTP validators and the derive are stubbed."""
from __future__ import annotations
import os, shutil, sqlite3
import pytest
pytest.importorskip("pyarrow"); pytest.importorskip("duckdb")
from tests.test_fhfa_ar477 import IDS, _homes  # noqa: E402
from tests.test_fhfa_diff_changed_keys import D, F, _keys, _master, rig  # noqa: E402,F401
from tests.test_fhfa_ar477_r2 import _putting, _deferring  # noqa: E402
from updater import blob as blob_mod, config, derive, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.bulk_snapshot_if_changed import BulkSnapshotIfChanged  # noqa: E402


def _orch(tmp_path, monkeypatch, rig, state_path, cubes, derive_fn, validators):
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
    monkeypatch.setattr(config, "DATA_ROOT", str(tmp_path / "data" / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(state_path))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    orchestrate._REG_ENTRIES = None
    monkeypatch.setattr(derive, "derive_and_put", derive_fn)
    rig.cubes = dict(cubes)
    monkeypatch.setattr(F, "_validators", lambda sess, v=validators: dict(v))
    got = {"update_called": 0}
    real_update = F.update

    def upd(unit, since=None):
        got["update_called"] += 1
        got["res"] = real_update(unit, since)
        return got["res"]
    monkeypatch.setattr(F, "update", upd)

    class _Due(BulkSnapshotIfChanged):
        def is_due(self, unit, us, **k):
            return True
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: _Due())
    st = StateStore(path=str(state_path))
    orchestrate.run_once(sources=["fhfa"], store=st, blob=blob_mod.R2Blob())
    us = tuple(st.db.execute("SELECT status, upstream_vintage, last_error FROM unit_state WHERE source_id='fhfa'").fetchone())
    owed = st.db.execute("SELECT count(*) FROM sqlite_master WHERE name='full_rederive_owed'").fetchone()[0]
    fro = [tuple(r) for r in st.db.execute("SELECT * FROM full_rederive_owed WHERE source_id='fhfa'").fetchall()] if owed else []
    return got, st, us, fro


V1 = {"hpi_master.csv": "v1"}
V2 = {"hpi_master.csv": "v2"}


def test_r3a_an_unchanged_weekly_pass_is_clean_and_books_nothing(rig, monkeypatch, tmp_path):
    a = tmp_path / "A.db"
    g1, st1, us1, fro1 = _orch(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _putting([]), V1)
    print("PASS1", g1["update_called"], us1, fro1, getattr(g1.get("res"), "new_vintage", None))
    assert us1[0] == "ok"
    st1.db.commit(); st1.db.close()
    for n in (2, 3, 4):
        g, st, us, fro = _orch(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _putting([]), V1)
        o, q, d = _homes(st)
        print(f"PASS{n} update_called={g['update_called']} unit_state={us} full_rederive_owed={fro} "
              f"homes={o} {q} {[r['series_id'] for r in d]}")
        st.db.commit(); st.db.close()
    assert us[0] in ("ok", "no_change") and not fro, (us, fro)


def test_r3b_discarded_state_with_real_strategy(rig, monkeypatch, tmp_path):
    a = tmp_path / "A.db"
    _g, st, us, _f = _orch(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _putting([]), V1)
    assert us[0] == "ok"; st.db.commit(); st.db.close()
    a1 = tmp_path / "A1.db"; shutil.copyfile(a, a1)
    g1, st1, us1, _f = _orch(tmp_path, monkeypatch, rig, a1, {"hpi_master": _master(nsa={("K1", D[0]): 99.0})},
                             _deferring([]), V2)
    print("RUN1", us1, [r["series_id"] for r in st1.csv_desktop_owed("fhfa")])
    assert [r["series_id"] for r in st1.csv_desktop_owed("fhfa")] == ["fhfa:hpi_master:K1"]
    st1.db.close()
    a2 = tmp_path / "A2.db"; shutil.copyfile(a, a2)          # run 1's state thrown away
    asked = []
    g2, st2, us2, fro2 = _orch(tmp_path, monkeypatch, rig, a2, {"hpi_master": _master(nsa={("K1", D[0]): 99.0})},
                               _putting(asked), V2)
    o, q, d = _homes(st2)
    print("RUN2 update_called", g2["update_called"], "asked", asked, "homes", o, q, d, us2)
    assert any("fhfa:hpi_master:K1" in c for c in asked) or o or d


def _failing_k1(asked):
    def f(ids, blob, **kw):
        ids = list(dict.fromkeys(ids))
        asked.append(list(ids))
        bad = [i for i in ids if i == "fhfa:hpi_master:K1"]
        good = [i for i in ids if i not in bad]
        return {"put": len(good), "put_ids": good, "failed": bad, "deferred": 0, "deferred_ids": [],
                "failed_reasons": {i: "persistent" for i in bad}, "skipped_identical": 0, "deferred_large": {}}
    return f


def test_r3c_a_persistent_csv_failure_after_C6(rig, monkeypatch, tmp_path, capsys):
    a = tmp_path / "A.db"
    _g, st, us, _f = _orch(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _putting([]), V1)
    st.db.commit(); st.db.close()
    log = []
    cubes = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}
    for n, val in ((2, V2), (3, V2), (4, V2), (5, V2)):
        asked = []
        g, st, us, fro = _orch(tmp_path, monkeypatch, rig, a, cubes, _failing_k1(asked), val)
        out = capsys.readouterr().out
        rebuilt = "unchanged" not in out
        q = [tuple(r) for r in st.db.execute("SELECT series_id FROM csv_retry_queue WHERE source_id='fhfa'")]
        log.append((n, us[0], rebuilt, sum(len(c) for c in asked), len(fro), len(q)))
        st.db.commit(); st.db.close()
    for row in log:
        print("PASS%d status=%s rebuilt=%s derived=%d full_rederive_owed=%d retry_q=%d" % row)
    assert not any(r[2] for r in log[1:]), "an unchanged pass rebuilt the store after a partial"
