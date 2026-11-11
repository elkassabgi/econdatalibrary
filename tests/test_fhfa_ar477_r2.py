"""fhfa: review AR-477 round 2 (R1887) - C6 (the PULLED state must confirm the last rebuild) and C7.

F3: the C1 booking lives in state.db, while the cubes, the sidecar and the cleared PENDING marker live in R2. A
run whose state is thrown away (a refused --push-state, a kill before the push) used to leave the next rebuild
diffing against its published cubes -> {} -> the deferred change lost. Now every rebuild writes a build id into
the sidecar and returns it as new_vintage; the next pass trusts the store only if the pulled state's
unit_state.upstream_vintage equals it.

  O1  the fetch runs INSIDE the real run_once on a copy of a pushed state; run 1 defers K1 and its state is
      thrown away; run 2 on the older pushed state falls back and derives K1 (and books the debt when capped)
  O2  negative control: run 1's state KEPT -> run 2 diffs to {} and K1 stays booked in csv_desktop_owed
  S1  the skip path does not skip over an unconfirmed rebuild; S2 a confirmed one skips and keeps the id
  N1  no state.db / an unreadable one is NOT a confirmation; N2 a pre-C6 sidecar (no build id) is trusted
  V1  a rebuild returns its build id as new_vintage and writes it to the sidecar and the cleared marker
  C7  blob.read_bytes RAISING for the marker falls back (kills "read error treated as clear")
"""
from __future__ import annotations

import json
import os
import shutil
import sqlite3

import pytest

pytest.importorskip("pyarrow")
pytest.importorskip("duckdb")

from tests.test_fhfa_ar477 import IDS, _homes  # noqa: E402
from tests.test_fhfa_diff_changed_keys import D, F, _keys, _master, rig  # noqa: E402,F401
from updater import blob as blob_mod, config, derive, orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402


def _deferring(asked):
    def f(ids, blob, **kw):
        ids = list(dict.fromkeys(ids))
        asked.append(list(ids))
        return {"put": 0, "put_ids": [], "failed": [], "deferred": len(ids), "deferred_ids": list(ids),
                "failed_reasons": {}, "skipped_identical": 0, "deferred_large": {}}
    return f


def _putting(asked):
    def f(ids, blob, **kw):
        ids = list(dict.fromkeys(ids))
        asked.append(list(ids))
        return {"put": len(ids), "put_ids": ids, "failed": [], "deferred": 0, "deferred_ids": [],
                "failed_reasons": {}, "skipped_identical": 0, "deferred_large": {}}
    return f


def _orch(tmp_path, monkeypatch, rig, state_path, cubes, derive_fn):
    """One pass of the REAL run_once whose strategy runs fhfa.update() itself, against `state_path` - the state
    that run pulled. Returns (Result, StateStore)."""
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
        con.commit()
        con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(cat))
    monkeypatch.setattr(config, "DATA_ROOT", str(tmp_path / "data" / "clean_full"))
    monkeypatch.setattr(config, "STATE_DIR", str(tmp_path / "_aqueduct"))
    monkeypatch.setattr(config, "STATE_DB", str(state_path))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    orchestrate._REG_ENTRIES = None
    monkeypatch.setattr(derive, "derive_and_put", derive_fn)
    rig.cubes = dict(cubes)
    rig.n += 1
    monkeypatch.setattr(F, "_validators", lambda sess, n=rig.n: {"hpi_master.csv": f"v{n}"})
    got = {}

    class _S:
        def is_due(self, unit, us, **k):
            return True

        def detect_change(self, unit, us):
            return "v2"

        def run(self, unit, since=None):
            got["res"] = F.update(unit, since)
            return got["res"]
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: _S())
    st = StateStore(path=str(state_path))
    orchestrate.run_once(sources=["fhfa"], store=st, blob=blob_mod.R2Blob())
    return got["res"], st


def _copy(st, src, dst):
    st.db.commit()
    st.db.close()
    shutil.copyfile(src, dst)


@pytest.fixture
def pushed(rig, monkeypatch, tmp_path):
    """A store built once and a PUSHED state that confirms that build (state A)."""
    a = tmp_path / "A.db"
    res, st = _orch(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _putting([]))
    assert res.status == "ok"
    st.db.close()
    return a


# ------------------------------------------------------------------------------------- O1 / O2
def test_o1_a_thrown_away_state_makes_the_next_rebuild_fall_back(rig, monkeypatch, tmp_path, pushed):
    a1 = tmp_path / "A1.db"
    shutil.copyfile(pushed, a1)
    res1, st1 = _orch(tmp_path, monkeypatch, rig, a1, {"hpi_master": _master(nsa={("K1", D[0]): 99.0})},
                      _deferring([]))
    assert _keys(res1) == ["hpi_master:K1"]
    assert [r["series_id"] for r in st1.csv_desktop_owed("fhfa")] == ["fhfa:hpi_master:K1"], "precondition"
    st1.db.close()                                   # run 1's state is NOT pushed: A1 is thrown away
    a2 = tmp_path / "A2.db"
    shutil.copyfile(pushed, a2)                      # run 2 pulls the older pushed state A
    asked = []
    res2, st2 = _orch(tmp_path, monkeypatch, rig, a2, {"hpi_master": _master(nsa={("K1", D[0]): 99.0})},
                      _putting(asked))
    assert res2.changed_keys is None, "the store's record must not be trusted over a state that lost it"
    assert any("fhfa:hpi_master:K1" in call for call in asked), asked


def test_o1b_capped_the_fallback_books_the_debt(rig, monkeypatch, tmp_path, pushed):
    a1 = tmp_path / "A1.db"
    shutil.copyfile(pushed, a1)
    _r, st1 = _orch(tmp_path, monkeypatch, rig, a1, {"hpi_master": _master(nsa={("K1", D[0]): 99.0})},
                    _deferring([]))
    st1.db.close()
    a2 = tmp_path / "A2.db"
    shutil.copyfile(pushed, a2)
    monkeypatch.setattr(F, "CURSOR_CAP", 3)
    res2, st2 = _orch(tmp_path, monkeypatch, rig, a2, {"hpi_master": _master(nsa={("K1", D[0]): 99.0})},
                      _putting([]))
    assert res2.cursor_cap_hit is True
    o, _q, _d = _homes(st2)
    assert len(o) == 1 and "cursor cap hit" in o[0]["note"], o


def test_o2_negative_control_a_kept_state_diffs_normally(rig, monkeypatch, tmp_path, pushed):
    a1 = tmp_path / "A1.db"
    shutil.copyfile(pushed, a1)
    _r, st1 = _orch(tmp_path, monkeypatch, rig, a1, {"hpi_master": _master(nsa={("K1", D[0]): 99.0})},
                    _deferring([]))
    st1.db.close()                                   # run 1's state IS pushed: run 2 pulls A1
    a2 = tmp_path / "A2.db"
    shutil.copyfile(a1, a2)
    asked = []
    res2, st2 = _orch(tmp_path, monkeypatch, rig, a2, {"hpi_master": _master(nsa={("K1", D[0]): 99.0})},
                      _putting(asked))
    assert res2.changed_keys == {}, res2.changed_keys
    # owed-first repay needs a changed cube; nothing changed, so K1 stays owed (visible), never lost
    assert [r["series_id"] for r in st2.csv_desktop_owed("fhfa")] == ["fhfa:hpi_master:K1"]


# ------------------------------------------------------------------------------------- S1 / S2
def _skip_rig(rig, monkeypatch):
    rig.rebuild({"hpi_master": _master()})
    monkeypatch.setattr(F, "_validators", lambda sess, n=rig.n: {"hpi_master.csv": f"v{n}"})   # unchanged


def test_s1_an_unconfirmed_rebuild_is_never_skipped_over(rig, monkeypatch, tmp_path, capsys):
    _skip_rig(rig, monkeypatch)
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "older_state.db"))
    StateStore(path=str(tmp_path / "older_state.db")).upsert_unit(
        "fhfa", "_all", strategy="bulk_snapshot_if_changed", upstream_vintage="fhfa-build:old")
    capsys.readouterr()
    res = F.update(None, None)
    out = capsys.readouterr().out
    assert "skipped" not in out and "not confirmed by the pulled state" in out, out
    assert res.changed_keys is None and res.new_vintage.startswith("fhfa-build:")


def test_s2_a_confirmed_rebuild_skips_and_keeps_its_id(rig, monkeypatch, capsys):
    _skip_rig(rig, monkeypatch)
    sidecar = json.loads((rig.out / F.SIDECAR).read_text())
    capsys.readouterr()
    res = F.update(None, None)
    assert "skipped" in capsys.readouterr().out
    assert res.new_vintage == sidecar["build_id"]


# ------------------------------------------------------------------------------------- N1 / N2
def test_n1_no_state_db_is_not_a_confirmation(rig, monkeypatch, tmp_path):
    rig.rebuild({"hpi_master": _master()})
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "absent.db"))
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert res.changed_keys is None


def test_n1b_an_unreadable_state_db_is_not_a_confirmation(rig, monkeypatch, tmp_path):
    rig.rebuild({"hpi_master": _master()})
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"not a sqlite file at all")
    monkeypatch.setattr(config, "STATE_DB", str(bad))
    monkeypatch.setattr(rig, "record", lambda res: None)        # nothing can write that file either
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert res.changed_keys is None


def test_n2_a_pre_c6_sidecar_is_trusted_once(rig, monkeypatch, tmp_path):
    rig.rebuild({"hpi_master": _master()})
    p = rig.out / F.SIDECAR
    sc = json.loads(p.read_text())
    sc.pop("build_id")
    p.write_text(json.dumps(sc))
    monkeypatch.setattr(config, "STATE_DB", str(tmp_path / "absent.db"))
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert _keys(res) == ["hpi_master:K1"]


# ------------------------------------------------------------------------------------- V1
def test_v1_the_build_id_is_the_vintage_and_is_in_the_store(rig):
    res = rig.rebuild({"hpi_master": _master()})
    sc = json.loads((rig.out / F.SIDECAR).read_text())
    mk = json.loads((rig.out / F.PENDING).read_text())
    assert res.new_vintage.startswith("fhfa-build:") and res.new_vintage == sc["build_id"] == mk["build_id"]
    res2 = rig.rebuild({"hpi_master": _master()})
    assert res2.new_vintage != res.new_vintage, "each rebuild has its own id"


def test_v2_a_forced_rebuild_of_the_same_token_gets_a_new_id(rig, monkeypatch, tmp_path):
    """The MAX_AGE rebuild re-runs the SAME validators. If its id repeated, a state that kept only the
    previous same-token rebuild would 'confirm' this one after its own state was thrown away."""
    res1 = rig.rebuild({"hpi_master": _master()})
    p = rig.out / F.SIDECAR
    sc = json.loads(p.read_text())
    sc["built"] = "2000-01-01"                                 # stale -> forced rebuild, token unchanged
    p.write_text(json.dumps(sc))
    monkeypatch.setattr(F, "_validators", lambda sess, n=rig.n: {"hpi_master.csv": f"v{n}"})
    res2 = F.update(None, None)
    assert res2.new_vintage.split(":")[1] == res1.new_vintage.split(":")[1], "same token"
    assert res2.new_vintage != res1.new_vintage


# ------------------------------------------------------------------------------------- C7
def test_c7_a_marker_read_error_falls_back(rig, monkeypatch):
    rig.rebuild({"hpi_master": _master()})
    real = F.blob.read_bytes

    def flaky(path):
        if path.endswith(F.PENDING):
            raise OSError("R2 GET failed after retries")
        return real(path)
    monkeypatch.setattr(F.blob, "read_bytes", flaky)
    res = rig.rebuild({"hpi_master": _master(nsa={("K1", D[0]): 99.0})})
    assert res.changed_keys is None
