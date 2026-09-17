"""The freshness sync never publishes a gated source's rows (2026-09-17).

WHY. core/sync_state_d1.py upserts every unit_state/source_state row, plus a data_through row per
catalogued source, into D1 twice a day and never deletes. So a gated source's freshness rows were
re-published on every run: /v1/last-updates named such a source with cadence, status and freshness
until the worker filtered at the read, and rows the owner had a reviewed delete remove from D1
would have come back at the next sync. The sync now withholds gated ids from all three projections.
It deletes nothing - the list that ENFORCES a gate is not the list of what to delete (R889 rule 3).

Every test here can fail: the positive control (a non-gated source) must still be projected, the
verifier must REFUSE SQL that carries a gated row, and an unreadable gate must stop the sync rather
than read as an empty one (R900).
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import sync_state_d1 as d1sync  # noqa: E402

GATED = "srcgated"
KEPT = "srckept"


def _state_db(path):
    from updater.state import DDL
    con = sqlite3.connect(path)
    con.executescript(DDL)
    for sid in (GATED, KEPT):
        con.execute(
            "INSERT INTO unit_state(source_id,unit_id,strategy,upstream_vintage,last_success_utc,"
            "last_attempt_utc,status,last_obs_date,obs_count,attempt_count,last_error) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (sid, "_all", "extend_by_date", "v1", "2026-07-01T00:00:00+00:00",
             "2026-07-02T00:00:00+00:00", "ok", "2026-06-30", 10, 1, None))
        con.execute(
            "INSERT INTO source_state(source_id,strategy,cadence,status,last_success_utc,"
            "last_attempt_utc,owner,enabled,note) VALUES (?,'extend_by_date','daily','ok',"
            "'2026-07-01T00:00:00+00:00','2026-07-02T00:00:00+00:00',NULL,1,NULL)", (sid,))
    con.commit()
    con.close()


def _catalog(path):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, end_date TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?,?)",
                    [(f"{GATED}:A", GATED, "2026-08-01"), (f"{KEPT}:A", KEPT, "2026-08-01")])
    con.commit()
    con.close()


def _setup(tmp_path, monkeypatch):
    db = str(tmp_path / "state.db")
    _state_db(db)
    cat = str(tmp_path / "catalog.db")
    _catalog(cat)
    monkeypatch.setenv("ECONDL_CATALOG", cat)
    out = str(tmp_path / "sql")
    os.makedirs(out, exist_ok=True)
    return db, out


def _replay(files):
    mem = sqlite3.connect(":memory:")
    for p in files:
        mem.executescript(open(p, encoding="utf-8").read())
    got = {t: {r[0] for r in mem.execute(f"SELECT source_id FROM {t}")}
           for t in ("unit_state", "source_state", "source_data_through")}
    mem.close()
    return got


def test_a_gated_source_is_withheld_from_all_three_projections(tmp_path, monkeypatch):
    db, out = _setup(tmp_path, monkeypatch)
    # upper-cased on purpose: the gate is matched case-insensitively
    files, counts = d1sync.emit_sql(db, out, gated={GATED.upper()})
    got = _replay(files)
    for table, ids in got.items():
        assert GATED not in ids, f"a gated source reached {table}"
        assert KEPT in ids, f"positive control: the non-gated source is missing from {table}"
    assert counts["unit_state"] == 1 and counts["source_state"] == 1
    d1sync.verify_replay(db, files, counts, gated={GATED})


def test_without_a_gate_both_are_projected(tmp_path, monkeypatch):
    """Negative control on the fixture: with nothing gated, the gated fixture row IS emitted, so the
    test above passes because of the filter and not because the fixture never produced the row."""
    db, out = _setup(tmp_path, monkeypatch)
    files, counts = d1sync.emit_sql(db, out, gated=set())
    got = _replay(files)
    for table, ids in got.items():
        assert {GATED, KEPT} <= ids, table


def test_the_default_gate_is_the_committed_one(tmp_path, monkeypatch):
    db, out = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(d1sync, "_gated_ids", lambda: {GATED})
    files, counts = d1sync.emit_sql(db, out)
    assert GATED not in _replay(files)["unit_state"]
    d1sync.verify_replay(db, files, counts)


def test_the_verifier_refuses_sql_that_carries_a_gated_row(tmp_path, monkeypatch):
    db, out = _setup(tmp_path, monkeypatch)
    files, counts = d1sync.emit_sql(db, out, gated=set())      # SQL built without the gate
    # the GATED-ROW refusal specifically, not the generic mismatch that would also fire
    with pytest.raises(SystemExit, match="gated source's row reached"):
        d1sync.verify_replay(db, files, counts, gated={GATED})


def test_an_absent_gate_stops_the_sync(tmp_path, monkeypatch):
    """committed_gate returns an EMPTY set for an absent worker file; the publisher must not."""
    db, out = _setup(tmp_path, monkeypatch)
    from core import gen_denylist
    monkeypatch.setattr(gen_denylist, "OUT", str(tmp_path / "no_such_denylist.ts"))
    with pytest.raises(SystemExit, match="absent"):
        d1sync.emit_sql(db, out)


def test_it_runs_as_a_script_the_way_ci_calls_it(tmp_path):
    """Both workflows run `python core/sync_state_d1.py`, where sys.path[0] is core/ and not the repo
    root. The in-process tests above cannot see an import that only fails there - the first version
    of this change shipped exactly that (ModuleNotFoundError: core), caught in review."""
    import subprocess
    db = str(tmp_path / "state.db")
    _state_db(db)
    env = dict(os.environ, ECONDL_CATALOG=str(tmp_path / "no_such_catalog.db"), PYTHONPATH="")
    p = subprocess.run([sys.executable, os.path.join("core", "sync_state_d1.py"), "--dry-run", "--state-db", db],
                       cwd=ROOT, capture_output=True, text=True, env=env, timeout=300)
    assert p.returncode == 0, p.stderr[-800:]
    assert "DRY RUN" in p.stdout


def test_an_unreadable_gate_stops_the_sync(tmp_path, monkeypatch):
    db, out = _setup(tmp_path, monkeypatch)
    from core import gen_denylist

    def unreadable():
        raise gen_denylist.GateParseError("simulated unreadable gate")
    monkeypatch.setattr(gen_denylist, "committed_gate", unreadable)
    with pytest.raises(gen_denylist.GateParseError):
        d1sync.emit_sql(db, out)


def test_the_real_gate_reads_non_empty():
    """Positive control on the parser against the committed worker file. Nothing is printed."""
    if not os.path.exists(os.path.join(ROOT, "api", "worker", "src", "denylist.ts")):
        pytest.skip("no worker checkout")
    assert d1sync._gated_ids(), "the committed gate parsed as empty"
