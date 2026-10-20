"""The freshness projection labels a DISCONTINUED source 'discontinued' (2026-10-20, AR-354 item 1).

WHY. updater/discontinued.yaml lists sources the publisher retired: served frozen, no fetcher. The 52 relay-era
entries never had a state row, so /v1/sources showed them with status, last_updated and cadence null - no
label at all. The first successor-era entry, unctad_trademerchgr, had run for two months; state.db keeps its rows
for ever (history), and core/sync_state_d1.py upserted them every run, so /v1/sources kept saying
{"status":"ok","last_updated":"2026-10-01...","cadence":"weekly"} for a report UNCTAD had withdrawn. A live:false
flag or a removed registry entry changes nothing there (only a success writes source_state; the sync never
deletes). So the projection itself now carries the rule (core.sync_state_d1._project), used by the D1 sync and by
the self-hosted origin builder:
  unit_state    a RECORDED unit row is published with status 'discontinued', every other column kept (AR-355:
                metadata.json falls back to the `_all` unit row's last_success for `last_updated`, so dropping it
                would null that date on all 1,754 trademerchgr ids); no unit row is MADE for a source that never
                had one (it would add a row to /v1/last-updates and the Status board)
  source_state  exactly one row per discontinued source: status 'discontinued', cadence NULL; a recorded row keeps
                its last_success (when it was last refreshed), a missing one is created with it NULL. The D1 sync
                is upsert-all, so the stale D1 rows are OVERWRITTEN - no delete is needed for either table.
  data_through  untouched (a fact about the served data)

Every test here can fail: positive controls (a live source unchanged), a negative control showing the fixture
DOES produce the stale row without the rule, the verifier refusing SQL that carries a stale row, and an
unparseable list stopping the sync rather than reading as empty.
"""
from __future__ import annotations

import os
import re
import sqlite3
import subprocess
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import sync_state_d1 as d1sync  # noqa: E402

FROZEN = "unctad_trademerchgr"     # the real id: the script test below reads the real discontinued.yaml
NOROW = "zz_disc_without_row"      # a discontinued source state.db never recorded (the relay-era shape)
KEPT = "srckept_made_up"
LAST = "2026-10-01T22:27:34+00:00"


def _state_db(path):
    from updater.state import DDL
    con = sqlite3.connect(path)
    con.executescript(DDL)
    for sid in (FROZEN, KEPT):
        con.execute(
            "INSERT INTO unit_state(source_id,unit_id,strategy,upstream_vintage,last_success_utc,"
            "last_attempt_utc,status,last_obs_date,obs_count,attempt_count,last_error) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (sid, "_all", "bulk_snapshot_if_changed", "10003|2026-03-11T08:15:49", LAST,
             "2026-10-19T22:40:40+00:00", "transient_fail", "2025-12-31", 28863, 1, None))
        con.execute(
            "INSERT INTO source_state(source_id,strategy,cadence,status,last_success_utc,"
            "last_attempt_utc,owner,enabled,note) VALUES (?,'bulk_snapshot_if_changed','weekly','ok',?,?,NULL,NULL,NULL)",
            (sid, LAST, LAST))
    con.commit()
    con.close()


def _catalog(path):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, end_date TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?,?)",
                    [(f"{FROZEN}:0000.01.M4017", FROZEN, "2025-12-31"), (f"{KEPT}:A", KEPT, "2026-08-01")])
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
    return mem


def _ids(mem, table):
    return {r[0] for r in mem.execute(f"SELECT source_id FROM {table}")}


def _ss(mem, sid):
    return mem.execute("SELECT status, cadence, last_success_utc FROM source_state WHERE source_id=?",
                       (sid,)).fetchone()


def _us(mem, sid):
    return mem.execute("SELECT status, last_success_utc, upstream_vintage, obs_count FROM unit_state "
                       "WHERE source_id=? AND unit_id='_all'", (sid,)).fetchone()


def test_a_discontinued_source_is_labelled_not_left_stale(tmp_path, monkeypatch):
    db, out = _setup(tmp_path, monkeypatch)
    # upper-cased on purpose: matched case-insensitively, like the gate
    files, counts = d1sync.emit_sql(db, out, gated=set(), discontinued={FROZEN.upper(), NOROW})
    mem = _replay(files)
    # the recorded unit row stays (metadata.json's last_updated falls back to it, AR-355), relabelled, history kept
    assert _us(mem, FROZEN) == ("discontinued", LAST, "10003|2026-03-11T08:15:49", 28863)
    assert NOROW not in _ids(mem, "unit_state"), "no unit row may be MADE for a source that never had one"
    assert _us(mem, KEPT) == ("transient_fail", LAST, "10003|2026-03-11T08:15:49", 28863), \
        "positive control: a live source's unit row is unchanged"
    assert _ss(mem, FROZEN) == ("discontinued", None, LAST), "recorded row: relabelled, history kept"
    assert _ss(mem, NOROW) == ("discontinued", None, None), "no recorded row: one is made, with no date"
    assert _ss(mem, KEPT) == ("ok", "weekly", LAST), "positive control: a live source's row is unchanged"
    assert {FROZEN, KEPT} <= _ids(mem, "source_data_through"), "data_through stays for a discontinued source"
    assert counts["unit_state"] == 2 and counts["source_state"] == 3
    d1sync.verify_replay(db, files, counts, gated=set(), discontinued={FROZEN, NOROW})


def test_metadata_json_last_updated_keeps_its_date(tmp_path, monkeypatch):
    """AR-355. api/worker/src/metadata.ts: last_updated = series.last_updated, else the source's `_all` unit row's
    last_success_utc (UNIT_STATE_FOR_SOURCE). All 1,754 trademerchgr series rows have last_updated NULL, so their
    metadata.json date IS that unit row. Run the worker's own fallback query on the replayed projection."""
    db, out = _setup(tmp_path, monkeypatch)
    files, _ = d1sync.emit_sql(db, out, gated=set(), discontinued={FROZEN})
    mem = _replay(files)
    sql = open(os.path.join(ROOT, "api", "worker", "src", "sql.ts"), encoding="utf-8").read()
    m = re.search(r"export const UNIT_STATE_FOR_SOURCE\s*=\s*`([^`]*)`", sql)
    assert m, "UNIT_STATE_FOR_SOURCE not found in sql.ts - the fallback this test mirrors has moved"
    rows = mem.execute(m.group(1), (FROZEN,)).fetchall()
    cols = [d[0] for d in mem.execute(m.group(1), (FROZEN,)).description]
    by_unit = {dict(zip(cols, r))["unit_id"]: dict(zip(cols, r)) for r in rows}
    assert by_unit["_all"]["last_success_utc"] == LAST


def test_without_the_list_the_stale_row_is_projected(tmp_path, monkeypatch):
    """Negative control on the fixture: with nothing discontinued the stale row IS emitted, so the test above
    passes because of the rule and not because the fixture never produced it."""
    db, out = _setup(tmp_path, monkeypatch)
    files, _ = d1sync.emit_sql(db, out, gated=set(), discontinued=set())
    mem = _replay(files)
    assert FROZEN in _ids(mem, "unit_state")
    assert _ss(mem, FROZEN) == ("ok", "weekly", LAST)


def test_the_upsert_overwrites_a_stale_row_already_published(tmp_path, monkeypatch):
    """The point of emitting a row rather than withholding one: D1 already HOLDS the stale row and the sync
    never deletes. Replaying the new SQL over the old must leave 'discontinued', not 'ok'."""
    db, out = _setup(tmp_path, monkeypatch)
    old, _ = d1sync.emit_sql(db, out, gated=set(), discontinued=set())
    mem = _replay(old)
    out2 = str(tmp_path / "sql2")
    os.makedirs(out2)
    new, _ = d1sync.emit_sql(db, out2, gated=set(), discontinued={FROZEN})
    for p in new:
        mem.executescript(open(p, encoding="utf-8").read())
    assert _ss(mem, FROZEN) == ("discontinued", None, LAST)
    assert _us(mem, FROZEN)[:2] == ("discontinued", LAST), "the stale transient_fail unit row is overwritten, kept"


def test_a_gated_discontinued_source_is_never_named(tmp_path, monkeypatch):
    db, out = _setup(tmp_path, monkeypatch)
    files, counts = d1sync.emit_sql(db, out, gated={FROZEN, NOROW}, discontinued={FROZEN, NOROW})
    mem = _replay(files)
    for t in ("unit_state", "source_state", "source_data_through"):
        assert not ({FROZEN, NOROW} & _ids(mem, t)), f"the gate must win over the discontinued label in {t}"
    d1sync.verify_replay(db, files, counts, gated={FROZEN, NOROW}, discontinued={FROZEN, NOROW})


def test_the_default_list_is_updater_discontinued_yaml(tmp_path, monkeypatch):
    db, out = _setup(tmp_path, monkeypatch)
    ids = d1sync._discontinued_ids()
    assert FROZEN in ids and "unctad_rfia" in ids and "imf_ifs" in ids, "the real file must be the default"
    assert "unctad_trademerchtotal" not in ids, "negative control: a live sibling is not discontinued"
    monkeypatch.setattr(d1sync, "_gated_ids", lambda: set())
    files, counts = d1sync.emit_sql(db, out)                        # no discontinued= -> reads the file
    mem = _replay(files)
    assert _ss(mem, FROZEN)[0] == "discontinued" and _ss(mem, "unctad_rfia")[0] == "discontinued"
    assert _ss(mem, KEPT)[0] == "ok"
    d1sync.verify_replay(db, files, counts)


def test_the_verifier_refuses_sql_that_carries_a_stale_row(tmp_path, monkeypatch):
    db, out = _setup(tmp_path, monkeypatch)
    files, counts = d1sync.emit_sql(db, out, gated=set(), discontinued=set())   # SQL built without the rule
    with pytest.raises(SystemExit, match=r"wrong in the unit_state SQL \(stale status: \['unctad_trademerchgr'\]"):
        d1sync.verify_replay(db, files, counts, gated=set(), discontinued={FROZEN})


def _broken_project(monkeypatch, fn):
    real = d1sync._project
    monkeypatch.setattr(d1sync, "_project", lambda table, rows, cols, gated, frozen:
                        fn(table, real(table, rows, cols, gated, frozen), cols))


def test_the_verifier_catches_a_dropped_unit_row(tmp_path, monkeypatch):
    """The AR-355 defect, as a verifier refusal: a projection that drops the recorded unit row (and expects it
    dropped) would null metadata.json last_updated on every served id."""
    db, out = _setup(tmp_path, monkeypatch)
    _broken_project(monkeypatch, lambda t, rows, cols: [r for r in rows if not (t == "unit_state" and r[0] == FROZEN)])
    files, counts = d1sync.emit_sql(db, out, gated=set(), discontinued={FROZEN})
    with pytest.raises(SystemExit, match=r"recorded but missing: \[\('unctad_trademerchgr', '_all'\)\]"):
        d1sync.verify_replay(db, files, counts, gated=set(), discontinued={FROZEN})


def test_the_verifier_catches_a_made_up_unit_row(tmp_path, monkeypatch):
    db, out = _setup(tmp_path, monkeypatch)

    def add(t, rows, cols):
        if t != "unit_state":
            return rows
        made = dict.fromkeys(cols)
        made.update(source_id=NOROW, unit_id="_all", status="discontinued")
        return rows + [tuple(made[c] for c in cols)]
    _broken_project(monkeypatch, add)
    files, counts = d1sync.emit_sql(db, out, gated=set(), discontinued={FROZEN, NOROW})
    with pytest.raises(SystemExit, match=r"made up: \[\('zz_disc_without_row', '_all'\)\]"):
        d1sync.verify_replay(db, files, counts, gated=set(), discontinued={FROZEN, NOROW})


def test_the_verifier_catches_a_missing_source_state_row(tmp_path, monkeypatch):
    """AR-355 surviving mutant m6: the 'no row' branch of the source_state check. A projection that forgets to
    MAKE the row for an unrecorded discontinued source (and expects it absent) must be refused."""
    db, out = _setup(tmp_path, monkeypatch)
    _broken_project(monkeypatch, lambda t, rows, cols: [r for r in rows if not (t == "source_state" and r[0] == NOROW)])
    files, counts = d1sync.emit_sql(db, out, gated=set(), discontinued={FROZEN, NOROW})
    with pytest.raises(SystemExit, match=r"no row: \['zz_disc_without_row'\]"):
        d1sync.verify_replay(db, files, counts, gated=set(), discontinued={FROZEN, NOROW})


def test_an_upper_case_id_in_the_file_is_matched(tmp_path, monkeypatch):
    """AR-355 mutant m4: the loader lower-cases, so a hand-typed 'UNCTAD_TradeMerchGR' still labels the row
    state.db records in lower case."""
    f = tmp_path / "discontinued.yaml"
    f.write_text("sources:\n  - source_id: UNCTAD_TradeMerchGR\n", encoding="utf-8")
    assert d1sync._discontinued_ids(str(f)) == {FROZEN}
    db, out = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(d1sync, "DISCONTINUED_YAML", str(f))
    files, counts = d1sync.emit_sql(db, out, gated=set())          # default loader, no discontinued= argument
    mem = _replay(files)
    assert _ss(mem, FROZEN)[0] == "discontinued" and _us(mem, FROZEN)[0] == "discontinued"
    d1sync.verify_replay(db, files, counts, gated=set())


def test_the_verifier_checks_the_replay_not_only_project(tmp_path, monkeypatch):
    """verify_replay compares against _project; if _project itself went wrong (emitting and expecting the
    stale row), equality would still hold. The independent check on the replay must catch it."""
    db, out = _setup(tmp_path, monkeypatch)
    real = d1sync._project

    def broken(table, rows, cols, gated, frozen):          # forgets the rule for source_state only
        return real(table, rows, cols, gated, set()) if table == "source_state" else real(table, rows, cols, gated, frozen)
    monkeypatch.setattr(d1sync, "_project", broken)
    files, counts = d1sync.emit_sql(db, out, gated=set(), discontinued={FROZEN})
    with pytest.raises(SystemExit, match="not marked in the source_state SQL"):
        d1sync.verify_replay(db, files, counts, gated=set(), discontinued={FROZEN})


def test_an_empty_state_db_is_still_refused(tmp_path, monkeypatch):
    """The made-up 'discontinued' rows must not satisfy the zero-row guard (a skipped --pull-state)."""
    from updater.state import DDL
    db = str(tmp_path / "empty.db")
    con = sqlite3.connect(db)
    con.executescript(DDL)
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(tmp_path / "none.db"))
    with pytest.raises(SystemExit, match="zero unit_state/source_state rows"):
        d1sync.emit_sql(db, str(tmp_path), gated=set(), discontinued={FROZEN, NOROW})


def test_an_absent_list_labels_nothing(tmp_path):
    assert d1sync._discontinued_ids(str(tmp_path / "no_such.yaml")) == set()


def test_an_unparseable_list_stops_the_sync(tmp_path, monkeypatch):
    bad = tmp_path / "discontinued.yaml"
    bad.write_text("sources:\n  - source_id: x\n   bad: [indent\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="unreadable"):
        d1sync._discontinued_ids(str(bad))
    db, out = _setup(tmp_path, monkeypatch)
    monkeypatch.setattr(d1sync, "DISCONTINUED_YAML", str(bad))
    with pytest.raises(SystemExit, match="unreadable"):
        d1sync.emit_sql(db, out, gated=set())


def test_an_entry_without_a_source_id_stops_the_sync(tmp_path):
    bad = tmp_path / "discontinued.yaml"
    bad.write_text("sources:\n  - publisher: X\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="unreadable"):
        d1sync._discontinued_ids(str(bad))


def test_a_file_that_names_nobody_stops_the_sync(tmp_path):
    """An existing file with no entry is a broken file (a truncated write, `sources:` emptied by a bad merge),
    not 'nothing is discontinued' - reading it as empty would re-publish every stale row."""
    for text in ("", "sources:\n", "sources: []\n"):
        bad = tmp_path / "discontinued.yaml"
        bad.write_text(text, encoding="utf-8")
        with pytest.raises(SystemExit, match="unreadable"):
            d1sync._discontinued_ids(str(bad))


def test_the_script_ci_runs_labels_it(tmp_path):
    """Both workflows run `python core/sync_state_d1.py`. Run it the CI way on a state db holding the real
    discontinued id, and read the SQL it emitted."""
    db = str(tmp_path / "state.db")
    _state_db(db)
    env = dict(os.environ, ECONDL_CATALOG=str(tmp_path / "no_such_catalog.db"), PYTHONPATH="")
    p = subprocess.run([sys.executable, os.path.join("core", "sync_state_d1.py"), "--dry-run", "--state-db", db],
                       cwd=ROOT, capture_output=True, text=True, env=env, timeout=300)
    assert p.returncode == 0, p.stderr[-800:]
    assert "published as 'discontinued'" in p.stdout
    paths = re.findall(r"^\s+(\S+state_delta_\d+\.sql)\s*$", p.stdout, flags=re.M)
    assert paths, p.stdout[-800:]
    mem = _replay(paths)
    assert _ss(mem, KEPT)[0] == "ok", "positive control: the live source must be in the emitted SQL"
    assert _ss(mem, FROZEN)[:2] == ("discontinued", None)
    assert _us(mem, FROZEN)[:2] == ("discontinued", LAST)
