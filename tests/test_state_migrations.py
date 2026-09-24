"""updater/state_migrations.py - the 13F entry's state rows move from `sec_edgar` to `sec_edgar_13f` on every
StateStore open, idempotently and in either order of old and new code (review R1197)."""
import sqlite3

from updater import state_migrations as M
from updater.state import StateStore


def _seed(path, rows):
    StateStore(path).close()                                  # the real schema (and a no-op migration)
    c = sqlite3.connect(path)
    for sql, args in rows:
        c.execute(sql, args)
    c.commit()
    c.close()


def _count(path, table, sid):
    c = sqlite3.connect(path)
    try:
        return c.execute(f"SELECT COUNT(*) FROM {table} WHERE source_id=?", (sid,)).fetchone()[0]
    finally:
        c.close()


THIRTEEN_F = [
    ("INSERT INTO source_state(source_id, strategy, cadence, status) VALUES "
     "('sec_edgar','giant_changed_units','quarterly','ok')", ()),
    ("INSERT INTO unit_state(source_id, unit_id, strategy, status, obs_count) VALUES ('sec_edgar','_all','giant_changed_units','no_change',4333518)", ()),
    ("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-09-23T00:00:00+00:00','sec_edgar','_all')", ()),
    ("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-09-22T00:00:00+00:00','sec_edgar','_all')", ()),
    ("INSERT INTO full_rederive_owed(source_id, note) VALUES ('sec_edgar','csv coherence unmet')", ()),
    ("INSERT INTO source_state(source_id, status) VALUES ('ecb','ok')", ()),
]


def test_the_13f_rows_move_on_open_and_the_false_debt_goes(tmp_path):
    p = str(tmp_path / "state.db")
    _seed(p, THIRTEEN_F)
    StateStore(p).close()                                     # the open applies it
    assert [_count(p, t, "sec_edgar") for t in ("source_state", "unit_state", "runs", "full_rederive_owed")] == [0] * 4
    assert [_count(p, t, "sec_edgar_13f") for t in ("source_state", "unit_state", "runs", "full_rederive_owed")] == \
        [1, 1, 2, 0]
    assert _count(p, "source_state", "ecb") == 1, "other sources untouched"


def test_a_second_open_changes_nothing(tmp_path):
    p = str(tmp_path / "state.db")
    _seed(p, THIRTEEN_F)
    StateStore(p).close()
    c = sqlite3.connect(p)
    assert M.apply_all(c) == {}
    c.close()


def test_the_xbrl_products_own_row_is_never_moved(tmp_path):
    """After T0 the XBRL product's local writer writes source_state('sec_edgar') with its own strategy."""
    p = str(tmp_path / "state.db")
    _seed(p, [("INSERT INTO source_state(source_id, strategy, cadence, status) VALUES "
               "('sec_edgar','edgar_delta','daily','ok')", ())])
    StateStore(p).close()
    c = sqlite3.connect(p)
    assert c.execute("SELECT strategy FROM source_state WHERE source_id='sec_edgar'").fetchone() == ("edgar_delta",)
    c.close()
    assert _count(p, "source_state", "sec_edgar_13f") == 0


def test_old_code_after_new_code_the_new_rows_win(tmp_path):
    """New code ran first (sec_edgar_13f rows exist), then a run of OLD code wrote sec_edgar rows again."""
    p = str(tmp_path / "state.db")
    _seed(p, [
        ("INSERT INTO source_state(source_id, strategy, status, last_success_utc) VALUES "
         "('sec_edgar_13f','giant_changed_units','ok','2026-09-25T00:00:00+00:00')", ()),
        ("INSERT INTO unit_state(source_id, unit_id, status) VALUES ('sec_edgar_13f','_all','ok')", ()),
        ("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-09-25T00:00:00+00:00','sec_edgar_13f','_all')", ()),
    ] + THIRTEEN_F)
    StateStore(p).close()
    c = sqlite3.connect(p)
    assert c.execute("SELECT status, last_success_utc FROM source_state WHERE source_id='sec_edgar_13f'").fetchone() \
        == ("ok", "2026-09-25T00:00:00+00:00"), "the new-id row wins"
    assert c.execute("SELECT status FROM unit_state WHERE source_id='sec_edgar_13f'").fetchone() == ("ok",)
    c.close()
    assert _count(p, "source_state", "sec_edgar") == 0 and _count(p, "unit_state", "sec_edgar") == 0
    assert _count(p, "runs", "sec_edgar_13f") == 3, "every run is kept (surrogate key)"


def test_a_series_cursor_on_both_ids_keeps_the_new_one(tmp_path):
    p = str(tmp_path / "state.db")
    StateStore(p).close()
    cols = [r[1] for r in sqlite3.connect(p).execute("PRAGMA table_info(series_cursor)")]
    extra = {c: None for c in cols if c not in ("source_id", "series_key")}
    c = sqlite3.connect(p)
    for sid in ("sec_edgar", "sec_edgar_13f"):
        c.execute(f"INSERT INTO series_cursor(source_id, series_key{''.join(',' + k for k in extra)}) "
                  f"VALUES (?, 'k1'{''.join(',NULL' for _ in extra)})", (sid,))
    c.execute(f"INSERT INTO series_cursor(source_id, series_key{''.join(',' + k for k in extra)}) "
              f"VALUES ('sec_edgar', 'k2'{''.join(',NULL' for _ in extra)})")
    c.commit()
    assert M.apply_all(c)
    got = sorted(c.execute("SELECT source_id, series_key FROM series_cursor").fetchall())
    c.close()
    assert got == [("sec_edgar_13f", "k1"), ("sec_edgar_13f", "k2")]


# ---- after T0: the XBRL product owns `sec_edgar` (review R1201) -----------------------------------------------
POST_T0 = [
    ("INSERT INTO source_state(source_id, strategy, cadence, status) VALUES "
     "('sec_edgar','edgar_delta','daily','ok')", ()),
    ("INSERT INTO source_state(source_id, strategy, cadence, status) VALUES "
     "('sec_edgar_13f','giant_changed_units','quarterly','ok')", ()),
    ("INSERT INTO unit_state(source_id, unit_id, strategy, status) VALUES ('sec_edgar_13f','_all','giant_changed_units','ok')", ()),
    # rows an XBRL writer may keep under its own id - including ones shaped like 13F's
    ("INSERT INTO unit_state(source_id, unit_id, strategy, status) VALUES ('sec_edgar','_all','giant_changed_units','ok')", ()),
    ("INSERT INTO unit_state(source_id, unit_id, strategy, status) VALUES ('sec_edgar','daily','edgar_delta','ok')", ()),
    ("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-10-02T00:00:00+00:00','sec_edgar','_all')", ()),
    ("INSERT INTO csv_retry_queue(series_id, source_id) VALUES ('sec_edgar:0000320193','sec_edgar')", ()),
    ("INSERT INTO full_rederive_owed(source_id, note) VALUES ('sec_edgar','xbrl debt')", ()),
]


def _snapshot(path):
    c = sqlite3.connect(path)
    try:
        return {t: sorted(map(tuple, c.execute(f"SELECT * FROM {t}").fetchall()))
                for t in ("source_state", "unit_state", "runs", "series_cursor", "csv_retry_queue",
                          "full_rederive_owed", "csv_desktop_owed")}
    finally:
        c.close()


def test_after_t0_nothing_under_the_xbrl_id_is_touched(tmp_path):
    p = str(tmp_path / "state.db")
    _seed(p, POST_T0)
    before = _snapshot(p)
    StateStore(p).close()
    assert _snapshot(p) == before


def test_a_row_of_unknown_strategy_is_left_alone(tmp_path):
    p = str(tmp_path / "state.db")
    _seed(p, [("INSERT INTO source_state(source_id, status) VALUES ('sec_edgar','ok')", ())] + THIRTEEN_F[1:])
    before = _snapshot(p)
    StateStore(p).close()
    assert _snapshot(p) == before


def _locked(path):
    """A second connection holding the write lock, as a running writer would."""
    h = sqlite3.connect(path, isolation_level=None)
    h.execute("BEGIN IMMEDIATE")
    return h


def test_with_nothing_to_move_the_open_takes_no_write_lock(tmp_path):
    """R1201 finding 1: the early exit counted every `sec_edgar` row, so after T0 every open ran BEGIN
    IMMEDIATE, for ever. Both steady states - after the move, and after T0 - must be plain reads."""
    for name, rows in (("moved", THIRTEEN_F), ("post_t0", POST_T0)):
        p = str(tmp_path / f"{name}.db")
        _seed(p, rows)
        StateStore(p).close()                                 # the move, if any, is done
        h = _locked(p)
        try:
            c = sqlite3.connect(p, timeout=0.2)
            assert M.apply_all(c) == {}, name                 # raises "database is locked" if it locks
            c.close()
        finally:
            h.rollback()
            h.close()
    # control: with something to move, the same open DOES need the lock
    p = str(tmp_path / "pending.db")
    _seed(p, THIRTEEN_F)
    h = _locked(p)
    try:
        c = sqlite3.connect(p, timeout=0.2)
        try:
            M.apply_all(c)
        except sqlite3.OperationalError as e:
            assert "locked" in str(e)
        else:
            raise AssertionError("the control moved rows without the write lock - the probe sees nothing")
        c.close()
    finally:
        h.rollback()
        h.close()


def test_each_statement_keeps_its_own_guard_without_the_gate(tmp_path):
    """_move is called directly, bypassing the ownership gate, on a post-T0 store: every statement's own
    predicate must still spare the XBRL product's rows (kills a guard dropped from any one statement)."""
    p = str(tmp_path / "state.db")
    _seed(p, POST_T0[:3] + [                   # the XBRL row, and the 13F rows already under the new id
        ("INSERT INTO unit_state(source_id, unit_id, strategy, status) VALUES ('sec_edgar','daily','edgar_delta','ok')", ()),
        ("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-10-02T00:00:00+00:00','sec_edgar','daily')", ()),
        ("INSERT INTO csv_retry_queue(series_id, source_id) VALUES ('sec_edgar:0000320193','sec_edgar')", ()),
    ])
    c = sqlite3.connect(p)
    c.execute("BEGIN IMMEDIATE")
    M._move(c)
    c.execute("COMMIT")
    c.close()
    assert _count(p, "source_state", "sec_edgar") == 1, "the XBRL row survives the DELETE"
    cc = sqlite3.connect(p)
    assert cc.execute("SELECT strategy FROM source_state WHERE source_id='sec_edgar'").fetchone() == ("edgar_delta",)
    assert cc.execute("SELECT unit_id FROM unit_state WHERE source_id='sec_edgar'").fetchall() == [("daily",)]
    assert cc.execute("SELECT unit_id FROM runs WHERE source_id='sec_edgar'").fetchall() == [("daily",)]
    cc.close()
    assert _count(p, "csv_retry_queue", "sec_edgar") == 1, "the csv queues are never moved"


def test_the_csv_queues_are_never_moved(tmp_path):
    """They are keyed by series_id and 13F has no series CSVs: any `sec_edgar` row there is XBRL's."""
    p = str(tmp_path / "state.db")
    _seed(p, THIRTEEN_F + [
        ("INSERT INTO csv_retry_queue(series_id, source_id) VALUES ('sec_edgar:0000320193','sec_edgar')", ()),
        ("INSERT INTO csv_desktop_owed(series_id, source_id) VALUES ('sec_edgar:0000789019','sec_edgar')", ()),
    ])
    StateStore(p).close()
    assert _count(p, "source_state", "sec_edgar_13f") == 1, "precondition: the move ran"
    assert _count(p, "csv_retry_queue", "sec_edgar") == 1 and _count(p, "csv_desktop_owed", "sec_edgar") == 1


def test_the_xbrl_row_must_carry_its_own_strategy(tmp_path):
    """R1201 finding 4: a `sec_edgar` write without a strategy of its own is refused."""
    import pytest
    s = StateStore(str(tmp_path / "state.db"))
    try:
        for kw in ({"status": "ok"}, {"status": "ok", "strategy": M.THIRTEEN_F_STRATEGY}):
            with pytest.raises(ValueError, match="own strategy"):
                s.upsert_source("sec_edgar", **kw)
        s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
        s.upsert_source("sec_edgar", status="ok", last_success_utc="2026-10-02T00:00:00+00:00")   # keeps its own
        assert s.get_source("sec_edgar")["strategy"] == "edgar_delta"
        s.upsert_source("ecb", status="ok")                                   # other ids unaffected
    finally:
        s.close()


def test_no_registry_entry_uses_the_old_id():
    """While this migration exists, a registry entry named `sec_edgar` (renaming sec_edgar_xbrl to match its
    catalogue id is the natural next step) would have its orchestrator rows moved away. It must first
    write source_state('sec_edgar') with its own strategy - and this test must then be revisited."""
    import os
    import yaml
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    reg = yaml.safe_load(open(os.path.join(root, "updater", "registry.yaml"), encoding="utf-8"))
    ids = {s["source_id"] for s in reg["sources"]}
    assert M.NEW in ids, "precondition: the 13F entry is read"
    assert M.OLD not in ids


def test_a_failure_rolls_the_whole_move_back(tmp_path, monkeypatch):
    p = str(tmp_path / "state.db")
    _seed(p, THIRTEEN_F)
    c = sqlite3.connect(p)
    real_execute = c.execute

    class Wrap:
        def __getattr__(self, name):
            return getattr(c, name)

        def execute(self, sql, args=()):
            if sql.startswith("UPDATE runs"):
                raise sqlite3.OperationalError("disk I/O error")
            return real_execute(sql, args)
    try:
        M.move_13f_state(Wrap())
    except sqlite3.OperationalError:
        pass
    else:
        raise AssertionError("expected the injected failure")
    c.close()
    assert _count(p, "source_state", "sec_edgar") == 1 and _count(p, "source_state", "sec_edgar_13f") == 0, \
        "rolled back"
