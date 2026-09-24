"""updater/state_migrations.py - the 13F entry's state rows move from `sec_edgar` to `sec_edgar_13f` on every
StateStore open, idempotently and in either order of old and new code (review R1197)."""
import sqlite3

import pytest

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



# ---- after T0: the XBRL product owns `sec_edgar` (reviews R1201, R1202) --------------------------------------
POST_T0 = [
    ("INSERT INTO source_state(source_id, strategy, cadence, status) VALUES "
     "('sec_edgar','edgar_delta','daily','ok')", ()),
    ("INSERT INTO source_state(source_id, strategy, cadence, status) VALUES "
     "('sec_edgar_13f','giant_changed_units','quarterly','ok')", ()),
    ("INSERT INTO unit_state(source_id, unit_id, strategy, status) VALUES ('sec_edgar_13f','_all','giant_changed_units','ok')", ()),
    # rows the XBRL writer may keep under its own id - including a run shaped like 13F's (unit '_all')
    ("INSERT INTO unit_state(source_id, unit_id, strategy, status) VALUES ('sec_edgar','daily','edgar_delta','ok')", ()),
    ("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-10-02T00:00:00+00:00','sec_edgar','_all')", ()),
    ("INSERT INTO csv_retry_queue(series_id, source_id) VALUES ('sec_edgar:0000320193','sec_edgar')", ()),
    ("INSERT INTO full_rederive_owed(source_id, note) VALUES ('sec_edgar','xbrl debt')", ()),
    # R1205 N1: a cursor the XBRL writer keeps - a series_cursor that always moved survived without it
    ("INSERT INTO series_cursor(source_id, series_key, last_obs_date) VALUES ('sec_edgar','0000320193','2026-09-30')", ()),
]
STRANDED_13F_UNIT = ("INSERT INTO unit_state(source_id, unit_id, strategy, status) "
                     "VALUES ('sec_edgar','_all','giant_changed_units','no_change')", ())


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


def test_a_stranded_13f_unit_row_moves_even_after_ownership(tmp_path):
    """R1202 finding 5: a race can leave unit_state('sec_edgar','_all') with the 13F strategy after the XBRL
    product owns the id; the sync would push it to D1 as the XBRL freshness. StateStore refuses a sec_edgar
    unit row with that strategy, so the row can only be 13F's: it moves (here: the new-id row wins)."""
    p = str(tmp_path / "state.db")
    _seed(p, POST_T0 + [STRANDED_13F_UNIT])
    StateStore(p).close()
    c = sqlite3.connect(p)
    assert c.execute("SELECT unit_id FROM unit_state WHERE source_id='sec_edgar'").fetchall() == [("daily",)]
    c.close()
    assert _count(p, "runs", "sec_edgar") == 1, "the XBRL product's own '_all' run stays (ambiguous: gated)"
    assert _count(p, "full_rederive_owed", "sec_edgar") == 1


def test_a_row_of_unknown_strategy_turns_off_only_the_ambiguous_tables(tmp_path):
    p = str(tmp_path / "state.db")
    _seed(p, [("INSERT INTO source_state(source_id, status) VALUES ('sec_edgar','ok')", ())] + THIRTEEN_F[1:])
    StateStore(p).close()
    assert _count(p, "source_state", "sec_edgar") == 1, "a NULL-strategy row is not guessed at"
    assert [_count(p, t, "sec_edgar") for t in ("runs", "full_rederive_owed")] == [2, 1], "gated off"
    assert _count(p, "unit_state", "sec_edgar") == 0 and _count(p, "unit_state", "sec_edgar_13f") == 1, \
        "the 13F-strategy unit row is unambiguous and moves"


def _locked(path):
    """A second connection holding the write lock, as a running writer would."""
    h = sqlite3.connect(path, isolation_level=None)
    h.execute("BEGIN IMMEDIATE")
    return h


# before ownership, rows under `sec_edgar` that no predicate selects: the early exit must not count them
UNMATCHED = [
    ("INSERT INTO unit_state(source_id, unit_id, strategy, status) VALUES ('sec_edgar','daily','edgar_delta','ok')", ()),
    ("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-10-02T00:00:00+00:00','sec_edgar','daily')", ()),
    ("INSERT INTO csv_retry_queue(series_id, source_id) VALUES ('sec_edgar:0000320193','sec_edgar')", ()),
]


def test_with_nothing_to_move_the_open_takes_no_write_lock(tmp_path):
    """R1201 finding 1: the early exit counted every `sec_edgar` row, so after T0 every open ran BEGIN
    IMMEDIATE, for ever. Every steady state must be a plain read - including one with sec_edgar rows that
    no predicate selects (R1202: a pending() counting every row survived the first version of this test)."""
    for name, rows in (("moved", THIRTEEN_F), ("post_t0", POST_T0), ("unmatched", THIRTEEN_F + UNMATCHED)):
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


ALONE = {
    "source_state": ("INSERT INTO source_state(source_id, strategy, status) VALUES ('sec_edgar','giant_changed_units','ok')", ()),
    "unit_state": ("INSERT INTO unit_state(source_id, unit_id, strategy, status) VALUES ('sec_edgar','_all','giant_changed_units','ok')", ()),
    "runs": ("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-09-23T00:00:00+00:00','sec_edgar','_all')", ()),
    "series_cursor": ("INSERT INTO series_cursor(source_id, series_key, last_obs_date) VALUES ('sec_edgar','k1','2026-01-01')", ()),
    "full_rederive_owed": ("INSERT INTO full_rederive_owed(source_id, note) VALUES ('sec_edgar','csv coherence unmet')", ()),
}


@pytest.mark.parametrize("table", sorted(ALONE))
def test_each_table_alone_is_seen_by_the_early_exit(tmp_path, table):
    """R1202: pending() ignoring one table survived - with only that table's rows pending, nothing moved."""
    p = str(tmp_path / "state.db")
    _seed(p, [ALONE[table]])
    StateStore(p).close()
    assert _count(p, table, "sec_edgar") == 0, f"{table}: the only pending rows were never processed"


def test_the_predicates_are_reread_under_the_lock(tmp_path, monkeypatch):
    """R1202: the XBRL writer may take the id between the lock-free read and the lock. Simulated by an early
    exit that says 'something to do' on a store the XBRL product owns: the ambiguous rows must stay."""
    p = str(tmp_path / "state.db")
    _seed(p, POST_T0)
    before = _snapshot(p)
    monkeypatch.setattr(M, "pending", lambda db: 1)
    c = sqlite3.connect(p)
    assert M.apply_all(c) == {}
    c.close()
    assert _snapshot(p) == before


def test_the_move_takes_the_write_lock_first(tmp_path):
    """BEGIN IMMEDIATE, not a deferred BEGIN: the predicates are read and the rows written under ONE lock."""
    p = str(tmp_path / "state.db")
    _seed(p, THIRTEEN_F)
    c = sqlite3.connect(p)
    seen = []
    c.set_trace_callback(seen.append)
    assert M.apply_all(c)
    c.close()
    begins = [s for s in seen if s.strip().upper().startswith("BEGIN")]
    assert begins == ["BEGIN IMMEDIATE"], begins
    under_lock = seen[seen.index("BEGIN IMMEDIATE") + 1:]
    first_write = next(i for i, s in enumerate(under_lock) if s.lstrip().upper().startswith(("UPDATE", "DELETE")))
    assert any("lower(trim(strategy," in s and "<>" in s for s in under_lock[:first_write]), \
        "the ownership is re-read under the lock, before the first write"


def test_each_statement_keeps_its_own_guard_without_the_gate(tmp_path):
    """_move is called directly with EVERY table, bypassing the ownership gate, on a post-T0 store: every
    statement's own predicate must still spare the XBRL product's rows (kills a guard dropped from any one)."""
    p = str(tmp_path / "state.db")
    _seed(p, POST_T0[:4] + [                   # the XBRL row, its unit, and the 13F rows under the new id
        ("INSERT INTO runs(ts_utc, source_id, unit_id) VALUES ('2026-10-02T00:00:00+00:00','sec_edgar','daily')", ()),
        ("INSERT INTO csv_retry_queue(series_id, source_id) VALUES ('sec_edgar:0000320193','sec_edgar')", ()),
    ])
    c = sqlite3.connect(p)
    c.execute("BEGIN IMMEDIATE")
    M._move(c, {**M._SELECT_ALWAYS, **M._SELECT_GATED})
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


def test_the_xbrl_rows_must_carry_their_own_strategy(tmp_path):
    """R1201 finding 4, R1202 finding 6: a `sec_edgar` source or unit write without a strategy of its own -
    none, empty, or the 13F one in any case - is refused."""
    s = StateStore(str(tmp_path / "state.db"))
    try:
        for kw in ({"status": "ok"}, {"status": "ok", "strategy": M.THIRTEEN_F_STRATEGY},
                   {"strategy": ""}, {"strategy": " GIANT_CHANGED_UNITS "}):
            with pytest.raises(ValueError, match="own strategy"):
                s.upsert_source("sec_edgar", **kw)
            with pytest.raises(ValueError, match="own strategy"):
                s.upsert_unit("sec_edgar", "_all", **kw)
        s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
        s.upsert_source("sec_edgar", status="ok", last_success_utc="2026-10-02T00:00:00+00:00")   # keeps its own
        assert s.get_source("sec_edgar")["strategy"] == "edgar_delta"
        s.upsert_unit("sec_edgar", "daily", strategy="edgar_delta", status="ok")
        s.upsert_source("ecb", status="ok")                                   # other ids unaffected
        s.upsert_unit("ecb", "_all", status="ok")
    finally:
        s.close()


def test_an_xbrl_write_over_an_unmoved_13f_row_is_refused(tmp_path):
    """R1205 probe D: old code wrote the 13F row AFTER this store was opened (so the migration has not moved
    it). The XBRL write carries its own strategy, but merging it into that row would keep 13F's cadence and
    dates - refused, with the way out: reopen the store."""
    p = str(tmp_path / "state.db")
    s = StateStore(p)
    try:
        c = sqlite3.connect(p)
        for sql, args in THIRTEEN_F[:2]:                       # old code, behind this store's back
            c.execute(sql, args)
        c.commit()
        c.close()
        with pytest.raises(ValueError, match="still holds the 13F row"):
            s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
        with pytest.raises(ValueError, match="still holds the 13F row"):
            s.upsert_unit("sec_edgar", "_all", strategy="edgar_delta", status="ok")
    finally:
        s.close()
    s = StateStore(p)                                         # the reopen moves it; now the write lands
    try:
        s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
        assert s.get_source("sec_edgar")["cadence"] is None, "nothing of 13F's row merged in"
        assert s.get_source("sec_edgar_13f")["strategy"] == M.THIRTEEN_F_STRATEGY
    finally:
        s.close()


def test_the_refusal_holds_when_the_new_id_row_exists_too(tmp_path):
    """R1207 V6: the realistic probe-B state - sec_edgar_13f already has its row, then old code writes the 13F
    row under sec_edgar behind this store's back. Refused; the reopen DROPS the old row (the new one wins)."""
    p = str(tmp_path / "state.db")
    _seed(p, [("INSERT INTO source_state(source_id, strategy, status) VALUES "
               "('sec_edgar_13f','giant_changed_units','ok')", ())])
    s = StateStore(p)
    try:
        c = sqlite3.connect(p)
        c.execute(*THIRTEEN_F[0])
        c.commit()
        c.close()
        with pytest.raises(ValueError, match="still holds the 13F row"):
            s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
    finally:
        s.close()
    s = StateStore(p)
    try:
        s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
        assert s.get_source("sec_edgar")["cadence"] is None
    finally:
        s.close()


def test_a_null_strategy_row_does_not_block_the_xbrl_write(tmp_path):
    """R1207 V4: a row of unknown (NULL) strategy is not the 13F row - refusing it would block the XBRL
    product's first write for ever (the migration leaves such a row alone)."""
    p = str(tmp_path / "state.db")
    _seed(p, [("INSERT INTO source_state(source_id, status) VALUES ('sec_edgar','ok')", ())])
    s = StateStore(p)
    try:
        s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
        assert s.get_source("sec_edgar")["strategy"] == "edgar_delta"
    finally:
        s.close()


def test_a_variant_spelling_of_the_13f_strategy_is_moved_not_refused_for_ever(tmp_path):
    """R1207 probe E: the guard compared lower(strip()) while the migration compared exactly, so a
    ' GIANT_CHANGED_UNITS ' row was refused on every open and never moved. Both now normalise."""
    p = str(tmp_path / "state.db")
    _seed(p, [("INSERT INTO source_state(source_id, strategy, status) VALUES "
               "('sec_edgar',' GIANT_CHANGED_UNITS ','ok')", ()),
              ("INSERT INTO unit_state(source_id, unit_id, strategy, status) VALUES "
               "('sec_edgar','_all','Giant_Changed_Units','ok')", ())])
    s = StateStore(p)
    try:
        assert s.get_source("sec_edgar") is None and s.get_unit("sec_edgar", "_all") is None, "moved on open"
        assert s.get_source("sec_edgar_13f") is not None and s.get_unit("sec_edgar_13f", "_all") is not None
        s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
    finally:
        s.close()


def test_strategyless_writes_under_the_old_id_wait_for_ownership(tmp_path):
    """R1202 finding 4: 'the XBRL writer's first write is its source_state row' was prose. Runs, cursors and
    owed rows under `sec_edgar` are refused until the XBRL product owns the id."""
    s = StateStore(str(tmp_path / "state.db"))
    try:
        writes = (lambda: s.log_run("sec_edgar", "_all", "ok"),
                  lambda: s.put_series_cursors("sec_edgar", {"k": "2026-01-01"}),
                  lambda: s.note_full_rederive_owed("sec_edgar", note="x"))
        for w in writes:
            with pytest.raises(ValueError, match="owns the id"):
                w()
        s.log_run("ecb", "_all", "ok")                                        # other ids unaffected
        s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
        for w in writes:
            w()
        assert _count(str(tmp_path / "state.db"), "runs", "sec_edgar") == 1
    finally:
        s.close()


def test_the_migration_line_goes_to_stderr(tmp_path, capsys):
    """R1201 finding 6: a read-purpose caller (health --json) prints JSON on stdout."""
    p = str(tmp_path / "state.db")
    _seed(p, THIRTEEN_F)
    capsys.readouterr()
    StateStore(p).close()
    got = capsys.readouterr()
    assert "[state] migrated" in got.err and "[state] migrated" not in got.out


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
    assert not c.in_transaction, "the failed move left its transaction open on the caller's connection"
    c.close()
    assert _count(p, "source_state", "sec_edgar") == 1 and _count(p, "source_state", "sec_edgar_13f") == 0, \
        "rolled back"


@pytest.mark.parametrize("ws", ["\t", "\n", "\r", " \t ", "\r\n", "\x0b", "\x0c"])
def test_whitespace_around_the_13f_strategy_is_trimmed_the_same_on_both_sides(tmp_path, ws):
    """R1211: SQLite trim() removed spaces only; Python strip() all whitespace. A 13F row with a tab was never
    moved, refused the XBRL write for ever, and read "moved" to t0_ready. Now it moves, like a space would."""
    p = str(tmp_path / "state.db")
    _seed(p, [("INSERT INTO source_state(source_id, strategy, status) VALUES ('sec_edgar', ?, 'ok')",
               (ws + "giant_changed_units" + ws,))])
    s = StateStore(p)
    try:
        assert s.get_source("sec_edgar") is None and s.get_source("sec_edgar_13f") is not None, "moved on open"
        s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
    finally:
        s.close()
    assert M.is_thirteen_f_strategy(ws + "GIANT_CHANGED_UNITS" + ws) and M.is_thirteen_f_strategy(ws)


@pytest.mark.parametrize("ws", ["\t", "\x0b", "\x0c", "\r\n"])
def test_a_unit_row_with_whitespace_around_the_13f_strategy_moves_too(tmp_path, ws):
    """The unit_state predicate carries WS as well (a mutant without it survived review R1218)."""
    p = str(tmp_path / "state.db")
    _seed(p, [("INSERT INTO unit_state(source_id, unit_id, strategy) VALUES ('sec_edgar', '_all', ?)",
               (ws + "giant_changed_units",))])
    StateStore(p).close()
    assert _count(p, "unit_state", "sec_edgar") == 0 and _count(p, "unit_state", "sec_edgar_13f") == 1


@pytest.mark.parametrize("existing", [" ", "\t", "\r\n", "\x0b\x0c", "", None, " giant_changed_units"])
def test_a_row_the_migration_does_not_move_does_not_block_the_xbrl_write(tmp_path, existing):
    """R1218 finding 2: a whitespace-only strategy read as 13F to the guard ("reopen so the migration moves
    it") while the migration never moves it, pending() is 0 and t0_ready says "moved" - the XBRL write was
    refused for ever. The guard now asks exactly what the migration selects. A non-breaking space is not in
    WS on either side, so that row is not 13F to either (a strip()-everything mutant survived R1218)."""
    p = str(tmp_path / "state.db")
    _seed(p, [("INSERT INTO source_state(source_id, strategy, status) VALUES ('sec_edgar', ?, 'ok')",
               (existing,))])
    s = StateStore(p)
    try:
        assert M.pending(s.db) == 0 and not M.is_thirteen_f_row(existing)
        assert s.get_source("sec_edgar") is not None, "not moved"
        s.upsert_source("sec_edgar", strategy="edgar_delta", status="ok")
        assert s.get_source("sec_edgar")["strategy"] == "edgar_delta"
    finally:
        s.close()


def test_the_guard_and_the_migration_agree_on_what_a_13f_row_is():
    for v in ("giant_changed_units", " GIANT_changed_units\t", "\x0bgiant_changed_units\x0c"):
        assert M.is_thirteen_f_row(v)
    for v in ("", " ", None, "edgar_delta", " giant_changed_units", "giant_changed_units x"):
        assert not M.is_thirteen_f_row(v)
