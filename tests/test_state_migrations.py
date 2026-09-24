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
    ("INSERT INTO unit_state(source_id, unit_id, status, obs_count) VALUES ('sec_edgar','_all','no_change',4333518)", ()),
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
