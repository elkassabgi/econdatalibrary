"""state.db migrations that must survive the order in which old and new code run (review R1197).

WHY IN CODE, NOT A ONE-OFF TOOL. The 13F/insider registry entry was renamed `sec_edgar` -> `sec_edgar_13f`
(2026-09-24). Its state rows have to move with it, and a one-off "pull -> move -> push" cannot be
sequenced safely: a scheduled CI run keeps the commit it was created with (start lags of hours), so OLD
code can run after the move and write `sec_edgar` rows again, and NEW code can run before it and create
`sec_edgar_13f` rows first; push_state's compare-and-swap also leaves a window of minutes (R1102). So the
move is applied by every StateStore open, inside whichever writer already holds the state, and it is
idempotent: rows that are already moved are left alone; a new-id row that already exists wins over an
old-id one; a later old-code row is moved or dropped the next time new code opens the store.

WHAT IS 13F, AND WHAT IS NOT. The catalogue id `sec_edgar` is the SERVED XBRL product. Today its refresher
writes D1 only, so every `sec_edgar` row in state.db is the 13F product's (measured read-only 2026-09-24:
source_state 1, unit_state 1, runs 10, full_rederive_owed 1). After the self-hosting cutover the XBRL
product's own LOCAL writer will write source_state('sec_edgar') - with its own strategy. So the source_state
row moves only while its strategy is the 13F one; unit_state, runs and cursors under `sec_edgar` are the
orchestrator's (the XBRL refresher writes none of them: it is not an orchestrator source), and the owed row
is R1050's false debt booked against the XBRL corpus by 13F runs."""
from __future__ import annotations

import sqlite3

OLD, NEW = "sec_edgar", "sec_edgar_13f"
THIRTEEN_F_STRATEGY = "giant_changed_units"     # the 13F entry's strategy (updater/registry.yaml)


def _tables(db: sqlite3.Connection) -> set[str]:
    return {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def move_13f_state(db: sqlite3.Connection) -> dict[str, int]:
    """Move the 13F product's rows from `sec_edgar` to `sec_edgar_13f`. Returns what it changed per table
    (all zero when there is nothing to do - the usual case once it has run). One transaction."""
    have = _tables(db)
    changed: dict[str, int] = {}
    old_rows = sum(db.execute(f"SELECT COUNT(*) FROM {t} WHERE source_id=?", (OLD,)).fetchone()[0]
                   for t in ("source_state", "unit_state", "runs", "series_cursor", "csv_retry_queue",
                             "full_rederive_owed", "csv_desktop_owed") if t in have)
    if not old_rows:
        return changed
    in_tx = db.in_transaction
    if not in_tx:
        db.execute("BEGIN IMMEDIATE")
    try:
        def run(label, sql, args=()):
            n = db.execute(sql, args).rowcount
            if n:
                changed[label] = changed.get(label, 0) + n

        # source_state: ONLY the 13F row (its strategy), never the XBRL product's own row
        if "source_state" in have:
            run("source_state dropped (new exists)",
                "DELETE FROM source_state WHERE source_id=? AND strategy=? "
                "AND EXISTS (SELECT 1 FROM source_state WHERE source_id=?)", (OLD, THIRTEEN_F_STRATEGY, NEW))
            run("source_state moved",
                "UPDATE source_state SET source_id=? WHERE source_id=? AND strategy=?", (NEW, OLD, THIRTEEN_F_STRATEGY))
        # unit_state: per unit, the new-id row wins; otherwise move
        if "unit_state" in have:
            run("unit_state dropped (new exists)",
                "DELETE FROM unit_state WHERE source_id=? AND unit_id IN "
                "(SELECT unit_id FROM unit_state WHERE source_id=?)", (OLD, NEW))
            run("unit_state moved", "UPDATE unit_state SET source_id=? WHERE source_id=?", (NEW, OLD))
        # runs has a surrogate key: always moves
        if "runs" in have:
            run("runs moved", "UPDATE runs SET source_id=? WHERE source_id=?", (NEW, OLD))
        # series_cursor: PK (source_id, series_key) - the new-id cursor wins
        if "series_cursor" in have:
            run("series_cursor dropped (new exists)",
                "DELETE FROM series_cursor WHERE source_id=? AND series_key IN "
                "(SELECT series_key FROM series_cursor WHERE source_id=?)", (OLD, NEW))
            run("series_cursor moved", "UPDATE series_cursor SET source_id=? WHERE source_id=?", (NEW, OLD))
        # keyed by series_id; source_id is a plain column
        for t in ("csv_retry_queue", "csv_desktop_owed"):
            if t in have:
                run(f"{t} moved", f"UPDATE {t} SET source_id=? WHERE source_id=?", (NEW, OLD))
        # R1050: debt booked against the XBRL corpus by 13F runs - meaningless under either id
        if "full_rederive_owed" in have:
            run("full_rederive_owed dropped", "DELETE FROM full_rederive_owed WHERE source_id=?", (OLD,))
        if not in_tx:
            db.execute("COMMIT")
    except BaseException:
        if not in_tx:
            db.execute("ROLLBACK")
        raise
    return changed


def apply_all(db: sqlite3.Connection) -> dict[str, int]:
    """Every migration, in order. Called by StateStore.__init__."""
    return move_13f_state(db)
