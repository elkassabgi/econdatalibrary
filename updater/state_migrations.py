"""state.db migrations that must survive the order in which old and new code run (reviews R1197, R1201).

WHY IN CODE, NOT A ONE-OFF TOOL. The 13F/insider registry entry was renamed `sec_edgar` -> `sec_edgar_13f`
(2026-09-24). Its state rows have to move with it, and a one-off "pull -> move -> push" cannot be
sequenced safely: a scheduled CI run keeps the commit it was created with (start lags of hours), so OLD
code can run after the move and write `sec_edgar` rows again, and NEW code can run before it and create
`sec_edgar_13f` rows first; push_state's compare-and-swap also leaves a window of minutes (R1102). So the
move is applied by every StateStore open, and it is idempotent: rows that are already moved are left
alone; a new-id row that already exists wins over an old-id one; a later old-code row is moved or dropped
the next time new code opens the store.

WHAT IS 13F, AND WHAT IS NOT. The catalogue id `sec_edgar` is the SERVED XBRL product. Today its refresher
writes D1 only, so every `sec_edgar` row in state.db is the 13F product's (measured read-only 2026-09-24:
source_state 1 and unit_state 1, both strategy giant_changed_units; runs 10, all unit '_all';
full_rederive_owed 1; csv queues 0). After the self-hosting cutover the XBRL product's own LOCAL writer
will write source_state('sec_edgar') with its own strategy. So:
  - OWNERSHIP ENDS THE MIGRATION. Once source_state('sec_edgar') holds any strategy but the 13F one, the
    XBRL product owns the id and nothing under `sec_edgar` is touched again, in any table (R1201 finding 3).
    StateStore.upsert_source refuses a `sec_edgar` row without a strategy of its own (finding 4), so the
    XBRL writer's first write establishes the ownership.
  - Until then each table's rows are selected by the narrowest predicate its columns allow (the 13F
    strategy where the table has one, the 13F product's only unit '_all' for runs).
  - The csv queues are NEVER moved: they are keyed by series_id, 13F has no series CSVs, so any
    `sec_edgar` row there is the XBRL product's.
  - The owed row is R1050's false debt booked against the XBRL corpus by 13F runs: dropped.
  - The early exit counts EXACTLY the rows the move would touch (one predicate list, _SELECT), so a store
    with nothing to move - including every store after ownership - takes no write lock (finding 1)."""
from __future__ import annotations

import sqlite3

OLD, NEW = "sec_edgar", "sec_edgar_13f"
THIRTEEN_F_STRATEGY = "giant_changed_units"     # the 13F entry's strategy (updater/registry.yaml)
THIRTEEN_F_UNIT = "_all"                        # the 13F entry's only unit

# table -> the WHERE clause (and its arguments) that selects the 13F product's rows under OLD. Shared by the
# early exit and the move, so the two can never disagree about what there is to do.
_SELECT = {
    "source_state": ("source_id=? AND strategy=?", (OLD, THIRTEEN_F_STRATEGY)),
    "unit_state": ("source_id=? AND strategy=?", (OLD, THIRTEEN_F_STRATEGY)),
    "runs": ("source_id=? AND unit_id=?", (OLD, THIRTEEN_F_UNIT)),
    "series_cursor": ("source_id=?", (OLD,)),
    "full_rederive_owed": ("source_id=?", (OLD,)),
}


def _tables(db: sqlite3.Connection) -> set[str]:
    return {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def xbrl_owns(db: sqlite3.Connection) -> bool:
    """True once source_state('sec_edgar') is anything but the 13F row - the XBRL product's own row, or a row
    of unknown strategy (left alone rather than guessed at)."""
    if "source_state" not in _tables(db):
        return False
    return db.execute("SELECT 1 FROM source_state WHERE source_id=? AND strategy IS NOT ?",
                      (OLD, THIRTEEN_F_STRATEGY)).fetchone() is not None


def pending(db: sqlite3.Connection) -> int:
    """How many rows the move would touch. A plain read: no lock."""
    if xbrl_owns(db):
        return 0
    have = _tables(db)
    return sum(db.execute(f"SELECT COUNT(*) FROM {t} WHERE {w}", a).fetchone()[0]
               for t, (w, a) in _SELECT.items() if t in have)


def _move(db: sqlite3.Connection) -> dict[str, int]:
    """The move itself, inside the caller's transaction. Every statement carries its table's _SELECT
    predicate, so it is safe on its own even where the ownership gate is bypassed (tests call it so)."""
    have = _tables(db)
    changed: dict[str, int] = {}

    def run(label, sql, args=()):
        n = db.execute(sql, args).rowcount
        if n:
            changed[label] = changed.get(label, 0) + n

    def sel(t):
        return _SELECT[t]

    if "source_state" in have:
        w, a = sel("source_state")
        run("source_state dropped (new exists)",
            f"DELETE FROM source_state WHERE {w} AND EXISTS (SELECT 1 FROM source_state WHERE source_id=?)",
            (*a, NEW))
        run("source_state moved", f"UPDATE source_state SET source_id=? WHERE {w}", (NEW, *a))
    if "unit_state" in have:                    # per unit, the new-id row wins; otherwise move
        w, a = sel("unit_state")
        run("unit_state dropped (new exists)",
            f"DELETE FROM unit_state WHERE {w} AND unit_id IN (SELECT unit_id FROM unit_state WHERE source_id=?)",
            (*a, NEW))
        run("unit_state moved", f"UPDATE unit_state SET source_id=? WHERE {w}", (NEW, *a))
    if "runs" in have:                          # surrogate key: always moves
        w, a = sel("runs")
        run("runs moved", f"UPDATE runs SET source_id=? WHERE {w}", (NEW, *a))
    if "series_cursor" in have:                 # PK (source_id, series_key) - the new-id cursor wins
        w, a = sel("series_cursor")
        run("series_cursor dropped (new exists)",
            f"DELETE FROM series_cursor WHERE {w} AND series_key IN "
            "(SELECT series_key FROM series_cursor WHERE source_id=?)", (*a, NEW))
        run("series_cursor moved", f"UPDATE series_cursor SET source_id=? WHERE {w}", (NEW, *a))
    if "full_rederive_owed" in have:            # R1050: meaningless under either id
        w, a = sel("full_rederive_owed")
        run("full_rederive_owed dropped", f"DELETE FROM full_rederive_owed WHERE {w}", a)
    return changed


def move_13f_state(db: sqlite3.Connection) -> dict[str, int]:
    """Move the 13F product's rows from `sec_edgar` to `sec_edgar_13f`. Returns what it changed per table
    (empty when there is nothing to do, which takes no lock). One transaction."""
    if not pending(db):
        return {}
    in_tx = db.in_transaction
    if not in_tx:
        db.execute("BEGIN IMMEDIATE")
    try:
        # re-checked under the lock: the XBRL writer may have taken the id since the read above
        changed = {} if xbrl_owns(db) else _move(db)
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
