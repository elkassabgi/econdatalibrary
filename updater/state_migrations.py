"""state.db migrations that must survive the order in which old and new code run (reviews R1197, R1201, R1202).

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
full_rederive_owed 1; series_cursor and csv queues 0). After the self-hosting cutover the XBRL product's
own LOCAL writer will write source_state('sec_edgar') with its own strategy. So:
  - UNAMBIGUOUS ROWS MOVE ALWAYS: source_state and unit_state rows under `sec_edgar` with the 13F strategy.
    StateStore refuses a `sec_edgar` source or unit row without the XBRL product's own strategy, so a
    13F-strategy row under `sec_edgar` can only be 13F's - even after ownership (R1202 finding 5: a row
    stranded by a race would otherwise stay, and the sync would push it to D1 as the XBRL freshness).
  - AMBIGUOUS ROWS MOVE ONLY BEFORE OWNERSHIP: runs (unit '_all'), series_cursor, full_rederive_owed carry
    no strategy. Once source_state('sec_edgar') holds any strategy but the 13F one, the XBRL product owns
    the id and these are never touched again. StateStore refuses them under `sec_edgar` until then, so the
    XBRL writer cannot write one before it owns the id (R1202 finding 4).
  - The csv queues are NEVER moved: they are keyed by series_id, 13F has no series CSVs, so any
    `sec_edgar` row there is the XBRL product's.
  - The owed row is R1050's false debt booked against the XBRL corpus by 13F runs: dropped.
  - The early exit counts EXACTLY the rows the move would touch (pending() and _move() read the same
    predicate tables), so a store with nothing to move takes no write lock (R1201 finding 1)."""
from __future__ import annotations

import sqlite3

OLD, NEW = "sec_edgar", "sec_edgar_13f"
THIRTEEN_F_STRATEGY = "giant_changed_units"     # the 13F entry's strategy (updater/registry.yaml)
THIRTEEN_F_UNIT = "_all"                        # the 13F entry's only unit

# table -> the WHERE clause (and its arguments) that selects the 13F product's rows under OLD.
# ALWAYS: unambiguous (the 13F strategy) - moved whoever owns the id.
_SELECT_ALWAYS = {
    "source_state": ("source_id=? AND strategy=?", (OLD, THIRTEEN_F_STRATEGY)),
    "unit_state": ("source_id=? AND strategy=?", (OLD, THIRTEEN_F_STRATEGY)),
}
# BEFORE OWNERSHIP ONLY: no strategy column, so only the ownership gate says whose they are.
_SELECT_GATED = {
    "runs": ("source_id=? AND unit_id=?", (OLD, THIRTEEN_F_UNIT)),
    "series_cursor": ("source_id=?", (OLD,)),
    "full_rederive_owed": ("source_id=?", (OLD,)),
}


def _tables(db: sqlite3.Connection) -> set[str]:
    return {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def is_thirteen_f_strategy(strategy) -> bool:
    """The 13F strategy, or no strategy at all - neither may name a row the XBRL product owns."""
    return not strategy or str(strategy).strip().lower() == THIRTEEN_F_STRATEGY


def xbrl_owns(db: sqlite3.Connection) -> bool:
    """True once source_state('sec_edgar') is anything but the 13F row - the XBRL product's own row, or a row
    of unknown strategy (left alone rather than guessed at)."""
    if "source_state" not in _tables(db):
        return False
    return db.execute("SELECT 1 FROM source_state WHERE source_id=? AND strategy IS NOT ?",
                      (OLD, THIRTEEN_F_STRATEGY)).fetchone() is not None


def _selects(db: sqlite3.Connection) -> dict:
    """The predicates in force: the unambiguous ones always, the gated ones only before ownership."""
    return {**_SELECT_ALWAYS, **({} if xbrl_owns(db) else _SELECT_GATED)}


def pending(db: sqlite3.Connection) -> int:
    """How many rows the move would touch. A plain read: no lock."""
    have = _tables(db)
    return sum(db.execute(f"SELECT COUNT(*) FROM {t} WHERE {w}", a).fetchone()[0]
               for t, (w, a) in _selects(db).items() if t in have)


def _move(db: sqlite3.Connection, selects: dict) -> dict[str, int]:
    """The move itself, inside the caller's transaction, for the tables in `selects`. Every statement carries
    its table's predicate, so it is safe on its own (tests call it with every table)."""
    have = _tables(db)
    changed: dict[str, int] = {}

    def run(label, sql, args=()):
        n = db.execute(sql, args).rowcount
        if n:
            changed[label] = changed.get(label, 0) + n

    def on(t):
        return t in selects and t in have

    if on("source_state"):
        w, a = selects["source_state"]
        run("source_state dropped (new exists)",
            f"DELETE FROM source_state WHERE {w} AND EXISTS (SELECT 1 FROM source_state WHERE source_id=?)",
            (*a, NEW))
        run("source_state moved", f"UPDATE source_state SET source_id=? WHERE {w}", (NEW, *a))
    if on("unit_state"):                        # per unit, the new-id row wins; otherwise move
        w, a = selects["unit_state"]
        run("unit_state dropped (new exists)",
            f"DELETE FROM unit_state WHERE {w} AND unit_id IN (SELECT unit_id FROM unit_state WHERE source_id=?)",
            (*a, NEW))
        run("unit_state moved", f"UPDATE unit_state SET source_id=? WHERE {w}", (NEW, *a))
    if on("runs"):                              # surrogate key: always moves
        w, a = selects["runs"]
        run("runs moved", f"UPDATE runs SET source_id=? WHERE {w}", (NEW, *a))
    if on("series_cursor"):                     # PK (source_id, series_key) - the new-id cursor wins
        w, a = selects["series_cursor"]
        run("series_cursor dropped (new exists)",
            f"DELETE FROM series_cursor WHERE {w} AND series_key IN "
            "(SELECT series_key FROM series_cursor WHERE source_id=?)", (*a, NEW))
        run("series_cursor moved", f"UPDATE series_cursor SET source_id=? WHERE {w}", (NEW, *a))
    if on("full_rederive_owed"):                # R1050: meaningless under either id
        w, a = selects["full_rederive_owed"]
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
        # the predicates are re-read under the lock: the XBRL writer may have taken the id since the read above
        changed = _move(db, _selects(db))
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
