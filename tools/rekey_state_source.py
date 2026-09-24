"""Move one source's rows in state.db to a new source_id - the state half of a registry rename.

Built for the 13F entry's rename, sec_edgar -> sec_edgar_13f (2026-09-24, R275/R1193): under the shared id
the 13F product's source_state and unit_state rows were pushed to D1 as the freshness of the SERVED XBRL
product (catalogue id `sec_edgar`). The rename in updater/registry.yaml moves the entry; this moves its
rows, so no old-id row is left for the D1 sync to keep pushing (sync_state_d1 upserts, never deletes).

It works on a LOCAL state.db file - the one `python -m updater.run --pull-state` fetched - and never
pushes; `--push-state` is the operator's next step, in the same window as the code merge (the old code
would recreate old-id rows, the new code would create new-id rows this then collides with).

  python tools/rekey_state_source.py --from sec_edgar --to sec_edgar_13f \\
      --expect source_state=1,unit_state=1,runs=10 --drop full_rederive_owed            # dry run
  ... --apply                                                                            # writes

REFUSES unless the rows it finds are exactly --expect (a count that moved means someone else wrote since
the plan was made), refuses when the target id already has rows in any moved table, moves the rows of
every table keyed by source_id in ONE transaction, deletes the tables named in --drop for the old id (a row
that has no meaning under either id - e.g. the R1050 owed row booked against the XBRL corpus), and then
asserts that no old-id row is left anywhere. Leases (key "<source>/<unit>") must be free: a held one means a
run is in flight."""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

KEYED = ("source_state", "unit_state", "series_cursor", "runs", "csv_retry_queue", "full_rederive_owed",
         "csv_desktop_owed")


def counts(con: sqlite3.Connection, sid: str) -> dict[str, int]:
    have = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    return {t: con.execute(f"SELECT COUNT(*) FROM {t} WHERE source_id=?", (sid,)).fetchone()[0]
            for t in KEYED if t in have}


def plan(con: sqlite3.Connection, old: str, new: str, expect: dict[str, int], drop: set[str]) -> dict:
    now, target = counts(con, old), counts(con, new)
    wrong = {t: (now.get(t, 0), n) for t, n in expect.items() if now.get(t, 0) != n}
    unexpected = {t: n for t, n in now.items() if n and t not in expect and t not in drop}
    if wrong or unexpected:
        raise SystemExit(f"REFUSING: rows for {old!r} are not what was planned - differ {wrong}, "
                         f"not planned {unexpected}. Re-plan from a fresh pull.")
    clash = {t: n for t, n in target.items() if n and t not in drop}
    if clash:
        raise SystemExit(f"REFUSING: {new!r} already has rows {clash}")
    # the same ISO shape updater/state.py writes (…+00:00), so the text comparison is a time comparison
    from datetime import datetime, timezone                               # noqa: PLC0415
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    held = con.execute("SELECT key FROM leases WHERE key LIKE ? AND expires_utc > ?",
                       (old + "/%", now_iso)).fetchall() if "leases" in {r[0] for r in con.execute(
                           "SELECT name FROM sqlite_master WHERE type='table'")} else []
    if held:
        raise SystemExit(f"REFUSING: a run holds {[h[0] for h in held]} - wait until it ends")
    return {"move": {t: n for t, n in now.items() if n and t not in drop},
            "drop": {t: now.get(t, 0) for t in drop}}


def apply(con: sqlite3.Connection, old: str, new: str, the_plan: dict) -> None:
    con.execute("BEGIN IMMEDIATE")
    try:
        for t in the_plan["drop"]:
            con.execute(f"DELETE FROM {t} WHERE source_id=?", (old,))
        for t in the_plan["move"]:
            con.execute(f"UPDATE {t} SET source_id=? WHERE source_id=?", (new, old))
        left = {t: n for t, n in counts(con, old).items() if n}
        if left:
            raise RuntimeError(f"rows for {old!r} are still present after the move: {left}")
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def _expect(text: str) -> dict[str, int]:
    out = {}
    for part in filter(None, (p.strip() for p in (text or "").split(","))):
        t, n = part.split("=")
        if t not in KEYED:
            raise SystemExit(f"unknown table {t!r}; known: {KEYED}")
        out[t] = int(n)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--from", dest="old", required=True)
    ap.add_argument("--to", dest="new", required=True)
    ap.add_argument("--expect", required=True, help="table=count,... for the OLD id, exactly")
    ap.add_argument("--drop", default="", help="tables whose OLD-id rows are deleted, not moved")
    ap.add_argument("--db", default=None, help="state.db (default: updater.config's STATE_DB)")
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args(argv)
    drop = set(filter(None, (t.strip() for t in a.drop.split(","))))
    if drop - set(KEYED):
        raise SystemExit(f"unknown --drop table(s) {drop - set(KEYED)}")
    if a.db is None:
        from updater import config                                        # noqa: PLC0415
        a.db = config.STATE_DB
    con = sqlite3.connect(a.db, isolation_level=None)
    try:
        p = plan(con, a.old, a.new, _expect(a.expect), drop)
        print(f"{a.db}: move {p['move']} from {a.old!r} to {a.new!r}; delete {p['drop']}")
        if not a.apply:
            print("dry run - nothing written (pass --apply)")
            return 0
        apply(con, a.old, a.new, p)
        print(f"done: {counts(con, a.new)} now under {a.new!r}; {a.old!r} has none")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
