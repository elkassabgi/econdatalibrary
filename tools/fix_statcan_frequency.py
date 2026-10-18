"""Rewrite the `frequency` of the statcan catalogue rows to the corrected FREQ map (R1615). ONE-OFF, LOCAL only.

WHY. tools/catalog_statcan_tables.FREQ disagreed with StatCan's own getCodeSets on 15 of 17 codes, and every
statcan row carries its cube's label (measured 2026-10-18: 415,015 of 467,611 rows wrong, in 4,723 cubes; e.g.
18 Occasional -> M on 384,641 rows, 9 Quarterly -> A, 1 Daily -> A). The map is fixed and pinned by
tests/test_statcan_freq_codeset.py; this tool brings the rows already written into line, in the LOCAL catalogue
only, before the T0 snapshot. It never touches D1, R2, series_fts (it has no frequency column - checked at run
time) or any other column.

WHAT A ROW SHOULD HOLD: FREQ.get(<the cube's .done frequencyCode>) - the cataloguer's own input and its own map
(cube_rows), read from tools/catalog_statcan_tables.py, so the two cannot drift. A row whose cube has no sidecar,
or whose id is not `statcan:<digits>[#part]`, is never guessed: the run REFUSES.

    python tools/fix_statcan_frequency.py                          # DRY: the transition table, writes nothing
    python tools/fix_statcan_frequency.py --apply --expect-rows N  # N = the dry run's "rows to change"
    python tools/fix_statcan_frequency.py --rollback DIR/rollback.jsonl --apply --expect-rows N

--apply REFUSES: after T0 (core.cutover); unless the catalogue and the sidecar store are the live checkout's;
while any other Python process holds the catalogue open (catalog_statcan_tables.db_holders - one it cannot
inspect counts as a holder); unless it can take logs/statcan_writer.lock (the lock tools/statcan_new_cubes.py
writes the statcan rows under; never waits); when the plan's row count is not --expect-rows.

ORDER: plan -> rollback file (every planned row: series_id, old, new) and plan written and fsynced -> batches of
--batch rows, each ONE BEGIN IMMEDIATE transaction: `UPDATE series SET frequency=? WHERE series_id=? AND
frequency IS ?` (exactly 1 row, else the batch rolls back and the run stops), re-read and checked BEFORE commit
-> the whole range re-read: exactly the planned rows changed, nothing else did -> receipt.json and changed_ids.txt
(the ids a later D1 update would need; D1 is not touched here). A run killed part-way is resumed by re-running:
rows already corrected are no longer in the plan. Receipts: logs/statcan_freq_fix/<UTC stamp>_<pid>/. The
receipt also holds a sha256 over every other column of the statcan rows, before and after (they must match).

AFTER --apply (review AR-344): the self-hosted catalogue snapshot taken before it still carries the old labels,
and the snapshot step refuses while that copy exists. So: receipt OK -> move the old snapshot aside -> take a
new snapshot -> read two control rows in it: statcan:10100008 (daily) = D and statcan:10100004 (quarterly) = Q.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import glob
import hashlib
import importlib.util
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from core import catalog_path, cutover                    # noqa: E402
from updater import writer_lock                            # noqa: E402

LOCK_NAME = "statcan_writer"          # jobs/statcan_lane.LOCK_NAME (not imported: the lane module pins a backend)
WHAT = "tools/fix_statcan_frequency.py (an --apply run)"
LIVE_ROOT = catalog_path.LIVE_STORE_ROOT
CATALOGUE = catalog_path.under(ROOT)
STORE = os.path.join(ROOT, "data", "clean_full", "statcan")
RECEIPTS = os.path.join(ROOT, "logs", "statcan_freq_fix")
BATCH = 5_000
_CAT = os.path.join(ROOT, "tools", "catalog_statcan_tables.py")


class Refused(SystemExit):
    """The run must not write (the reason is the message)."""


def _cat():
    """tools/catalog_statcan_tables.py, for FREQ, SOURCE, source_range and db_holders (loaded by path)."""
    m = sys.modules.get("_catalog_statcan_tables_for_freq_fix")
    if m is None:
        tools_dir = os.path.join(ROOT, "tools")
        if tools_dir not in sys.path:
            sys.path.insert(0, tools_dir)
        spec = importlib.util.spec_from_file_location("_catalog_statcan_tables_for_freq_fix", _CAT)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        sys.modules["_catalog_statcan_tables_for_freq_fix"] = m
    return m


def _stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H%M%SZ")


def sidecar_codes(store: str) -> dict:
    """{productId (str): frequencyCode} from every .done sidecar in the store."""
    out = {}
    for f in glob.glob(os.path.join(store, "*.done")):
        with open(f, encoding="utf-8") as fh:
            d = json.load(fh)
        out[str(d.get("productId"))] = d.get("frequencyCode")
    return out


def read_range(con) -> dict:
    """{series_id: frequency} over the statcan primary-key range (an index search, never a scan)."""
    lo, hi = _cat().source_range()
    return dict(con.execute("SELECT series_id, frequency FROM series WHERE series_id >= ? AND series_id < ?",
                            (lo, hi)).fetchall())


def other_columns_hash(con) -> str:
    """sha256 over every statcan row's columns OTHER than frequency, in series_id order: the receipt's
    measured proof that the run changed nothing but frequency (review AR-344, finding 9)."""
    cols = [r[1] for r in con.execute("PRAGMA table_info(series)") if r[1] != "frequency"]
    lo, hi = _cat().source_range()
    h = hashlib.sha256()
    q = f"SELECT {', '.join(cols)} FROM series WHERE series_id >= ? AND series_id < ? ORDER BY series_id"
    for row in con.execute(q, (lo, hi)):
        h.update(json.dumps(row, ensure_ascii=False, default=str).encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def make_plan(rows: dict, codes: dict) -> tuple:
    """(changes [(series_id, old, new)], census {(code, old, new): rows}, problems [str])."""
    freq = _cat().FREQ
    lo, _hi = _cat().source_range()
    changes, census, problems = [], collections.Counter(), []
    for sid in sorted(rows):
        pid = sid[len(lo):].split("#", 1)[0]
        if not pid.isdigit():
            problems.append(f"{sid}: not a statcan:<productId> id")
            continue
        if pid not in codes:
            problems.append(f"{sid}: no .done sidecar for cube {pid}")
            continue
        old, new = rows[sid], freq.get(codes[pid])
        census[(codes[pid], old, new)] += 1
        if old != new:
            changes.append((sid, old, new))
    return changes, census, problems


def census_lines(census) -> list:
    key = lambda kv: (str(kv[0][0]), str(kv[0][1]))       # noqa: E731
    return [f"  code {str(c):>4}  {str(o):>9} -> {str(n):<9} rows {v:>9,}{'   CHANGE' if o != n else ''}"
            for (c, o, n), v in sorted(census.items(), key=key)]


def fts_has_frequency(con) -> bool:
    return any(r[1] == "frequency" for r in con.execute("PRAGMA table_info(series_fts)"))


def _write_lines(path: str, lines) -> None:
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for ln in lines:
            fh.write(ln + "\n")
        fh.flush()
        os.fsync(fh.fileno())


def apply_changes(con, changes: list, batch: int) -> int:
    """Write `changes` [(series_id, old, new)] in batches, each one transaction verified before commit."""
    done = 0
    for i in range(0, len(changes), batch):
        part = changes[i:i + batch]
        con.execute("BEGIN IMMEDIATE")
        try:
            for sid, old, new in part:
                n = con.execute("UPDATE series SET frequency=? WHERE series_id=? AND source_id='statcan' "
                                "AND frequency IS ?", (new, sid, old)).rowcount
                if n != 1:
                    raise RuntimeError(f"{sid}: {n} row(s) held frequency {old!r} - the row changed since the plan")
            for sid, _old, new in part:
                got = con.execute("SELECT frequency FROM series WHERE series_id=?", (sid,)).fetchone()
                if got is None or got[0] != new:
                    raise RuntimeError(f"{sid}: reads {got!r} after the update, want {new!r}")
        except BaseException:
            con.rollback()
            raise
        con.commit()
        done += len(part)
        print(f"  batch {i // batch + 1}: {done:,} of {len(changes):,} row(s) written", flush=True)
    return done


def live_problems(db: str, store: str) -> list:
    """--apply writes only the live checkout's catalogue, from its own sidecars, under its own lock dir."""
    same = lambda a, b: os.path.normcase(os.path.realpath(a)) == os.path.normcase(os.path.realpath(b))  # noqa: E731
    want = (("the catalogue", db, catalog_path.under(LIVE_ROOT)),
            ("the sidecar store", store, os.path.join(LIVE_ROOT, "data", "clean_full", "statcan")),
            ("the writer-lock dir", writer_lock.LOCK_DIR, os.path.join(LIVE_ROOT, "logs")))
    return [f"{name} is {got}, not {exp}" for name, got, exp in want if not same(got, exp)]


def load_rollback(path: str) -> list:
    """The rollback file as changes that restore it: [(series_id, new_now, old_then)]."""
    out = []
    with open(path, encoding="utf-8") as fh:
        for ln in fh:
            if ln.strip():
                r = json.loads(ln)
                out.append((r["series_id"], r["new"], r["old"]))
    return out


def main(argv=None) -> int:
    # allow_abbrev=False: __main__ takes the catalogue writer lock on the exact "--apply"; a prefix such as
    # "--app" must not reach main() as --apply without it (tests/test_catalogue_writers_main.py)
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    ap.add_argument("--apply", action="store_true", help="write (default: a dry report that writes nothing)")
    ap.add_argument("--expect-rows", type=int, default=None, help="--apply: the dry run's rows-to-change count")
    ap.add_argument("--rollback", default=None, help="a rollback.jsonl of an earlier --apply: restore it")
    ap.add_argument("--db", default=CATALOGUE, help="the catalogue (default: this checkout's)")
    ap.add_argument("--store", default=STORE, help="the statcan store holding the .done sidecars")
    ap.add_argument("--batch", type=int, default=BATCH)
    ap.add_argument("--receipts", default=RECEIPTS)
    a = ap.parse_args(argv)
    cutover.refuse_if_cut_over("fix_statcan_frequency - a pre-T0 fix of the local catalogue; after T0 the "
                               "catalogue build is the snapshot's source and this one-off is not its writer")
    if a.batch < 1:
        raise Refused("REFUSING: --batch must be at least 1")
    if a.apply:
        bad = live_problems(a.db, a.store)
        if bad:
            raise Refused("REFUSING --apply: only the live checkout may be written - " + "; ".join(bad))
        if a.expect_rows is None:
            raise Refused("REFUSING --apply without --expect-rows (run the dry report first and pass its count)")
        holders = _cat().db_holders(a.db)
        if holders:
            raise Refused(f"REFUSING --apply: other process(es) hold the catalogue open: {holders} - a catalogue "
                          "writer may be running; stop it (or wait) and re-run")
        writer_lock.hold_or_refuse(LOCK_NAME, WHAT)
    try:
        return _run(a)
    finally:
        if a.apply:
            writer_lock.release(LOCK_NAME)


def _run(a) -> int:
    con = catalog_path.connect_path(a.db, write=a.apply, timeout=180)
    try:
        con.execute("PRAGMA busy_timeout = 180000")
        if fts_has_frequency(con):
            raise Refused("REFUSING: series_fts has a frequency column - this tool updates series only")
        before = read_range(con)
        if a.rollback:
            changes = [c for c in load_rollback(a.rollback) if c[0] in before]
            problems = [f"{sid}: no longer in the catalogue" for sid, _n, _o in load_rollback(a.rollback)
                        if sid not in before]
            census = collections.Counter(("rollback", n, o) for _s, n, o in changes)
            changes = [c for c in changes if before[c[0]] == c[1]]      # rows still holding the fixed value
        else:
            changes, census, problems = make_plan(before, sidecar_codes(a.store))
        print(f"statcan rows in the catalogue: {len(before):,}; "
              f"{'rollback entries' if a.rollback else 'sidecars'} read; rows to change: {len(changes):,}")
        print("\n".join(census_lines(census)))
        if problems:
            print(f"PROBLEMS ({len(problems):,}), first: {problems[:5]}")
            if a.apply:
                raise Refused("REFUSING --apply: rows the plan cannot judge (above) - never guessed")
        if not a.apply:
            print("\nDRY: nothing written (--apply --expect-rows %d to write)" % len(changes))
            return 1 if problems else 0
        if len(changes) != a.expect_rows:
            raise Refused(f"REFUSING --apply: the plan holds {len(changes):,} row(s), --expect-rows says "
                          f"{a.expect_rows:,} - the catalogue or the sidecars moved since the dry run")
        if not changes:
            print("nothing to change: no row written, no receipt")
            return 0

        out = os.path.join(a.receipts, f"{_stamp()}_{os.getpid()}" + ("_rollback" if a.rollback else ""))
        os.makedirs(out)                     # never exist_ok: one run, one folder
        _write_lines(os.path.join(out, "rollback.jsonl"),
                     (json.dumps({"series_id": s, "old": o, "new": n}, ensure_ascii=False) for s, o, n in changes))
        _write_lines(os.path.join(out, "plan.json"), [json.dumps(
            {"db": os.path.realpath(a.db), "store": os.path.realpath(a.store), "rows_in_range": len(before),
             "rows_to_change": len(changes), "rollback_of": a.rollback,
             "census": [[c, o, n, v] for (c, o, n), v in census.items()]}, indent=1, ensure_ascii=False)])
        print(f"rollback file written first: {os.path.join(out, 'rollback.jsonl')}", flush=True)

        others_before = other_columns_hash(con)
        written = apply_changes(con, changes, a.batch)
        after = read_range(con)
        others_after = other_columns_hash(con)
        want = dict(before)
        want.update({s: n for s, _o, n in changes})
        wrong = sorted(s for s in want.keys() | after.keys() if want.get(s, "<absent>") != after.get(s, "<absent>"))
        receipt = {"finished_utc": _stamp(), "db": os.path.realpath(a.db), "rows_written": written,
                   "rows_in_range_before": len(before), "rows_in_range_after": len(after),
                   "before": dict(collections.Counter(str(v) for v in before.values())),
                   "after": dict(collections.Counter(str(v) for v in after.values())),
                   "rows_not_as_planned": len(wrong), "rows_not_as_planned_first": wrong[:20],
                   "other_columns_sha256_before": others_before, "other_columns_sha256_after": others_after,
                   "other_columns_unchanged": others_before == others_after,
                   "d1": "NOT touched - changed_ids.txt lists the ids a later D1 frequency update needs"}
        _write_lines(os.path.join(out, "receipt.json"), [json.dumps(receipt, indent=1)])
        _write_lines(os.path.join(out, "changed_ids.txt"), (s for s, _o, _n in changes))
        print(json.dumps(receipt, indent=1))
        if wrong:
            print(f"FAILED: {len(wrong):,} row(s) in the range differ from the plan - see {out}")
            return 1
        if others_before != others_after:
            print(f"FAILED: a column other than frequency changed in the statcan range during the run - see {out}")
            return 1
        print(f"OK: {written:,} row(s) rewritten, every other statcan row unchanged; receipts in {out}")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    if "--apply" in sys.argv[1:]:
        with catalog_path.write_session():   # after T0: the single-writer lock (refused anyway: see main())
            sys.exit(main())
    sys.exit(main())                         # a dry run reads only
