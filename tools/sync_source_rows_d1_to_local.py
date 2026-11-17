"""Copy catalogue rows that exist on D1 but not in the local catalog.db, for ONE source - by primary key.

WHY (2026-09-05). sec_edgar's CI refresher writes D1 only (a runner has no local catalogue), so
new registrants catalogued by CI since 2026-08-22 - and the retained legacy CIK-keyed ids of the
two-products history (task #81) - exist on D1 and are served, while the curated local copy lacks
them: 17,467 rows on D1 vs 17,306 locally after today's catch-up. verify_source_served compares
R2 with the LOCAL catalogue and reports every such row as "orphaned", which is the local mirror
lagging, not a served defect - but a lagging mirror is exactly how the R2 coherence copy ends up
missing rows the next time it is refreshed (R245/R250). This tool closes that gap the cheap way:
one D1 primary-key-RANGE read of the source (an index range, free class - never `WHERE
source_id=`, never a series_fts predicate), one local PK-range read, INSERT OR IGNORE of the
difference in chunks of a few hundred rows per IMMEDIATE transaction (R734: catalog.db is
journal_mode=delete and shared with the crawlers; one big COMMIT held them off for 25 minutes),
plus the FTS row for each inserted id (INSERT only - series_fts is fts5(series_id UNINDEXED, ...),
so any DELETE by id is a full scan).

Direction is D1 -> local ONLY, rows the local copy does not have. Nothing is updated (unless
--update-differing), nothing is deleted, D1 is never written. Dry run by default.

--update-differing (2026-11-17, review C2 of the sec_edgar repair): a row present on both sides
DIFFERS when ANY series column both sides carry disagrees - metadata, last_updated, frequency, unit,
geography, category, license_id, as well as start/end/title - and the UPDATE copies each differing
cell from D1. The receipt names the columns that differed for each id ("differing_columns") and holds
`before_after` = {id: {column: [local before, D1]}} for EVERY cell the UPDATE writes, so a rollback can
restore them from the receipt alone.

A D1 NULL over a local value is NOT copied by default (review of PR #274, R1919: on sec_edgar, 32
metadata cells holding a company's CIK, which D1 never had, would have been set to NULL). Those cells
are listed in the receipt ("null_over_local", [local, null]) either way, and copied only with
--allow-null-overwrite.

FTS (R1919 finding 3): when the UPDATE writes a row's title or geography, the row's series_fts entry is
replaced in the same chunk transaction by the rowid road (as tools/clean_catalogue_titles.py): MATCH the
longest word of the OLD title (an index lookup), keep the hit whose series_id and title equal the old
values, DELETE that rowid, INSERT the new row. Never a series_fts DELETE by series_id (a full scan per
row). Exactly one such hit is required: the dry run reports the ids without one ("fts_unfound") and
--apply then refuses before any write; the transaction checks again.

Values are compared as text (str() on both sides, NULL only equals NULL). A D1 JSON number and a local
text of a different spelling (1.0 against '1') report a false difference, and the UPDATE would store
the D1 spelling. sec_edgar's values are text or NULL only; check the types before running this for a
source with numeric JSON values.

Concurrency, before T0 (the only time this tool runs): --apply refuses while logs/local_heavy.lock is
present (as clear_csv_desktop_owed.py does); each chunk transaction re-reads its rows and refuses if
any changed since the read; afterwards the local rows are read again and compared with the SAME D1
rows (no second D1 read): exit 1 unless nothing is missing, nothing (outside the kept D1-NULL cells)
differs, and each replaced FTS row is found. Counters count only committed rows.

The catalogue is opened through core.catalog_path (t0_ready legacy-catalogue): read-only for the
reads, write=True for the apply, and the __main__ block holds the single-writer lock for an --apply
run (tests/test_catalogue_writers_main.py). The tool is refused after T0 before anything else.

    python tools/sync_source_rows_d1_to_local.py --source sec_edgar [--apply] [--chunk 400]
                                                 [--update-differing] [--allow-null-overwrite]
                                                 [--receipt-dir DIR]
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
import re
import sqlite3
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
from core import catalog_path  # noqa: E402 - the one catalogue resolver

DB = catalog_path.under(ROOT)
# The receipt's default folder: the checkout's logs/ (git-ignored). It was a temp folder on the system drive (where
# no work may go) when that folder existed, and the checkout root otherwise.
RECEIPT_DIR = os.path.join(ROOT, "logs")
# A heavy local pass is mid-run while this exists (clear_csv_desktop_owed.py, derive_csv_bulk.py): --apply refuses.
HEAVY_LOCK = os.path.join(ROOT, "logs", "local_heavy.lock")
FTS_COLS = ("title", "geography")


class Refused(Exception):
    """A write the tool will not make; the chunk is rolled back and the run stops (exit 1)."""


def _same(a, b) -> bool:
    """One column's two values agree. Text-compared, as the span columns always were (str() on both sides),
    so a D1 JSON number and a local integer of the same value agree; NULL agrees only with NULL."""
    if a is None or b is None:
        return a is None and b is None
    return str(a) == str(b)


def d1_rows(source: str):
    """Every D1 `series` row of the source, by primary-key range. One statement; (rows, rows_read).
    The road, the auth-transient retries and the post-T0 rules are core.d1_remote's (plan step 1)."""
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core import d1_remote                                           # noqa: PLC0415 - plan step 1
    sql = (f"SELECT * FROM series WHERE series_id >= '{source}:' AND series_id < '{source};'")
    try:
        return d1_remote.rows("econ-catalog", sql)
    except RuntimeError as e:
        raise RuntimeError(f"D1 read failed: {e}") from None


def _read_con():
    con = catalog_path.connect_path(DB, write=False, timeout=120)
    con.execute("PRAGMA busy_timeout=120000")
    return con


def local_rows(source: str) -> dict:
    """{series_id: {column: value}} - EVERY series column - by primary-key range. Read-only."""
    con = _read_con()
    try:
        cur = con.execute("SELECT * FROM series WHERE series_id >= ? AND series_id < ?",
                          (source + ":", source + ";"))
        cols = [d[0] for d in cur.description]
        return {r[0]: dict(zip(cols, r)) for r in cur}
    finally:
        con.close()


def fts_hits(con, sid: str, title) -> list:
    """The series_fts rows (rowid, series_id, title, geography) of `sid` holding `title`, found by an FTS MATCH on
    the title's longest word - an index lookup, never a scan (tools/clean_catalogue_titles.py _index_row)."""
    words = re.findall(r"\w{3,}", title or "")
    if not words:
        return []
    q = '"%s"' % max(words, key=len).replace('"', '""')
    return [r for r in con.execute("SELECT rowid, series_id, title, geography FROM series_fts WHERE series_fts MATCH ?",
                                   (f"title:{q}",)) if r[1] == sid and r[2] == title]


def differing_columns(d1_row: dict, local_row: dict) -> list:
    """The series columns both sides carry (series_id aside) whose values disagree, in the local order."""
    return [c for c in local_row if c != "series_id" and c in d1_row and not _same(d1_row[c], local_row[c])]


def plan_cells(d1_row: dict, local_row: dict, allow_null: bool) -> tuple[dict, dict]:
    """(write, kept) for one row on both sides: write = {column: [local before, D1]} for each differing cell the
    UPDATE copies; kept = the D1-NULL-over-a-local-value cells it leaves as they are (none with allow_null)."""
    write, kept = {}, {}
    for c in differing_columns(d1_row, local_row):
        pair = [local_row[c], d1_row[c]]
        if d1_row[c] is None and local_row[c] is not None and not allow_null:
            kept[c] = pair
        else:
            write[c] = pair
    return write, kept


def residual(rows: list, now: dict, kept: dict, check_diff: bool) -> tuple[list, list]:
    """After the apply, against the SAME D1 rows: (ids still missing locally, ids still differing). A kept cell
    must still hold its local value; any other differing cell counts. check_diff=False: missing only."""
    miss = [r["series_id"] for r in rows if r.get("series_id") and r["series_id"] not in now]
    bad = []
    if check_diff:
        for r in rows:
            sid = r.get("series_id")
            if sid not in now:
                continue
            k = kept.get(sid, {})
            cols = [c for c in differing_columns(r, now[sid]) if c not in k]
            cols += [c for c, (before, _d1) in k.items() if not _same(now[sid].get(c), before)]
            if cols:
                bad.append(sid)
    return miss, bad


def local_ids(source: str) -> set:
    return set(local_rows(source))


def _dump(receipt: dict, rpath: str) -> None:
    with open(rpath, "w", encoding="utf-8") as f:
        json.dump(receipt, f, indent=1)


def _chunked(con, part: list, body, label: str) -> tuple:
    """Run body(part) -> counters inside ONE IMMEDIATE transaction; retry a busy catalogue (R734). The counters are
    returned only after COMMIT, so a COMMIT that fails and is retried never counts a row twice. Refused propagates
    after the rollback; None after 12 failed attempts."""
    for attempt in range(12):
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                counts = body(part)
            except Refused:
                con.execute("ROLLBACK")
                raise
            con.execute("COMMIT")
            return counts
        except sqlite3.OperationalError as e:
            print(f"   {label} attempt {attempt + 1}: {e} - retrying in 20 s", flush=True)
            try:
                con.execute("ROLLBACK")
            except Exception:      # noqa: BLE001
                pass
            time.sleep(20)
    return None


def main() -> int:
    # AFTER T0 this tool reads the FROZEN cloud copy and writes the live local data: refused first, before
    # its arguments (neither the R2 guard nor d1_remote stops a READ; tests/test_verifiers_6d.py lists why).
    if ROOT not in sys.path:
        sys.path.insert(0, ROOT)
    from core import cutover                                          # noqa: PLC0415
    cutover.refuse_if_cut_over('sync_source_rows_d1_to_local - it copies FROZEN D1 rows into the live catalogue (and would bring back rows removed after T0)')
    # allow_abbrev=False: __main__ takes the writer lock on the exact "--apply"; a prefix such as "--app" must not
    # reach main() as --apply without it (tests/test_catalogue_writers_main.py)
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0], allow_abbrev=False)
    ap.add_argument("--source", required=True)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true",
                    help="explicit no-write run (already the default without --apply); named in ARGV so the D1 cost "
                         "guard can tell a free run from a charged one (R323)")
    ap.add_argument("--chunk", type=int, default=400)
    ap.add_argument("--update-differing", action="store_true",
                    help="also UPDATE local rows where ANY series column both sides carry differs from D1, copying "
                         "each differing cell (D1 is the truth for a CI-catalogued source, R737 item c; review C2); "
                         "the differing rows are reported either way")
    ap.add_argument("--allow-null-overwrite", action="store_true",
                    help="with --update-differing: also copy a D1 NULL over a local value (default: keep the local "
                         "value and list the cell in the receipt's null_over_local)")
    ap.add_argument("--receipt-dir", default=RECEIPT_DIR,
                    help="folder for the JSON receipt (default: the checkout's logs/, git-ignored)")
    a = ap.parse_args()
    if not os.path.exists(DB):
        print(f"no local catalogue at {DB}"); return 2
    if a.apply and os.path.exists(HEAVY_LOCK):
        print(f"REFUSED: {HEAVY_LOCK} is present - a heavy local pass may be mid-run (R340/R529). Re-run after it.")
        return 2
    t0 = dt.datetime.now(dt.timezone.utc)
    rows, rows_read = d1_rows(a.source)
    loc = local_rows(a.source)
    have = set(loc)
    missing = [r for r in rows if r.get("series_id") and r["series_id"] not in have]
    # THE DIFF (R737 item c): rows present on both sides that disagree. The served-state verify compares the
    # two stores by COUNT only, so a `--skip-local` respan, a non-fatal local branch, or a CI-only write leaves
    # them apart invisibly (the control row sec_edgar:AAPL sat at 2026-04-17 locally and 2026-07-17 on D1).
    # Review C2 (2026-11-17): EVERY series column both sides carry, not only start/end/title.
    diff_cols = {r["series_id"]: differing_columns(r, loc[r["series_id"]])
                 for r in rows if r.get("series_id") in loc}
    differing = [r for r in rows if diff_cols.get(r.get("series_id"))]
    plans = {r["series_id"]: plan_cells(r, loc[r["series_id"]], a.allow_null_overwrite) for r in differing}
    by_id = {r["series_id"]: r for r in differing}
    null_over_local = {sid: {c: [loc[sid][c], None] for c in diff_cols[sid]
                             if loc[sid][c] is not None and by_id[sid][c] is None} for sid in by_id}
    null_over_local = {sid: cells for sid, cells in null_over_local.items() if cells}
    kept = {sid: p[1] for sid, p in plans.items() if p[1]}
    todo_upd = [r for r in differing if plans[r["series_id"]][0]] if a.update_differing else []
    fts_todo = {r["series_id"]: loc[r["series_id"]].get("title") for r in todo_upd
                if set(plans[r["series_id"]][0]) & set(FTS_COLS)}
    fts_unfound = []
    if fts_todo:
        con = _read_con()
        try:
            fts_unfound = [sid for sid, old in fts_todo.items() if len(fts_hits(con, sid, old)) != 1]
        finally:
            con.close()
    print(f"{t0:%H:%M:%SZ} {a.source}: D1 rows {len(rows):,} (rows_read {rows_read:,}); local rows {len(have):,}; "
          f"on D1 and NOT local: {len(missing):,}; on both and DIFFERING (any column): {len(differing):,}; "
          f"D1 NULL over a local value: {sum(map(len, null_over_local.values())):,} cell(s) in "
          f"{len(null_over_local):,} row(s) ({'copied' if a.allow_null_overwrite else 'kept'})", flush=True)
    for r in differing[:8]:
        l = loc[r["series_id"]]
        cols = diff_cols[r["series_id"]]
        other = [c for c in cols if c not in ("start_date", "end_date", "title")]
        print(f"   differ {r['series_id']:<26} local ({l.get('start_date')}, {l.get('end_date')}) "
              f"D1 ({r.get('start_date')}, {r.get('end_date')})"
              f"{'  title differs' if 'title' in cols else ''}"
              f"{'  also: ' + ', '.join(other) if other else ''}")
    if len(differing) > 8:
        print(f"   ... {len(differing) - 8:,} more differing")
    for r in missing[:12]:
        print(f"   {r['series_id']:<28} {str(r.get('start_date')):10} {str(r.get('end_date')):10} {str(r.get('title'))[:60]}")
    if len(missing) > 12:
        print(f"   ... {len(missing) - 12:,} more")
    if fts_todo:
        print(f"   FTS rows to replace: {len(fts_todo):,}; without exactly one index row: {len(fts_unfound):,} "
              f"{fts_unfound[:10]}")
    receipt = {"utc": t0.isoformat(), "source": a.source, "d1_rows": len(rows), "d1_rows_read": rows_read,
               "local_rows_before": len(have), "missing": [r["series_id"] for r in missing],
               "differing": [r["series_id"] for r in differing], "apply": a.apply, "update_differing": a.update_differing,
               "differing_columns": {r["series_id"]: diff_cols[r["series_id"]] for r in differing},
               "allow_null_overwrite": a.allow_null_overwrite, "null_over_local": null_over_local,
               "before_after": {r["series_id"]: plans[r["series_id"]][0] for r in todo_upd},
               "fts_to_replace": sorted(fts_todo), "fts_unfound": fts_unfound}
    os.makedirs(a.receipt_dir, exist_ok=True)
    rpath = os.path.join(a.receipt_dir, f"sync_rows_{a.source}_{t0:%Y%m%dT%H%M%SZ}.json")
    _dump(receipt, rpath)                     # the plan, with every before value, is on disk before any write
    if not a.apply:
        print(f"dry run - nothing written; receipt {rpath}")
        return 0
    if fts_unfound:
        receipt["refused"] = f"{len(fts_unfound)} row(s) without exactly one FTS index row for the old title"
        _dump(receipt, rpath)
        print(f"REFUSED before any write: {receipt['refused']}: {fts_unfound[:10]}; receipt {rpath}")
        return 1
    if not missing and not todo_upd:
        print(f"nothing to write; receipt {rpath}")
        return 0

    # The write road: before T0 the checkout's catalogue read-write (mode=rw, never created); after T0 this tool
    # is refused above, and connect_path(write=True) would refuse anyway without the writer lock __main__ holds.
    con = catalog_path.connect_path(DB, write=True, timeout=120, isolation_level=None)
    con.execute("PRAGMA busy_timeout=120000")
    updated = fts_replaced = inserted = fts = 0

    def recheck(sid):
        cur = con.execute("SELECT * FROM series WHERE series_id=?", (sid,))
        cols = [d[0] for d in cur.description]
        now = cur.fetchone()
        if now is None or dict(zip(cols, now)) != loc[sid]:
            raise Refused(f"{sid}: the local row changed after the read")

    def upd_body(part):
        n_upd = n_fts = 0
        for r in part:
            sid = r["series_id"]
            recheck(sid)
            cells = plans[sid][0]
            hits = None
            if sid in fts_todo:
                hits = fts_hits(con, sid, loc[sid].get("title"))
                if len(hits) != 1:
                    raise Refused(f"{sid}: {len(hits)} FTS index row(s) for the old title, not 1")
            if con.execute(f"UPDATE series SET {', '.join(c + '=?' for c in cells)} WHERE series_id=?",
                           [r[c] for c in cells] + [sid]).rowcount != 1:
                raise Refused(f"{sid}: the UPDATE matched no row")
            n_upd += 1
            if hits:
                new = {**loc[sid], **{c: r[c] for c in cells}}
                if con.execute("DELETE FROM series_fts WHERE rowid=? AND series_id=? AND title=?",
                               (hits[0][0], sid, loc[sid].get("title"))).rowcount != 1:
                    raise Refused(f"{sid}: the FTS index row changed under the delete")
                con.execute("INSERT INTO series_fts (series_id, title, geography) VALUES (?,?,?)",
                            (sid, new.get("title"), new.get("geography")))
                n_fts += 1
        return n_upd, n_fts

    local_cols = [c[1] for c in con.execute("PRAGMA table_info(series)")]
    d1_cols = [c for c in missing[0].keys() if c in local_cols] if missing else []
    ins = f"INSERT OR IGNORE INTO series ({', '.join(d1_cols)}) VALUES ({', '.join('?' for _ in d1_cols)})"

    def ins_body(part):
        n_ins = n_fts = 0
        for r in part:
            if con.execute(ins, [r.get(c) for c in d1_cols]).rowcount != 1:
                raise Refused(f"{r['series_id']}: a local row appeared after the read")
            n_ins += 1
            con.execute("INSERT INTO series_fts (series_id, title, geography) VALUES (?,?,?)",
                        (r["series_id"], r.get("title"), r.get("geography")))
            n_fts += 1
        return n_ins, n_fts

    try:
        for k in range(0, len(todo_upd), a.chunk):
            got = _chunked(con, todo_upd[k:k + a.chunk], upd_body, f"update chunk {k // a.chunk + 1}")
            if got is None:
                receipt.update({"updated": updated, "fts_replaced": fts_replaced})
                _dump(receipt, rpath)
                print(f"update chunk never committed after {updated:,} row(s); receipt {rpath}"); return 1
            updated += got[0]
            fts_replaced += got[1]
        if todo_upd:
            print(f"   updated {updated:,} differing local row(s) from D1; {fts_replaced:,} FTS row(s) replaced", flush=True)
        if missing:
            absent = [c for c in local_cols if c not in d1_cols]
            print(f"  columns copied: {d1_cols}; local-only columns left NULL: {absent}")
            chunks = [missing[i:i + a.chunk] for i in range(0, len(missing), a.chunk)]
            for k, part in enumerate(chunks, 1):
                got = _chunked(con, part, ins_body, f"chunk {k}")
                if got is None:
                    receipt.update({"updated": updated, "fts_replaced": fts_replaced, "inserted": inserted,
                                    "fts_inserted": fts, "stopped_at_chunk": k})
                    _dump(receipt, rpath)
                    print(f"chunk {k} never committed after {inserted:,} row(s); receipt {rpath}"); return 1
                inserted += got[0]
                fts += got[1]
                print(f"   chunk {k}/{len(chunks)}: {inserted:,} row(s) inserted so far", flush=True)
    except Refused as e:
        receipt.update({"updated": updated, "fts_replaced": fts_replaced, "inserted": inserted, "fts_inserted": fts,
                        "refused": str(e)})
        _dump(receipt, rpath)
        print(f"REFUSED (that chunk rolled back): {e}; committed before it: {updated:,} updated, "
              f"{inserted:,} inserted; receipt {rpath}")
        return 1
    finally:
        con.close()

    # VERIFY against the SAME D1 rows (no second D1 read): nothing missing, nothing differing outside the kept
    # D1-NULL cells, and each replaced FTS row found by its new title.
    now = local_rows(a.source)
    miss_after, diff_after = residual(rows, now, kept, a.update_differing)
    rcon = _read_con()
    try:
        fts_bad = [sid for sid in fts_todo if sid in now and len(fts_hits(rcon, sid, now[sid].get("title"))) != 1]
    finally:
        rcon.close()
    ok = not miss_after and not diff_after and not fts_bad
    receipt.update({"updated": updated, "fts_replaced": fts_replaced, "inserted": inserted, "fts_inserted": fts,
                    "local_rows_after": len(now),
                    "verify": {"ok": ok, "missing": miss_after, "differing": diff_after if a.update_differing else None,
                               "fts_not_found": fts_bad}})
    _dump(receipt, rpath)
    print(f"DONE: updated {updated:,} row(s) ({fts_replaced:,} FTS replaced); inserted {inserted:,} row(s) + {fts:,} "
          f"FTS row(s); local {a.source} rows {len(have):,} -> {len(now):,} (D1 {len(rows):,}); verify "
          f"{'OK' if ok else 'FAILED'} (missing {len(miss_after):,}, differing {len(diff_after):,}, "
          f"FTS not found {len(fts_bad):,}); receipt {rpath}")
    return 0 if ok else 1


if __name__ == "__main__":
    if "--apply" not in sys.argv[1:]:
        sys.exit(main())                     # a dry run reads only: no lock
    else:
        with catalog_path.write_session():   # after T0: the single-writer lock (main() refuses after T0 anyway)
            sys.exit(main())
