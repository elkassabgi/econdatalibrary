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
geography, category, license_id, as well as start/end/title - and the UPDATE copies every one of
those columns from D1, not only the span and the title. The receipt names the columns that
differed for each id ("differing_columns"). The FTS row is not touched by an UPDATE (as before: a
series_fts DELETE by id is a full scan), so a title or geography change reaches search only on the
next FTS rebuild.

The catalogue is opened through core.catalog_path (t0_ready legacy-catalogue): read-only for the
reads, write=True for the apply, and the __main__ block holds the single-writer lock for an --apply
run (tests/test_catalogue_writers_main.py). The tool is refused after T0 before anything else.

    python tools/sync_source_rows_d1_to_local.py --source sec_edgar [--apply] [--chunk 400]
                                                 [--update-differing] [--receipt-dir DIR]
"""
from __future__ import annotations
import argparse
import datetime as dt
import json
import os
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


def local_rows(source: str) -> dict:
    """{series_id: {column: value}} - EVERY series column - by primary-key range. Read-only."""
    con = catalog_path.connect_path(DB, write=False, timeout=120)
    con.execute("PRAGMA busy_timeout=120000")
    try:
        cur = con.execute("SELECT * FROM series WHERE series_id >= ? AND series_id < ?",
                          (source + ":", source + ";"))
        cols = [d[0] for d in cur.description]
        return {r[0]: dict(zip(cols, r)) for r in cur}
    finally:
        con.close()


def differing_columns(d1_row: dict, local_row: dict) -> list:
    """The series columns both sides carry (series_id aside) whose values disagree, in the local order."""
    return [c for c in local_row if c != "series_id" and c in d1_row and not _same(d1_row[c], local_row[c])]


def local_ids(source: str) -> set:
    return set(local_rows(source))


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
                         "every such column (D1 is the truth for a CI-catalogued source, R737 item c; review C2); "
                         "the differing rows are reported either way")
    ap.add_argument("--receipt-dir", default=RECEIPT_DIR,
                    help="folder for the JSON receipt (default: the checkout's logs/, git-ignored)")
    a = ap.parse_args()
    if not os.path.exists(DB):
        print(f"no local catalogue at {DB}"); return 2
    t0 = dt.datetime.now(dt.timezone.utc)
    rows, rows_read = d1_rows(a.source)
    loc = local_rows(a.source)
    have = set(loc)
    missing = [r for r in rows if r.get("series_id") and r["series_id"] not in have]
    # THE DIFF (R737 item c): rows present on both sides whose start/end/title disagree. The
    # served-state verify compares the two stores by COUNT only, so a `--skip-local` respan, a
    # non-fatal local branch, or a CI-only write leaves them apart invisibly (the control row
    # sec_edgar:AAPL sat at 2026-04-17 locally and 2026-07-17 on D1).
    # Review C2 (2026-11-17): EVERY series column both sides carry, not only start/end/title - a stale
    # metadata, last_updated, frequency, unit, geography, category or license_id was invisible before.
    diff_cols = {r["series_id"]: differing_columns(r, loc[r["series_id"]])
                 for r in rows if r.get("series_id") in loc}
    differing = [r for r in rows if diff_cols.get(r.get("series_id"))]
    print(f"{t0:%H:%M:%SZ} {a.source}: D1 rows {len(rows):,} (rows_read {rows_read:,}); local rows {len(have):,}; "
          f"on D1 and NOT local: {len(missing):,}; on both and DIFFERING (any column): {len(differing):,}", flush=True)
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
    receipt = {"utc": t0.isoformat(), "source": a.source, "d1_rows": len(rows), "d1_rows_read": rows_read,
               "local_rows_before": len(have), "missing": [r["series_id"] for r in missing],
               "differing": [r["series_id"] for r in differing], "apply": a.apply, "update_differing": a.update_differing,
               "differing_columns": {r["series_id"]: diff_cols[r["series_id"]] for r in differing}}
    os.makedirs(a.receipt_dir, exist_ok=True)
    rpath = os.path.join(a.receipt_dir, f"sync_rows_{a.source}_{t0:%Y%m%dT%H%M%SZ}.json")
    todo_upd = differing if (a.apply and a.update_differing) else []
    if not a.apply or (not missing and not todo_upd):
        json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
        print(f"dry run - nothing written; receipt {rpath}" if not a.apply else f"nothing to write; receipt {rpath}")
        return 0

    # The write road: before T0 the checkout's catalogue read-write (mode=rw, never created); after T0 this tool
    # is refused above, and connect_path(write=True) would refuse anyway without the writer lock __main__ holds.
    con = catalog_path.connect_path(DB, write=True, timeout=120, isolation_level=None)
    con.execute("PRAGMA busy_timeout=120000")
    updated = 0
    if todo_upd:
        # ALL the columns both sides carry, from D1 (review C2), in the local table's order.
        local_cols = [c[1] for c in con.execute("PRAGMA table_info(series)")]
        upd_cols = [c for c in local_cols if c != "series_id" and c in todo_upd[0]]
        upd = f"UPDATE series SET {', '.join(c + '=?' for c in upd_cols)} WHERE series_id=?"
        for k in range(0, len(todo_upd), a.chunk):
            part = todo_upd[k:k + a.chunk]
            for attempt in range(12):
                try:
                    con.execute("BEGIN IMMEDIATE")
                    for r in part:
                        con.execute(upd, [r.get(c) for c in upd_cols] + [r["series_id"]])
                        updated += 1
                    con.execute("COMMIT")
                    break
                except sqlite3.OperationalError as e:
                    print(f"   update chunk {k // a.chunk + 1} attempt {attempt + 1}: {e} - retrying in 20 s", flush=True)
                    try:
                        con.execute("ROLLBACK")
                    except Exception:      # noqa: BLE001
                        pass
                    time.sleep(20)
            else:
                receipt["updated"] = updated
                json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
                print(f"update chunk never committed after {updated:,} row(s); receipt {rpath}"); return 1
        print(f"   updated {updated:,} differing local row(s) from D1", flush=True)
        receipt["updated"] = updated
    if not missing:
        json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
        print(f"DONE: updated {updated:,} differing row(s), nothing to insert; receipt {rpath}")
        con.close()
        return 0
    local_cols = [c[1] for c in con.execute("PRAGMA table_info(series)")]
    d1_cols = [c for c in missing[0].keys() if c in local_cols]
    absent = [c for c in local_cols if c not in d1_cols]
    print(f"  columns copied: {d1_cols}; local-only columns left NULL: {absent}")
    ins = f"INSERT OR IGNORE INTO series ({', '.join(d1_cols)}) VALUES ({', '.join('?' for _ in d1_cols)})"
    inserted = fts = 0
    chunks = [missing[i:i + a.chunk] for i in range(0, len(missing), a.chunk)]
    for k, part in enumerate(chunks, 1):
        for attempt in range(12):
            try:
                con.execute("BEGIN IMMEDIATE")
                for r in part:
                    cur = con.execute(ins, [r.get(c) for c in d1_cols])
                    if cur.rowcount == 1:
                        inserted += 1
                        con.execute("INSERT INTO series_fts (series_id, title, geography) VALUES (?,?,?)",
                                    (r["series_id"], r.get("title"), r.get("geography")))
                        fts += 1
                con.execute("COMMIT")
                break
            except sqlite3.OperationalError as e:
                print(f"   chunk {k} attempt {attempt + 1}: {e} - retrying in 20 s", flush=True)
                try:
                    con.execute("ROLLBACK")
                except Exception:              # noqa: BLE001
                    pass
                time.sleep(20)
        else:
            receipt.update({"inserted": inserted, "fts_inserted": fts, "stopped_at_chunk": k})
            json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
            print(f"chunk {k} never committed after {inserted:,} row(s); receipt {rpath}"); return 1
        print(f"   chunk {k}/{len(chunks)}: {inserted:,} row(s) inserted so far", flush=True)
    after = len(local_ids(a.source))
    receipt.update({"inserted": inserted, "fts_inserted": fts, "local_rows_after": after})
    json.dump(receipt, open(rpath, "w", encoding="utf-8"), indent=1)
    print(f"DONE: inserted {inserted:,} row(s) + {fts:,} FTS row(s); local {a.source} rows {len(have):,} -> {after:,} "
          f"(D1 {len(rows):,}); receipt {rpath}")
    con.close()
    return 0 if after == len(rows) else 1


if __name__ == "__main__":
    if "--apply" not in sys.argv[1:]:
        sys.exit(main())                     # a dry run reads only: no lock
    else:
        with catalog_path.write_session():   # after T0: the single-writer lock (main() refuses after T0 anyway)
            sys.exit(main())
