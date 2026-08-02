"""Delta-sync CATALOG SERIES ROWS to Cloudflare D1 — the half the pipeline was missing.

WHY: core/sync_state_d1.py syncs the freshness projection (unit_state, source_state)
after every updater run, and its docstring is explicit that it "never full-dumps the
catalog". That was the right call for freshness — but it left NO automatic path for a
NEW SERIES to reach the serving catalog. The only catalog path was a manual ~945 MB
full re-dump via core/export_d1.py.

The consequence was invisible and cumulative: a fetcher merges rows, the orchestrator
derives the series' CSV and PUTs it to R2, and the data is genuinely hosted and
downloadable by id — but it never appears in /v1/catalog, so nobody can find it. A
2026-07-27 reconciliation across all series-level sources found 31,259 such series:

    boe        30,674 local /     21 in D1   (20,650 already had CSVs sitting in R2)
    unhcr      18,670 local / 18,367 in D1
    ksh_stadat 97,520 local / 97,297 in D1
    insee_bdm 101,848 local /101,768 in D1

boe is the clearest case: the fetcher was promoted to live and had been updating daily
for weeks while users could see 21 of its 30,674 series.

This module closes that loop. Rows are upserted (INSERT OR REPLACE), so re-running is
harmless, and series_fts is kept in step — a search index that silently lags the table
it indexes is the same class of bug one layer down.

D1 rules honored, same as its sibling: no BEGIN/COMMIT/PRAGMA, ~20-row multi-VALUES
statements, files chunked under the wrangler payload limit, and the emitted SQL is
verified by replay into in-memory SQLite BEFORE any wrangler call.

Usage:
    python core/sync_catalog_d1.py --ids-file data/_aqueduct/pending_catalog_sync.txt
    python core/sync_catalog_d1.py --source boe          # reconcile one whole source
    python core/sync_catalog_d1.py --source boe --dry-run
"""
from __future__ import annotations

import argparse
import os
import sqlite3
import sys
import tempfile

# Run as a script (`python core/sync_catalog_d1.py`, which is how the workflow calls
# it) the repo root is not on sys.path, so `import core.*` fails. Bootstrap before
# the sibling import rather than relying on the caller's cwd.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))

from core.sync_state_d1 import (MAX_FILE_BYTES, ROWS_PER_STMT, ROOT, _lit,  # noqa: E402
                                execute_remote)

CATALOG_DB = os.path.abspath(os.environ.get("ECONDL_CATALOG")
                             or os.path.join(ROOT, "data", "catalog.db"))
# Written by the orchestrator: one series_id per line, appended whenever a CSV is
# derived. Consumed and truncated by this script so a series is synced once.
PENDING = os.path.join(
    os.path.abspath(os.environ.get("AQUEDUCT_STATE_DIR")
                    or os.path.join(ROOT, "data", "_aqueduct")),
    "pending_catalog_sync.txt")


def _rows_for(conn: sqlite3.Connection, ids: list[str]) -> tuple[list[str], list[dict]]:
    cols = [d[0] for d in conn.execute("SELECT * FROM series LIMIT 1").description]
    out, seen = [], set()
    for sid in ids:
        if sid in seen:
            continue
        seen.add(sid)
        r = conn.execute("SELECT * FROM series WHERE series_id=?", (sid,)).fetchone()
        if r is not None:            # absent locally => nothing to advertise; skip quietly
            out.append(dict(zip(cols, r)))
    return cols, out


def emit_sql(cols: list[str], rows: list[dict], out_dir: str) -> list[str]:
    """Chunked INSERT OR REPLACE for `series`, plus matching `series_fts` rows."""
    collist = ", ".join(cols)
    stmts: list[str] = []
    for i in range(0, len(rows), ROWS_PER_STMT):
        ch = rows[i:i + ROWS_PER_STMT]
        vals = ",\n  ".join("(%s)" % ", ".join(_lit(r[c]) for c in cols) for r in ch)
        stmts.append(f"INSERT OR REPLACE INTO series ({collist}) VALUES\n  {vals};")
    # FTS is a contentless-style mirror here: delete-then-insert would need a
    # matching delete command per row, so we mirror export_d1's approach and just
    # insert. Duplicate FTS rows only ever cost a repeated search hit, whereas a
    # MISSING one makes the series unfindable — the asymmetry favours inserting.
    for i in range(0, len(rows), ROWS_PER_STMT):
        ch = rows[i:i + ROWS_PER_STMT]
        vals = ",\n  ".join(
            "(%s,%s,%s)" % (_lit(r["series_id"]), _lit(r.get("title")),
                            _lit(r.get("geography"))) for r in ch)
        stmts.append("INSERT INTO series_fts (series_id,title,geography) VALUES\n  "
                     f"{vals};")

    os.makedirs(out_dir, exist_ok=True)
    files, buf, n = [], [], 0
    for s in stmts:
        if buf and n + len(s) > MAX_FILE_BYTES:
            p = os.path.join(out_dir, f"catalog_{len(files):04d}.sql")
            with open(p, "w", encoding="utf-8") as fh:
                fh.write("\n".join(buf) + "\n")
            files.append(p); buf, n = [], 0
        buf.append(s); n += len(s) + 1
    if buf:
        p = os.path.join(out_dir, f"catalog_{len(files):04d}.sql")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write("\n".join(buf) + "\n")
        files.append(p)
    return files


def verify_replay(cols: list[str], rows: list[dict], files: list[str]) -> None:
    """Replay the emitted SQL into a fresh in-memory SQLite and assert row-for-row
    equality with what we intended to send. Broken SQL never reaches remote D1."""
    mem = sqlite3.connect(":memory:")
    mem.execute(f"CREATE TABLE series ({', '.join(c + ' TEXT' for c in cols)}, "
                "PRIMARY KEY (series_id))")
    mem.execute("CREATE VIRTUAL TABLE series_fts USING fts5"
                "(series_id UNINDEXED, title, geography)")
    for p in files:
        with open(p, encoding="utf-8") as fh:
            mem.executescript(fh.read())
    got = mem.execute("SELECT COUNT(*) FROM series").fetchone()[0]
    if got != len(rows):
        raise SystemExit(f"FATAL: replay has {got} series rows, expected {len(rows)} "
                         "— refusing to send SQL that does not round-trip")
    for r in rows[:50]:
        hit = mem.execute("SELECT title FROM series WHERE series_id=?",
                          (r["series_id"],)).fetchone()
        if hit is None or hit[0] != (r.get("title") if r.get("title") is not None
                                     else None):
            raise SystemExit(f"FATAL: replay lost/altered {r['series_id']}")
    mem.close()
    print(f"  verified: {len(rows)} series rows replay cleanly ({len(files)} file(s))")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--source", help="reconcile every local catalog row for this source")
    ap.add_argument("--ids-file", help="file of series_ids, one per line "
                                       f"(default: {PENDING} when it exists)")
    ap.add_argument("--dry-run", action="store_true",
                    help="emit + verify, execute nothing")
    ap.add_argument("--keep-pending", action="store_true",
                    help="do not truncate the pending file after a successful sync")
    a = ap.parse_args(argv)

    # WAIT FOR A WRITER INSTEAD OF DYING ON IT. Every other tool here opens catalog.db with a
    # busy timeout; this one did not, so a concurrent catalogue build -- an ordinary thing, since
    # cataloguing a source and syncing another are independent jobs -- aborted the sync outright
    # with "database is locked". A read-only connection still has to wait out a writer's lock.
    conn = sqlite3.connect(f"file:{CATALOG_DB}?mode=ro", uri=True, timeout=300.0)
    conn.execute("PRAGMA busy_timeout = 300000")
    if a.source:
        ids = [r[0] for r in conn.execute(
            "SELECT series_id FROM series WHERE source_id=?", (a.source,))]
        src = f"source={a.source}"
    else:
        path = a.ids_file or PENDING
        if not os.path.exists(path):
            print(f"nothing to sync: {path} does not exist")
            return
        with open(path, encoding="utf-8") as fh:
            ids = [ln.strip() for ln in fh if ln.strip()]
        src = path
    if not ids:
        print(f"nothing to sync ({src} yielded 0 ids)")
        return

    cols, rows = _rows_for(conn, ids)
    conn.close()
    print(f"catalog sync: {len(ids)} id(s) from {src} -> {len(rows)} local row(s)")
    if not rows:
        print("  none of those ids exist in the local catalog — nothing to advertise")
        return

    out_dir = tempfile.mkdtemp(prefix="d1catalog_")
    files = emit_sql(cols, rows, out_dir)
    verify_replay(cols, rows, files)
    if a.dry_run:
        for p in files:
            print("  (dry-run)", p)
        return
    execute_remote(files)
    if not a.source and not a.keep_pending:
        path = a.ids_file or PENDING
        open(path, "w", encoding="utf-8").close()
        print(f"  cleared {path}")
    print(f"catalog sync OK: {len(rows)} series row(s) upserted to D1")


if __name__ == "__main__":
    sys.exit(main())
