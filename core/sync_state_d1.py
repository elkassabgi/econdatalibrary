"""Delta-sync Aqueduct freshness state (unit_state + source_state) to Cloudflare D1.

WHY (UPDATER_BUILD_PLAN.md §1.3, fixes G8): the only existing state→D1 path is
core/export_d1.py — a manual 945 MB full re-dump of catalog + state, which is why
/v1/last-updates froze at the June-24 snapshot. After every updater run, the ONLY
thing D1 actually needs refreshed is the freshness projection: unit_state (48 rows
today) and source_state (39 rows today). v1 simplification per the plan: these two
tables are tiny (a few thousand rows at full rollout), so we upsert ALL rows every
run — no watermark to get wrong, idempotent by construction (INSERT ... ON
CONFLICT(pk) DO UPDATE, primary keys from the live schema). Never full-dumps the
catalog. Rows are never deleted from state, so upsert-only cannot strand D1 rows.

D1 rules honored (same as core/export_d1.py): NO BEGIN/COMMIT/PRAGMA (D1 wraps each
file in its own transaction and rejects raw txn statements); small multi-VALUES
batches (~20 rows) against the D1 statement cap; files chunked to <= 900 KB against
the wrangler payload limit (api/worker/README.md:86). Emitted SQL is verified by
replay into in-memory SQLite (row-for-row equality with the source db) BEFORE any
wrangler call — broken SQL never reaches remote D1.

Execution: each chunk runs via `npx wrangler d1 execute econ-catalog --remote
--file=<abs path>` with cwd=api/worker (wrangler.toml + the version-pinned local
wrangler install live there; we refuse to run if node_modules/wrangler is absent so
npx can never float to an unpinned version). Any nonzero wrangler exit aborts
loudly (honesty rule §5.3: failures are loud, never silent). Headless auth needs
CLOUDFLARE_API_TOKEN + CLOUDFLARE_ACCOUNT_ID in the environment (plan item A2);
local runs may use the machine's wrangler OAuth instead.

Usage:
    python core/sync_state_d1.py             # emit + verify + execute against remote D1
    python core/sync_state_d1.py --dry-run   # emit + verify, print SQL paths, execute nothing
"""
from __future__ import annotations

import argparse
import os
import shutil
import sqlite3
import subprocess
import time
import sys
import tempfile

_THIS = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.environ.get("ECONDL_ROOT")
                       or os.path.join(_THIS, ".."))
STATE_DB = os.path.join(
    os.path.abspath(os.environ.get("AQUEDUCT_STATE_DIR")
                    or os.path.join(ROOT, "data", "_aqueduct")),
    "state.db")
WORKER_DIR = os.path.join(ROOT, "api", "worker")

TABLES = ["unit_state", "source_state"]  # the freshness projection — nothing else
D1_DATABASE = "econ-catalog"             # wrangler.toml [[d1_databases]] database_name
# Sources whose CATALOG rows live on a D1 shard, not the primary (task #45: noaa's
# 3,137,871 rows moved to econ-catalog-climate to free primary headroom for bea/fdic;
# the worker routes reads for them to the shard binding). Catalog sync and serving
# verification MUST consult this map — pushing noaa's rows back to the primary would
# silently re-consume the freed ~2.4 GB and the worker would never read them there.
# The freshness projection (TABLES above) stays on the primary for ALL sources.
CATALOG_SHARD_FOR = {"noaa": "econ-catalog-climate"}
ROWS_PER_STMT = 20        # matches core/export_d1.py (D1 statement-length cap)
MAX_FILE_BYTES = 900_000  # per-file cap under wrangler's payload limit


def _echo(s: str) -> str:
    """Make wrangler's output printable on THIS stdout, whatever its encoding.

    wrangler emits emoji (a 🪵 in its log banner). On a Windows console stdout is cp1252, so
    echoing that raw raises UnicodeEncodeError '\\U0001fab5' — and it does so from the RETRY and
    FATAL paths, i.e. exactly when a sync is already failing. The crash then replaces the
    diagnostic it was trying to print, so the real wrangler error is never seen and the traceback
    blames an encoding instead. Hit while syncing the IMF direct sources; worked around at the
    time with PYTHONIOENCODING=utf-8, which fixes my shell and not the next person's.

    Round-trips through the actual stdout encoding with errors='replace': unprintable characters
    degrade to '?' and the message still arrives.
    """
    enc = (getattr(sys.stdout, "encoding", None) or "utf-8")
    return s.encode(enc, errors="replace").decode(enc, errors="replace")


def _lit(v) -> str:
    """SQL literal, same rules as core/export_d1.py (D1 IS SQLite)."""
    if v is None:
        return "NULL"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, bytes):
        return "X'" + v.hex() + "'"
    return "'" + str(v).replace("'", "''") + "'"


def _table_shape(conn: sqlite3.Connection, table: str) -> tuple[list[str], list[str], str]:
    """(all columns, pk columns, CREATE TABLE IF NOT EXISTS ddl) from the LIVE db.

    Columns and primary keys are read from PRAGMA table_info, never hardcoded, so
    a schema change in updater/state.py flows through without editing this file.
    """
    info = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if not info:
        raise SystemExit(f"FATAL: table {table!r} not found in {conn}")
    cols = [r[1] for r in info]
    pk = [r[1] for r in sorted((r for r in info if r[5]), key=lambda r: r[5])]
    if not pk:
        raise SystemExit(f"FATAL: table {table!r} has no PRIMARY KEY — upsert impossible")
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    ddl = row[0].replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1).rstrip(";") + ";"
    return cols, pk, ddl


def emit_sql(state_db: str, out_dir: str) -> tuple[list[str], dict[str, int]]:
    """Emit chunked upsert .sql files for ALL rows of the freshness tables.

    Returns (ordered file paths, {table: row count}). Files must be executed in
    the returned order (DDL for a table always precedes its upserts).
    """
    # Strictly read-only: this script must never write (or WAL-touch) state.db.
    conn = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    counts: dict[str, int] = {}
    stmts: list[str] = []
    try:
        for table in TABLES:
            cols, pk, ddl = _table_shape(conn, table)
            collist = ", ".join(f'"{c}"' for c in cols)
            upd = ", ".join(f'"{c}"=excluded."{c}"' for c in cols if c not in pk)
            conflict = ", ".join(f'"{c}"' for c in pk)
            stmts.append(ddl)  # no-op on the live D1; makes a fresh D1 workable
            rows = conn.execute(f"SELECT {collist} FROM {table}").fetchall()
            counts[table] = len(rows)
            for i in range(0, len(rows), ROWS_PER_STMT):
                chunk = rows[i:i + ROWS_PER_STMT]
                vals = ",\n".join(
                    "(" + ", ".join(_lit(v) for v in r) + ")" for r in chunk)
                stmts.append(
                    f"INSERT INTO {table} ({collist}) VALUES\n{vals}\n"
                    f"ON CONFLICT({conflict}) DO UPDATE SET {upd};")
    finally:
        conn.close()

    # DATA_THROUGH (task #138, 2026-08-20): per-source newest served observation,
    # computed FREE from the local catalog — never a D1 table scan (R430: an
    # unindexable aggregate on the 13M-row series table bills real money; this
    # one costs one local GROUP BY). 93 of 318 live sources rotate 'partial' by
    # design, so R231's gate (correctly) never writes their source_state row and
    # the public freshness read null while data merged daily. data_through
    # answers what a user actually asks — "data through when?" — from
    # series.end_date, which the derive/catalogue chain maintains. Its OWN tiny
    # table (not an ALTER on source_state): CREATE IF NOT EXISTS is idempotent
    # where ADD COLUMN is fatal-on-rerun, and a fresh D1 stays workable.
    # verify_replay ignores it deliberately — it audits the state projection.
    cat_path = os.environ.get("ECONDL_CATALOG") or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "catalog.db")
    if os.path.exists(cat_path):
        cconn = sqlite3.connect(f"file:{cat_path}?mode=ro", uri=True)
        try:
            dt_rows = cconn.execute(
                "SELECT source_id, MAX(end_date) FROM series "
                "WHERE end_date IS NOT NULL GROUP BY source_id").fetchall()
        finally:
            cconn.close()
        stmts.append("CREATE TABLE IF NOT EXISTS source_data_through ("
                     "source_id TEXT PRIMARY KEY, data_through TEXT);")
        for i in range(0, len(dt_rows), ROWS_PER_STMT):
            chunk = dt_rows[i:i + ROWS_PER_STMT]
            vals = ",\n".join(
                "(" + ", ".join(_lit(v) for v in r) + ")" for r in chunk)
            stmts.append(
                f"INSERT INTO source_data_through (source_id, data_through) VALUES\n{vals}\n"
                f'ON CONFLICT(source_id) DO UPDATE SET data_through=excluded.data_through;')
        counts["source_data_through"] = len(dt_rows)
    else:
        print(f"  data_through SKIPPED: no catalog at {cat_path} (state tables still sync)")

    if sum(counts.values()) == 0:
        raise SystemExit(
            f"FATAL: {state_db} has zero unit_state/source_state rows — refusing to "
            "sync an empty freshness projection (was --pull-state skipped?)")

    header = ("-- Aqueduct freshness delta for D1 (upsert-all; no txn/pragma).\n"
              "-- Generated by core/sync_state_d1.py. Execute files IN ORDER.\n")
    files: list[str] = []
    part, buf, bb = 0, [], 0

    def flush():
        nonlocal part, buf, bb
        if not buf:
            return
        p = os.path.join(out_dir, f"state_delta_{part:03d}.sql")
        with open(p, "w", encoding="utf-8") as fh:
            fh.write(header + "\n".join(buf) + "\n")
        files.append(p)
        part, buf, bb = part + 1, [], 0

    for stmt in stmts:
        sb = len(stmt.encode("utf-8")) + 1
        if buf and bb + sb > MAX_FILE_BYTES:
            flush()
        buf.append(stmt)
        bb += sb
    flush()
    return files, counts


def verify_replay(state_db: str, files: list[str], counts: dict[str, int]) -> None:
    """Replay the emitted SQL into in-memory SQLite; require row-for-row equality.

    Runs the files TWICE to also prove idempotency (second pass must change
    nothing). Any mismatch is fatal — broken SQL must never reach remote D1.
    """
    src = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    mem = sqlite3.connect(":memory:")
    try:
        for _pass in (1, 2):
            for p in files:
                with open(p, encoding="utf-8") as fh:
                    mem.executescript(fh.read())
        for table in TABLES:
            cols, pk, _ = _table_shape(src, table)
            order = ", ".join(f'"{c}"' for c in pk)
            collist = ", ".join(f'"{c}"' for c in cols)
            q = f"SELECT {collist} FROM {table} ORDER BY {order}"
            want = src.execute(q).fetchall()
            got = mem.execute(q).fetchall()
            if got != want:
                raise SystemExit(
                    f"FATAL: replay verify failed for {table} "
                    f"({len(got)} vs {len(want)} rows, or content differs) — not executing")
            print(f"  verify {table:13} {len(got):>5} rows  OK (replayed twice, idempotent)")
        assert all(counts[t] == len(src.execute(f"SELECT 1 FROM {t}").fetchall())
                   for t in TABLES)
    finally:
        src.close()
        mem.close()


def execute_remote(files: list[str], database: str | None = None) -> None:
    """Run each chunk via wrangler from api/worker (wrangler.toml lives there).

    `database` overrides the primary for shard-routed work (CATALOG_SHARD_FOR)."""
    npx = shutil.which("npx")
    if not npx:
        raise SystemExit("FATAL: npx not on PATH — install Node.js")
    if not os.path.isdir(os.path.join(WORKER_DIR, "node_modules", "wrangler")):
        raise SystemExit(
            f"FATAL: no local wrangler install under {WORKER_DIR} — run `npm install` "
            "there first (npx would otherwise float to an unpinned wrangler version)")
    # RETRY, because one transient blip used to cost the whole sync. A usda run of 93 chunks
    # died on chunk 0 with Cloudflare "Authentication error [code: 10000]" from the /d1/import
    # endpoint -- while `d1 execute` against the same database, with the same credentials,
    # worked seconds later, and an identical sync had succeeded an hour before. So the error
    # text was misleading and the condition was transient. Aborting the remaining 92 chunks on
    # it left D1 holding 25 stale rows whose R2 objects had already been replaced: the
    # catalogue advertised series that 404.
    #
    # Retries are bounded and the FINAL failure still aborts loudly -- a half-written D1 is
    # worse than a failed sync, so this makes the transient case survivable without making the
    # real case quiet.
    TRIES = 4
    for p in files:
        cmd = [npx, "wrangler", "d1", "execute", database or D1_DATABASE,
               "--remote", "--yes", f"--file={os.path.abspath(p)}"]
        print(f"  executing {os.path.basename(p)} ...")
        res = None
        for attempt in range(TRIES):
            try:
                # encoding/errors pinned explicitly: text=True decodes with the LOCALE
                # codec, and on Windows (cp1252) wrangler's box-drawing output raises
                # UnicodeDecodeError. That turns a SUCCESSFUL deploy into a crash — and
                # worse, a crash midway through a chunked sync leaves D1 half-updated.
                # The bytes we care about (row counts, error text) are ASCII; replace the
                # rest rather than letting cosmetics abort a write.
                res = subprocess.run(cmd, cwd=WORKER_DIR, capture_output=True,
                                     text=True, encoding="utf-8", errors="replace",
                                     timeout=600)
            except subprocess.TimeoutExpired:
                if attempt == TRIES - 1:
                    raise SystemExit(
                        f"FATAL: wrangler timed out (600s) on {p} after {TRIES} attempts "
                        f"— aborting sync")
                print(f"    timed out, retry {attempt + 1}/{TRIES - 1} in "
                      f"{5 * (attempt + 1)}s", flush=True)
                time.sleep(5 * (attempt + 1))
                continue
            if res.returncode == 0:
                break
            if attempt < TRIES - 1:
                first = ((res.stderr or res.stdout or "").strip().splitlines() or [""])[-1]
                print(_echo(f"    exit {res.returncode}, retry {attempt + 1}/{TRIES - 1} in "
                            f"{5 * (attempt + 1)}s — {first[:110]}"), flush=True)
                time.sleep(5 * (attempt + 1))
        if res is None or res.returncode != 0:
            sys.stderr.write(_echo((res.stdout if res else "") or ""))
            sys.stderr.write(_echo((res.stderr if res else "") or ""))
            raise SystemExit(
                f"FATAL: wrangler exited {res.returncode if res else '?'} on {p} after "
                f"{TRIES} attempts — D1 sync aborted; remaining chunks NOT executed; "
                f"SQL kept for inspection")
        tail = (res.stdout or "").strip().splitlines()
        if tail:
            print(f"    {tail[-1]}")


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true",
                    help="emit + verify the SQL, print file paths, execute nothing")
    ap.add_argument("--state-db", default=STATE_DB,
                    help=f"path to state.db (default: {STATE_DB})")
    args = ap.parse_args(argv)

    if not os.path.exists(args.state_db):
        raise SystemExit(f"FATAL: state db not found: {args.state_db} "
                         "(run `python -m updater.run --pull-state` first in CI)")

    out_dir = tempfile.mkdtemp(prefix="d1_state_sync_")
    files, counts = emit_sql(args.state_db, out_dir)
    total = sum(counts.values())
    print(f"emitted {len(files)} file(s), {total} rows "
          f"({', '.join(f'{t}={n}' for t, n in counts.items())}) -> {out_dir}")
    verify_replay(args.state_db, files, counts)

    if args.dry_run:
        print("DRY RUN — not executing. SQL files:")
        for p in files:
            print(f"  {p}")
        return

    execute_remote(files)
    shutil.rmtree(out_dir, ignore_errors=True)
    print(f"D1 sync OK: {total} rows upserted across {len(files)} file(s)")


if __name__ == "__main__":
    main()
