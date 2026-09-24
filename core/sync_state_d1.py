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

Execution: each chunk runs through core.d1_remote.execute_file - `node
api/worker/node_modules/wrangler/bin/wrangler.js d1 execute econ-catalog --remote
--file=<abs path>` with cwd=api/worker (wrangler.toml + the version-pinned local
wrangler install live there; we refuse to run if that install is absent, so the
version can never float). Any nonzero wrangler exit aborts
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
import sys
import tempfile

_THIS = os.path.dirname(os.path.abspath(__file__))
# Run as a script (`python core/sync_state_d1.py`, which is how BOTH workflows call it) sys.path[0]
# is core/, not the repo root, so `import core.*` fails. The gate reader below needs it. The
# sibling core/sync_catalog_d1.py documents the same trap and bootstraps the same way.
_REPO = os.path.abspath(os.path.join(_THIS, ".."))
if _REPO not in sys.path:
    sys.path.insert(0, _REPO)
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

# Sources whose data_through is NOT computed here. sec_edgar is catalogued by its own CI refresher
# on D1 only (R726), so the catalogue copy this job reads is not that source's truth: it carried
# filer-typo rows (2215-09-30) and old-rule forward rows, and a "MAX of ended periods" over it
# stamped a forward row that crept with the calendar (R737). tools/stamp_source_data_through.py
# stamps these sources from D1's own rows after every refresher run; this job leaves them alone.
DATA_THROUGH_FROM_D1 = frozenset({"sec_edgar"})
# ...and after T0 D1 is frozen, so each of them needs a LOCAL writer of its catalogue rows, its source_state
# row and its data_through (from its own refresher, not from a statistic over the catalogue - R737). This
# names the ones that have one. tools/selfhost/t0_ready.py refuses READY while any DATA_THROUGH_FROM_D1
# source is missing here (R1191 finding 1: after the first swap sec_edgar's data_through would read null).
LOCAL_FRESHNESS_WRITERS: dict[str, str] = {}      # source_id -> the module that writes it locally
MAX_FILE_BYTES = 900_000  # per-file cap under wrangler's payload limit


def _gated_ids() -> set[str]:
    """Every source id the committed worker gate blocks, lower-cased. Read, never typed.

    WITHHELD FROM THE PROJECTION, NOT DELETED FROM D1 (2026-09-17). This job upserts every
    unit_state/source_state row and a data_through row per catalogued source, and never deletes,
    so a gated source's freshness row was re-published into D1 twice a day: /v1/last-updates named
    it with cadence, status and freshness until the worker learned to filter at the read, and a
    row the owner had deleted from D1 came back at the next sync. Filtering here keeps new rows
    out; it removes nothing, because the list that ENFORCES a gate is not the list of what to
    delete (ledger R889 rule 3) - rows already in D1 stay until a reviewed delete takes them.

    Delegates to core/gen_denylist.committed_gate, which RAISES on an unreadable gate: a gate that
    cannot be read is not an empty gate, and syncing blind would re-publish every gated row.
    committed_gate deliberately returns an EMPTY set when the worker file is ABSENT (a checkout
    without the worker); that is right for a generator and wrong for a publisher, so an absent
    gate stops this sync too.
    """
    from core import gen_denylist                  # reads the worker's own denylist.ts
    if not os.path.exists(gen_denylist.OUT):
        raise SystemExit(f"FATAL: the worker gate {gen_denylist.OUT} is absent - refusing to publish "
                         "freshness rows without it (they would include every gated source)")
    return {s.lower() for s in gen_denylist.committed_gate()}


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


def _servable(rows, cols, gated):
    """Rows whose source_id the gate does not block (case-insensitive)."""
    i = cols.index("source_id")
    return [r for r in rows if str(r[i]).lower() not in gated]


def data_through_rows(cconn: sqlite3.Connection, gated: set[str]) -> list[tuple[str, str]]:
    """(source_id, newest end_date) per catalogued source, from an open catalogue (or a copy of it)."""
    # end_date < 2900: a handful of series carry the publisher's
    # open-ended sentinel 9999-12-31 (task #91's class) — eurostat's MAX
    # leaked it as data_through on the first live stamp. Genuine long
    # projection horizons (boc publishes through 2095) stay in.
    dt_rows = cconn.execute(
        "SELECT source_id, MAX(end_date) FROM series "
        "WHERE end_date IS NOT NULL AND end_date < '2900-01-01' "
        "GROUP BY source_id").fetchall()
    # SOURCES STAMPED FROM D1, NOT FROM THIS COPY (R730 -> R737, 2026-09-05). sec_edgar's
    # rows in the copy are not its truth (its refresher writes D1 only), and any statistic
    # over them - MAX(<2900) gave 2215-09-30; MAX(<= today) gave a forward row that would
    # creep with the calendar - overwrote the correct stamp at every sync. Such sources are
    # left out here entirely and stamped by tools/stamp_source_data_through.py from D1.
    return [(sid, mx) for sid, mx in dt_rows
            if sid not in DATA_THROUGH_FROM_D1 and str(sid).lower() not in gated]


def local_writer_rows(cconn: sqlite3.Connection, gated: set[str]) -> list[tuple[str, str]]:
    """(source_id, data_through) for the DATA_THROUGH_FROM_D1 sources that have a registered LOCAL writer:
    the value comes from that writer's own module - `data_through(conn) -> str | None` - never from a
    statistic over the catalogue (R737). A registered name that does not import, or has no such function,
    fails here: a name alone must not make a source look covered (R1195). The self-hosted origin uses this;
    the D1 sync does not (before T0, D1's own stamp is the truth)."""
    import importlib                                                      # noqa: PLC0415
    out = []
    for sid in sorted(DATA_THROUGH_FROM_D1):
        writer = LOCAL_FRESHNESS_WRITERS.get(sid)
        if writer is None or str(sid).lower() in gated:
            continue
        value = importlib.import_module(writer).data_through(cconn)
        if value is not None:
            out.append((sid, value))
    return out


def data_through_stmts(dt_rows: list[tuple[str, str]]) -> list[str]:
    stmts = ["CREATE TABLE IF NOT EXISTS source_data_through (source_id TEXT PRIMARY KEY, data_through TEXT);"]
    for i in range(0, len(dt_rows), ROWS_PER_STMT):
        chunk = dt_rows[i:i + ROWS_PER_STMT]
        vals = ",\n".join(
            "(" + ", ".join(_lit(v) for v in r) + ")" for r in chunk)
        stmts.append(
            f"INSERT INTO source_data_through (source_id, data_through) VALUES\n{vals}\n"
            f'ON CONFLICT(source_id) DO UPDATE SET data_through=excluded.data_through;')
    return stmts


def emit_sql(state_db: str, out_dir: str, gated: set[str] | None = None, catalogue: str | None = None,
             data_through: bool = True) -> tuple[list[str], dict[str, int]]:
    """Emit chunked upsert .sql files for every NON-GATED row of the freshness tables.

    Returns (ordered file paths, {table: row count}). Files must be executed in
    the returned order (DDL for a table always precedes its upserts). `gated`
    defaults to the committed worker gate (_gated_ids); tests pass their own. `catalogue` names the
    catalogue data_through is computed from (default: ECONDL_CATALOG or the checkout's). data_through=False
    leaves source_data_through out: the self-hosted origin reads only state.db inside the writer lock and
    computes data_through from its own copy afterwards (tools/selfhost/origin_copies.py; R1191 finding 5).
    """
    gated = _gated_ids() if gated is None else {s.lower() for s in gated}
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
            rows = _servable(conn.execute(f"SELECT {collist} FROM {table}").fetchall(), cols, gated)
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
    cat_path = catalogue or os.environ.get("ECONDL_CATALOG") or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "catalog.db")
    if not data_through:
        pass                     # the caller computes it elsewhere (the self-hosted origin: from its copy)
    elif os.path.exists(cat_path):
        cconn = sqlite3.connect(f"file:{cat_path}?mode=ro", uri=True)
        try:
            dt_rows = data_through_rows(cconn, gated)
        finally:
            cconn.close()
        stmts.extend(data_through_stmts(dt_rows))
        counts["source_data_through"] = len(dt_rows)
    else:
        print(f"  data_through SKIPPED: no catalog at {cat_path} (state tables still sync)")

    # GUARD ON THE STATE TABLES ONLY. source_data_through is read from the
    # CATALOG, a different database, so counting it here let a completely empty
    # state.db sail past this check the moment a catalog existed — which is
    # exactly the condition the guard is for (a skipped --pull-state). Caught by
    # tests/test_updater_phase1.py::test_zero_row_projection_refused after the
    # data_through emission was added, 2026-08-23.
    state_rows = counts.get("unit_state", 0) + counts.get("source_state", 0)
    if state_rows == 0:
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


def verify_replay(state_db: str, files: list[str], counts: dict[str, int],
                  gated: set[str] | None = None) -> None:
    """Replay the emitted SQL into in-memory SQLite; require row-for-row equality.

    Runs the files TWICE to also prove idempotency (second pass must change
    nothing). Any mismatch is fatal — broken SQL must never reach remote D1.
    Equality is against the NON-GATED rows of the source db, and the replay must
    hold no gated row at all: a withheld row that still reached the SQL is fatal.
    """
    gated = _gated_ids() if gated is None else {s.lower() for s in gated}
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
            want = _servable(src.execute(q).fetchall(), cols, gated)
            got = mem.execute(q).fetchall()
            # BEFORE the equality test, so this refusal is the one that fires for a gated row: after
            # it, the filtered `want` already makes any gated row a plain mismatch and this line
            # would be unreachable (review of PR #36).
            if len(_servable(got, cols, gated)) != len(got):
                raise SystemExit(f"FATAL: a gated source's row reached the {table} SQL — not executing")
            if got != want:
                raise SystemExit(
                    f"FATAL: replay verify failed for {table} "
                    f"({len(got)} vs {len(want)} rows, or content differs) — not executing")
            if counts[table] != len(want):
                raise SystemExit(f"FATAL: emitted count for {table} disagrees with the replay — not executing")
            print(f"  verify {table:13} {len(got):>5} rows  OK (replayed twice, idempotent)")
        if "source_data_through" in counts:
            dt = mem.execute("SELECT source_id FROM source_data_through").fetchall()
            if any(str(r[0]).lower() in gated for r in dt):
                raise SystemExit("FATAL: a gated source's row reached the source_data_through SQL — not executing")
    finally:
        src.close()
        mem.close()


def execute_remote(files: list[str], database: str | None = None, *, idempotent: bool = False,
                   tries: int = 4) -> None:
    """Run each chunk via wrangler from api/worker (wrangler.toml lives there).

    `database` overrides the primary for shard-routed work (CATALOG_SHARD_FOR). A failed EXIT is retried
    `tries` - 1 times. That is NOT always safe: wrangler 3.114 can exit nonzero after the server took the
    file (a failure in its poll step, R1191 finding 2). So a caller passes tries=1 for a file that holds
    bare `INSERT INTO series_fts` rows: sync_catalog_d1 does this per file (see its reapplicable()), and
    migrate_noaa_shard does it for every file. A TIMEOUT is retried only when the caller says the files are
    safe to apply twice (idempotent=True: this module's own freshness sync, INSERT OR REPLACE / deletes by
    key; sync_catalog_d1's self-cleaning files)."""
    from core import d1_remote                                               # noqa: PLC0415
    if not shutil.which("node"):
        raise SystemExit("FATAL: node not on PATH — install Node.js")
    if not os.path.isfile(d1_remote.WRANGLER_JS):     # the wrangler that actually runs (R1185 finding 6)
        raise SystemExit(
            f"FATAL: no local wrangler install at {d1_remote.WRANGLER_JS} — run `npm ci` "
            "in api/worker first (the pinned wrangler is the one core.d1_remote runs)")
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
    # The road is core.d1_remote.execute_file (plan step 1): the same wrangler call with the encoding pinned
    # to utf-8/replace (cp1252 once turned a SUCCESSFUL write into a crash), refused after T0.
    TRIES = max(1, tries)             # 1 for a file that is NOT safe to apply twice (R1191 finding 2)
    for p in files:
        print(f"  executing {os.path.basename(p)} ...")
        try:
            out = d1_remote.execute_file(
                database or D1_DATABASE, p, timeout=600, tries=TRIES, retry_timeouts=idempotent,
                on_retry=lambda n, why: print(_echo(f"    {why[:160]} - retry {n}/{TRIES - 1} in {5 * n}s"),
                                              flush=True))
        except RuntimeError as e:
            sys.stderr.write(_echo(getattr(e, "stdout", "") or ""))          # wrangler's output IN FULL, as before
            sys.stderr.write(_echo(getattr(e, "stderr", "") or ""))
            sys.stderr.write(_echo(str(e)) + "\n")
            raise SystemExit(
                f"FATAL: {p}: {str(e)[:300]} — D1 sync aborted; remaining chunks NOT executed; SQL kept for "
                f"inspection") from None
        tail = (out or "").strip().splitlines()
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
    gated = _gated_ids()   # read ONCE, so the emit and its verifier judge the same gate
    files, counts = emit_sql(args.state_db, out_dir, gated=gated)
    total = sum(counts.values())
    print(f"emitted {len(files)} file(s), {total} rows "
          f"({', '.join(f'{t}={n}' for t, n in counts.items())}) -> {out_dir}")
    verify_replay(args.state_db, files, counts, gated=gated)

    if args.dry_run:
        print("DRY RUN — not executing. SQL files:")
        for p in files:
            print(f"  {p}")
        return

    execute_remote(files, idempotent=True)     # INSERT OR REPLACE / deletes by key: safe to re-apply
    shutil.rmtree(out_dir, ignore_errors=True)
    print(f"D1 sync OK: {total} rows upserted across {len(files)} file(s)")


if __name__ == "__main__":
    main()
