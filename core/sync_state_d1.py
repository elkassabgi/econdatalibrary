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
# A NAME HERE IS NOT READINESS (R1195): it only lets an origin copy be built from the local rows. Whether those
# rows are the refresher's truth is a separate gate with a receipt - t0_ready's `sec-edgar-local` check, which
# stays closed until tools/selfhost/sec_edgar_local_check.py is clean AND the D1-to-local proof exists.
LOCAL_FRESHNESS_WRITERS: dict[str, str] = {       # source_id -> the module whose data_through(conn) stamps it
    "sec_edgar": "core.sec_edgar_local",
}
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


# The publisher-discontinued list, read from the CODE checkout (not ECONDL_ROOT, which can point at a data root).
DISCONTINUED_YAML = os.path.join(_REPO, "updater", "discontinued.yaml")


def _discontinued_ids(path: str | None = None) -> set[str]:
    """Every source id in updater/discontinued.yaml, lower-cased. _project() publishes them as 'discontinued'.

    WHY (2026-10-20, AR-354 item 1). A discontinued source is served frozen and no fetcher runs it, so whatever
    state.db last recorded for it is a claim about runs that will never happen again. The 52 relay-era entries
    never had a state row (/v1/sources showed status, last_updated and cadence null - no label at all). The
    first successor-era entry, unctad_trademerchgr, ran for two months under the registry: its leftover row
    ("ok", "weekly", last success 2026-10-01) kept being published, so /v1/sources told users a report UNCTAD
    had withdrawn was refreshed weekly. A live:false flag or a removed registry entry does not change that row
    (only a success writes source_state, R231; the sync is upsert-only), and state.db rows are never deleted
    (they are history). So the rule lives here, where the projection is built: the D1 sync and the self-hosted
    origin builder (tools/selfhost/origin_copies.py) both call emit_sql. source_data_through is NOT touched:
    "data through" is a fact about the served data.

    An ABSENT file labels nothing (a checkout without it makes no claim). A file that exists but cannot be
    parsed, or names no source, STOPS the sync: publishing blind would re-publish every frozen source's stale row, and
    tests/test_discontinued_registry.py parses the same file in CI, so this cannot reach a run unnoticed.
    """
    p = path or DISCONTINUED_YAML
    if not os.path.exists(p):
        return set()
    try:
        import yaml                                                          # noqa: PLC0415
        with open(p, encoding="utf-8") as fh:
            doc = yaml.safe_load(fh) or {}
        ids = {str(e["source_id"]).lower() for e in (doc.get("sources") or [])}
        if not ids:                     # a file that EXISTS and names nobody is a broken file, not "none"
            raise ValueError("no source_id in it")
        return ids
    except Exception as e:                                                   # noqa: BLE001
        raise SystemExit(f"FATAL: {p} is unreadable ({type(e).__name__}: {e}) - refusing to publish freshness "
                         "rows without it (every discontinued source's stale row would go out)") from None


DISCONTINUED_STATUS = "discontinued"
# Neutral on purpose (AR-359 #4): 7 entries carry a softer `label` because their own finding shows "the publisher
# discontinued it" overstates (codes still published, a revised edition). "Archival copy; not updated" is true of
# all of them. This column is not selected by /v1/sources or /v1/last-updates (api/worker/src/sql.ts).
DISCONTINUED_NOTE = "archival copy; not updated (updater/discontinued.yaml)"


def _project(table: str, rows: list, cols: list[str], gated: set[str], frozen: set[str]) -> list[tuple]:
    """The rows of `table` the projection publishes. ONE definition, used by emit_sql and verify_replay.

      every table      a gated source's rows are withheld (the gate wins over everything, and is never named)
      unit_state       a RECORDED unit row of a discontinued source is published with status 'discontinued'; every
                       other column is kept. It is NOT dropped: the worker's metadata.json falls back to the
                       source's `_all` unit row for `last_updated` when the series row has none (metadata.ts), so
                       dropping it would null a true last-refresh date on every served id (AR-355, all 1,754 of
                       unctad_trademerchgr). The upsert overwrites the stale "transient_fail" row in D1. No unit row
                       is MADE for a source state.db never recorded one for (the 52 relay-era entries): that would
                       add rows to /v1/last-updates and the Status board for sources nothing ever ran.
      source_state     a discontinued source gets exactly ONE row with status 'discontinued' and cadence NULL.
                       A recorded row keeps its strategy and last_success/last_attempt (true history: when it
                       was last refreshed); a source with no recorded row gets one with those NULL. The D1 sync
                       is upsert-all, so this OVERWRITES a stale row ("ok", "weekly") instead of leaving it -
                       no delete is needed. /v1/sources then says status "discontinued", cadence null.
    """
    keep = _servable(rows, cols, gated)
    i_sid = cols.index("source_id")
    if table == "unit_state":
        i_st = cols.index("status")
        return [tuple(r) if str(r[i_sid]).lower() not in frozen
                else tuple(DISCONTINUED_STATUS if i == i_st else v for i, v in enumerate(r)) for r in keep]
    if table != "source_state":
        return keep
    blank = {c: None for c in cols}
    out, seen = [], set()
    for r in keep:
        sid = str(r[i_sid]).lower()
        if sid not in frozen:
            out.append(tuple(r))
            continue
        d = dict(zip(cols, r))
        d.update(status=DISCONTINUED_STATUS, cadence=None, note=DISCONTINUED_NOTE)
        out.append(tuple(d[c] for c in cols))
        seen.add(sid)
    for sid in sorted(frozen - seen - gated):
        d = dict(blank, source_id=sid, status=DISCONTINUED_STATUS, note=DISCONTINUED_NOTE)
        out.append(tuple(d[c] for c in cols))
    return out


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
             data_through: bool = True, discontinued: set[str] | None = None) -> tuple[list[str], dict[str, int]]:
    """Emit chunked upsert .sql files for every NON-GATED row of the freshness tables.

    Returns (ordered file paths, {table: row count}). Files must be executed in
    the returned order (DDL for a table always precedes its upserts). `gated`
    defaults to the committed worker gate (_gated_ids); tests pass their own. `discontinued` (default:
    _discontinued_ids) is applied by _project to unit_state/source_state only, never to data_through. `catalogue` names the
    catalogue data_through is computed from (default: ECONDL_CATALOG or the checkout's). data_through=False
    leaves source_data_through out: the self-hosted origin reads only state.db inside the writer lock and
    computes data_through from its own copy afterwards (tools/selfhost/origin_copies.py; R1191 finding 5).
    """
    gated = _gated_ids() if gated is None else {s.lower() for s in gated}
    frozen = _discontinued_ids() if discontinued is None else {s.lower() for s in discontinued}
    # Strictly read-only: this script must never write (or WAL-touch) state.db.
    conn = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    counts: dict[str, int] = {}
    recorded: dict[str, int] = {}
    stmts: list[str] = []
    try:
        for table in TABLES:
            cols, pk, ddl = _table_shape(conn, table)
            collist = ", ".join(f'"{c}"' for c in cols)
            upd = ", ".join(f'"{c}"=excluded."{c}"' for c in cols if c not in pk)
            conflict = ", ".join(f'"{c}"' for c in pk)
            stmts.append(ddl)  # no-op on the live D1; makes a fresh D1 workable
            raw = conn.execute(f"SELECT {collist} FROM {table}").fetchall()
            recorded[table] = len(_servable(raw, cols, gated))     # what state.db HOLDS, for the zero-row guard
            rows = _project(table, raw, cols, gated, frozen)
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
    # And on the rows state.db RECORDS, not on what is emitted: the 'discontinued' source_state rows are
    # made here from updater/discontinued.yaml, so an EMPTY state.db still emits ~50 of them, and counting
    # those would let a skipped --pull-state through again (caught by that same test, 2026-10-20).
    state_rows = recorded.get("unit_state", 0) + recorded.get("source_state", 0)
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
        with open(p, "w", encoding="utf-8", newline="\n") as fh:
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
                  gated: set[str] | None = None, discontinued: set[str] | None = None) -> None:
    """Replay the emitted SQL into in-memory SQLite; require row-for-row equality.

    Runs the files TWICE to also prove idempotency (second pass must change
    nothing). Any mismatch is fatal — broken SQL must never reach remote D1.
    Equality is against _project() of the source db (gate withheld; discontinued sources: their RECORDED unit rows
    and one source_state row, all marked 'discontinued'). Then, independently of _project: the replay must hold no
    gated row; exactly the discontinued unit rows state.db records (none dropped, none made up), each marked
    'discontinued'; and for every discontinued source exactly one source_state row whose status is 'discontinued'
    and whose cadence is NULL - a stale "ok"/"weekly"/"transient_fail" row that reached the SQL is fatal.
    """
    gated = _gated_ids() if gated is None else {s.lower() for s in gated}
    frozen = _discontinued_ids() if discontinued is None else {s.lower() for s in discontinued}
    src = sqlite3.connect(f"file:{state_db}?mode=ro", uri=True)
    mem = sqlite3.connect(":memory:")
    try:
        for _pass in (1, 2):
            for p in files:
                with open(p, encoding="utf-8", newline="") as fh:
                    mem.executescript(fh.read())
        for table in TABLES:
            cols, pk, _ = _table_shape(src, table)
            order = ", ".join(f'"{c}"' for c in pk)
            collist = ", ".join(f'"{c}"' for c in cols)
            q = f"SELECT {collist} FROM {table} ORDER BY {order}"
            ipk = [cols.index(c) for c in pk]
            want = sorted(_project(table, src.execute(q).fetchall(), cols, gated, frozen),
                          key=lambda r: tuple(r[i] for i in ipk))
            got = mem.execute(q).fetchall()
            # BEFORE the equality test, so this refusal is the one that fires for a gated row: after
            # it, the filtered `want` already makes any gated row a plain mismatch and this line
            # would be unreachable (review of PR #36).
            if len(_servable(got, cols, gated)) != len(got):
                raise SystemExit(f"FATAL: a gated source's row reached the {table} SQL — not executing")
            # The discontinued rule, checked on the REPLAY and not through _project (so a broken _project that
            # emits and expects the same stale row cannot pass).
            i_sid = cols.index("source_id")
            mine = [r for r in got if str(r[i_sid]).lower() in frozen]
            if table == "unit_state":
                i_st = cols.index("status")
                had = {tuple(r[i] for i in ipk) for r in src.execute(q).fetchall()
                       if str(r[i_sid]).lower() in frozen and str(r[i_sid]).lower() not in gated}
                have = {tuple(r[i] for i in ipk) for r in mine}
                stale = [r[i_sid] for r in mine if r[i_st] != DISCONTINUED_STATUS]
                if stale or have != had:
                    raise SystemExit(
                        f"FATAL: discontinued source(s) wrong in the unit_state SQL (stale status: {stale[:5]}; "
                        f"recorded but missing: {sorted(had - have)[:5]}; made up: {sorted(have - had)[:5]}) — a "
                        "dropped row would null metadata.json last_updated, a made-up one adds a board row; "
                        "not executing")
            if table == "source_state":
                i_st, i_cad = cols.index("status"), cols.index("cadence")
                stale = [r[i_sid] for r in mine if r[i_st] != DISCONTINUED_STATUS or r[i_cad] is not None]
                missing = sorted(frozen - gated - {str(r[i_sid]).lower() for r in mine})
                if stale or missing:
                    raise SystemExit(f"FATAL: discontinued source(s) not marked in the source_state SQL "
                                     f"(stale status/cadence: {stale[:5]}, no row: {missing[:5]}) — not executing")
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
    frozen = _discontinued_ids()   # ...and the same discontinued list
    files, counts = emit_sql(args.state_db, out_dir, gated=gated, discontinued=frozen)
    total = sum(counts.values())
    print(f"emitted {len(files)} file(s), {total} rows "
          f"({', '.join(f'{t}={n}' for t, n in counts.items())}) -> {out_dir}"
          f"; {len(frozen)} discontinued source(s) published as '{DISCONTINUED_STATUS}' (recorded unit rows relabelled, none made)")
    verify_replay(args.state_db, files, counts, gated=gated, discontinued=frozen)

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
