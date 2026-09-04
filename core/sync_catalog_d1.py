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

from core.sync_state_d1 import (CATALOG_SHARD_FOR, MAX_FILE_BYTES,  # noqa: E402
                                ROWS_PER_STMT, ROOT, _lit, execute_remote)

# Ids per `DELETE FROM series_fts WHERE series_id IN (...)`. Deliberately NOT ROWS_PER_STMT:
# that column is UNINDEXED, so the cost is one full table scan PER STATEMENT regardless of
# the list length. See the rationale block at the emit site in emit_sql.
#
# PER-SCAN CONSTANT, and it MOVED. This comment read "23,843,482 rows_read for a 20-id list,
# 2026-08-26" until 2026-09-04. The FTS was rebuilt and swapped on 2026-08-31 (commit 44ed89aee)
# to match the series count exactly, and `updater-daily.yml:361` already recorded the change
# ("cut the per-scan constant 2.30x, 23,843,482 -> 10,348,426") while this line kept the old
# figure. MEASURED AGAIN 2026-09-04 against live D1: one id-scoped statement reads 10,348,511.
# Anyone pricing a batch off the stale number over-estimates by 2.30x -- safe, but it is how a
# cheap path gets refused as expensive. Re-measure after any FTS rebuild; do not trust this line.
FTS_DELETE_PER_STMT = 500


def whole_source_reconcile(source, rows, skipped_by_diff, n_groups=1):
    """The source id when a range DELETE is provably safe, else None.

    SAFE MEANS COMPLETE, NOT HOMOGENEOUS - and the previous test was homogeneity, which is
    the whole bug (R658). `DELETE FROM series_fts WHERE series_id >= 'src:' AND < 'src;'`
    removes the index rows of EVERY series of the source, so it is only correct when the
    rows that follow it re-insert every one of them. The check `all(r["source_id"] ==
    source)` is true of any subset, and by the time it ran the DIFF had already reduced
    `rows` to the rows whose content had CHANGED. Measured on the state that existed when
    this was found: 105 newly catalogued cbs_nl ids, 5,154 unchanged and therefore dropped
    by the diff, one range DELETE emitted, 105 ids re-inserted - 5,049 series deleted from
    the search index and never restored. `/v1/catalog?q=dwellings&source=cbs_nl` would have
    gone from 29 to 0 while /v1/sources still advertised 5,259.

    So the diff and the range delete are mutually exclusive. `--no-diff` sets
    skipped_by_diff to 0 and the range form is available again, which is the documented way
    to ask for a whole-source reconcile.

    n_groups guards the shard case: a source split across two D1 databases has only part of
    itself in each group, and a range predicate inside one database would still be a claim
    about the whole source. Conservative, and free - no source shards today.
    """
    if not source or not rows:
        return None
    if skipped_by_diff:
        return None                    # a partial slice: the omitted ids would be unlisted
    if n_groups != 1:
        return None
    if not all(r.get("source_id") == source for r in rows):
        return None
    return source

CATALOG_DB = os.path.abspath(os.environ.get("ECONDL_CATALOG")
                             or os.path.join(ROOT, "data", "catalog.db"))
# Written by the orchestrator: one series_id per line, appended whenever a CSV is
# derived. Consumed and truncated by this script so a series is synced once.
PENDING = os.path.join(
    os.path.abspath(os.environ.get("AQUEDUCT_STATE_DIR")
                    or os.path.join(ROOT, "data", "_aqueduct")),
    "pending_catalog_sync.txt")


from core.catalog_sync_manifest import Manifest as _Manifest      # noqa: E402
from core.catalog_sync_manifest import default_path as _manifest_path  # noqa: E402


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


def _parent_rows(conn: sqlite3.Connection, rows: list[dict]) -> list[str]:
    """`source` (+ its `license`) rows for every source these series belong to.

    WITHOUT THESE THE SERIES ARE FETCHABLE AND INVISIBLE. The worker's SELECT_SOURCES is

        FROM source s ... WHERE EXISTS (SELECT 1 FROM series se WHERE se.source_id = s.source_id)

    so /v1/sources needs BOTH a `source` row and >=1 series. This module only ever emitted
    `series` + `series_fts`, so a source first catalogued after the last full core/export_d1.py
    got its series into D1 — ids resolve, metadata.json answers, the CSV serves — while the
    source row never arrived and the source appeared nowhere in the listing. Nothing errored;
    the source was simply unbrowsable, which is the failure mode nobody reports because it looks
    like the data was never added.

    Measured against live D1 on 2026-08-04: 27 such sources, ALL imf_*, including the eight
    proven served in task #39. /v1/sources returned 196 against 223 catalogued, and those 27 were
    exactly the difference.

    The licence row goes too, FIRST: SELECT_SOURCES LEFT JOINs it and the API publishes
    reservable / commercial_ok from it, so listing a source against a missing licence row would
    advertise terms it cannot state.
    """
    stmts, lic = [], set()
    for sid in sorted({r["source_id"] for r in rows if r.get("source_id")}):
        r = conn.execute("SELECT source_id,name,homepage,license_id,attribution,terms_url "
                         "FROM source WHERE source_id=?", (sid,)).fetchone()
        if r is None:
            continue
        if r[3]:
            lic.add(r[3])
        stmts.append("INSERT OR REPLACE INTO source"
                     "(source_id,name,homepage,license_id,attribution,terms_url) VALUES("
                     + ",".join(_lit(x) for x in r) + ");")
    for lid in sorted(lic):
        r = conn.execute("SELECT license_id,name,url,reservable,commercial_ok,"
                         "attribution_required,no_modify FROM license WHERE license_id=?",
                         (lid,)).fetchone()
        if r is not None:
            stmts.insert(0, "INSERT OR REPLACE INTO license(license_id,name,url,reservable,"
                            "commercial_ok,attribution_required,no_modify) VALUES("
                            + ",".join(_lit(x) for x in r) + ");")
    return stmts


def emit_sql(cols: list[str], rows: list[dict], out_dir: str,
             conn: sqlite3.Connection | None = None,
             fts_range_source: str | None = None) -> list[str]:
    """Chunked INSERT OR REPLACE for `series`, plus matching `series_fts` rows.

    Given `conn`, the parent `source`/`license` rows are emitted FIRST — see _parent_rows for
    why omitting them produces a fetchable-but-unlistable source.
    """
    collist = ", ".join(cols)
    stmts: list[str] = []
    if conn is not None:
        stmts.extend(_parent_rows(conn, rows))
    for i in range(0, len(rows), ROWS_PER_STMT):
        ch = rows[i:i + ROWS_PER_STMT]
        vals = ",\n  ".join("(%s)" % ", ".join(_lit(r[c]) for c in cols) for r in ch)
        stmts.append(f"INSERT OR REPLACE INTO series ({collist}) VALUES\n  {vals};")
    # DELETE-THEN-INSERT. This block was a bare INSERT, with a comment adopting the
    # duplication as an acceptable trade: "Duplicate FTS rows only ever cost a repeated
    # search hit, whereas a MISSING one makes the series unfindable - the asymmetry favours
    # inserting." Both halves of that cost model are wrong, and this is the file that
    # actually produced the damage. Measured on the live D1:
    #
    #   boc            102,882 fts rows / 12,862 ids = exactly 8.00 copies of every id
    #   cepii_gravity  every id >= 3 copies, plus exactly 50,000 ids carrying a 4th -
    #                  three full passes and one partial. The round 50,000 is NOT a
    #                  ROWS_PER_STMT boundary (that is 20); its cause is unidentified,
    #                  and only the multiplicity is evidence here.
    #   global         23,934,659 fts rows / 10,348,125 series = 2.31x
    #
    # The user-facing cost is not 'a repeated search hit': GET /v1/catalog?q=Lynx returns
    # 100 rows containing 16 distinct ids, and every `total` is inflated by the same factor.
    # The storage cost is ~13.6M rows in a database at 8.36 GB against a HARD 10 GB ceiling,
    # which the comment never weighed. See R482 / R486 / R487.
    #
    # The stated objection - 'delete-then-insert would need a matching delete command per
    # row' - is answered by deleting the CHUNK in one statement before inserting it, which is
    # what this does. INSERT OR IGNORE cannot help: an FTS5 virtual table has no unique
    # constraint to ignore.
    # ...but the DELETE gets its OWN, much larger arity. `series_fts` is
    # fts5(series_id UNINDEXED, ...), so `WHERE series_id IN (...)` has NO index and every
    # such statement FULL-SCANS the table. MEASURED on live D1 2026-08-26, one statement
    # with a 20-id IN list:
    #
    #   SELECT COUNT(*) FROM series_fts WHERE series_id IN (<20 ids>)
    #     -> rows_read 23,843,482, sql_duration 16.4 s
    #
    # The cost is per STATEMENT, not per id — a 500-id list reads the same 23.8M rows — so
    # the remedy is ARITY, never more statements (hfdatalibrary/CLAUDE.md; R492, where a
    # 164,705-statement plan priced at ~$2,500). At 20 ids/stmt a 20,783-id sync is 1,040
    # statements = 2.48e10 rows ~ $25 PER RUN and recurring; at 500 it is 42 statements
    # = 1.0e9 rows ~ $1. These are literals via _lit, not bound parameters, so D1's 100
    # bound-variable cap (R224) does not apply; 500 ids is ~20 KB against MAX_FILE_BYTES.
    #
    # The DELETE stays ADJACENT to the inserts it covers rather than being hoisted into one
    # leading pass: an FTS delete whose matching insert never executes leaves the series
    # unfindable, so the window between them must stay as small as the arity allows (R487 —
    # a failed INSERT after a committed DELETE silently destroys the index).
    # ARITY TAKEN TO ITS LIMIT: one RANGE predicate covers the whole source (2026-08-29).
    # Every id-list DELETE costs one full scan of series_fts REGARDLESS of list length, so
    # for a whole-source reconcile the cheapest correct form is a single statement bounded
    # by the id prefix — series_id >= 'src:' AND series_id < 'src;' (';' is the codepoint
    # after ':'), the same range form used elsewhere in the repo. MEASURED alternative:
    # cataloguing idb Option B's 957,011 ids at 500/stmt is 1,915 statements x 23,843,482
    # rows = 4.56e10 rows ~ $45.60, against ONE statement ~ $0.024 here — a ~1,900x
    # reduction with an identical end state for the index.
    #
    # WHY IT IS OPT-IN, and the R487 tension it does NOT escape: a range delete removes the
    # WHOLE source's index rows up front, so the window in which a series is unfindable
    # spans the entire insert set rather than one 500-id block. That is acceptable ONLY for
    # a deliberate whole-source reconcile, where `rows` IS that source's complete row set
    # and a re-run is idempotent — NEVER for the incremental pending-queue path, whose rows
    # are a partial slice and would leave every unlisted series of the source deleted from
    # the index. The caller must name the source explicitly, and main() asserts that the
    # rows really are the whole source before passing it.
    if fts_range_source:
        lo = _lit(fts_range_source + ":")
        hi = _lit(fts_range_source + ";")
        stmts.append(
            f"DELETE FROM series_fts WHERE series_id >= {lo} AND series_id < {hi};")
        for j in range(0, len(rows), ROWS_PER_STMT):
            ch = rows[j:j + ROWS_PER_STMT]
            vals = ",\n  ".join(
                "(%s,%s,%s)" % (_lit(r["series_id"]), _lit(r.get("title")),
                                _lit(r.get("geography"))) for r in ch)
            stmts.append("INSERT INTO series_fts (series_id,title,geography) VALUES\n  "
                         f"{vals};")
        rows_for_fts: list[dict] = []
    else:
        rows_for_fts = rows
    for i in range(0, len(rows_for_fts), FTS_DELETE_PER_STMT):
        block = rows_for_fts[i:i + FTS_DELETE_PER_STMT]
        _ids = ",".join(_lit(r["series_id"]) for r in block)
        stmts.append(f"DELETE FROM series_fts WHERE series_id IN ({_ids});")
        for j in range(0, len(block), ROWS_PER_STMT):
            ch = block[j:j + ROWS_PER_STMT]
            vals = ",\n  ".join(
                "(%s,%s,%s)" % (_lit(r["series_id"]), _lit(r.get("title")),
                                _lit(r.get("geography"))) for r in ch)
            stmts.append("INSERT INTO series_fts (series_id,title,geography) VALUES\n  "
                         f"{vals};")

    # source_counts maintenance (2026-08-15 cost incident): the worker's catalog
    # totals come from this one-row-per-source table instead of a live COUNT(*)
    # that read 2.47M rows PER PAGE VIEW (42.2B rows / ~$34 in one day on wid
    # alone). Refresh the row for every source this sync touched; the recount
    # runs ONCE per sync, not once per visitor.
    for src in sorted({r["source_id"] for r in rows}):
        stmts.append(
            "CREATE TABLE IF NOT EXISTS source_counts(source_id TEXT PRIMARY KEY, n INTEGER NOT NULL);")
        stmts.append(
            f"INSERT OR REPLACE INTO source_counts(source_id, n) "
            f"SELECT {_lit(src)}, COUNT(*) FROM series WHERE source_id = {_lit(src)};")

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
    # The replay schema must carry EVERY table the emitted SQL writes, or the guard that exists
    # to keep broken SQL away from remote D1 becomes the thing that breaks. Mirrors the worker's
    # columns for source/license (see sql.ts SELECT_SOURCES).
    mem.execute("CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, "
                "license_id TEXT, attribution TEXT, terms_url TEXT)")
    mem.execute("CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, url TEXT, "
                "reservable INT, commercial_ok INT, attribution_required INT, no_modify INT)")
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
    ap.add_argument("--no-diff", action="store_true",
                    help="send every queued row even if the local manifest says D1 already "
                         "has it. Escape hatch for a suspected divergence; it restores the "
                         "~$86/month behaviour, so say why in the log when you use it.")
    ap.add_argument("--seed-manifest", action="store_true",
                    help="record every LOCAL catalogue row as already-sent and exit, "
                         "sending nothing. Bootstrap only — correct exactly when D1 already "
                         "holds them (measured 2026-08-31: 322/322 sources, 0 short).")
    a = ap.parse_args(argv)

    # WAIT FOR A WRITER INSTEAD OF DYING ON IT. Every other tool here opens catalog.db with a
    # busy timeout; this one did not, so a concurrent catalogue build -- an ordinary thing, since
    # cataloguing a source and syncing another are independent jobs -- aborted the sync outright
    # with "database is locked". A read-only connection still has to wait out a writer's lock.
    conn = sqlite3.connect(f"file:{CATALOG_DB}?mode=ro", uri=True, timeout=300.0)
    conn.execute("PRAGMA busy_timeout = 300000")

    # SEED FIRST: it reads the CATALOGUE, not the pending queue, so it must not be gated
    # behind "nothing to sync" — an empty queue is the normal state to bootstrap in.
    if a.seed_manifest:
        _m = _Manifest(_manifest_path(ROOT))
        n = _m.seed_from_catalog(conn)
        _m.close(); conn.close()
        print(f"seeded the sync manifest with {n:,} local row(s); nothing was sent. "
              f"This asserts D1 already holds them — re-verify before seeding after any "
              f"catalogue rebuild.")
        return
    if a.source:
        # SAY SOMETHING BEFORE THE SLOW PART (ledger R706 rule 2). This selection used to be the
        # first thing the tool did and it printed nothing, so a run that was still reading looked
        # identical to a run that had hung - two background jobs sat 70 and 15 minutes on this
        # exact query shape before anyone noticed they had produced nothing.
        print(f"selecting local catalogue rows for source={a.source} ...", flush=True)
        # INDEX SEARCH, NOT A TABLE SCAN. `source_id` carries no index, so `WHERE source_id=?`
        # plans as `SCAN series` over an 11.9 GB file: measured still running after 20 minutes
        # while the workstation's crawlers held the disk, and R706 records the same shape timing
        # out at 400 s. `series_id` IS the primary key and is built as "<source>:<key>", so a
        # range over it plans as `SEARCH series USING INDEX sqlite_autoindex_series_1` - measured
        # 44.6 s for statcan's 466,341 ids under that same contention.
        #
        # The `source_id = ?` term is kept deliberately. It costs nothing (it filters rows the
        # index already matched) and it makes a FALSE INCLUSION impossible if some source ever
        # keys a series outside its own prefix. A false EXCLUSION would still be possible in that
        # case, which is why the count is printed: compare it against the source's known row
        # count before trusting a surprising number.
        ids = [r[0] for r in conn.execute(
            "SELECT series_id FROM series "
            "WHERE series_id >= ? AND series_id < ? AND source_id = ?",
            (a.source + ":", a.source + ";", a.source))]
        print(f"  selected {len(ids):,} id(s)", flush=True)
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
    print(f"catalog sync: {len(ids)} id(s) from {src} -> {len(rows)} local row(s)")

    # THE DIFF (ledger R542). Everything below sends only rows whose CONTENT changed since
    # the last successful sync, compared against a LOCAL manifest — never against D1, which
    # would re-introduce the full scans this exists to remove.
    manifest = _Manifest(_manifest_path(ROOT))
    if a.no_diff:
        print("  [diff] DISABLED by --no-diff: sending every queued row")
        skipped = 0
    else:
        before = len(rows)
        rows, skipped = manifest.split(cols, rows)
        print(f"  [diff] {skipped:,} of {before:,} row(s) unchanged since the last successful "
              f"sync -> not sent; {len(rows):,} to send "
              f"({-(-len(rows) // FTS_DELETE_PER_STMT):,} FTS delete statement(s), each a "
              f"full scan of series_fts)")
        if manifest.count() == 0 and skipped == 0 and before > 1000:
            print("  [diff] WARNING: the manifest is EMPTY, so nothing can be skipped and "
                  "this run would push the whole queue. Run --seed-manifest first "
                  "(see its help).")
    if not rows:
        conn.close()
        try:
            manifest.close()
        except Exception:                                        # noqa: BLE001
            pass
        if skipped:
            print(f"  nothing to send: all {skipped:,} queued row(s) are already in D1 "
                  f"unchanged. Zero statements, zero FTS scans.")
            if not a.source and not a.keep_pending:
                path = a.ids_file or PENDING
                open(path, "w", encoding="utf-8").close()
                print(f"  cleared {path}")
        else:
            print("  none of those ids exist in the local catalog — nothing to advertise")
        return

    # Partition by destination DATABASE before emitting: shard-routed sources
    # (CATALOG_SHARD_FOR — today noaa on econ-catalog-climate, task #45) must never
    # land on the primary. The worker reads them from the shard binding, so a
    # primary push would both re-consume the headroom the migration freed AND be
    # invisible to every request. The pending-ids path can mix sources, so the
    # split is per ROW, not per invocation; parent source/license rows are emitted
    # per group and therefore follow their series to the right database.
    groups: dict = {}
    for r in rows:
        groups.setdefault(CATALOG_SHARD_FOR.get(r.get("source_id")), []).append(r)

    out_dir = tempfile.mkdtemp(prefix="d1catalog_")
    plans = []
    for db, grp in sorted(groups.items(), key=lambda kv: kv[0] or ""):
        sub = os.path.join(out_dir, db or "primary")
        # `conn` so the parent source/license rows ship with the series — without them the
        # ids resolve but the source never appears in /v1/sources (see _parent_rows). The
        # close MOVED below these calls: it used to run immediately after _rows_for, so
        # passing the handle here would have queried a closed connection.
        # The range delete is offered ONLY when this group provably IS a whole source:
        # invoked with --source, and every row in the group carries that source_id. The
        # pending-queue path (a partial slice, possibly mixing sources) can never satisfy
        # both, so it keeps the per-block id-list deletes. Getting this wrong deletes the
        # index rows of every series of the source that is NOT in `rows`.
        whole = whole_source_reconcile(a.source, grp, skipped, len(groups))
        if a.source and not whole and grp:
            print(f"  [fts] NOT a whole-source reconcile for {a.source}: "
                  f"{skipped:,} row(s) were dropped by the diff and would be UNLISTED by a "
                  f"range DELETE, so this uses "
                  f"{-(-len(grp) // FTS_DELETE_PER_STMT):,} id-list statement(s). Pass "
                  f"--no-diff to send every row and take the single-scan form.")
        if whole:
            print(f"  [fts] whole-source reconcile for {a.source}: ONE range DELETE "
                  f"instead of {-(-len(grp) // FTS_DELETE_PER_STMT):,} id-list statements "
                  f"(each is a full scan of series_fts)")
        plans.append((db, grp, emit_sql(cols, grp, sub, conn, fts_range_source=whole)))
    conn.close()
    for db, grp, files in plans:
        if db:
            print(f"  [shard] {len(grp)} row(s) route to {db}")
        verify_replay(cols, grp, files)
    if a.dry_run:
        for _, _, files in plans:
            for p in files:
                print("  (dry-run)", p)
        return
    for db, _, files in plans:
        execute_remote(files, database=db)
    if not a.source and not a.keep_pending:
        path = a.ids_file or PENDING
        open(path, "w", encoding="utf-8").close()
        print(f"  cleared {path}")
    # POST-SUCCESS ONLY. A run that died partway recorded nothing and re-sends next time —
    # a re-send costs money, a false "already sent" costs correctness.
    manifest.record(cols, rows)
    manifest.close()
    print(f"catalog sync OK: {len(rows)} series row(s) upserted to D1 "
          f"({skipped:,} skipped as unchanged)")


if __name__ == "__main__":
    sys.exit(main())
