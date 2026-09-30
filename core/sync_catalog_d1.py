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
import re
import sqlite3
import sys
import tempfile
import uuid

# Run as a script (`python core/sync_catalog_d1.py`, which is how the workflow calls
# it) the repo root is not on sys.path, so `import core.*` fails. Bootstrap before
# the sibling import rather than relying on the caller's cwd.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")))

from core.sync_state_d1 import (CATALOG_SHARD_FOR, MAX_FILE_BYTES,  # noqa: E402
                                ROWS_PER_STMT, ROOT, _gated_ids, _lit, execute_remote)

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
#
# RAISED 500 -> 1,000, WITH A BYTE CAP (2026-09-30, R1308/R1312). Measured through the import road
# (core.d1_remote.execute_file, one DELETE of ids that cannot exist): 500 ids / 21 KB, 1,000 / 42 KB,
# 1,047 / 44 KB and 1,999 / 84 KB each read 10,816,861 rows in 16.8-19.4 s - the SAME one scan, so the
# id count does not change the cost. Two earlier 2,000-id attempts failed with APIError 7009 at import
# start, but the 1,999-id retry passed: that 7009 was TRANSIENT, not a size limit. The 45 KB cap is a
# precaution inside the measured-ok range, not a measured limit.
FTS_DELETE_PER_STMT = 1000
FTS_DELETE_MAX_BYTES = 45_000
# ONE FTS id-list DELETE PER IMPORT FILE (R1308). The yale_epi sync's catalog_0002.sql held 8 of them
# and wrangler 3.114's import was cancelled 4 times with "no poll() received in 15000ms". A single
# statement of 16.8-19.4 s passes, so 15 s is NOT a per-statement limit and the real mechanism is not
# known; files of 2-7 scans were never measured. One per file is the shape measured to pass - see the
# real-shape measurement recorded with this change - so the splitter starts a new file before a second.
FTS_DELETES_PER_FILE = 1


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
from core.catalog_sync_manifest import ManifestBusy as _ManifestBusy  # noqa: E402
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
             fts_range_source: str | None = None,
             fts_skip: set | frozenset = frozenset()) -> list[str]:
    """Chunked INSERT OR REPLACE for `series`, plus matching `series_fts` rows.

    Given `conn`, the parent `source`/`license` rows are emitted FIRST — see _parent_rows for
    why omitting them produces a fetchable-but-unlistable source.

    `fts_skip`: series_ids whose index row (series_id, title, geography) D1 already holds exactly as
    now - the manifest recorded that FTS content after a real send (Manifest.fts_current). Their
    series row still goes; their FTS delete+insert does not, so a date-only refresh costs no scan.
    Ignored by the whole-source range form, which deletes the source's whole index and must
    re-insert every row.
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
        rows_for_fts = [r for r in rows if r["series_id"] not in fts_skip]
    # ONE ELEMENT PER BLOCK: the DELETE and the INSERTs it covers are joined into one element of
    # `stmts`, so the file splitter below can never put them in different files. Every file of this
    # form therefore re-applies safely: its own DELETE runs again before its own INSERTs. That matters
    # because a failed wrangler EXIT can come after the server already took the file (wrangler 3.114
    # polls after the import; R1191 finding 2), and a retry of a file holding bare FTS INSERTs whose
    # DELETE ran in an earlier file duplicates the index (R1185).
    for block in _fts_blocks(rows_for_fts):
        _ids = ",".join(_lit(r["series_id"]) for r in block)
        unit = [f"DELETE FROM series_fts WHERE series_id IN ({_ids});"]
        for j in range(0, len(block), ROWS_PER_STMT):
            ch = block[j:j + ROWS_PER_STMT]
            vals = ",\n  ".join(
                "(%s,%s,%s)" % (_lit(r["series_id"]), _lit(r.get("title")),
                                _lit(r.get("geography"))) for r in ch)
            unit.append("INSERT INTO series_fts (series_id,title,geography) VALUES\n  "
                        f"{vals};")
        stmts.append("\n".join(unit))

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
    run = uuid.uuid4().hex[:16]
    cap = MAX_FILE_BYTES - RECEIPT_RESERVE              # every file ends with its receipt (receipt_sql)
    files, buf, n, scans = [], [], 0, 0

    def _write():
        p = os.path.join(out_dir, f"catalog_{len(files):04d}.sql")
        with open(p, "w", encoding="utf-8", newline="\n") as fh:
            fh.write("\n".join(buf + [receipt_sql(f"cat-{run}-{len(files):04d}")]) + "\n")
        files.append(p)

    for s in stmts:
        if len(s) > cap:
            raise SystemExit(f"FATAL: one statement block is {len(s):,} B, over the {cap:,} B file cap; "
                             "it cannot be sent whole, and splitting it would break its re-application")
        s_scans = sum(1 for m in _FTS_WRITE.finditer(s) if m.group(1) == "DELETE FROM")
        if buf and (n + len(s) > cap or scans + s_scans > FTS_DELETES_PER_FILE):
            _write()
            buf, n, scans = [], 0, 0
        buf.append(s); n += len(s) + 1; scans += s_scans
    if buf:
        _write()
    return files


# THE FILE'S OWN RECEIPT (review R1313). wrangler 3.114 can exit 1 with "Not currently importing anything" after a
# file that DID commit (measured 2026-09-30 21:02Z), and that text is also what a rollback leaves - so the output
# cannot say which. Every emitted file therefore ENDS with a row keyed to that file; the import is one transaction,
# so the row exists in D1 exactly when the whole file committed, and core.d1_remote.committed_by_receipt reads it
# back by primary key (1 row) before calling such an exit a success. Rows older than 30 days are pruned by the same
# statement group, so the table stays a few thousand rows.
# The key is per FILE here; core.d1_remote.execute_file sends each ATTEMPT as a copy whose key carries a fresh
# nonce, so a row committed by an earlier send of the same file can never vouch for a later one (review R1314).
# sync_receipt is a D1-ONLY bookkeeping table (created 2026-09-30): it is not in the local catalogue, the worker
# never reads it, and a table audit should expect it on econ-catalog and on any shard the sync writes.
RECEIPT_RESERVE = 400


def receipt_sql(key: str) -> str:
    assert re.fullmatch(r"[A-Za-z0-9_.:-]+", key), key
    return ("CREATE TABLE IF NOT EXISTS sync_receipt(k TEXT PRIMARY KEY, at TEXT);\n"
            "DELETE FROM sync_receipt WHERE at < datetime('now', '-30 days');\n"
            f"INSERT OR REPLACE INTO sync_receipt(k, at) VALUES('{key}', datetime('now'));")


def _fts_blocks(rows: list[dict]):
    """Blocks for the id-list FTS DELETE: up to FTS_DELETE_PER_STMT ids, closed early when the DELETE
    statement would pass FTS_DELETE_MAX_BYTES, or when the block's whole unit (the DELETE plus its
    INSERTs) would pass half the file cap - a unit is never split across files (R1185)."""
    block, del_bytes, unit_bytes = [], 0, 0
    for r in rows:
        idb = len(_lit(r["series_id"])) + 1
        rowb = idb + len(_lit(r.get("title"))) + len(_lit(r.get("geography"))) + 8
        if block and (len(block) >= FTS_DELETE_PER_STMT or del_bytes + idb > FTS_DELETE_MAX_BYTES
                      or unit_bytes + rowb > MAX_FILE_BYTES // 2):
            yield block
            block, del_bytes, unit_bytes = [], 0, 0
        block.append(r)
        del_bytes += idb
        unit_bytes += rowb
    if block:
        yield block


def reapplicable(path: str) -> bool:
    """True when applying the file twice leaves D1 as applying it once: it holds no bare FTS INSERT, or a
    series_fts DELETE precedes its first one. emit_sql keeps every id-list DELETE in one file with its
    INSERTs, so each such file passes. In the whole-source (range) form ONE DELETE covers the source, and
    only the file that carries it passes; the files after it hold bare INSERTs. Everything else
    emit_sql writes is INSERT OR REPLACE, CREATE ... IF NOT EXISTS or a DELETE, which are safe twice."""
    with open(path, encoding="utf-8") as fh:
        kinds = [m.group(1) for m in _FTS_WRITE.finditer(fh.read())]
    return "INSERT INTO" not in kinds or kinds[0] == "DELETE FROM"


_FTS_WRITE = re.compile(r"(DELETE FROM|INSERT INTO) series_fts\b")


def execute_plans(plans) -> None:
    """Send each plan's files in order. A file that re-applies safely keeps the retries (a timeout
    included); one that does not - bare FTS INSERTs after a whole-source range DELETE - runs ONCE (R1191
    finding 2). If that one fails, re-run the whole command: its first FTS file repeats the range DELETE,
    so the source comes out clean."""
    for db, _, files in plans:
        i, restarted = 0, False
        while i < len(files):
            p = files[i]
            safe = reapplicable(p)
            try:
                execute_remote([p], database=db, idempotent=safe, tries=4 if safe else 1)
            except SystemExit:
                if safe:
                    if restarted:            # the DELETE file itself failed during the restart (R1200)
                        print(f"FATAL: {os.path.basename(p)} failed while restarting after a whole-source "
                              "DELETE: the source's search index is PARTLY rebuilt. Re-run the same "
                              "command - it starts again with the DELETE and leaves the index whole.",
                              file=sys.stderr, flush=True)
                    raise
                # a bare-INSERT file after the range DELETE failed: the source's search index is PARTLY
                # rebuilt. Go back ONCE to the file that holds the DELETE (re-deleting, then re-inserting)
                # - the auth error the retries exist for fails before the server takes a file (R1195)
                back = next((k for k in range(i - 1, -1, -1) if _has_fts_delete(files[k])), None)
                if restarted or back is None:
                    print(f"FATAL: {os.path.basename(p)} failed after the whole-source DELETE: the source's "
                          "search index is PARTLY rebuilt. Re-run the same command - it starts again with "
                          "the DELETE and leaves the index whole.", file=sys.stderr, flush=True)
                    raise
                print(f"  {os.path.basename(p)} failed after the whole-source DELETE - starting once more "
                      f"from {os.path.basename(files[back])}", flush=True)
                restarted, i = True, back
                continue
            i += 1


def _has_fts_delete(path: str) -> bool:
    with open(path, encoding="utf-8") as fh:
        return any(m.group(1) == "DELETE FROM" for m in _FTS_WRITE.finditer(fh.read()))


def verify_replay(cols: list[str], rows: list[dict], files: list[str], fts_ids: set | None = None) -> None:
    """Replay the emitted SQL into a fresh in-memory SQLite and assert row-for-row
    equality with what we intended to send. Broken SQL never reaches remote D1."""
    mem = sqlite3.connect(":memory:")
    # NO declared column types: SQLite then stores each literal as it was written (int, float, text, NULL) with no
    # affinity conversion, so the replay can be compared with the rows EXACTLY, every column (review R1316).
    mem.execute(f"CREATE TABLE series ({', '.join(cols)}, PRIMARY KEY (series_id))")
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
        with open(p, encoding="utf-8", newline="") as fh:
            mem.executescript(fh.read())
    got = mem.execute("SELECT COUNT(*) FROM series").fetchone()[0]
    if got != len(rows):
        raise SystemExit(f"FATAL: replay has {got} series rows, expected {len(rows)} "
                         "— refusing to send SQL that does not round-trip")
    # EVERY row, EVERY column, byte for byte (review R1316: this checked the title of rows[:50] only, so a
    # damaged title at row 55 passed as "verified"). The replay is in memory; this costs milliseconds.
    got_rows = {r[0]: r for r in mem.execute(f"SELECT {', '.join(cols)} FROM series")}
    sid_at = cols.index("series_id")
    assert all(r[sid_at] == k for k, r in got_rows.items())
    for r in rows:
        want = tuple(r.get(c) for c in cols)
        hit = got_rows.get(r["series_id"])
        if hit != want:
            bad = [c for c, a, b in zip(cols, hit or (), want) if a != b] if hit else ["<missing>"]
            raise SystemExit(f"FATAL: replay lost/altered {r['series_id']} (columns {bad}) - refusing to send")
    n_receipts = mem.execute("SELECT count(*) FROM sync_receipt").fetchone()[0] if mem.execute(
        "SELECT 1 FROM sqlite_master WHERE name='sync_receipt'").fetchone() else 0
    if n_receipts != len(files):
        raise SystemExit(f"FATAL: replay found {n_receipts} receipt row(s) for {len(files)} file(s) - every file "
                         "must end with its own (R1313) - refusing to send")
    if fts_ids is not None:
        # the index rows the files write: exactly one per id meant to be rewritten, none for a skipped id
        got_fts = {}
        for (sid,) in mem.execute("SELECT series_id FROM series_fts"):
            got_fts[sid] = got_fts.get(sid, 0) + 1
        if set(got_fts) != set(fts_ids) or any(v != 1 for v in got_fts.values()):
            raise SystemExit(f"FATAL: replay wrote index rows for {len(got_fts)} id(s) "
                             f"(max {max(got_fts.values(), default=0)} each), expected exactly one for each of "
                             f"{len(fts_ids)} - refusing to send")
        # and each index row carries the row's own title and geography, exactly (R1316)
        by_id = {r["series_id"]: r for r in rows}
        for sid, title, geo in mem.execute("SELECT series_id, title, geography FROM series_fts"):
            want = (by_id[sid].get("title"), by_id[sid].get("geography"))
            if (title, geo) != want:
                raise SystemExit(f"FATAL: replay index row for {sid} carries altered title/geography - refusing to send")
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
    ap.add_argument("--refresh-counts", metavar="SRC[,SRC...]",
                    help="recompute source_counts for these sources and exit, sending no series "
                         "rows. Repair path for a cache that drifted from `series` — see R709 and "
                         "tools/audit_d1_source_counts.py, which finds the drift and prints the "
                         "source list to pass here.")
    a = ap.parse_args(argv)

    # THE GATE APPLIES TO THIS PUBLISHER TOO (2026-09-17). This sync reads the catalogue - in CI the
    # R2 coherence copy, which was measured the same day still holding a gated source's series and
    # source row - and upserts series, series_fts, source, license and source_counts into D1 with
    # no gate consult. It is frozen behind CATALOG_SYNC_ENABLED today; unfreezing it would have
    # re-published rows a reviewed delete had just removed. Gated ids are refused as a --source or
    # --refresh-counts argument and withheld from the rows below. Nothing is deleted here (R889
    # rule 3). _gated_ids refuses an absent or unreadable gate.
    # Read only on the paths that PUBLISH: --seed-manifest sends nothing, and reading the gate there
    # would make the bootstrap depend on the worker checkout for no benefit.
    gated = set() if a.seed_manifest else _gated_ids()
    for named in ([a.source] if a.source else []) + \
            [s.strip() for s in (a.refresh_counts or "").split(",") if s.strip()]:
        if named.lower() in gated:
            raise SystemExit("FATAL: a named source is gated by the worker's denylist - refusing to "
                             "publish its catalogue rows or counts")

    # WAIT FOR A WRITER INSTEAD OF DYING ON IT. Every other tool here opens catalog.db with a
    # busy timeout; this one did not, so a concurrent catalogue build -- an ordinary thing, since
    # cataloguing a source and syncing another are independent jobs -- aborted the sync outright
    # with "database is locked". A read-only connection still has to wait out a writer's lock.
    conn = sqlite3.connect(f"file:{CATALOG_DB}?mode=ro", uri=True, timeout=300.0)
    conn.execute("PRAGMA busy_timeout = 300000")

    # SEED FIRST: it reads the CATALOGUE, not the pending queue, so it must not be gated
    # behind "nothing to sync" — an empty queue is the normal state to bootstrap in.
    if a.seed_manifest:
        if a.dry_run:
            # --dry-run writes NOTHING (R1305): seeding under it recorded changed, queued rows as
            # already sent, so the next real sync skipped them and cleared the queue.
            conn.close()
            print(f"  (dry-run) would seed the sync manifest {_manifest_path(ROOT)} from every local "
                  f"catalogue row; nothing was written.")
            return
        _m = _Manifest(_manifest_path(ROOT))
        n = _m.seed_from_catalog(conn)
        _m.close(); conn.close()
        print(f"seeded the sync manifest with {n:,} local row(s); nothing was sent. "
              f"This asserts D1 already holds them — re-verify before seeding after any "
              f"catalogue rebuild.")
        return

    # REPAIR PATH FOR A DRIFTED source_counts ROW (R709 / noaa +42).
    #
    # It lives HERE, in the sync, on purpose. Two documents state that source_counts has exactly
    # one writer (`skills/econ-completion/references/state-baseline.md:25`, `protocols.md:38`), and
    # a hand-run `wrangler d1 execute` to patch a row would quietly make both false. Repair through
    # the single writer instead.
    #
    # The predicate is the PK RANGE, not `WHERE source_id = ?` as the post-insert refresh at the
    # bottom of emit_sql() uses. ONE reason, not two — and the reason I first wrote here was
    # wrong, so it is worth stating what was measured.
    #
    # I claimed the range "rides the autoindex so a repair costs a bounded read instead of a full
    # scan". That is true of the LOCAL catalog.db, whose only index is the series_id primary key
    # (a `GROUP BY source_id` there runs past 900 s). It is NOT true of D1. Measured 2026-09-04
    # on econ-catalog, both forms are index seeks and read exactly n+1 rows:
    #
    #   WHERE source_id='bea'                        n=913,230  rows_read=913,231   421 ms
    #   WHERE series_id >= 'bea:' AND < 'bea;'       n=913,230  rows_read=913,231   911 ms
    #   WHERE source_id='nyfed'                      n=8        rows_read=9         1.6 ms
    #
    # So D1 indexes source_id, the range is not cheaper there, and it is in fact SLOWER. There is
    # no cost argument for changing emit_sql's predicate, and it has not been changed.
    #
    # What survives is the semantic reason: the range is the population `browseSourceSql` actually
    # pages (sql.ts:216), which is what `total` claims to describe. Measured on noaa the two agree
    # exactly (3,138,159 both ways, 0 rows in either asymmetric difference), and
    # tests/test_source_prefix_range.py proves the equivalence in general. If they ever DISAGREE
    # the two writers would fight — this one repairs, the next sync overwrites — and
    # `tools/audit_d1_source_counts.py --remote-truth` is what would surface that oscillation.
    if a.refresh_counts:
        conn.close()
        srcs = [s.strip() for s in a.refresh_counts.split(",") if s.strip()]
        if not srcs:
            raise SystemExit("--refresh-counts needs at least one source id")
        if a.dry_run:
            # --dry-run MUST NOT WRITE. Beyond the plain contract at the flag's own help text
            # ("emit + verify, execute nothing"), the cost guard depends on it:
            # .claude/hooks/d1_cost_guard.py DRIVER_FREE matches `--dry-run` and allows the call
            # UNCOUNTED, on the stated grounds that "neither reaches D1". Without this branch
            # `--dry-run --refresh-counts noaa` performed a remote write while being waved
            # through as free -- a write the budget could not see.
            for src in srcs:
                db = CATALOG_SHARD_FOR.get(src) or "econ-catalog"
                print(f"  (dry-run) would refresh source_counts for {src} on {db} "
                      f"via COUNT(*) over the PK range {src}: .. {src};")
            return
        tmp = tempfile.mkdtemp(prefix="d1counts_")
        for src in srcs:
            db = CATALOG_SHARD_FOR.get(src)
            path = os.path.join(tmp, f"counts_{src}.sql")
            with open(path, "w", encoding="utf-8", newline="\n") as fh:
                fh.write("CREATE TABLE IF NOT EXISTS source_counts("
                         "source_id TEXT PRIMARY KEY, n INTEGER NOT NULL);\n")
                fh.write("INSERT OR REPLACE INTO source_counts(source_id, n)\n"
                         f"  SELECT {_lit(src)}, COUNT(*) FROM series\n"
                         f"   WHERE series_id >= {_lit(src + ':')} "
                         f"AND series_id < {_lit(src + ';')};\n")
            print(f"  refreshing source_counts for {src} on "
                  f"{db or 'econ-catalog'} ...", flush=True)
            execute_remote([path], db)
        print(f"source_counts refreshed for {len(srcs)} source(s); no series rows were sent. "
              f"Verify with: python tools/audit_d1_source_counts.py --remote-truth")
        return

    if a.source:
        # SAY SOMETHING BEFORE THE SLOW PART (ledger R706 rule 2). This selection used to be the
        # first thing the tool did and it printed nothing, so a run that was still reading looked
        # identical to a run that had hung - two background jobs sat 70 and 15 minutes on this
        # exact query shape before anyone noticed they had produced nothing.
        print(f"selecting local catalogue rows for source={a.source} ...", flush=True)
        # INDEX SEARCH, NOT A TABLE SCAN. (`source_id` carried no index when this was written;
        # it does now - idx_series_source in D1, ix_series_source_id locally, both verified
        # 2026-09-22. The PK-range form below is still correct and still cheap, so nothing
        # here changes - but do not repeat the missing-index premise elsewhere.) `WHERE source_id=?`
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
    kept = [r for r in rows if str(r.get("source_id") or "").lower() not in gated]
    if len(kept) != len(rows):
        # the rows are withheld; which sources they belong to is not printed
        print(f"  [gate] withheld {len(rows) - len(kept):,} row(s) of gated sources - not sent")
    rows = kept

    # THE DIFF (ledger R542). Everything below sends only rows whose CONTENT changed since
    # the last successful sync, compared against a LOCAL manifest — never against D1, which
    # would re-introduce the full scans this exists to remove.
    # read-only under --dry-run: no WAL switch, no DDL, no file created (R1305); a manifest being
    # written is refused, not copied (R1306)
    try:
        manifest = _Manifest(_manifest_path(ROOT), read_only=a.dry_run)
    except _ManifestBusy as e:
        conn.close()
        raise SystemExit(f"refused: {e}") from None
    fts_skip: set = set()
    try:
        if a.no_diff:
            print("  [diff] DISABLED by --no-diff: sending every queued row (and every index row)")
            skipped = 0
        else:
            before = len(rows)
            rows, skipped = manifest.split(cols, rows)
            # index rows D1 already holds exactly (R1308): their FTS delete+insert is left out
            fts_skip = manifest.fts_current(rows)
            n_fts = len(rows) - len(fts_skip)
            print(f"  [diff] {skipped:,} of {before:,} row(s) unchanged since the last successful "
                  f"sync -> not sent; {len(rows):,} to send, of which {len(fts_skip):,} keep their "
                  f"index row -> {n_fts:,} index row(s) rewritten in "
                  f"~{-(-n_fts // FTS_DELETE_PER_STMT):,} FTS delete statement(s), each a full scan of "
                  f"series_fts and each in its own import file")
            # is_empty(), NOT count() == 0, and the cheap operands FIRST. count() is a full scan of a
            # 2.17 GB file; asked left-to-right on a full-source push (where skipped == 0 and
            # before > 1000 are both true) it ran before either cheap test and stalled the statcan
            # push for fifteen minutes at 0.1 s of CPU, before a single statement was emitted.
            if skipped == 0 and before > 1000 and manifest.is_empty():
                print("  [diff] WARNING: the manifest is EMPTY, so nothing can be skipped and "
                      "this run would push the whole queue. Run --seed-manifest first "
                      "(see its help).")
        if a.dry_run and not manifest.stable():
            # the immutable read holds no lock: a sync that started meanwhile can make the numbers
            # above wrong, so they are withdrawn rather than trusted (R1306)
            conn.close()
            raise SystemExit("refused: the sync manifest changed while the dry run read it - the "
                             "numbers above are not reliable; re-run the dry run when no sync is running.")
    finally:
        if a.dry_run:
            manifest.close()                  # a dry run never records; nothing may stay open
    if not rows:
        conn.close()
        try:
            manifest.close()
        except Exception:                                        # noqa: BLE001
            pass
        if skipped:
            print(f"  nothing to send: all {skipped:,} queued row(s) are already in D1 "
                  f"unchanged. Zero statements, zero FTS scans.")
            # --dry-run writes NOTHING, the queue included. This clear ran before the dry-run
            # return below, so a dry run over an all-unchanged queue emptied the production
            # pending file (54,619 lines, no copy, 2026-09-30 - R1304).
            if not a.source and not a.keep_pending:
                path = a.ids_file or PENDING
                if a.dry_run:
                    print(f"  (dry-run) would clear {path}")
                else:
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
    plans, fts_expect = [], {}
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
        grp_skip = frozenset() if whole else frozenset(r["series_id"] for r in grp) & frozenset(fts_skip)
        fts_expect[db] = {r["series_id"] for r in grp} - grp_skip
        plans.append((db, grp, emit_sql(cols, grp, sub, conn, fts_range_source=whole, fts_skip=grp_skip)))
    conn.close()
    for db, grp, files in plans:
        if db:
            print(f"  [shard] {len(grp)} row(s) route to {db}")
        verify_replay(cols, grp, files, fts_ids=fts_expect[db])
    if a.dry_run:
        manifest.close()
        for _, _, files in plans:
            for p in files:
                print("  (dry-run)", p)
        return
    # Index rows this run rewrites are UNKNOWN until it succeeds (R1312 finding 7): a run that dies after
    # some files may already have replaced them in D1, and the old record must not vouch for them.
    manifest.forget_fts(sorted(set().union(*fts_expect.values())) if fts_expect else [])
    execute_plans(plans)
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
