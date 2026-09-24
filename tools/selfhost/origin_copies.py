"""Build the origin's two catalogue copies from the one catalogue (docs/ECON_SELF_HOSTING_PLAN.md, section 2:
"per-swap DISPOSABLE COPIES of the build, made with the SQLite backup API", then checked before the flip).

The origin's worker is the production code: it reads the primary catalogue from the CATALOG binding and the
climate shard's sources (core.sync_state_d1.CATALOG_SHARD_FOR, today noaa) from CATALOG_CLIMATE, and its
search, browse and /v1/stats MERGE the two. So the one local catalogue is split exactly as the D1 sync splits
it - measured 2026-09-24 on the probe copy: the primary held all 13,952,906 series INCLUDING 3,138,159 of the
shard's, and the climate copy was empty, so those series were unreachable and would have been double-counted
had both held them:

  primary  = every table of the catalogue, minus the shard sources' series / source_counts rows
  climate  = the shard sources' series rows, their source and license parent rows, and their source_counts
  source_counts is recomputed in both from `series` itself (a copy cannot inherit a drifted count - R709)
  the FRESHNESS projection the worker reads from CATALOG - unit_state, source_state, source_data_through -
             is built into the primary copy by the D1 sync's own emitter (core.sync_state_d1.emit_sql, the
             same licence gate) from state.db and the catalogue being copied, when state_db is given (R1186:
             the production catalog.db holds only license/series/series_fts/source - these tables lived in
             D1 alone, so an origin copied from it answered /v1/last-updates with nothing)
  series_fts is REBUILT in both from `series` itself (review R1183: 31 writers change `series` without
             touching series_fts - a retitle, a new series, a re-keyed id - and a count check cannot see a
             retitle; the catalogue's own index is never copied, so search answers from what `series` holds)

Checks, all before a copy can be used (any failure deletes the half-built copies and raises):
  PRAGMA quick_check = ok on both; primary + climate series = the catalogue's series; per file,
  series_fts rows = series rows; no shard series in the primary; only shard series in the climate copy.

The catalogue is read through the SQLite backup API only (a plain file copy of a rollback-journal database
can be torn - R1176), never written. Run:
  python tools/selfhost/origin_copies.py --catalogue <catalog.db> --out <dir>
"""
from __future__ import annotations

import argparse
import contextlib
import json
import os
import pathlib
import sqlite3
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
from core.sync_state_d1 import CATALOG_SHARD_FOR  # noqa: E402

SHARD_SOURCES = tuple(sorted(s for s, db in CATALOG_SHARD_FOR.items() if db == "econ-catalog-climate"))
COUNTS_DDL = "CREATE TABLE source_counts(source_id TEXT PRIMARY KEY, n INTEGER NOT NULL)"


def _ro(path: str) -> sqlite3.Connection:
    return sqlite3.connect(pathlib.Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=120)


def _recount(con: sqlite3.Connection) -> None:
    con.execute("DROP TABLE IF EXISTS source_counts")
    con.execute(COUNTS_DDL)
    con.execute("INSERT INTO source_counts(source_id, n) SELECT source_id, COUNT(*) FROM series GROUP BY source_id")


def _rebuild_fts(con: sqlite3.Connection, ddl: str) -> None:
    """Drop and re-create series_fts from the catalogue's own DDL, then fill it from this copy's `series`
    (every FTS column is a `series` column: series_id UNINDEXED, title, geography)."""
    con.execute("DROP TABLE IF EXISTS series_fts")
    con.execute(ddl)
    cols = ", ".join(r[1] for r in con.execute("PRAGMA table_info(series_fts)"))
    con.execute(f"INSERT INTO series_fts({cols}) SELECT {cols} FROM series")


def build(catalogue: str, out_dir: str, lock=None, state_db: str | None = None) -> dict:
    """Write out_dir/primary.sqlite and out_dir/climate.sqlite; return the checked counts.

    `lock` (a callable returning a context manager, e.g. the catalogue writer lock after T0) is held ONLY
    while the catalogue is READ - the count, the backup and the climate rows, all from one state of the
    file. The shard deletes, the recount, the index rebuild and the checks touch only the copies and run
    after it is released (R1185: holding it for the whole build doubled the time writers are refused)."""
    os.makedirs(out_dir, exist_ok=True)
    primary, climate = os.path.join(out_dir, "primary.sqlite"), os.path.join(out_dir, "climate.sqlite")
    for p in (primary, climate):
        for suffix in ("", "-journal", "-wal", "-shm"):
            if os.path.exists(p + suffix):
                os.remove(p + suffix)
    opened: list[sqlite3.Connection] = []
    try:
        with (lock() if lock else contextlib.nullcontext()):
            src = _ro(catalogue)
            opened.append(src)
            total = src.execute("SELECT COUNT(*) FROM series").fetchone()[0]
            schema = {n: s for n, s in src.execute(
                "SELECT name, sql FROM sqlite_master WHERE name IN ('series','series_fts','source','license')")}
            missing = {"series", "series_fts", "source", "license"} - set(schema)
            if missing:
                raise RuntimeError(f"the catalogue has no {sorted(missing)} table")

            # primary: a consistent copy through the backup API (the shard sources come out below)
            dst = sqlite3.connect(primary)
            opened.append(dst)
            src.backup(dst)

            # climate: the shard sources only, from the catalogue itself (not from the primary copy). Opened
            # by URI: ATTACH reads a file: URI only on a connection that was itself opened with URIs enabled.
            c = sqlite3.connect(pathlib.Path(climate).resolve().as_uri() + "?mode=rwc", uri=True)
            opened.append(c)
            for name in ("series", "source", "license"):
                c.execute(schema[name])
            c.execute("CREATE INDEX ix_series_source_id ON series(source_id)")
            c.execute("ATTACH DATABASE ? AS b", (pathlib.Path(catalogue).resolve().as_uri() + "?mode=ro",))
            for s in SHARD_SOURCES:
                c.execute("INSERT INTO series SELECT * FROM b.series WHERE source_id=?", (s,))
                c.execute("INSERT OR IGNORE INTO source SELECT * FROM b.source WHERE source_id=?", (s,))
            c.execute("INSERT OR IGNORE INTO license SELECT * FROM b.license WHERE license_id IN "
                      "(SELECT license_id FROM source UNION SELECT license_id FROM series)")
            c.commit()
            c.execute("DETACH DATABASE b")
            src.close()
            opened.remove(src)
            # the state half of the freshness projection, from the same moment (state.db is written under
            # the same lock). ONLY state.db is read here: data_through is a GROUP BY over 13.9M series rows
            # (1,833 s cold on production), and it is computed below from the copy (R1191 finding 5)
            fresh_sql = _emit_freshness(state_db, out_dir) if state_db else None

        # the catalogue is no longer read: only the copies are written from here on. data_through first,
        # while the primary copy still holds every source (the shard's rows leave it next)
        dt_rows = None
        if fresh_sql:
            from core import sync_state_d1
            gated = sync_state_d1._gated_ids()
            dt_rows = (sync_state_d1.data_through_rows(dst, gated)
                       + sync_state_d1.local_writer_rows(dst, gated))       # D1-only sources: their writer
        for s in SHARD_SOURCES:
            dst.execute("DELETE FROM series WHERE source_id=?", (s,))
        _recount(dst)
        _rebuild_fts(dst, schema["series_fts"])
        if fresh_sql:
            # the emitted projection is the ONLY source: a table the catalogue happened to carry (a copy
            # taken from D1 has them, possibly stale or with other columns) is replaced, never merged
            for t in FRESHNESS:
                dst.execute(f"DROP TABLE IF EXISTS {t}")
            for f in fresh_sql:
                dst.executescript(open(f, encoding="utf-8").read())
                os.remove(f)
            os.rmdir(os.path.dirname(fresh_sql[0]))
            dst.executescript("\n".join(sync_state_d1.data_through_stmts(dt_rows)))
        dst.commit()
        _recount(c)
        _rebuild_fts(c, schema["series_fts"])
        c.commit()
        for con in opened:
            con.close()
        opened.clear()

        return check(primary, climate, total, freshness=bool(state_db))
    except BaseException:
        for con in opened:                              # closed first: Windows cannot delete an open file
            con.close()
        for p in (primary, climate):
            if os.path.exists(p):
                os.remove(p)
        raise


def _emit_freshness(state_db: str, out_dir: str) -> list[str]:
    """The D1 sync's own state-table SQL (gate applied), written beside the copies; applied, then removed.
    source_data_through is left out here (data_through=False) and built from the copy by build()."""
    from core import sync_state_d1
    tmp = os.path.join(out_dir, "freshness_sql")
    os.makedirs(tmp, exist_ok=True)
    try:
        files, _counts = sync_state_d1.emit_sql(state_db, tmp, data_through=False)
    except SystemExit as e:               # e.g. its zero-row refusal: a failed build here, not an exit
        raise RuntimeError(f"freshness projection refused: {e}") from None
    return files


FRESHNESS = ("unit_state", "source_state", "source_data_through")


def check(primary: str, climate: str, total: int, freshness: bool = False) -> dict:
    """The checks that must pass before a copy is served. Raises RuntimeError naming the first failure."""
    out = {"catalogue_series": total}
    if freshness:
        con = _ro(primary)
        try:
            have = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0] for t in FRESHNESS
                    if con.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (t,)).fetchone()}
        finally:
            con.close()
        missing = [t for t in FRESHNESS if t not in have]
        if missing:
            raise RuntimeError(f"primary: the freshness table(s) {missing} are missing")
        if not have["unit_state"] or not have["source_state"]:
            raise RuntimeError(f"primary: an empty freshness projection {have} - /v1/last-updates would be empty")
        # EVERY served source with a dated series has a data_through row - checked on the RESULT, not on a
        # registry of writers (R1195: one dict entry made T0 READY while the copy carried none for sec_edgar)
        from core import sync_state_d1                                     # noqa: PLC0415
        gated = sync_state_d1._gated_ids()
        dated: set[str] = set()
        for path in (primary, climate):
            c = _ro(path)
            try:
                if "end_date" in {r[1] for r in c.execute("PRAGMA table_info(series)")}:
                    dated |= {r[0] for r in c.execute(
                        "SELECT DISTINCT source_id FROM series WHERE end_date IS NOT NULL AND end_date < '2900-01-01'")}
            finally:
                c.close()
        c = _ro(primary)
        try:
            stamped = {r[0] for r in c.execute("SELECT source_id FROM source_data_through WHERE data_through IS NOT NULL")}
        finally:
            c.close()
        unstamped = sorted(s for s in dated - stamped if str(s).lower() not in gated)
        if unstamped:
            raise RuntimeError(f"primary: no data_through for {unstamped} - /v1/sources would serve null "
                               "(a D1-only source needs its local writer: sync_state_d1.LOCAL_FRESHNESS_WRITERS)")
        out["freshness"] = have
    for label, path in (("primary", primary), ("climate", climate)):
        con = _ro(path)
        qc = con.execute("PRAGMA quick_check").fetchone()[0]
        n = con.execute("SELECT COUNT(*) FROM series").fetchone()[0]
        fts = con.execute("SELECT COUNT(*) FROM series_fts").fetchone()[0]
        shard = con.execute(f"SELECT COUNT(*) FROM series WHERE source_id IN ({','.join('?' * len(SHARD_SOURCES))})",
                            SHARD_SOURCES).fetchone()[0] if SHARD_SOURCES else 0
        counted = con.execute("SELECT COALESCE(SUM(n), 0) FROM source_counts").fetchone()[0]
        con.close()
        if qc != "ok":
            raise RuntimeError(f"{label}: quick_check says {qc!r}")
        if fts != n:
            raise RuntimeError(f"{label}: series_fts has {fts:,} rows, series {n:,}")
        if counted != n:
            raise RuntimeError(f"{label}: source_counts sums to {counted:,}, series has {n:,}")
        if label == "primary" and shard:
            raise RuntimeError(f"primary still holds {shard:,} series of the shard sources {SHARD_SOURCES}")
        if label == "climate" and shard != n:
            raise RuntimeError(f"climate holds {n - shard:,} series of other sources")
        out[label] = {"series": n, "series_fts": fts}
    if out["primary"]["series"] + out["climate"]["series"] != total:
        raise RuntimeError(f"primary {out['primary']['series']:,} + climate {out['climate']['series']:,} "
                           f"!= catalogue {total:,}")
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--catalogue", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--state-db", help="state.db: build the freshness projection into the primary copy")
    a = ap.parse_args()
    print(json.dumps(build(a.catalogue, a.out, state_db=a.state_db), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
