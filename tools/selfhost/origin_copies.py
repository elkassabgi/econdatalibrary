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


def build(catalogue: str, out_dir: str) -> dict:
    """Write out_dir/primary.sqlite and out_dir/climate.sqlite; return the checked counts."""
    os.makedirs(out_dir, exist_ok=True)
    primary, climate = os.path.join(out_dir, "primary.sqlite"), os.path.join(out_dir, "climate.sqlite")
    for p in (primary, climate):
        for suffix in ("", "-journal", "-wal", "-shm"):
            if os.path.exists(p + suffix):
                os.remove(p + suffix)
    opened: list[sqlite3.Connection] = []
    try:
        src = _ro(catalogue)
        opened.append(src)
        total = src.execute("SELECT COUNT(*) FROM series").fetchone()[0]
        schema = {n: s for n, s in src.execute(
            "SELECT name, sql FROM sqlite_master WHERE name IN ('series','series_fts','source','license')")}
        missing = {"series", "series_fts", "source", "license"} - set(schema)
        if missing:
            raise RuntimeError(f"the catalogue has no {sorted(missing)} table")

        # primary: a consistent copy through the backup API, then the shard sources taken out
        dst = sqlite3.connect(primary)
        opened.append(dst)
        src.backup(dst)
        for s in SHARD_SOURCES:
            dst.execute("DELETE FROM series WHERE source_id=?", (s,))
        _recount(dst)
        _rebuild_fts(dst, schema["series_fts"])
        dst.commit()

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
        _recount(c)
        _rebuild_fts(c, schema["series_fts"])
        c.commit()
        for con in opened:
            con.close()
        opened.clear()

        return check(primary, climate, total)
    except BaseException:
        for con in opened:                              # closed first: Windows cannot delete an open file
            con.close()
        for p in (primary, climate):
            if os.path.exists(p):
                os.remove(p)
        raise


def check(primary: str, climate: str, total: int) -> dict:
    """The checks that must pass before a copy is served. Raises RuntimeError naming the first failure."""
    out = {"catalogue_series": total}
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
    a = ap.parse_args()
    print(json.dumps(build(a.catalogue, a.out), indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
