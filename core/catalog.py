"""Series catalog -- LOCAL SQLite stand-in for Cloudflare D1.

Faithful: D1 *is* SQLite, and FTS5 (full-text search) is the same engine. Moving
to production = pointing this same SQL at `wrangler d1 execute --remote`.
"""
from __future__ import annotations
import json
import os
import sqlite3

DB = os.path.join(os.path.dirname(__file__), "..", "data", "catalog.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS license (
  license_id TEXT PRIMARY KEY, name TEXT, reservable INTEGER, commercial_ok INTEGER,
  attribution_required INTEGER, no_modify INTEGER DEFAULT 0, url TEXT);
CREATE TABLE IF NOT EXISTS source (
  source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT,
  attribution TEXT, terms_url TEXT);
CREATE TABLE IF NOT EXISTS series (
  series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
  geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT,
  last_updated TEXT, metadata TEXT);
-- WITHOUT THIS, EVERY source-scoped QUERY FULL-SCANS THE WHOLE CATALOGUE.
--
-- series has only its PRIMARY KEY index, so `WHERE source_id = ?` planned as
--     SCAN series USING INDEX sqlite_autoindex_series_1
-- and at 8.5 GB / 10.8M rows that is minutes of silent work before a caller's first line of
-- output. Measured 2026-08-03: `SELECT COUNT(*) FROM series WHERE source_id='noaa'` did not
-- finish inside 120 s, and core/derive_csv.py --source noaa sat at zero bytes of log for long
-- enough that I diagnosed it three times as a hang — buffering, then disk contention, then a
-- corrupt database — and killed it three times before it could finish scanning (R308).
--
-- source_id is THE filter this catalogue is queried by: derive_csv, the coherence map, the D1
-- exporters and every audit tool select on it. Declared here, in the schema, rather than
-- applied by hand to the live file: tools/refresh_r2_catalog.py regenerates catalog.db, so a
-- hand-built index silently disappears at the next refresh and the next person re-diagnoses
-- the same hang from scratch.
CREATE INDEX IF NOT EXISTS ix_series_source_id ON series(source_id);
"""


def connect(db: str | None = None) -> sqlite3.Connection:
    path = os.path.abspath(db or DB)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # WAIT FOR A BUSY DATABASE INSTEAD OF FAILING ON IT. sqlite's default is 5 s, which is far
    # shorter than real work on this file: the catalogue is 8.5 GB, a long-running reader
    # (core/derive_csv.py scanning one source) holds its lock for minutes, and the index build
    # added to SCHEMA below is itself a multi-minute write. At 5 s an ingester's init() would
    # raise "database is locked" and take the whole job down for something that only needed
    # patience. 10 minutes is longer than any single operation observed here.
    conn = sqlite3.connect(path, timeout=600)
    conn.row_factory = sqlite3.Row
    return conn


def init(conn) -> bool:
    conn.executescript(SCHEMA)
    try:
        conn.execute("CREATE VIRTUAL TABLE IF NOT EXISTS series_fts USING fts5(series_id UNINDEXED, title, geography);")
        fts = True
    except sqlite3.OperationalError:
        fts = False
    conn.commit()
    return fts


def upsert_source(conn, source_id, name, license_id, attribution, homepage=None):
    conn.execute("INSERT OR REPLACE INTO source(source_id,name,homepage,license_id,attribution) VALUES(?,?,?,?,?)",
                 (source_id, name, homepage, license_id, attribution))
    conn.commit()


def upsert_series(conn, m, start=None, end=None):
    conn.execute(
        """INSERT OR REPLACE INTO series
           (series_id,source_id,title,frequency,unit,geography,category,license_id,start_date,end_date,metadata)
           VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
        (m.series_id, m.series_id.split(":")[0], m.title, m.frequency, m.unit,
         m.geography, m.category, m.license_id, start, end, json.dumps(m.metadata)))


def rebuild_fts(conn, fts):
    if not fts:
        return
    conn.execute("DELETE FROM series_fts;")
    conn.execute("INSERT INTO series_fts(series_id,title,geography) SELECT series_id,title,geography FROM series;")
    conn.commit()


def search(conn, q, limit=10):
    try:
        rows = conn.execute(
            "SELECT s.* FROM series_fts f JOIN series s ON s.series_id=f.series_id WHERE series_fts MATCH ? LIMIT ?",
            (q, limit)).fetchall()
        if rows:
            return rows
    except sqlite3.OperationalError:
        pass
    return conn.execute("SELECT * FROM series WHERE title LIKE ? LIMIT ?", (f"%{q}%", limit)).fetchall()


def get_series(conn, series_id):
    return conn.execute("SELECT * FROM series WHERE series_id=?", (series_id,)).fetchone()
