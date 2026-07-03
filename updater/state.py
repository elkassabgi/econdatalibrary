"""StateStore — SQLite local backend (schema ports verbatim to Cloudflare D1).

The orchestrator's source of truth. "Is this unit due?" and "did upstream change?"
are answered from persisted facts here, never inferred from file presence — which
is the architectural fix for the 79 sources that froze existing series on re-run.
"""
from __future__ import annotations
import sqlite3
from datetime import datetime, timezone, timedelta

from . import config


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


DDL = """
CREATE TABLE IF NOT EXISTS source_state(
  source_id TEXT PRIMARY KEY, strategy TEXT, cadence TEXT, status TEXT,
  last_success_utc TEXT, last_attempt_utc TEXT, owner TEXT,
  enabled INTEGER DEFAULT 1, note TEXT);
CREATE TABLE IF NOT EXISTS unit_state(
  source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT,
  last_success_utc TEXT, last_attempt_utc TEXT, status TEXT,
  last_obs_date TEXT, obs_count INTEGER DEFAULT 0, attempt_count INTEGER DEFAULT 0,
  last_error TEXT, PRIMARY KEY(source_id, unit_id));
CREATE TABLE IF NOT EXISTS series_cursor(
  source_id TEXT, series_key TEXT, last_obs_date TEXT,
  PRIMARY KEY(source_id, series_key));
CREATE TABLE IF NOT EXISTS runs(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, source_id TEXT, unit_id TEXT,
  status TEXT, obs INTEGER, dur_s REAL, note TEXT);
CREATE TABLE IF NOT EXISTS leases(
  key TEXT PRIMARY KEY, owner TEXT, expires_utc TEXT);
CREATE TABLE IF NOT EXISTS csv_retry_queue(
  series_id TEXT PRIMARY KEY, source_id TEXT, enqueued_utc TEXT,
  attempts INTEGER DEFAULT 0, last_error TEXT);
"""

_SRC_COLS = ["source_id", "strategy", "cadence", "status", "last_success_utc",
             "last_attempt_utc", "owner", "enabled", "note"]
_UNIT_COLS = ["source_id", "unit_id", "strategy", "upstream_vintage", "last_success_utc",
              "last_attempt_utc", "status", "last_obs_date", "obs_count",
              "attempt_count", "last_error"]


class StateStore:
    def __init__(self, path: str | None = None):
        config.ensure_dirs()
        self.path = path or config.STATE_DB
        self.db = sqlite3.connect(self.path, timeout=60)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.executescript(DDL)
        self.db.commit()

    def close(self):
        self.db.close()

    # ---- generic upsert helper ----
    def _upsert(self, table, cols, pk, current, kw):
        rec = dict(current) if current else {}
        rec.update({"source_id": kw.get("source_id", rec.get("source_id"))})
        rec.update(kw)
        vals = [rec.get(c) for c in cols]
        ph = ",".join("?" * len(cols))
        upd = ",".join(f"{c}=excluded.{c}" for c in cols if c not in pk)
        pkcols = ",".join(pk)
        self.db.execute(
            f"INSERT INTO {table}({','.join(cols)}) VALUES({ph}) "
            f"ON CONFLICT({pkcols}) DO UPDATE SET {upd}", vals)
        self.db.commit()

    # ---- source_state ----
    def get_source(self, sid):
        r = self.db.execute("SELECT * FROM source_state WHERE source_id=?", (sid,)).fetchone()
        return dict(r) if r else None

    def upsert_source(self, source_id, **kw):
        self._upsert("source_state", _SRC_COLS, ["source_id"],
                     self.get_source(source_id), {"source_id": source_id, **kw})

    def all_sources(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM source_state")]

    # ---- unit_state ----
    def get_unit(self, sid, uid):
        r = self.db.execute("SELECT * FROM unit_state WHERE source_id=? AND unit_id=?",
                            (sid, uid)).fetchone()
        return dict(r) if r else None

    def upsert_unit(self, source_id, unit_id, **kw):
        self._upsert("unit_state", _UNIT_COLS, ["source_id", "unit_id"],
                     self.get_unit(source_id, unit_id),
                     {"source_id": source_id, "unit_id": unit_id, **kw})

    def units_for_source(self, sid):
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM unit_state WHERE source_id=?", (sid,))]

    def all_units(self):
        return [dict(r) for r in self.db.execute("SELECT * FROM unit_state")]

    # ---- series cursors ----
    def series_cursors(self, sid) -> dict:
        return {r["series_key"]: r["last_obs_date"] for r in self.db.execute(
            "SELECT series_key,last_obs_date FROM series_cursor WHERE source_id=?", (sid,))}

    def put_series_cursors(self, sid, mapping: dict):
        self.db.executemany(
            "INSERT INTO series_cursor(source_id,series_key,last_obs_date) VALUES(?,?,?) "
            "ON CONFLICT(source_id,series_key) DO UPDATE SET last_obs_date=excluded.last_obs_date",
            [(sid, k, v) for k, v in mapping.items()])
        self.db.commit()

    # ---- csv retry queue (honesty rule §5.7: a CSV derive/PUT that fails AFTER its
    # parquet published demotes the run to `partial` and the series ids land here for
    # a later re-derive — never silently dropped, never rolls back the data publish) ----
    def enqueue_csv_retry(self, source_id, series_ids, error=None):
        self.db.executemany(
            "INSERT INTO csv_retry_queue(series_id,source_id,enqueued_utc,attempts,last_error) "
            "VALUES(?,?,?,1,?) "
            "ON CONFLICT(series_id) DO UPDATE SET enqueued_utc=excluded.enqueued_utc, "
            "attempts=csv_retry_queue.attempts+1, last_error=excluded.last_error",
            [(s, source_id, now_utc(), error) for s in series_ids])
        self.db.commit()

    def csv_retries(self, source_id=None):
        if source_id:
            rows = self.db.execute(
                "SELECT * FROM csv_retry_queue WHERE source_id=?", (source_id,))
        else:
            rows = self.db.execute("SELECT * FROM csv_retry_queue")
        return [dict(r) for r in rows]

    def clear_csv_retries(self, series_ids):
        self.db.executemany("DELETE FROM csv_retry_queue WHERE series_id=?",
                            [(s,) for s in series_ids])
        self.db.commit()

    # ---- leases (prevent double-runs) ----
    def claim_lease(self, key, owner, ttl_s=3600) -> bool:
        """Atomically acquire a lease. The DB (not Python) decides the winner: the
        conflicting UPDATE only fires if the held lease is expired or already ours,
        so two concurrent runners can never both acquire the same key (TOCTOU-safe)."""
        now = now_utc()
        exp = (datetime.now(timezone.utc) + timedelta(seconds=ttl_s)).isoformat(timespec="seconds")
        self.db.execute(
            "INSERT INTO leases(key,owner,expires_utc) VALUES(?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET owner=excluded.owner, expires_utc=excluded.expires_utc "
            "WHERE leases.expires_utc < ? OR leases.owner = ?",
            (key, owner, exp, now, owner))
        self.db.commit()
        row = self.db.execute("SELECT owner FROM leases WHERE key=?", (key,)).fetchone()
        return bool(row and row["owner"] == owner)

    def release_lease(self, key, owner=None):
        """Release a lease. Owner-scoped so a run can't drop a lease it doesn't hold."""
        if owner is None:
            self.db.execute("DELETE FROM leases WHERE key=?", (key,))
        else:
            self.db.execute("DELETE FROM leases WHERE key=? AND owner=?", (key, owner))
        self.db.commit()

    # ---- run log ----
    def log_run(self, sid, uid, status, obs=0, dur_s=0.0, note=None):
        self.db.execute(
            "INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) VALUES(?,?,?,?,?,?,?)",
            (now_utc(), sid, uid, status, obs, dur_s, note))
        self.db.commit()

    def recent_runs(self, limit=50):
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,))]
