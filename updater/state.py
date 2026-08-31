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
-- obs_count IS NOT COMPARABLE ACROSS RUNS. Read this before drawing a conclusion from it.
--
-- It is whatever the fetcher passed finalize() as `total_rows`, and fetchers mean two different
-- things by that. Most pass ROWS MERGED THIS RUN. Thirteen of them then do
--
--     if published == 0:
--         published = sum(blob.row_count(f) for f in every file in the store)
--
-- so on a run that wrote nothing the same column silently becomes TOTAL ROWS IN THE STORE.
-- Those differ by orders of magnitude and nothing in the row says which one you are reading.
--
-- MEASURED 2026-08-04 on ecb, whose budget was cut from ~72 min to 35 min on 08-01:
--
--     2026-07-31   obs=218,396,836   dur=4362.8s   +5,866,080 new rows
--     2026-08-01   obs= 62,928,444   dur=2108.4s   243/540 deferred
--     2026-08-03   obs= 49,851,636   dur=2102.9s   290/540 deferred
--
-- Read straight, that is a source losing 168 million observations in three days. The store was
-- counted directly: 218,396,859 rows across all 540 files — intact, and 23 rows MORE than the
-- 07-31 figure. Nothing was lost; the source simply began touching a fraction of its files per
-- tick, so the number it reports collapsed (ledger R326).
--
-- A metric that silently changes denominator is worse than a missing one: a missing number
-- prompts a question, this one answers confidently and wrongly, in the direction of alarm. Same
-- shape as R231, where a `partial` never setting last_success_utc makes healthy sources read as
-- having never succeeded.
--
-- TO ASK "how big is this source", COUNT THE STORE. Do not read it from here.
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
CREATE TABLE IF NOT EXISTS full_rederive_owed(
  source_id TEXT PRIMARY KEY, vintage TEXT, noted_utc TEXT, note TEXT);
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
        # `error` is one string for the whole batch, OR a {series_id: reason} dict —
        # per-id reasons keep the actual exception on the row (a summary like
        # "csv_derive failed 22/22" left cso's stuck ids undiagnosable for 10 days).
        def _err(s):
            if isinstance(error, dict):
                return error.get(s) or error.get(str(s))
            return error
        self.db.executemany(
            "INSERT INTO csv_retry_queue(series_id,source_id,enqueued_utc,attempts,last_error) "
            "VALUES(?,?,?,1,?) "
            "ON CONFLICT(series_id) DO UPDATE SET enqueued_utc=excluded.enqueued_utc, "
            "attempts=csv_retry_queue.attempts+1, last_error=excluded.last_error",
            [(s, source_id, now_utc(), _err(s)) for s in series_ids])
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

    def lease_holder(self, key):
        """{'owner','expires_utc'} for a held lease, else None.

        Exists so a REFUSED unit can say WHO is holding it. A lease is claimed by a process
        and processes die; without this the orchestrator could only report "locked", which
        downstream is indistinguishable from "not due". eia/_all sat behind a lease owned by
        a run that died on 2026-08-05 with a 64-hour TTL, and nothing named the holder — a
        daily source went two days stale in silence.
        """
        row = self.db.execute(
            "SELECT owner, expires_utc FROM leases WHERE key=?", (key,)).fetchone()
        return {"owner": row["owner"], "expires_utc": row["expires_utc"]} if row else None

    def held_leases(self):
        """Every lease still in force, as facts — [{key, owner, expires_utc}].

        Deliberately NOT called "orphaned": this cannot know which owners are alive, and a
        method that guessed would eventually tell someone to clear a lock a live run was
        using. The caller pairs this with its own liveness check.
        """
        now = now_utc()
        return [{"key": r["key"], "owner": r["owner"], "expires_utc": r["expires_utc"]}
                for r in self.db.execute(
                    "SELECT key, owner, expires_utc FROM leases WHERE expires_utc > ? "
                    "ORDER BY expires_utc", (now,))]

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

    # ---- full-rederive debt (§5.7's no-cursors branch) ----
    # A fetcher that merges obs but reports no series_cursors leaves its ENTIRE served CSV
    # corpus stale, and until 2026-08-31 that debt EVAPORATED: nothing was queued ("ids
    # unknown"), the vintage sidecar was already written, so the next run skipped as
    # unchanged and reported clean. noaa served 3,138,159 CSVs one restatement behind that
    # way. This row is the debt's persistence; only a completed wholesale derive campaign
    # (derive_csv_bulk's success stamp) clears it.
    def note_full_rederive_owed(self, source_id, vintage=None, note=None):
        self.db.execute(
            "INSERT INTO full_rederive_owed(source_id,vintage,noted_utc,note) "
            "VALUES(?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET "
            "vintage=excluded.vintage, noted_utc=excluded.noted_utc, note=excluded.note",
            (source_id, vintage, now_utc(), note))
        self.db.commit()

    def clear_full_rederive_owed(self, source_id):
        self.db.execute("DELETE FROM full_rederive_owed WHERE source_id=?", (source_id,))
        self.db.commit()

    def full_rederives_owed(self):
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM full_rederive_owed ORDER BY source_id")]

    def recent_runs(self, limit=50):
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM runs ORDER BY id DESC LIMIT ?", (limit,))]

    def run_cost_estimate(self, sample=5) -> dict:
        """{source_id: seconds} — what a run of this source COSTS, for scheduling.

        MAX over the last `sample` runs, not mean or latest: the estimate decides whether a
        source is cheap enough to be guaranteed a nightly turn, so it must not be fooled by
        one fast `no_change` on a source that takes 40 minutes whenever there IS a change.
        Over-estimating costs a source its place in the fast lane, which is recoverable;
        under-estimating lets an expensive source into a lane sized for cheap ones, which is
        the failure the lane exists to prevent.

        A source with no runs on record is absent from the mapping — the caller decides what
        never-run means, rather than having a 0 here quietly assert "free".

        FLOORED at the latest NON-FAIL run's duration (2026-08-19, run 32195120699):
        a chronic failer's fast failures (ecb: transient_fails since Jul 16, ~seconds
        each) rolled its 2,400s success out of the window, the estimate collapsed,
        and it infiltrated the cheap band — where its next REAL attempt detonated
        for 40 minutes and the run died in band 1 with 46 sources unattempted.
        Failure durations say nothing about what an attempt that gets somewhere
        costs; the last ok/no_change/partial does. The floor only ever raises the
        estimate — under-estimating remains the direction this function must never err.
        """
        rows = self.db.execute(
            "SELECT source_id, MAX(dur_s) FROM ("
            "  SELECT source_id, dur_s,"
            "         ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY id DESC) AS rn"
            "  FROM runs) WHERE rn <= ? GROUP BY source_id", (sample,))
        est = {sid: (d or 0.0) for sid, d in rows}
        # `killed_external` joins the floor set (2026-08-31): a unit hard-killed from outside
        # spent AT LEAST dur_s on an attempt that got somewhere, which is exactly what the
        # floor exists to remember. Without it, five fast post-kill rows (locked 0.0s,
        # transient_fails during an upstream outage) roll the kill out of the MAX window and
        # the floor then restores the OLD cheap estimate — one starvation relapse per outage.
        floors = self.db.execute(
            "SELECT source_id, dur_s FROM ("
            "  SELECT source_id, dur_s,"
            "         ROW_NUMBER() OVER (PARTITION BY source_id ORDER BY id DESC) AS rn"
            "  FROM runs WHERE status IN ('ok','no_change','partial','killed_external')"
            ") WHERE rn = 1")
        for sid, d in floors:
            if d is not None and sid in est and d > est[sid]:
                est[sid] = d
        return est
