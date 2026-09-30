"""LOCAL record of what the catalogue sync has already sent to D1, so it can send a DIFF.

WHY THIS EXISTS (ledger R542). `orchestrate.py` appends every re-derived series id to
`pending_catalog_sync.txt` with no change detection, so a catalogue sync pushes a mean of
42,046 ids per run — against a D1 catalogue measured 2026-08-31 to be 0 sources short
and 285 rows AHEAD of local. Over 99% of that work re-writes rows D1 already holds correctly.
It is not free: every 500 ids costs one `DELETE FROM series_fts WHERE series_id IN (...)`, and
`series_fts` is `fts5(series_id UNINDEXED, ...)`, so each of those is a FULL TABLE SCAN
(10,348,426 rows post-rebuild). That is ~85 scans per run, ~$0.88 per run, ~$86/month — and it
is also the failure: the scans push chunk execution from 1.8-2.1 s to 15.9-51.5 s until the
import dies.

THE COMPARISON IS AGAINST A LOCAL MANIFEST, NEVER AGAINST D1. Asking D1 "what do you already
have?" would re-introduce exactly the scans this removes (DESKTOP_FIRST.md: decide locally,
verify remotely). The manifest is a plain sqlite file beside the state store.

HONEST BOOTSTRAP. An empty manifest means "nothing has been sent", which would make the first
run push the whole catalogue — the opposite of the intent. `seed_from_catalog()` therefore
records the current local hash of every row WITHOUT sending anything, and its correctness rests
on one measured premise: D1 already holds them. That premise was measured (source_counts vs a
local GROUP BY: 322/322 sources, 0 short, +285 rows in D1) and is re-checkable at any time.
Seed only when that holds; if the catalogue is ever rebuilt from scratch, re-verify first.

RECORDING IS POST-SUCCESS ONLY. Hashes are written after the sync reports success, so a run
that dies partway re-sends its rows next time. That is the conservative direction: a
re-send costs money, a false "already sent" costs correctness.

KNOWN HAZARD, disclosed rather than left latent: the manifest records WHAT was sent, not
WHERE. A series_id routes to exactly one database via CATALOG_SHARD_FOR, so today there is no
ambiguity — but if a source's shard assignment is ever CHANGED, its rows are unchanged in
content and would be skipped, leaving them in the old database and absent from the new one.
Any edit to CATALOG_SHARD_FOR must therefore be followed by a `--no-diff` reconcile of the
moved source (`--source <id> --no-diff`), which re-sends it to its new home.

SECOND KNOWN HAZARD, since fts_hash (2026-09-30, review R1312 finding 3). The sync now leaves a row's
index row alone when its recorded fts_hash says D1 already holds it. Before, any changed row rewrote its
index row, so an index row lost some other way came back at the row's next change; now a date-only change
does not bring it back. Every writer of D1's series_fts OUTSIDE this sync can make the record wrong:
tools/delist_timeless_tables.py (deletes index rows, no manifest reference), a gate purge, a shard move, and
any FTS rebuild or restore. After any of them, clear the record for the range it touched -
`UPDATE sent SET fts_hash = NULL WHERE series_id >= '<src>:' AND series_id < '<src>;'` - or run a
`--source <id> --no-diff` reconcile. Unknown is always safe: it costs one index rewrite per row, never a
missing one.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3

_DDL = """
CREATE TABLE IF NOT EXISTS sent(
  series_id TEXT PRIMARY KEY,
  row_hash  TEXT NOT NULL,
  fts_hash  TEXT
);
"""

# THE INDEX ROW'S OWN HASH (2026-09-30, R1308). row_hash changes whenever ANY column changes - a new
# end_date on every refresh - and each changed row used to cost an FTS delete+insert, i.e. a share of a
# full scan of series_fts. The index row is only (series_id, title, geography), so its own hash tells the
# sync when it may leave the index alone. NULL = unknown: every entry written before this column existed,
# and every entry --seed-manifest writes (seeding asserts D1 holds the SERIES rows; nobody measured the
# index rows, and D1's index has carried duplicates before - R482). Unknown is treated as changed, so the
# first real send of such a row still rewrites its index row, and records the hash only then.
FTS_COLS = ("series_id", "title", "geography")


def default_path(root: str) -> str:
    return os.path.join(
        os.path.abspath(os.environ.get("AQUEDUCT_STATE_DIR")
                        or os.path.join(root, "data", "_aqueduct")),
        "catalog_sync_sent.db")


def row_hash(cols: list[str], row: dict) -> str:
    """Stable content hash of one catalogue row.

    Column NAMES are folded in, so adding a column changes every hash and the next sync
    re-sends — which is correct: D1's rows would genuinely be missing that column.
    """
    h = hashlib.sha256()
    for c in cols:
        v = row.get(c)
        h.update(c.encode("utf-8"))
        h.update(b"\x00")
        h.update(b"\xff" if v is None else str(v).encode("utf-8"))
        h.update(b"\x01")
    return h.hexdigest()


def fts_hash(row: dict, bound_to: str) -> str:
    """Hash of the index row the sync writes for this series - (series_id, title, geography) - BOUND to the
    row_hash recorded beside it. The binding is what makes a stale value detectable (review R1312 finding 4):
    code from before this column (a rollback, an un-pulled checkout) rewrites row_hash and leaves fts_hash
    alone, and a bare content hash would then vouch for an index row that code may have replaced. Bound to the
    old row_hash, it no longer matches, so the entry reads as unknown."""
    return row_hash(list(FTS_COLS) + ["\x00row_hash"], {**row, "\x00row_hash": bound_to})


class ManifestBusy(RuntimeError):
    """A read-only (dry-run) open found the manifest being written."""


class Manifest:
    def __init__(self, path: str, read_only: bool = False):
        self.path = path
        if read_only:
            # --dry-run (R1304/R1305): nothing on disk may change, so no makedirs, no WAL switch, no DDL.
            # An absent manifest reads as empty - the same answer a fresh one would give.
            # `mode=ro` is NOT enough: opening a WAL database read-only still creates and leaves its -wal and
            # -shm files (measured by tests/test_sync_dryrun_keeps_queue.py). With no -wal present every
            # committed row is in the main file, so `immutable=1` reads it with no side files at all.
            # A -wal present means a writer is live (or died mid-write): its rows matter and an unlocked copy
            # of db + -wal is not a snapshot (review round 2 measured torn, malformed and silently wrong reads,
            # R1306), so REFUSE rather than guess. stable() re-checks after the read for a writer that
            # started while it ran; the caller refuses the numbers when it did.
            if os.path.isfile(path):
                if os.path.exists(path + "-wal"):
                    raise ManifestBusy(f"the sync manifest {path} has a -wal: a sync is writing it (or one "
                                       f"died mid-write). A dry run cannot read it without writing. If a sync "
                                       f"is running, re-run the dry run after it ends; if none is, the -wal is "
                                       f"left by a dead one and the next REAL sync folds it back in.")
                self._stat = self._fingerprint()
                uri = "file:" + os.path.abspath(path).replace("\\", "/") + "?mode=ro&immutable=1"
                self.db = sqlite3.connect(uri, uri=True, timeout=300.0)
            else:
                self.db = sqlite3.connect(":memory:")
                self.db.executescript(_DDL)
            self._has_fts = self._fts_column()      # a pre-column manifest is read as "all unknown"
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.db = sqlite3.connect(path, timeout=300.0)
        self.db.execute("PRAGMA busy_timeout = 300000")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(_DDL)
        if not self._fts_column():
            # ADD COLUMN with no default rewrites nothing: SQLite only edits the schema, so this is O(1)
            # on the 2.2 GB file, and every existing entry reads NULL (= unknown, see FTS_COLS).
            try:
                self.db.execute("ALTER TABLE sent ADD COLUMN fts_hash TEXT")
            except sqlite3.OperationalError as e:        # a second writer migrated it first (R1312 finding 7)
                if "duplicate column" not in str(e).lower():
                    raise
        self.db.commit()
        self._has_fts = True

    def _fts_column(self) -> bool:
        return any(r[1] == "fts_hash" for r in self.db.execute("PRAGMA table_info(sent)"))

    def _fingerprint(self):
        s = os.stat(self.path)
        return s.st_size, s.st_mtime_ns

    def stable(self) -> bool:
        """Read-only opens only: False when a writer visibly touched the manifest since it was opened (a -wal
        appeared, or the main file's size or mtime changed). BEST EFFORT: NTFS mtimes step in ~ms ticks
        (review AR-175 measured 694 distinct mtimes over 3,000 writes), so two complete writer cycles inside
        one tick can go unseen. The only writer is this sync, which never cycles that fast."""
        if not hasattr(self, "_stat"):
            return True                                      # the in-memory empty manifest cannot change
        return not os.path.exists(self.path + "-wal") and os.path.isfile(self.path) \
            and self._fingerprint() == self._stat

    def close(self) -> None:
        self.db.close()

    def count(self) -> int:
        """Total rows recorded. A FULL SCAN — do not call it to ask whether the manifest is empty.

        This file grows to the size of the catalogue: measured 2026-09-04 at **2.17 GB**, and a
        `COUNT(*)` over it did not finish inside 120 s. Its only caller was
        `sync_catalog_d1.py:489`, testing `manifest.count() == 0` to decide whether to print a
        warning — and Python evaluates left to right, so on a full-source push (`skipped == 0`,
        `before > 1000`, both true) that scan ran FIRST and stalled the whole job before a single
        statement was emitted. The statcan push sat at 0.1 s of CPU and zero page faults for
        fifteen minutes on exactly this.
        """
        return self.db.execute("SELECT COUNT(*) FROM sent").fetchone()[0]

    def is_empty(self) -> bool:
        """Has anything ever been recorded? Answers in O(1) instead of scanning 2.17 GB.

        `LIMIT 1` stops at the first row, so this is a seek rather than a count. It is what the
        caller actually wanted: "is the manifest empty", not "how many rows are in it".
        """
        return self.db.execute("SELECT 1 FROM sent LIMIT 1").fetchone() is None

    def split(self, cols: list[str], rows: list[dict]) -> tuple[list[dict], int]:
        """(rows_to_send, n_skipped). A row is skipped only if its hash matches exactly."""
        if not rows:
            return [], 0
        known = {}
        CH = 900                                   # under sqlite's parameter ceiling
        ids = [r["series_id"] for r in rows]
        for i in range(0, len(ids), CH):
            part = ids[i:i + CH]
            q = ",".join("?" * len(part))
            for sid, h in self.db.execute(
                    f"SELECT series_id, row_hash FROM sent WHERE series_id IN ({q})", part):
                known[sid] = h
        send, skipped = [], 0
        for r in rows:
            if known.get(r["series_id"]) == row_hash(cols, r):
                skipped += 1
            else:
                send.append(r)
        return send, skipped

    def fts_current(self, rows: list[dict]) -> set:
        """series_ids whose RECORDED index-row hash equals the row's current one: D1's index row for them is
        already exactly what the sync would write, so it may skip their FTS delete+insert. NULL (unknown)
        never matches. Empty for a manifest from before the column existed."""
        if not rows or not self._has_fts:
            return set()
        known = {}
        CH = 900
        ids = [r["series_id"] for r in rows]
        for i in range(0, len(ids), CH):
            part = ids[i:i + CH]
            q = ",".join("?" * len(part))
            for sid, rh, fh in self.db.execute(
                    f"SELECT series_id, row_hash, fts_hash FROM sent WHERE series_id IN ({q}) "
                    f"AND fts_hash IS NOT NULL", part):
                known[sid] = (rh, fh)
        return {r["series_id"] for r in rows
                if r["series_id"] in known
                and known[r["series_id"]][1] == fts_hash(r, known[r["series_id"]][0])}

    def forget_fts(self, ids: list[str]) -> None:
        """Mark these ids' index rows UNKNOWN before a real send rewrites them (review R1312 finding 7): a run
        that dies partway may already have changed D1's index row while the manifest still vouches for the old
        one. record() sets them again only after success."""
        for i in range(0, len(ids), 900):
            part = ids[i:i + 900]
            self.db.execute(f"UPDATE sent SET fts_hash=NULL WHERE series_id IN ({','.join('?' * len(part))})", part)
        self.db.commit()

    def record(self, cols: list[str], rows: list[dict], fts_sent: bool = True) -> int:
        """Record rows as sent. fts_sent=True (a real sync that wrote their index rows, or knew them current):
        the index-row hash is recorded too. False (--seed-manifest): the index-row hash is left UNKNOWN,
        and an existing one is cleared, because nothing checked D1's index for these rows."""
        if fts_sent:
            self.db.executemany(
                "INSERT INTO sent(series_id,row_hash,fts_hash) VALUES(?,?,?) "
                "ON CONFLICT(series_id) DO UPDATE SET row_hash=excluded.row_hash, fts_hash=excluded.fts_hash",
                [(r["series_id"], rh, fts_hash(r, rh)) for r in rows for rh in (row_hash(cols, r),)])
        else:
            self.db.executemany(
                "INSERT INTO sent(series_id,row_hash,fts_hash) VALUES(?,?,NULL) "
                "ON CONFLICT(series_id) DO UPDATE SET row_hash=excluded.row_hash, fts_hash=NULL",
                [(r["series_id"], row_hash(cols, r)) for r in rows])
        self.db.commit()
        return len(rows)

    def seed_from_catalog(self, conn: sqlite3.Connection, batch: int = 50_000) -> int:
        """Record every LOCAL catalogue row as already-sent. See the bootstrap note above."""
        cols = [d[0] for d in conn.execute("SELECT * FROM series LIMIT 1").description]
        cur = conn.execute("SELECT * FROM series")
        n = 0
        while True:
            chunk = cur.fetchmany(batch)
            if not chunk:
                break
            rows = [dict(zip(cols, r)) for r in chunk]
            n += self.record(cols, rows, fts_sent=False)
        return n
