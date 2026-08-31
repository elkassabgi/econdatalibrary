"""LOCAL record of what the catalogue sync has already sent to D1, so it can send a DIFF.

WHY THIS EXISTS (ledger R542). `orchestrate.py` appends every re-derived series id to
`pending_catalog_sync.txt` with no change detection, so a catalogue sync pushes a mean of
42,046 ids per run — against a D1 catalogue measured 2026-08-31 to be 0 of 322 sources short
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
"""
from __future__ import annotations

import hashlib
import os
import sqlite3

_DDL = """
CREATE TABLE IF NOT EXISTS sent(
  series_id TEXT PRIMARY KEY,
  row_hash  TEXT NOT NULL
);
"""


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


class Manifest:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.db = sqlite3.connect(path, timeout=300.0)
        self.db.execute("PRAGMA busy_timeout = 300000")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(_DDL)
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def count(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM sent").fetchone()[0]

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

    def record(self, cols: list[str], rows: list[dict]) -> int:
        self.db.executemany(
            "INSERT INTO sent(series_id,row_hash) VALUES(?,?) "
            "ON CONFLICT(series_id) DO UPDATE SET row_hash=excluded.row_hash",
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
            n += self.record(cols, rows)
        return n
