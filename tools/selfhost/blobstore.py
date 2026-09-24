"""The self-hosted origin's blob store (docs/ECON_SELF_HOSTING_PLAN.md, code change 3).

Holds what R2 `econ-data` holds - served CSVs (series/...), `_aqueduct/stats.json` and the rest -
without using object keys as file names: NTFS cannot hold them (27 keys collide case-insensitively and
4,389 encoded names exceed 255 characters over the 13,952,906 catalogued ids, review R1158 (5)).

Layout (root = a fixed directory):
    <root>/objects/<sha256[:2]>/<sha256>     content-addressed bytes, exactly as stored in R2
                                             (gzip bytes stay gzip; nothing is re-encoded)
    <root>/index.db                          SQLite: key -> sha256, etag, size, content_encoding,
                                             content_type, custom_metadata (JSON), stored_utc

The R2 etag is kept verbatim, so the worker's conditional reads (`onlyIf: {etagMatches}`,
series.ts:418) behave as they do on R2, and a copy from R2 can be checked object by object against it.
Files are immutable: replacing a key writes a new file and moves the index row; an old file is removed
only by `gc()` after a grace period, so an in-flight read or an etag check keeps working (R1160).
One writer at a time (the updater's lock); any number of readers (WAL).
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sqlite3
import tempfile

SCHEMA = """
CREATE TABLE IF NOT EXISTS blobs (
  key TEXT PRIMARY KEY,
  sha256 TEXT NOT NULL,
  etag TEXT NOT NULL,
  size INTEGER NOT NULL,
  content_encoding TEXT,
  content_type TEXT,
  custom_metadata TEXT,
  stored_utc TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS retired (
  sha256 TEXT NOT NULL,
  retired_utc TEXT NOT NULL
);
"""


def _now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


class BlobStore:
    def __init__(self, root: str, *, create: bool = False):
        self.root = os.path.abspath(root)
        idx = os.path.join(self.root, "index.db")
        if not create and not os.path.exists(idx):
            # never create an empty store by accident: a missing store is an error (R1167 A's rule
            # for the catalogue applies here too)
            raise FileNotFoundError(f"no blob store at {self.root}")
        os.makedirs(os.path.join(self.root, "objects"), exist_ok=True)
        self.db = sqlite3.connect(idx, timeout=60, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(SCHEMA)
        self.db.commit()

    def _path(self, sha: str) -> str:
        return os.path.join(self.root, "objects", sha[:2], sha)

    def put(self, key: str, data: bytes, *, etag: str, content_encoding: str | None = None,
            content_type: str | None = None, custom_metadata: dict | None = None) -> str:
        """Store bytes under key with the R2 etag they had. Returns the sha256."""
        sha = hashlib.sha256(data).hexdigest()
        path = self._path(sha)
        if not os.path.exists(path):
            os.makedirs(os.path.dirname(path), exist_ok=True)
            fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".tmp-")
            with os.fdopen(fd, "wb") as fh:
                fh.write(data)
            os.replace(tmp, path)
        old = self.db.execute("SELECT sha256 FROM blobs WHERE key=?", (key,)).fetchone()
        with self.db:
            self.db.execute(
                "INSERT INTO blobs(key, sha256, etag, size, content_encoding, content_type, custom_metadata,"
                " stored_utc) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(key) DO UPDATE SET sha256=excluded.sha256,"
                " etag=excluded.etag, size=excluded.size, content_encoding=excluded.content_encoding,"
                " content_type=excluded.content_type, custom_metadata=excluded.custom_metadata,"
                " stored_utc=excluded.stored_utc",
                (key, sha, etag.strip('"'), len(data), content_encoding, content_type,
                 json.dumps(custom_metadata or {}, sort_keys=True), _now()))
            if old and old[0] != sha:
                self.db.execute("INSERT INTO retired(sha256, retired_utc) VALUES (?,?)", (old[0], _now()))
        return sha

    def head(self, key: str) -> dict | None:
        row = self.db.execute(
            "SELECT sha256, etag, size, content_encoding, content_type, custom_metadata FROM blobs WHERE key=?",
            (key,)).fetchone()
        if row is None:
            return None
        sha, etag, size, enc, ctype, meta = row
        return {"key": key, "sha256": sha, "etag": etag, "size": size, "content_encoding": enc,
                "content_type": ctype, "custom_metadata": json.loads(meta or "{}"), "path": self._path(sha)}

    def count(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM blobs").fetchone()[0]

    def gc(self, grace_hours: float = 24.0) -> int:
        """Delete files retired more than grace_hours ago that no key references. Returns files removed."""
        cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=grace_hours)).isoformat(timespec="seconds")
        live = {r[0] for r in self.db.execute("SELECT DISTINCT sha256 FROM blobs")}
        gone = 0
        rows = self.db.execute("SELECT rowid, sha256 FROM retired WHERE retired_utc < ?", (cutoff,)).fetchall()
        with self.db:
            for rowid, sha in rows:
                if sha not in live and os.path.exists(self._path(sha)):
                    os.remove(self._path(sha))
                    gone += 1
                self.db.execute("DELETE FROM retired WHERE rowid=?", (rowid,))
        return gone
