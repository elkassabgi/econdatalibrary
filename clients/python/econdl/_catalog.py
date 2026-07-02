"""Read-only access to the central metadata registry (catalog.db).

The registry is the single source of truth (ARCHITECTURE.md §3) for license,
attribution and the facts a citation is built from. This module never writes to
it -- the client only reads license / attribution / source metadata so a bundle
can carry honest provenance.

Locally the registry is SQLite; in production it is Cloudflare D1, which *is*
SQLite, so the same SQL runs unchanged.
"""
from __future__ import annotations

import os
import sqlite3
from typing import Any

# Default location: <repo>/data/catalog.db, resolved relative to this file so the
# client works regardless of the caller's working directory.
_THIS = os.path.dirname(os.path.abspath(__file__))
_DEFAULT_DB = os.path.normpath(os.path.join(_THIS, "..", "..", "..", "data", "catalog.db"))


def default_db() -> str:
    """Path to the bundled local registry, overridable via $ECONDL_CATALOG."""
    return os.environ.get("ECONDL_CATALOG", _DEFAULT_DB)


def connect(db: str | None = None) -> sqlite3.Connection:
    path = os.path.abspath(db or default_db())
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"catalog registry not found at {path!r}. "
            "Set $ECONDL_CATALOG or pass db=... to point at catalog.db."
        )
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    # Source/series text was ingested as UTF-8; decode loosely so a stray byte
    # never crashes a read (registry is upstream-of-us, we only consume it).
    conn.text_factory = lambda b: b.decode("utf-8", "replace")
    conn.row_factory = sqlite3.Row
    return conn


def _row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def search(query: str, *, limit: int = 20, db: str | None = None) -> list[dict[str, Any]]:
    """Full-text search over the series catalog; returns plain dict rows.

    Mirrors core/catalog.py::search (FTS5 with a LIKE fallback) so the client
    and server agree on results.
    """
    conn = connect(db)
    try:
        try:
            rows = conn.execute(
                "SELECT s.* FROM series_fts f JOIN series s ON s.series_id = f.series_id "
                "WHERE series_fts MATCH ? LIMIT ?",
                (query, limit),
            ).fetchall()
            if rows:
                return [dict(r) for r in rows]
        except sqlite3.OperationalError:
            pass  # no FTS table -> fall back to LIKE
        rows = conn.execute(
            "SELECT * FROM series WHERE title LIKE ? OR series_id LIKE ? LIMIT ?",
            (f"%{query}%", f"%{query}%", limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_series(series_id: str, *, db: str | None = None) -> dict[str, Any] | None:
    conn = connect(db)
    try:
        return _row_to_dict(
            conn.execute("SELECT * FROM series WHERE series_id = ?", (series_id,)).fetchone()
        )
    finally:
        conn.close()


def get_source(source_id: str, *, db: str | None = None) -> dict[str, Any] | None:
    conn = connect(db)
    try:
        return _row_to_dict(
            conn.execute("SELECT * FROM source WHERE source_id = ?", (source_id,)).fetchone()
        )
    finally:
        conn.close()


def get_license(license_id: str, *, db: str | None = None) -> dict[str, Any] | None:
    conn = connect(db)
    try:
        return _row_to_dict(
            conn.execute("SELECT * FROM license WHERE license_id = ?", (license_id,)).fetchone()
        )
    finally:
        conn.close()


def source_of(series_id: str) -> str:
    """The provider segment of a catalog id (the first ':'-delimited token)."""
    return series_id.split(":", 1)[0]
