// ---------------------------------------------------------------------------
// src/sql.ts  --  the SAME SQL the Python dev shim / econdl client run.
//
// D1 *is* SQLite (CONTRACT.md "Backend binding"), so every query below is the
// byte-for-byte query the local SQLite path runs. They are kept here, in one
// place, so the Worker and the dev shim can never silently drift:
//   * catalog search  -> mirrors clients/python/econdl/_catalog.py::search
//                        and core/catalog.py::search (FTS5 + LIKE fallback).
//   * last-updates    -> the CANONICAL SQL spelled out verbatim in CONTRACT.md
//                        (validated: 48 datasets, 0 ok/no_change with null).
//   * series / source / license lookups -> the exact _catalog.py statements.
//
// Nothing here fabricates a value. Honest-status decisions live in the handlers;
// this file only holds the statements + their bound-parameter contracts.
// ---------------------------------------------------------------------------

// catalog.db (D1 binding `CATALOG`) ----------------------------------------

import { NON_REDISTRIBUTABLE } from "./denylist";

/** Compliance layer ON TOP of the shim-mirrored statements: series from
 *  non-redistributable sources must never surface in catalog search/browse
 *  (matching /v1/sources, /v1/series's 451, and the bundle gate). Generated
 *  from denylist.ts so SQL and handler logic cannot drift. The dev shim runs
 *  the un-excluded statements; the divergence is deliberate and documented —
 *  redistribution gating is a SERVING concern, not a catalog-semantics one.
 *  (source ids are static compile-time identifiers; no injection surface.) */
const DENY_LIST_SQL = [...NON_REDISTRIBUTABLE].map((s) => `'${s}'`).join(",");
const EXCL_ALIASED = NON_REDISTRIBUTABLE.size
  ? `AND s.source_id NOT IN (${DENY_LIST_SQL})` : "";
const EXCL_BARE = NON_REDISTRIBUTABLE.size
  ? `AND source_id NOT IN (${DENY_LIST_SQL})` : "";
const EXCL_BARE_WHERE = NON_REDISTRIBUTABLE.size
  ? `WHERE source_id NOT IN (${DENY_LIST_SQL})` : "";

/** One series row, exact id. Mirrors _catalog.get_series. */
export const SELECT_SERIES = `SELECT * FROM series WHERE series_id = ?`;

/** One source row, exact id. Mirrors _catalog.get_source. */
export const SELECT_SOURCE = `SELECT * FROM source WHERE source_id = ?`;

/** One license row, exact id. Mirrors _catalog.get_license. */
export const SELECT_LICENSE = `SELECT * FROM license WHERE license_id = ?`;

/** Every source + its license + freshness summary (309 rows). LEFT JOINs so a
 *  source with no license row or no freshness row still appears (never dropped). */
export const SELECT_SOURCES = `
SELECT s.source_id, s.name, s.homepage, s.license_id, s.attribution, s.terms_url,
       l.name AS license_name, l.url AS license_url,
       l.reservable, l.commercial_ok, l.attribution_required, l.no_modify,
       ss.cadence, ss.status AS source_status, ss.last_success_utc AS source_last_success
FROM source s
LEFT JOIN license l ON l.license_id = s.license_id
LEFT JOIN source_state ss ON ss.source_id = s.source_id
ORDER BY s.source_id`;

/** FTS5 search. Identical to core/catalog.py + _catalog.py::search (primary path).
 *  `?` is the MATCH query, `?` is the limit. */
export const SEARCH_FTS = `
SELECT s.series_id, s.source_id, s.title, s.frequency, s.unit, s.geography,
       s.license_id, s.start_date, s.end_date, s.metadata
FROM series_fts f JOIN series s ON s.series_id = f.series_id
WHERE series_fts MATCH ? ${EXCL_ALIASED} LIMIT ? OFFSET ?`;

/** LIKE fallback when FTS errors / matches nothing. Mirrors the Python fallback,
 *  extended with series_id LIKE (as _catalog.py does) so an id substring also hits. */
export const SEARCH_LIKE = `
SELECT series_id, source_id, title, frequency, unit, geography,
       license_id, start_date, end_date, metadata
FROM series
WHERE (title LIKE ? OR series_id LIKE ?) ${EXCL_BARE}
LIMIT ? OFFSET ?`;

/** Total count for a LIKE search (for the `total` field). */
export const SEARCH_LIKE_COUNT = `
SELECT COUNT(*) AS n FROM series WHERE (title LIKE ? OR series_id LIKE ?) ${EXCL_BARE}`;

/** Total count for an FTS search. */
export const SEARCH_FTS_COUNT = `
SELECT COUNT(*) AS n FROM series_fts f JOIN series s ON s.series_id = f.series_id
WHERE series_fts MATCH ? ${EXCL_ALIASED}`;

/** FTS search constrained to ONE source (q + source combined). Mirrors the dev
 *  shim, which ANDs the source filter onto the FTS JOIN. Binds: q, source, limit, offset. */
export const SEARCH_FTS_SOURCE = `
SELECT s.series_id, s.source_id, s.title, s.frequency, s.unit, s.geography,
       s.license_id, s.start_date, s.end_date, s.metadata
FROM series_fts f JOIN series s ON s.series_id = f.series_id
WHERE series_fts MATCH ? AND s.source_id = ? LIMIT ? OFFSET ?`;

export const SEARCH_FTS_SOURCE_COUNT = `
SELECT COUNT(*) AS n FROM series_fts f JOIN series s ON s.series_id = f.series_id
WHERE series_fts MATCH ? AND s.source_id = ?`;

/** LIKE fallback constrained to ONE source. Binds: like, like, source, limit, offset. */
export const SEARCH_LIKE_SOURCE = `
SELECT series_id, source_id, title, frequency, unit, geography,
       license_id, start_date, end_date, metadata
FROM series
WHERE (title LIKE ? OR series_id LIKE ?) AND source_id = ?
LIMIT ? OFFSET ?`;

export const SEARCH_LIKE_SOURCE_COUNT = `
SELECT COUNT(*) AS n FROM series WHERE (title LIKE ? OR series_id LIKE ?) AND source_id = ?`;

/** Browse one source (q absent). */
export const BROWSE_SOURCE = `
SELECT series_id, source_id, title, frequency, unit, geography,
       license_id, start_date, end_date, metadata
FROM series WHERE source_id = ? ORDER BY series_id LIMIT ? OFFSET ?`;

export const BROWSE_SOURCE_COUNT = `SELECT COUNT(*) AS n FROM series WHERE source_id = ?`;

/** Browse all series (no q, no source). */
export const BROWSE_ALL = `
SELECT series_id, source_id, title, frequency, unit, geography,
       license_id, start_date, end_date, metadata
FROM series ${EXCL_BARE_WHERE} ORDER BY series_id LIMIT ? OFFSET ?`;

export const BROWSE_ALL_COUNT = `SELECT COUNT(*) AS n FROM series ${EXCL_BARE_WHERE}`;

/** Every series id of one source (for /v1/bundle?source=). Mirrors _bundle.bundle. */
export const SERIES_IDS_FOR_SOURCE = `
SELECT series_id FROM series WHERE source_id = ? ORDER BY series_id`;

// state.db (D1 binding `STATE`, or same DB with both tables) -----------------

/** THE CANONICAL last-updates SQL, copied verbatim from CONTRACT.md §/v1/last-updates.
 *  Runs unchanged on D1 (D1 is SQLite). 48 rows; 0 ok/no_change with null. */
export const LAST_UPDATES = `
SELECT u.source_id, u.unit_id, u.status, u.last_success_utc, u.upstream_vintage,
       u.last_obs_date, u.obs_count, s.cadence
FROM unit_state u LEFT JOIN source_state s ON s.source_id = u.source_id
ORDER BY u.source_id, u.unit_id`;

/** Freshness for ONE source's units (used to enrich series metadata last_updated). */
export const UNIT_STATE_FOR_SOURCE = `
SELECT u.source_id, u.unit_id, u.status, u.last_success_utc, u.last_obs_date,
       u.obs_count, s.cadence
FROM unit_state u LEFT JOIN source_state s ON s.source_id = u.source_id
WHERE u.source_id = ?
ORDER BY u.unit_id`;
