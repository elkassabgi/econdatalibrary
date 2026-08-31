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

import {
  NON_REDISTRIBUTABLE, SERIES_CARVEOUT_LIKE, SERIES_CARVEOUT_EXACT, SERIES_CARVEOUTS, likeEscape,
} from "./denylist";

/** Compliance layer ON TOP of the shim-mirrored statements: series from
 *  non-redistributable sources must never surface in catalog search/browse
 *  (matching /v1/sources, /v1/series's 451, and the bundle gate). Generated
 *  from denylist.ts so SQL and handler logic cannot drift. The dev shim runs
 *  the un-excluded statements; the divergence is deliberate and documented —
 *  redistribution gating is a SERVING concern, not a catalog-semantics one.
 *  (source ids are static compile-time identifiers; no injection surface.) */
const DENY_LIST_SQL = [...NON_REDISTRIBUTABLE].map((s) => `'${s}'`).join(",");
// Series-level carve-outs (denylist.ts SERIES_CARVEOUTS): exclude specific
// third-party indicators within an otherwise-served source (e.g. worldbank's
// IMF-sourced CPI + ILO-sourced unemployment). Prefix match on `<src>:<ind>:`.
// Two terms per carve-out, because ids come in two shapes and the prefix only covers one.
// `<src>:<ind>:` matches three-part ids; the equality covers two-part ids like
// `worldbank_wdi:FP.CPI.TOTL.ZG` and `worldbank_pink:aluminum`, for which the prefix had
// always matched zero rows. ESCAPE '\' because `_` is a LIKE wildcard and two of the three
// carve-out source ids contain one.
const _exclFor = (col: string) =>
  [
    ...SERIES_CARVEOUT_LIKE.map((p) => `AND ${col} NOT LIKE '${p}%' ESCAPE '\\'`),
    ...SERIES_CARVEOUT_EXACT.map((e) => `AND ${col} <> '${e.replace(/'/g, "''")}'`),
  ].join(" ");
const SERIES_EXCL_A = _exclFor("s.series_id");
const SERIES_EXCL_B = _exclFor("series_id");
const EXCL_ALIASED = `${NON_REDISTRIBUTABLE.size ? `AND s.source_id NOT IN (${DENY_LIST_SQL})` : ""} ${SERIES_EXCL_A}`.trim();
const EXCL_BARE = `${NON_REDISTRIBUTABLE.size ? `AND source_id NOT IN (${DENY_LIST_SQL})` : ""} ${SERIES_EXCL_B}`.trim();
const EXCL_BARE_WHERE = NON_REDISTRIBUTABLE.size
  ? `WHERE source_id NOT IN (${DENY_LIST_SQL}) ${SERIES_EXCL_B}`.trim()
  : (SERIES_EXCL_B ? `WHERE 1=1 ${SERIES_EXCL_B}`.trim() : "");

/** One series row, exact id. Mirrors _catalog.get_series. */
export const SELECT_SERIES = `SELECT * FROM series WHERE series_id = ?`;

/** One source row, exact id. Mirrors _catalog.get_source. */
export const SELECT_SOURCE = `SELECT * FROM source WHERE source_id = ?`;

/** One license row, exact id. Mirrors _catalog.get_license. */
export const SELECT_LICENSE = `SELECT * FROM license WHERE license_id = ?`;

/** Every source WITH DATA + its license + freshness summary. LEFT JOINs so a
 *  source with no license row or no freshness row still appears. Sources with
 *  ZERO series are excluded (owner display policy 2026-07-15: no references to
 *  databases we don't host; gated-pending sources have series and remain). */
export const SELECT_SOURCES = `
SELECT s.source_id, s.name, s.homepage, s.license_id, s.attribution, s.terms_url,
       l.name AS license_name, l.url AS license_url,
       l.reservable, l.commercial_ok, l.attribution_required, l.no_modify,
       ss.cadence, ss.status AS source_status, ss.last_success_utc AS source_last_success,
       dt.data_through AS data_through
FROM source s
LEFT JOIN license l ON l.license_id = s.license_id
LEFT JOIN source_state ss ON ss.source_id = s.source_id
LEFT JOIN source_data_through dt ON dt.source_id = s.source_id
WHERE EXISTS (SELECT 1 FROM series se WHERE se.source_id = s.source_id)
ORDER BY s.source_id`;

/** One source row, joined exactly like SELECT_SOURCES but WITHOUT the
 *  `EXISTS (series)` filter. Needed for SHARDED sources: noaa's series rows live in
 *  CATALOG_CLIMATE, so the EXISTS check against the PRIMARY database is false and the
 *  source vanishes from /v1/sources even while 3.1M of its series are served. The
 *  existence test for those is done against the shard, in the handler, and then the
 *  descriptive row is fetched with this. `?` is the source_id. */
export const SELECT_SOURCE_JOINED = `
SELECT s.source_id, s.name, s.homepage, s.license_id, s.attribution, s.terms_url,
       l.name AS license_name, l.url AS license_url,
       l.reservable, l.commercial_ok, l.attribution_required, l.no_modify,
       ss.cadence, ss.status AS source_status, ss.last_success_utc AS source_last_success,
       dt.data_through AS data_through
FROM source s
LEFT JOIN license l ON l.license_id = s.license_id
LEFT JOIN source_state ss ON ss.source_id = s.source_id
LEFT JOIN source_data_through dt ON dt.source_id = s.source_id
WHERE s.source_id = ?`;

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

/** Carve-out exclusion for ONE source, or "" when that source has none.
 *
 * Built per source rather than globally, and this matters for cost. The exclusion is
 * index-resident (series_id lives in the PK autoindex, so `EXPLAIN QUERY PLAN` is unchanged
 * and D1's billed `rows_read` does not move), but the terms are evaluated per candidate
 * entry BEFORE OFFSET. Measured 2026-08-30 on the local catalogue at the MAX_OFFSET of
 * 100,000: applying all 11 carve-out terms globally costs 3.70M VDBE steps / 148 ms against
 * 400.8k / 29.6 ms unfiltered, while the 2 terms `worldbank` actually needs cost 1.00M /
 * 52.8 ms. Per source, ~318 of 321 sources therefore pay exactly nothing.
 *
 * BOTH id shapes are emitted. The historical `<src>:<ind>:` prefix cannot match a two-part
 * id, so the SQL exclusion for `worldbank_wdi:FP.CPI.TOTL.ZG` and `worldbank_pink:aluminum`
 * had always matched 0 rows; the equality term closes that. ESCAPE is required because `_`
 * is a LIKE wildcard and two of the three carve-out source ids contain one.
 *
 * Values are interpolated, not bound, because they must be inside a prepared statement that
 * also takes positional binds; they come from SERIES_CARVEOUTS in this repo, never from a
 * request. Quotes are doubled anyway so a future id containing one cannot break out.
 */
export function carveoutExcl(source: string, col: string = "series_id"): string {
  const inds = (SERIES_CARVEOUTS as Record<string, readonly string[]>)[source];
  if (!inds || inds.length === 0) return "";
  const lit = (s: string) => s.replace(/'/g, "''");
  return inds
    .map(
      (ind) =>
        ` AND ${col} NOT LIKE '${lit(likeEscape(`${source}:${ind}:`))}%' ESCAPE '\\'` +
        ` AND ${col} <> '${lit(`${source}:${ind}`)}'`,
    )
    .join("");
}

/** FTS search constrained to ONE source (q + source combined). Mirrors the dev
 *  shim, which ANDs the source filter onto the FTS JOIN. Binds: q, source, limit, offset.
 *
 *  A function, not a constant, because the carve-out exclusion is per source. Without it
 *  `?q=unemployment&source=worldbank` answered `total=235` with an EMPTY results array on
 *  EVERY page — the JS gate in catalog.ts filtered each page after LIMIT, and for this query
 *  every matching row is carved. Verified live 2026-08-30 before the fix. */
export function searchFtsSourceSql(source: string): string {
  return `
SELECT s.series_id, s.source_id, s.title, s.frequency, s.unit, s.geography,
       s.license_id, s.start_date, s.end_date, s.metadata
FROM series_fts f JOIN series s ON s.series_id = f.series_id
WHERE series_fts MATCH ? AND s.source_id = ?${carveoutExcl(source, "s.series_id")}
LIMIT ? OFFSET ?`;
}

export function searchFtsSourceCountSql(source: string): string {
  return `
SELECT COUNT(*) AS n FROM series_fts f JOIN series s ON s.series_id = f.series_id
WHERE series_fts MATCH ? AND s.source_id = ?${carveoutExcl(source, "s.series_id")}`;
}

/** LIKE fallback constrained to ONE source. Binds: like, like, source, limit, offset. */
export function searchLikeSourceSql(source: string): string {
  return `
SELECT series_id, source_id, title, frequency, unit, geography,
       license_id, start_date, end_date, metadata
FROM series
WHERE (title LIKE ? OR series_id LIKE ?) AND source_id = ?${carveoutExcl(source)}
LIMIT ? OFFSET ?`;
}

export function searchLikeSourceCountSql(source: string): string {
  return `
SELECT COUNT(*) AS n FROM series
WHERE (title LIKE ? OR series_id LIKE ?) AND source_id = ?${carveoutExcl(source)}`;
}

/** Browse one source (q absent) — PK-range form, COST-CRITICAL (2026-08-15).
 *
 * The old form (WHERE source_id = ? ORDER BY series_id LIMIT ? OFFSET ?) forced
 * SQLite to read and sort the source's ENTIRE row set on every page: 4.93M rows
 * read PER PAGE on wid, and a crawler paging through wid drove 87.3 BILLION rows
 * read in one day (~$82 of D1 reads, measured with `wrangler d1 insights`).
 * A composite index cannot be built — CREATE INDEX on the 9.2M-row table dies
 * with SQLITE_NOMEM on D1 — so the fix rides the PK: every series_id begins with
 * `source_id + ':'` (invariant VERIFIED on both DBs 2026-08-15: 0 violations in
 * 9,214,639 + 3,137,871 rows), so the [src+':', src+';') range walks the PK
 * autoindex already in series_id order and reads only offset+limit entries.
 * Binds: lo (src+':'), hi (src+';'), limit, offset. */
export function browseSourceSql(source: string): string {
  return `
SELECT series_id, source_id, title, frequency, unit, geography,
       license_id, start_date, end_date, metadata
FROM series WHERE series_id >= ? AND series_id < ?${carveoutExcl(source)}
ORDER BY series_id LIMIT ? OFFSET ?`;
}

/** Kept for callers/tests that want the un-scoped shape. Prefer browseSourceSql. */
export const BROWSE_SOURCE = `
SELECT series_id, source_id, title, frequency, unit, geography,
       license_id, start_date, end_date, metadata
FROM series WHERE series_id >= ? AND series_id < ? ORDER BY series_id LIMIT ? OFFSET ?`;

/** Visible-row count for a source that HAS carve-outs, over the PK range.
 *
 * Two things this must not be. It must not be `source_counts`, which counts carved rows and
 * so advertised 692 for worldbank where 262 are reachable. And it must not be
 * BROWSE_SOURCE_COUNT (`WHERE source_id = ?`): there is no index on source_id, and that form
 * measured 13.85 s locally for worldbank — the 2026-08-15 incident's exact shape.
 *
 * The PK range rides the autoindex, so this stays cheap for the sources that need it
 * (worldbank 692 rows / 1.1 ms, worldbank_wdi 1,486 / 2.8 ms). Call it ONLY when
 * `hasCarveouts(source)` — on a giant like wid (2.47M rows) or bea (913k) a full-range count
 * per page view is exactly what source_counts exists to avoid. */
export function browseSourceVisibleCountSql(source: string): string {
  return `
SELECT COUNT(*) AS n FROM series
WHERE series_id >= ? AND series_id < ?${carveoutExcl(source)}`;
}

/** Does this source carry any series-level carve-out? Gate the live count on THIS, never on
 *  a size heuristic — membership is the property that makes the bounded count correct. */
export function hasCarveouts(source: string): boolean {
  const inds = (SERIES_CARVEOUTS as Record<string, readonly string[]>)[source];
  return !!inds && inds.length > 0;
}

/** Same incident: COUNT(*) WHERE source_id ran on EVERY catalog page view —
 * 2.47M rows read per call, 42.2B rows in a day. source_counts (1 row per
 * source) is populated at catalog-sync time; the live COUNT below is only the
 * fallback for a source synced before its count row exists. */
export const BROWSE_SOURCE_COUNT_CACHED = `SELECT n FROM source_counts WHERE source_id = ?`;
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
