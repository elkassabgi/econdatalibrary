// ---------------------------------------------------------------------------
// src/types.ts  --  Worker environment bindings + row shapes.
//
// No `any`. Every D1 row is typed to the columns the SQL in src/sql.ts selects.
// ---------------------------------------------------------------------------

export interface Env {
  // D1 database holding catalog.db tables (license/source/series + series_fts)
  // AND the freshness tables (unit_state/source_state). One D1 db is simplest;
  // if you split them, add a second binding and route the freshness SQL there.
  // See README.md "Provisioning".
  CATALOG: D1Database;

  // D1 shard for SHARDED_SOURCES (util.ts, task #45): their series + series_fts
  // rows live here because the primary hit its 10 GB per-database ceiling.
  // source/license/unit_state/source_state rows for those sources stay in CATALOG.
  CATALOG_CLIMATE: D1Database;

  // R2 bucket holding the per-series CSV objects the .csv handler streams.
  // Object key = "series/" + encodeURIComponent(series_id) + ".csv"
  // (see src/series.ts header for the honest design rationale).
  SERIES_BUCKET: R2Bucket;

  // Optional: comma-separated override of the migrated-source allowlist. When
  // unset the compiled-in SUPPORTED_SOURCES constant is authoritative.
  SUPPORTED_SOURCES?: string;

  // COST GUARD (src/costGuard.ts), read by the scheduled handler only — never by a request
  // path. CF_ANALYTICS_TOKEN is a read-only "Account Analytics: Read" token, the same value
  // the repo .env carries for tools/billing_guard.py. Both are optional at the type level
  // and MANDATORY at runtime: a scheduled run without them writes a `blind` status object
  // and throws, because a meter that cannot measure has to be louder than one that can.
  CF_ANALYTICS_TOKEN?: string;
  CF_ACCOUNT_ID?: string;

  // SHARED LOGIN (owner directive; PLAN.md §6 "API keys + rate limit (echo
  // your rate limit)"): the hfdatalibrary users database is THE identity provider
  // for the whole Data Library family. This binding points at hfdatalibrary-db
  // (same Cloudflare account), so every existing hf api_key works here with no
  // migration. Data downloads validate against it; econ download logging goes
  // to econ_download_log inside the SAME db (separate table — hf's download
  // counts are never inflated by econ traffic).
  USERS: D1Database;

  // Optional Cloudflare Analytics access for the stats page's visitor map layer.
  // CF_API_TOKEN = a Zone Analytics:Read token; CF_ZONE_ID = econdatalibrary.com's
  // zone. When both are set, /v1/public-stats adds visitor_countries / visitors /
  // page_views (this site's own traffic). Absent -> the map stays user-only.
  CF_API_TOKEN?: string;
  CF_ZONE_ID?: string;
}

// --- D1 row shapes (one per SELECT column list in sql.ts) ------------------

export interface SeriesRow {
  series_id: string;
  source_id: string;
  title: string | null;
  frequency: string | null;
  unit: string | null;
  geography: string | null;
  category: string | null;
  license_id: string | null;
  start_date: string | null;
  end_date: string | null;
  last_updated: string | null;
  metadata: string | null;
}

export interface CatalogResultRow {
  series_id: string;
  source_id: string;
  title: string | null;
  frequency: string | null;
  unit: string | null;
  geography: string | null;
  license_id: string | null;
  start_date: string | null;
  end_date: string | null;
  // Selected so ?lang= can localize the title from metadata.titles[<lang>].
  // NEVER emitted in the response (the handler maps explicit fields only).
  metadata: string | null;
}

export interface SourceRow {
  source_id: string;
  name: string | null;
  homepage: string | null;
  license_id: string | null;
  attribution: string | null;
  terms_url: string | null;
}

export interface LicenseRow {
  license_id: string;
  name: string | null;
  reservable: number | null;
  commercial_ok: number | null;
  attribution_required: number | null;
  no_modify: number | null;
  url: string | null;
}

export interface SourceJoinedRow {
  source_id: string;
  name: string | null;
  homepage: string | null;
  license_id: string | null;
  attribution: string | null;
  terms_url: string | null;
  license_name: string | null;
  license_url: string | null;
  reservable: number | null;
  commercial_ok: number | null;
  attribution_required: number | null;
  no_modify: number | null;
  cadence: string | null;
  source_status: string | null;
  source_last_success: string | null;
  // SELECT_SOURCES has returned this since task #138 and sources.ts reads it; the
  // interface never declared it, so `npx tsc --noEmit` has been failing on two lines
  // of working code. Runtime was fine (D1 returns the column) - the type was wrong.
  data_through: string | null;
}

export interface LastUpdateRow {
  source_id: string;
  unit_id: string;
  status: string | null;
  last_success_utc: string | null;
  upstream_vintage: string | null;
  last_obs_date: string | null;
  obs_count: number | null;
  cadence: string | null;
}

export interface CountRow {
  n: number;
}

export interface SeriesIdRow {
  series_id: string;
}

// --- response-shape helpers ------------------------------------------------

/** A normalised license block, identical shape in metadata.json and /v1/sources. */
export interface LicenseBlock {
  id: string;
  name: string | null;
  url: string | null;
  reservable: boolean;
  commercial_ok: boolean;
  attribution_required: boolean;
  no_modify: boolean;
}
