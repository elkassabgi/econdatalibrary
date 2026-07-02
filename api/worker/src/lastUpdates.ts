// ---------------------------------------------------------------------------
// src/lastUpdates.ts  --  GET /v1/last-updates, fully live from D1.
//
// Runs the CANONICAL SQL from CONTRACT.md (src/sql.ts::LAST_UPDATES) verbatim on
// D1, then projects each unit to the contract's dataset shape. Per the contract:
//   - last_updated = unit_state.last_success_utc (null, never faked, if absent)
//   - source_version = unit_state.upstream_vintage (may be null)
//   - next_update_expected = last_success + cadence interval, or null for
//     non-deterministic / unknown cadences (util.nextUpdateExpected -- never
//     fabricates a date).
// Validated against state.db: 48 datasets, 0 ok/no_change rows with null
// last_success_utc (no "unknown laundered into fresh").
// ---------------------------------------------------------------------------

import type { Env, LastUpdateRow } from "./types";
import { LAST_UPDATES } from "./sql";
import { json, nextUpdateExpected } from "./util";

export async function handleLastUpdates(env: Env): Promise<Response> {
  const res = await env.CATALOG.prepare(LAST_UPDATES).all<LastUpdateRow>();
  const rows = res.results ?? [];

  const datasets = rows.map((u) => ({
    source: u.source_id,
    unit: u.unit_id,
    status: u.status, // ok | no_change | partial | transient_fail (whatever state holds)
    last_updated: u.last_success_utc, // null => "never succeeded", never a fake date
    source_date_accessed: u.last_success_utc,
    source_version: u.upstream_vintage, // may be null
    last_obs_date: u.last_obs_date,
    next_update_expected: nextUpdateExpected(u.last_success_utc, u.cadence),
    obs_count: u.obs_count,
  }));

  return json({
    generated: new Date().toISOString(),
    datasets,
  });
}
