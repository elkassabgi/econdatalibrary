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

import type { Env, LastUpdateRow } from "./types.ts";
import { LAST_UPDATES } from "./sql.ts";
import { NON_REDISTRIBUTABLE } from "./denylist.ts";
import { json, nextUpdateExpected } from "./util.ts";

export async function handleLastUpdates(env: Env): Promise<Response> {
  const res = await env.CATALOG.prepare(LAST_UPDATES).all<LastUpdateRow>();
  const rows = res.results ?? [];

  // THE REDISTRIBUTION GATE APPLIES HERE TOO, and did not until 2026-09-17.
  //
  // `/v1/sources` filters its rows through this same set (sources.ts:56) precisely so a gated
  // source is not merely undownloadable but unlisted. This route ran the canonical SQL, which
  // selects EVERY unit_state row with no exclusion, and published the result — so a gated
  // source was named here, with its cadence, status and freshness, while being hidden two
  // routes away.
  //
  // It is not a stale-rows artefact, which is the tempting explanation: the CATALOGUE sync is
  // frozen, but the STATE sync is not (core/sync_state_d1.py upserts all rows and never
  // deletes), so these rows are refreshed and would come back on their own even if D1 were
  // cleaned. Filtering at the read is what actually closes it.
  //
  // Measured live before the fix, reporting a boolean rather than the ids — disclosing the set
  // is the leak: GET /v1/last-updates returned HTTP 200, 74,004 bytes, 283 source ids, and the
  // intersection with this set was non-empty. Controls held (a known-served source present, an
  // invented id absent).
  const servable = rows.filter((u) => !NON_REDISTRIBUTABLE.has(u.source_id));

  const datasets = servable.map((u) => ({
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
