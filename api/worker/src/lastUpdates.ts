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
import { promisesNextUpdate, sourceLabel, unitStatus } from "./freshness.ts";

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

  // THE FRESHNESS VOCABULARY (src/freshness.ts; owner decisions 2026-10-20), judged once per SOURCE from
  // its source_state row and all its unit rows, then applied to each unit: "late" replaces ok /
  // no_change / partial / transient_fail; "paused" and "retired" replace the status and promise no next
  // update (ksh used to promise 2027-06-24). The note and the attempt stamp are read, never printed.
  const bySource = new Map<string, LastUpdateRow[]>();
  for (const u of servable) {
    const list = bySource.get(u.source_id) ?? [];
    list.push(u);
    bySource.set(u.source_id, list);
  }
  const now = Date.now();
  const labels = new Map<string, string | null>();
  for (const [sid, units] of bySource) {
    const first = units[0];
    const src = first.source_status === null && first.source_last_success === null && first.cadence === null
      ? null
      : { status: first.source_status, last_success_utc: first.source_last_success, cadence: first.cadence };
    labels.set(sid, sourceLabel(sid, src, units, now));
  }

  const datasets = servable.map((u) => {
    const label = labels.get(u.source_id) ?? null;
    return {
      source: u.source_id,
      unit: u.unit_id,
      // what state holds (ok | no_change | partial | transient_fail | discontinued), or late / paused /
      // retired from the vocabulary
      status: unitStatus(u.status, label),
      last_updated: u.last_success_utc, // null => "never succeeded", never a fake date
      source_date_accessed: u.last_success_utc,
      source_version: u.upstream_vintage, // may be null
      last_obs_date: u.last_obs_date,
      next_update_expected: promisesNextUpdate(label) ? nextUpdateExpected(u.last_success_utc, u.cadence) : null,
      obs_count: u.obs_count,
    };
  });

  return json({
    generated: new Date().toISOString(),
    datasets,
  });
}
