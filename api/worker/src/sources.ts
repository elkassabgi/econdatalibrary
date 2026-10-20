// ---------------------------------------------------------------------------
// src/sources.ts  --  GET /v1/sources, fully live from D1.
//
// Every registered source (count intentionally not hard-coded here -- it changes when a
// source is added, or removed because we cannot host it) with its license/attribution + a freshness
// summary (status, last_updated, cadence) joined from source_state. LEFT JOINs
// mean a source with no license or no freshness row still appears -- never
// dropped, and freshness fields are null (not fabricated) when absent.
// ---------------------------------------------------------------------------

import type { Env, SourceJoinedRow } from "./types.ts";
import { SELECT_SOURCES, SELECT_SOURCE_JOINED, unitFreshnessSql } from "./sql.ts";
import { needsUnits, sourceLabel, type UnitFresh } from "./freshness.ts";
import { json, SHARDED_SOURCES, dbFor } from "./util.ts";
import { NON_REDISTRIBUTABLE } from "./denylist.ts";

export async function handleSources(env: Env): Promise<Response> {
  const res = await env.CATALOG.prepare(SELECT_SOURCES).all<SourceJoinedRow>();
  const rows = res.results ?? [];

  // SHARDED SOURCES ARE SERVED BUT WERE INVISIBLE HERE. SELECT_SOURCES keeps a source
  // only if the PRIMARY database holds series rows for it, and noaa's 3.1M rows live in
  // CATALOG_CLIMATE. So /v1/sources reported 318 while noaa was fully served: catalogued,
  // in D1, 3,138,211 CSVs on R2, resolving through /v1/series and findable through
  // /v1/catalog — just absent from the list a person browses to discover it.
  // index.ts already carries this correction for /v1/stats ("a primary-only count
  // silently drops 3.1M entries"); the same shard has to be consulted here.
  // D1 cannot join across databases, so the existence test runs on the shard and the
  // descriptive row is then read from the primary with the same joins, unfiltered.
  const present = new Set(rows.map((r) => r.source_id));
  for (const src of SHARDED_SOURCES) {
    if (present.has(src)) continue;
    const shard = dbFor(env, src);
    const hit = await shard.prepare(
      "SELECT 1 AS ok FROM series WHERE source_id = ? LIMIT 1").bind(src).first<{ ok: number }>();
    if (!hit) continue;
    const row = await env.CATALOG.prepare(SELECT_SOURCE_JOINED).bind(src).first<SourceJoinedRow>();
    if (row) rows.push(row);
  }
  rows.sort((a, b) => (a.source_id < b.source_id ? -1 : a.source_id > b.source_id ? 1 : 0));

  // A SOURCE WE REFUSE TO SERVE MUST NOT BE ADVERTISED HERE. This handler applied no
  // denylist at all, so any gated source with a `source` row was listed with a full licence block
  // (audit-restricted) while every path to its data answered 451 — /v1/catalog?source=<id>
  // and /v1/series/<id>:<indicator>.csv both measured 451 live on 2026-08-30. That is a browsable entry a user cannot obtain,
  // which is exactly the "metadata-only" listing Ahmed's standing rule forbids: host it fully
  // or do not list it.
  //
  // Two comments elsewhere in this worker already ASSERT that this filtering happens
  // (catalog.ts and bundle.ts both say denylisted sources are hidden from /v1/sources), so
  // the code and its own documentation disagreed — R125's shape, where prose describes a
  // change that was never made.
  //
  // After this filter the listed count is exactly SUPPORTED_SOURCES minus
  // NON_REDISTRIBUTABLE. (Any gated id without a `source` row was already absent for an unrelated
  // reason, so no such id was the discrepancy.)
  const servable = rows.filter((r) => !NON_REDISTRIBUTABLE.has(r.source_id));

  // THE FRESHNESS VOCABULARY (src/freshness.ts; owner decisions 2026-10-20). source_state.status is
  // written only on a SUCCESS (R231), so a source that keeps running and ends partial read "ok" here
  // for months (istat 98 days). Now: "late" past 2 x cadence since that success, "paused" for a
  // registry pause, "retired" when no run mechanism names the source. Sources judged on their latest
  // pass (registry rotating / served_by lane) need their unit rows: one primary-key seek per such id,
  // and only for the ids that could read late at all.
  const now = Date.now();
  const wantUnits = servable
    .filter((r) => needsUnits(r.source_id) && (r.source_status === "ok" || r.source_status === "no_change"))
    .map((r) => r.source_id)
    .slice(0, 100);
  const unitsBy = new Map<string, UnitFresh[]>();
  if (wantUnits.length) {
    const ur = await env.CATALOG.prepare(unitFreshnessSql(wantUnits.length)).bind(...wantUnits)
      .all<UnitFresh & { source_id: string }>();
    for (const u of ur.results ?? []) {
      const list = unitsBy.get(u.source_id) ?? [];
      list.push(u);
      unitsBy.set(u.source_id, list);
    }
  }

  // CANONICAL v1.1 NESTED shape (CONTRACT.md "Canonical response shapes"):
  //   { source, name, homepage, license:{...}|null, freshness:{...}|null }
  // attribution/terms_url are NOT part of this pin (they live in metadata.json
  // and the bundle provenance). freshness is null when the source has no
  // source_state row at all (honest absence, not a fabricated {null,null,null}).
  return json({
    total: servable.length,
    sources: servable.map((r) => {
      const label = sourceLabel(
        r.source_id,
        { status: r.source_status, last_success_utc: r.source_last_success, cadence: r.cadence },
        unitsBy.get(r.source_id) ?? [],
        now,
      );
      // A paused or retired source with no state row at all still says so (same object shape).
      const hasFreshness =
        label !== null ||
        r.source_status !== null ||
        r.source_last_success !== null ||
        r.cadence !== null ||
        r.data_through !== null;
      return {
        source: r.source_id,
        name: r.name,
        homepage: r.homepage,
        license: r.license_id
          ? {
              id: r.license_id,
              name: r.license_name,
              url: r.license_url,
              reservable: !!r.reservable,
              commercial_ok: !!r.commercial_ok,
              attribution_required: !!r.attribution_required,
              no_modify: !!r.no_modify,
            }
          : null,
        freshness: hasFreshness
          ? {
              // The last SUCCESS's status (null when no source_state row), unless the vocabulary
              // says late / paused / retired (src/freshness.ts).
              status: label ?? r.source_status,
              last_updated: r.source_last_success, // null when never run (never faked)
              cadence: r.cadence,
              // Newest served observation (MAX series.end_date, stamped at sync
              // time from the local catalog — task #138). Present even for
              // rotating-'partial' sources whose runs never earn last_updated.
              data_through: r.data_through ?? null,
            }
          : null,
      };
    }),
  });
}
