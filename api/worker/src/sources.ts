// ---------------------------------------------------------------------------
// src/sources.ts  --  GET /v1/sources, fully live from D1.
//
// Every registered source (count intentionally not hard-coded here -- it changes when a
// source is added, or removed because we cannot host it) with its license/attribution + a freshness
// summary (status, last_updated, cadence) joined from source_state. LEFT JOINs
// mean a source with no license or no freshness row still appears -- never
// dropped, and freshness fields are null (not fabricated) when absent.
// ---------------------------------------------------------------------------

import type { Env, SourceJoinedRow } from "./types";
import { SELECT_SOURCES } from "./sql";
import { json } from "./util";

export async function handleSources(env: Env): Promise<Response> {
  const res = await env.CATALOG.prepare(SELECT_SOURCES).all<SourceJoinedRow>();
  const rows = res.results ?? [];

  // CANONICAL v1.1 NESTED shape (CONTRACT.md "Canonical response shapes"):
  //   { source, name, homepage, license:{...}|null, freshness:{...}|null }
  // attribution/terms_url are NOT part of this pin (they live in metadata.json
  // and the bundle provenance). freshness is null when the source has no
  // source_state row at all (honest absence, not a fabricated {null,null,null}).
  return json({
    total: rows.length,
    sources: rows.map((r) => {
      const hasFreshness =
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
              status: r.source_status, // null when no source_state row (honest)
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
