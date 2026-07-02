// ---------------------------------------------------------------------------
// src/metadata.ts  --  GET /v1/series/{id}.metadata.json, fully live from D1.
//
// Builds the CONTRACT.md metadata.json from catalog.db (series + source +
// license) and, where available, freshness (unit_state) for last_updated.
//
// HONEST FIELD POLICY (CONTRACT.md): description_key / description_processing /
// citation_short / citation_long come from the series-tier metadata pass
// (Task #5) and DO NOT EXIST in the catalog yet (verified: 0 series carry them).
// They are therefore OMITTED, never faked. We DO surface the per-series
// `description` and producer `citation` strings that some series already carry
// in metadata JSON, under clearly-named keys, and we never invent obs_count:
// it requires reading the parquet rows (the R2 path), so it is omitted here and
// the .csv body is the source of truth for the actual row count.
// ---------------------------------------------------------------------------

import type { Env, SeriesRow, SourceRow, LicenseRow, LastUpdateRow } from "./types";
import { SELECT_SERIES, SELECT_SOURCE, SELECT_LICENSE, UNIT_STATE_FOR_SOURCE } from "./sql";
import { json, notFound, licenseBlock, sourceOf, localizedTitle } from "./util";

interface SeriesMeta {
  citation?: string;
  description?: string;
  description_key?: unknown;
  description_processing?: unknown;
  citation_short?: string;
  citation_long?: string;
  [k: string]: unknown;
}

export async function handleMetadata(seriesId: string, env: Env, lang = "en"): Promise<Response> {
  const series = await env.CATALOG.prepare(SELECT_SERIES).bind(seriesId).first<SeriesRow>();
  if (!series) return notFound(seriesId);

  const source = sourceOf(seriesId);
  const srcRow = await env.CATALOG.prepare(SELECT_SOURCE).bind(source).first<SourceRow>();
  const licId = series.license_id ?? srcRow?.license_id ?? null;
  const licRow = licId
    ? await env.CATALOG.prepare(SELECT_LICENSE).bind(licId).first<LicenseRow>()
    : null;

  // Parse per-series metadata JSON for producer citation / description (honest:
  // only emit what is actually present).
  let meta: SeriesMeta = {};
  if (series.metadata) {
    try {
      meta = JSON.parse(series.metadata) as SeriesMeta;
    } catch {
      meta = {};
    }
  }

  // last_updated: prefer the series row's own column; fall back to the source's
  // freshness last_success_utc. Never fabricate -> null if neither exists.
  let lastUpdated: string | null = series.last_updated;
  if (!lastUpdated) {
    const units = await env.CATALOG.prepare(UNIT_STATE_FOR_SOURCE)
      .bind(source).all<LastUpdateRow>();
    const all = (units.results ?? []).find((u) => u.unit_id === "_all");
    lastUpdated = (all ?? units.results?.[0])?.last_success_utc ?? null;
  }

  const out: Record<string, unknown> = {
    series_id: seriesId,
    source,
    title: series.title,
    frequency: series.frequency,
    unit: series.unit,
    geography: series.geography,
    category: series.category,
    start_date: series.start_date,
    end_date: series.end_date,
    // obs_count intentionally OMITTED: it requires reading the parquet rows (R2);
    // the .csv response is the source of truth for the row count. Never faked.
    license: licenseBlock(licRow),
    attribution: srcRow?.attribution ?? null,
    homepage: srcRow?.homepage ?? null,
    terms_url: srcRow?.terms_url ?? null,
    last_updated: lastUpdated,
    csv_url: `/v1/series/${encodeURIComponent(seriesId)}.csv`,
  };

  // Human-context fields (CONTRACT.md v1.1 pin, byte-for-byte with the dev shim).
  // Prefer the series-tier metadata pass (Task #5) keys verbatim when present;
  // ELSE fall back to the catalog's existing `description` / `citation` keys.
  // Fields absent under both are OMITTED, never faked.
  if (meta.description_key != null && meta.description_key !== "") {
    out.description_key = meta.description_key;
  } else if (typeof meta.description === "string" && meta.description) {
    out.description = meta.description;
  }
  if (meta.description_processing != null && meta.description_processing !== "") {
    out.description_processing = meta.description_processing;
  }
  if (
    (typeof meta.citation_short === "string" && meta.citation_short) ||
    (typeof meta.citation_long === "string" && meta.citation_long)
  ) {
    if (typeof meta.citation_short === "string" && meta.citation_short) {
      out.citation_short = meta.citation_short;
    }
    if (typeof meta.citation_long === "string" && meta.citation_long) {
      out.citation_long = meta.citation_long;
    }
  } else if (typeof meta.citation === "string" && meta.citation) {
    // producer citation FIRST (CONTRACT.md), library compiled-by appended.
    out.citation_short = meta.citation;
    out.citation_long = `${meta.citation}. Compiled by Elkassabgi Data Library.`;
  }

  // i18n: serve the source-official localized title when ?lang= was asked for AND
  // we have it; English otherwise. `title_en` preserves the native label so it is
  // never lost. For lang=en this block is a no-op (byte-identical pre-i18n shape).
  if (lang !== "en") {
    const localized = localizedTitle(meta as { titles?: Record<string, string> }, out.title, lang);
    if (localized !== out.title) {
      out.title_en = out.title;
      out.title = localized;
    }
    out.lang = lang;
  }

  return json(out);
}
