// ---------------------------------------------------------------------------
// src/catalog.ts  --  GET /v1/catalog  (search + browse), fully live from D1.
//
// Params: q= (FTS5 over title/geography), source=, limit= (default 50, max 500),
// offset=. FTS5 primary path with a LIKE fallback -- the EXACT behaviour of
// core/catalog.py::search and econdl._catalog.search (see src/sql.ts).
// Response carries catalog_coverage so absence is never read as nonexistence.
// ---------------------------------------------------------------------------

import type { Env, CatalogResultRow, CountRow } from "./types";
import {
  SEARCH_FTS, SEARCH_FTS_COUNT, SEARCH_LIKE, SEARCH_LIKE_COUNT,
  SEARCH_FTS_SOURCE, SEARCH_FTS_SOURCE_COUNT, SEARCH_LIKE_SOURCE, SEARCH_LIKE_SOURCE_COUNT,
  BROWSE_SOURCE, BROWSE_SOURCE_COUNT, BROWSE_ALL, BROWSE_ALL_COUNT,
} from "./sql";
import { json, clampInt, offsetInt, reqLang, localizedTitle } from "./util";

const COVERAGE = "series-level for 33 sources; source-level for the rest";

export async function handleCatalog(url: URL, env: Env): Promise<Response> {
  const { lang, error } = reqLang(url);
  if (error) return error; // unsupported ?lang= -> honest 400, never silent English
  const q = url.searchParams.get("q");
  const source = url.searchParams.get("source");
  const src = source && source.trim() ? source : null; // null-narrowed for binds
  const limit = clampInt(url.searchParams.get("limit"), 50, 1, 500);
  const offset = offsetInt(url.searchParams.get("offset"));

  let results: CatalogResultRow[] = [];
  let total = 0;

  if (q && q.trim()) {
    // FTS5 primary path. D1 raises on a malformed MATCH expression, so we catch
    // and fall back to LIKE -- mirroring the Python try/except OperationalError.
    // When a source= is ALSO given, AND the source filter onto the search so the
    // two combine (q + source) -- matching the dev shim; without this the Worker
    // silently ignored source whenever q was present.
    let ftsOk = false;
    try {
      const res = src
        ? await env.CATALOG.prepare(SEARCH_FTS_SOURCE).bind(q, src, limit, offset).all<CatalogResultRow>()
        : await env.CATALOG.prepare(SEARCH_FTS).bind(q, limit, offset).all<CatalogResultRow>();
      results = res.results ?? [];
      if (results.length > 0) {
        const c = src
          ? await env.CATALOG.prepare(SEARCH_FTS_SOURCE_COUNT).bind(q, src).first<CountRow>()
          : await env.CATALOG.prepare(SEARCH_FTS_COUNT).bind(q).first<CountRow>();
        total = c?.n ?? results.length;
        ftsOk = true;
      }
    } catch {
      ftsOk = false; // malformed FTS query -> LIKE fallback below
    }
    if (!ftsOk) {
      const like = `%${q}%`;
      const res = src
        ? await env.CATALOG.prepare(SEARCH_LIKE_SOURCE).bind(like, like, src, limit, offset).all<CatalogResultRow>()
        : await env.CATALOG.prepare(SEARCH_LIKE).bind(like, like, limit, offset).all<CatalogResultRow>();
      results = res.results ?? [];
      const c = src
        ? await env.CATALOG.prepare(SEARCH_LIKE_SOURCE_COUNT).bind(like, like, src).first<CountRow>()
        : await env.CATALOG.prepare(SEARCH_LIKE_COUNT).bind(like, like).first<CountRow>();
      total = c?.n ?? results.length;
    }
  } else if (src) {
    const res = await env.CATALOG.prepare(BROWSE_SOURCE)
      .bind(src, limit, offset).all<CatalogResultRow>();
    results = res.results ?? [];
    const c = await env.CATALOG.prepare(BROWSE_SOURCE_COUNT).bind(src).first<CountRow>();
    total = c?.n ?? results.length;
  } else {
    const res = await env.CATALOG.prepare(BROWSE_ALL).bind(limit, offset).all<CatalogResultRow>();
    results = res.results ?? [];
    const c = await env.CATALOG.prepare(BROWSE_ALL_COUNT).first<CountRow>();
    total = c?.n ?? results.length;
  }

  const mapped = results.map((r) => {
    // Localize the title from metadata.titles[<lang>] when ?lang= was asked for;
    // English (r.title) otherwise. metadata is selected but NEVER emitted.
    let title: unknown = r.title;
    if (lang !== "en") {
      let parsed: { titles?: Record<string, string> } | null = null;
      if (r.metadata) {
        try {
          parsed = JSON.parse(r.metadata) as { titles?: Record<string, string> };
        } catch {
          parsed = null;
        }
      }
      title = localizedTitle(parsed, r.title, lang);
    }
    return {
      series_id: r.series_id,
      source: r.source_id,
      title,
      frequency: r.frequency,
      unit: r.unit,
      geography: r.geography,
      license_id: r.license_id,
      start_date: r.start_date,
      end_date: r.end_date,
    };
  });

  // For lang=en this body is byte-identical to the pre-i18n contract (no `lang`).
  const body: Record<string, unknown> = {
    total,
    limit,
    offset,
    catalog_coverage: COVERAGE,
    results: mapped,
  };
  if (lang !== "en") body.lang = lang;
  return json(body);
}
