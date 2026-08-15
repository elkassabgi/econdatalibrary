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
  BROWSE_SOURCE, BROWSE_SOURCE_COUNT, BROWSE_SOURCE_COUNT_CACHED, BROWSE_ALL, BROWSE_ALL_COUNT,
} from "./sql";
import { json, clampInt, offsetInt, reqLang, localizedTitle, dbFor } from "./util";
import { NON_REDISTRIBUTABLE, isSeriesCarvedOut } from "./denylist";

const COVERAGE = "series-level for 33 sources; source-level for the rest";

export async function handleCatalog(url: URL, env: Env): Promise<Response> {
  const { lang, error } = reqLang(url);
  if (error) return error; // unsupported ?lang= -> honest 400, never silent English
  const q = url.searchParams.get("q");
  const source = url.searchParams.get("source");
  const src = source && source.trim() ? source : null; // null-narrowed for binds
  const limit = clampInt(url.searchParams.get("limit"), 50, 1, 500);
  const offset = offsetInt(url.searchParams.get("offset"));

  // OFFSET cap (2026-08-15 cost incident): OFFSET N is O(N) rows read no matter
  // the plan, so an unbounded crawl of a multi-million-series source costs real
  // money per page. 100k covers every human browse; whole-source consumers get
  // the honest pointer to the bulk surface instead of a silent bill.
  const MAX_OFFSET = 100_000;
  if (offset > MAX_OFFSET) {
    return json({
      error: "offset_too_deep",
      detail: `offset is capped at ${MAX_OFFSET}; to enumerate a whole source use ` +
              "/v1/bundle?source= (all series ids) or the bulk parquet downloads",
    }, 400);
  }

  // Redistribution gate: a denylisted source is not browsable at all — same
  // rule as /v1/sources (hidden), /v1/series (451), and /v1/bundle (rejected).
  // The unscoped statements exclude these sources at the SQL layer (sql.ts);
  // a direct ?source= ask gets the honest refusal, not an empty result.
  if (src && NON_REDISTRIBUTABLE.has(src)) {
    return json({
      error: "non_redistributable",
      detail: `source '${src}' cannot be re-hosted under its licence; ` +
              "fetch it from the original publisher (see /v1/sources terms links)",
    }, 451);
  }

  let results: CatalogResultRow[] = [];
  let total = 0;

  // Sharding (util.ts, task #45): source-scoped queries route to the shard that
  // holds that source; GLOBAL queries hit BOTH databases and merge, or sharded
  // sources silently vanish from search/browse. Merge strategy: fetch the first
  // (offset+limit) window from each DB, concatenate primary-then-shard (browse
  // re-sorts by series_id, the unscoped SQL's order), slice the window, and SUM
  // the counts. Deep offsets cost proportionally on both DBs; limit is capped
  // at 500 and real offsets are shallow, so this stays bounded.
  const scopedDb = dbFor(env, src);
  const window = limit + offset;

  if (q && q.trim()) {
    // FTS5 primary path. D1 raises on a malformed MATCH expression, so we catch
    // and fall back to LIKE -- mirroring the Python try/except OperationalError.
    // When a source= is ALSO given, AND the source filter onto the search so the
    // two combine (q + source) -- matching the dev shim; without this the Worker
    // silently ignored source whenever q was present.
    let ftsOk = false;
    try {
      if (src) {
        const res = await scopedDb.prepare(SEARCH_FTS_SOURCE).bind(q, src, limit, offset).all<CatalogResultRow>();
        results = res.results ?? [];
        if (results.length > 0) {
          const c = await scopedDb.prepare(SEARCH_FTS_SOURCE_COUNT).bind(q, src).first<CountRow>();
          total = c?.n ?? results.length;
          ftsOk = true;
        }
      } else {
        const [p, sh] = await Promise.all([
          env.CATALOG.prepare(SEARCH_FTS).bind(q, window, 0).all<CatalogResultRow>(),
          env.CATALOG_CLIMATE.prepare(SEARCH_FTS).bind(q, window, 0).all<CatalogResultRow>(),
        ]);
        const merged = [...(p.results ?? []), ...(sh.results ?? [])];
        results = merged.slice(offset, offset + limit);
        if (results.length > 0) {
          const [cp, cs] = await Promise.all([
            env.CATALOG.prepare(SEARCH_FTS_COUNT).bind(q).first<CountRow>(),
            env.CATALOG_CLIMATE.prepare(SEARCH_FTS_COUNT).bind(q).first<CountRow>(),
          ]);
          total = (cp?.n ?? 0) + (cs?.n ?? 0);
          ftsOk = true;
        }
      }
    } catch {
      ftsOk = false; // malformed FTS query -> LIKE fallback below
    }
    if (!ftsOk) {
      const like = `%${q}%`;
      if (src) {
        const res = await scopedDb.prepare(SEARCH_LIKE_SOURCE).bind(like, like, src, limit, offset).all<CatalogResultRow>();
        results = res.results ?? [];
        const c = await scopedDb.prepare(SEARCH_LIKE_SOURCE_COUNT).bind(like, like, src).first<CountRow>();
        total = c?.n ?? results.length;
      } else {
        const [p, sh] = await Promise.all([
          env.CATALOG.prepare(SEARCH_LIKE).bind(like, like, window, 0).all<CatalogResultRow>(),
          env.CATALOG_CLIMATE.prepare(SEARCH_LIKE).bind(like, like, window, 0).all<CatalogResultRow>(),
        ]);
        results = [...(p.results ?? []), ...(sh.results ?? [])].slice(offset, offset + limit);
        const [cp, cs] = await Promise.all([
          env.CATALOG.prepare(SEARCH_LIKE_COUNT).bind(like, like).first<CountRow>(),
          env.CATALOG_CLIMATE.prepare(SEARCH_LIKE_COUNT).bind(like, like).first<CountRow>(),
        ]);
        total = (cp?.n ?? 0) + (cs?.n ?? 0);
      }
    }
  } else if (src) {
    // PK-range browse: [src+':', src+';') — ';' is ':'+1, closing the prefix
    // range. Reads offset+limit PK entries instead of sort-scanning the whole
    // source (the 87.3B-rows-read/day incident; rationale in sql.ts).
    const res = await scopedDb.prepare(BROWSE_SOURCE)
      .bind(src + ":", src + ";", limit, offset).all<CatalogResultRow>();
    results = res.results ?? [];
    const cached = await scopedDb.prepare(BROWSE_SOURCE_COUNT_CACHED).bind(src).first<CountRow>();
    if (cached && typeof cached.n === "number") {
      total = cached.n;
    } else {
      // Fallback: a source synced before its source_counts row exists. Live
      // COUNT once — the sync backfills the row and this path goes cold.
      const c = await scopedDb.prepare(BROWSE_SOURCE_COUNT).bind(src).first<CountRow>();
      total = c?.n ?? results.length;
    }
  } else {
    const [p, sh] = await Promise.all([
      env.CATALOG.prepare(BROWSE_ALL).bind(window, 0).all<CatalogResultRow>(),
      env.CATALOG_CLIMATE.prepare(BROWSE_ALL).bind(window, 0).all<CatalogResultRow>(),
    ]);
    // BROWSE_ALL orders by series_id; restore that order across the two windows.
    const merged = [...(p.results ?? []), ...(sh.results ?? [])]
      .sort((a, b) => (a.series_id < b.series_id ? -1 : a.series_id > b.series_id ? 1 : 0));
    results = merged.slice(offset, offset + limit);
    const [cp, cs] = await Promise.all([
      env.CATALOG.prepare(BROWSE_ALL_COUNT).first<CountRow>(),
      env.CATALOG_CLIMATE.prepare(BROWSE_ALL_COUNT).first<CountRow>(),
    ]);
    total = (cp?.n ?? 0) + (cs?.n ?? 0);
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

  // Redistribution gate: hide sources whose licence forbids re-hosting, and hide
  // individual third-party carve-out series within an otherwise-served source
  // (denylist.ts). Belt-and-suspenders over the SQL exclusion (covers the browse path).
  const visible = mapped.filter(
    (m) => !NON_REDISTRIBUTABLE.has(m.source as string) && !isSeriesCarvedOut(m.series_id as string),
  );

  // For lang=en this body is byte-identical to the pre-i18n contract (no `lang`).
  const body: Record<string, unknown> = {
    total,
    limit,
    offset,
    catalog_coverage: COVERAGE,
    results: visible,
  };
  if (lang !== "en") body.lang = lang;
  return json(body);
}
