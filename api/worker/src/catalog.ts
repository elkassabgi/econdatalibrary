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
  searchFtsSourceSql, searchFtsSourceCountSql, searchLikeSourceSql, searchLikeSourceCountSql,
  browseSourceSql, browseSourceVisibleCountSql, hasCarveouts,
  BROWSE_SOURCE_COUNT, BROWSE_SOURCE_COUNT_CACHED, BROWSE_ALL, BROWSE_ALL_COUNT,
} from "./sql";
import { json, clampInt, offsetInt, reqLang, localizedTitle, dbFor, supportedSources } from "./util";
import { NON_REDISTRIBUTABLE, isSeriesCarvedOut } from "./denylist";

// Carries no COUNT, and — the part that matters — it KEEPS THE CAVEAT. The old value,
// "series-level for 33 sources; source-level for the rest", had rotted (33 was accurate when
// written, months ago). My first repair replaced it with "series-level for every served
// source", which is FALSE and was caught in adversarial review before it shipped: measured
// 2026-08-30, some served sources are catalogued at TABLE or FLOW grain — ons_uk holds 42
// catalogue rows for 3,897,884 series, istat 14,267 flows for 43,564,079, insee_melodi 139.
// _resolve.py registers the sets (_FLOW_GRAIN, 11; _DOT_TABLE_GRAIN, 13) and each source's
// generated page states its own grain (catalog/site/istat.html: "Served at FLOW grain";
// usda.html: "Served at TABLE grain") — that page is the authority.
//
// Do NOT infer grain from the catalogue row count. An earlier version of this comment cited
// statcan (20), oecd (28), abs (18) and bls (9) as table-grain examples; all four are small
// hand-curated PER-SERIES catalogues (bls:CUUR0000SA0 is one series) carrying a scalar
// frequency and geography on every row, which a table row cannot. The converse fails too:
// wid has 2,465,197 rows with neither attribute and each still names one series.
//
// Deleting the "source-level for the rest" half would have removed exactly the warning line 7
// says this field exists to give: a caller who searches for an ISTAT indicator, gets nothing,
// and reads "series-level for every served source" concludes the series does not exist. It
// does — inside one of 14,267 flow CSVs. A stale number is a rot problem; that would have been
// a correctness problem, and worse than what it replaced.
//
// So: no number (nothing to keep it true), and no claim of uniform grain (it is not uniform).
const COVERAGE =
  "mixed grain: some sources are catalogued per series, others per table or flow — " +
  "absence from this catalogue does not mean a series is unavailable";

export async function handleCatalog(url: URL, env: Env): Promise<Response> {
  const { lang, error } = reqLang(url);
  if (error) return error; // unsupported ?lang= -> honest 400, never silent English
  const qRaw = url.searchParams.get("q");
  const source = url.searchParams.get("source");
  let src = source && source.trim() ? source : null; // null-narrowed for binds

  // A query that IS a source id resolves to that source (2026-08-25).
  //
  // `ftsOk` USED TO BE set from `results.length > 0` (tightened 2026-09-03 to judge the SQL:
  // `merged.length` globally, the COUNT scoped). Either way it means "FTS returned
  // something", NOT "FTS answered this query". Unscoped, `MATCH 'wid'` also
  // matched 10 unrelated unctad_rfia rows, so a non-empty-from-anywhere result
  // suppressed the LIKE fallback for everyone else. It was masked only because
  // wid's index still held 7,395,591 code-as-title rows -- `q=wid` returned
  // total=7,395,601 and looked fine. Deduplicating that index would have turned
  // the most obvious query for the source into 10 rows of a different source.
  //
  // Matching the id up front is cheaper and more correct than either path: it
  // routes to the PK-range browse instead of a leading-wildcard LIKE scan over
  // millions of rows, and it fixes every source at once rather than whichever
  // ones happen to have code-titled rows left in the index.
  let q = qRaw;
  if (q && q.trim() && !src) {
    const cand = q.trim().toLowerCase();
    if (supportedSources(env).has(cand)) {
      src = cand;
      q = null; // browse the source; do not also MATCH its own name
    }
  }
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
        // COUNT FIRST, for the same reason as the global branch below: this SQL carries
        // LIMIT/OFFSET, so an offset past the last page returns zero rows LEGITIMATELY and the
        // old `results.length > 0` test read that as "FTS failed" and re-ran everything through
        // the LIKE fallback - a different engine, a different `total`, for a query that worked.
        // The COUNT is index-backed and was already being run whenever results existed; the
        // only new cost is running it on an empty page, which is far cheaper than the scan it
        // replaces.
        const c = await scopedDb.prepare(searchFtsSourceCountSql(src)).bind(q, src).first<CountRow>();
        const n = c?.n ?? 0;
        const res = await scopedDb.prepare(searchFtsSourceSql(src)).bind(q, src, limit, offset).all<CatalogResultRow>();
        results = res.results ?? [];
        if (n > 0) {
          total = n;
          ftsOk = true;
        }
      } else {
        const [p, sh] = await Promise.all([
          env.CATALOG.prepare(SEARCH_FTS).bind(q, window, 0).all<CatalogResultRow>(),
          env.CATALOG_CLIMATE.prepare(SEARCH_FTS).bind(q, window, 0).all<CatalogResultRow>(),
        ]);
        const merged = [...(p.results ?? []), ...(sh.results ?? [])];
        results = merged.slice(offset, offset + limit);
        // JUDGE FTS ON WHAT THE SQL RETURNED, NOT ON WHAT SURVIVED THE SLICE.
        //
        // This read `results.length > 0` - the POST-SLICE page. So paging past the last page
        // of a SUCCESSFUL search made `ftsOk` false and re-ran the whole query through the
        // LIKE fallback, which is a full scan of both databases (~24.2M rows, 40-65 s) and
        // reports a DIFFERENT total, because LIKE and MATCH match different things.
        // Reproduced live: /v1/catalog?source=bls&q=employment returns total=2, and the same
        // query at &offset=99 returns total=4. Two totals, two engines, one query.
        //
        // A crawler paging a search that WORKED triggers it on every query, which is not the
        // "user typed something the index does not contain" case anyone was guarding against.
        // `merged` is the FTS result set; if it is non-empty, FTS worked.
        if (merged.length > 0) {
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
        const res = await scopedDb.prepare(searchLikeSourceSql(src)).bind(like, like, src, limit, offset).all<CatalogResultRow>();
        results = res.results ?? [];
        const c = await scopedDb.prepare(searchLikeSourceCountSql(src)).bind(like, like, src).first<CountRow>();
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
    const res = await scopedDb.prepare(browseSourceSql(src))
      .bind(src + ":", src + ";", limit, offset).all<CatalogResultRow>();
    results = res.results ?? [];
    if (hasCarveouts(src)) {
      // source_counts counts carved rows, so it advertised 692 for worldbank where 262 are
      // reachable. Only the 3 carve-out sources take this bounded PK-range count; every other
      // source keeps the free cached read (this path is why that cache exists).
      const c = await scopedDb.prepare(browseSourceVisibleCountSql(src))
        .bind(src + ":", src + ";").first<CountRow>();
      total = c?.n ?? results.length;
    } else {
      const cached = await scopedDb.prepare(BROWSE_SOURCE_COUNT_CACHED).bind(src).first<CountRow>();
      if (cached && typeof cached.n === "number") {
        total = cached.n;
      } else {
        // Fallback: a source synced before its source_counts row exists. Live
        // COUNT once — the sync backfills the row and this path goes cold.
        const c = await scopedDb.prepare(BROWSE_SOURCE_COUNT).bind(src).first<CountRow>();
        total = c?.n ?? results.length;
      }
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
