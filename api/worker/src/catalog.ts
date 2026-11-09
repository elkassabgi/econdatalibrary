// ---------------------------------------------------------------------------
// src/catalog.ts  --  GET /v1/catalog  (search + browse), fully live from D1.
//
// Params: q= (FTS5 over title/geography), source=, limit= (default 50, max 500),
// offset=. FTS5 primary path. With source= it keeps the LIKE fallback of
// core/catalog.py::search and econdl._catalog.search (see src/sql.ts); without
// source= the fallback is index-bounded instead (an id-prefix PK seek, or the
// MATCH retried as quoted terms) - never a whole-table LIKE (2026-11-09).
// Response carries catalog_coverage so absence is never read as nonexistence.
// ---------------------------------------------------------------------------

import type { Env, CatalogResultRow, CountRow } from "./types.ts";
import {
  searchFtsSql, searchFtsCountSql, searchIdPrefixSql, searchIdPrefixCountSql, searchBareCodeSql,
  searchFtsCountCappedSql, searchFtsSourceSql, searchFtsSourceCountSql, searchLikeSourceSql, searchLikeSourceCountSql,
  browseSourceSql, browseSourceVisibleCountSql, hasCarveouts, withheldPresentCountSql,
  BROWSE_SOURCE_COUNT, BROWSE_SOURCE_COUNT_CACHED, browseAllSql, BROWSE_ALL_COUNT,
} from "./sql.ts";
import { type Withheld, WITHHELD_ID_RE, forSource, isSourceClosed, isWithheld } from "./withheld.ts";
import {
  json, clampInt, offsetInt, reqLang, localizedTitle, dbFor, supportedSources, SHARDED_SOURCES,
} from "./util.ts";
import { NON_REDISTRIBUTABLE, isSeriesCarvedOut, promotesToSourceBrowse } from "./denylist.ts";

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
// FREQUENCY on every row, which a table row does not. The converse fails too: wid has
// 2,465,197 rows with no frequency and each still names one series.
//
// CORRECTED 2026-09-06: this test used to say "frequency AND geography", and the
// geography half is FALSE - refuted by usda, which this very comment cites as table grain
// three lines up. Measured over data/catalog.db:
//
//     usda   69,704 rows   frequency   0.0%   geography 100.0%   (site page: TABLE grain)
//     abs        18 rows   frequency 100.0%   geography 100.0%   (per-series)
//     bls         9        frequency 100.0%   geography 100.0%   (per-series)
//     bis        49        frequency 100.0%   geography 100.0%   (per-series)
//     bfs       582        frequency   0.0%   geography   0.0%   (_FLOW_GRAIN)
//     cbs_nl  5,255        frequency   0.0%   geography   0.0%   (file grain)
//     istat  14,267        frequency   0.0%   geography   0.0%   (FLOW grain)
//
// A TABLE ROW CAN CARRY A GEOGRAPHY. Only the frequency column separated cleanly across
// every source measured, so a guard keyed on "frequency and geography" would have passed
// usda as per-series - the same class of error this paragraph exists to prevent, one layer
// down. Key any such guard on FREQUENCY ALONE, and treat it as a positive test only:
// frequency present on every row is evidence of per-series; its absence is not proof of
// coarse grain, only an absence of evidence.
//
// Also stale above: _DOT_TABLE_GRAIN is 18, not 13 (read from _resolve.py, 2026-09-06).
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

/** Per list load (AR-341 finding 5): the present counts are memoised on the loaded list object, which the
 *  per-isolate cache of src/withheld.ts keeps for its 300 s (30 s after a failed read). So the PK count runs at
 *  most once per database and id set per isolate per list reload - not once per uncached request. A failed
 *  count is not memoised. */
const presentMemo = new WeakMap<Withheld, Map<string, Promise<number>>>();

/** How many of `ids` the database holds (0 for none): what a cached or whole-table count must lose. `dbKey`
 *  names the database (`ids` alone does not: the same ids are counted in both). */
export function presentCount(db: D1Database, ids: readonly string[], w: Withheld, dbKey: string): Promise<number> {
  if (ids.length === 0) return Promise.resolve(0);
  let m = presentMemo.get(w);
  if (!m) presentMemo.set(w, (m = new Map()));
  const k = `${dbKey}|${ids.join(",")}`;
  let p = m.get(k);
  if (!p) {
    const memo = m;
    p = db.prepare(withheldPresentCountSql(ids)).first<CountRow>().then((c) => c?.n ?? 0);
    p.catch(() => memo.delete(k));
    m.set(k, p);
  }
  return p;
}

/** A malformed MATCH expression rewritten as plain FTS5 terms: every run of letters/digits, double-quoted,
 *  space-joined (implicit AND). A quoted string holding only word characters always parses, and the tokenizer
 *  splits it exactly as it split the indexed titles. null when q has no word characters (nothing to search). */
export function ftsTermsQuery(q: string): string | null {
  const toks = q.match(/[\p{L}\p{N}]+/gu);
  return toks && toks.length ? toks.map((t) => `"${t}"`).join(" ") : null;
}

/** The shortest last word the prefix retry will expand: `"e"*` or `"in"*` would match a large share of every
 *  title in the catalogue. */
export const PREFIX_MIN_CHARS = 3;
/** The prefix retry's count ceiling per database; reaching it marks `total` as a lower bound. */
export const PREFIX_COUNT_CAP = 1_000;

/** The prefix retry: q's terms quoted as in ftsTermsQuery, the LAST one as a prefix (`"chôm"*`). null when the
 *  last word is shorter than PREFIX_MIN_CHARS characters, or there are no words. */
export function ftsPrefixQuery(q: string): string | null {
  const toks = q.match(/[\p{L}\p{N}]+/gu);
  if (!toks || !toks.length) return null;
  const last = toks[toks.length - 1];
  if ([...last].length < PREFIX_MIN_CHARS) return null;
  return [...toks.slice(0, -1).map((t) => `"${t}"`), `"${last}"*`].join(" ");
}

/** An id-shaped q -> the PK range that holds every series id starting with it, inside its source's database.
 *  Shape: `<source>:<native prefix>` with no whitespace (printable ASCII), the source part case-folded (source
 *  ids are lower case) and the native part kept as typed (the PK compares bytes). Only a SUPPORTED or a gated
 *  source qualifies - anything else is free text for FTS. hi is lo with its last character incremented, so
 *  [lo, hi) is exactly the ids that begin with lo under the PK's BINARY collation. */
export interface IdPrefix { source: string; lo: string; hi: string }
export function idPrefixQuery(q: string, supported: ReadonlySet<string>): IdPrefix | null {
  const m = /^([A-Za-z0-9_]+):([\x21-\x7e]+)$/.exec(q.trim());
  if (!m) return null;
  const source = m[1].toLowerCase();
  if (!supported.has(source) && !NON_REDISTRIBUTABLE.has(source)) return null;
  const lo = `${source}:${m[2]}`;
  const hi = lo.slice(0, -1) + String.fromCharCode(lo.charCodeAt(lo.length - 1) + 1);
  return { source, lo, hi };
}

/** A bare native code -> the full ids it could be, split by the database that holds each source: for every
 *  served source (not gated, not closed) `<source>:<code>` as typed and, when different, upper-cased (LIKE was
 *  ASCII-case-insensitive; PK equality is not, and native codes are overwhelmingly upper case). The code must
 *  look like an id's native part (WITHHELD_ID_RE's alphabet, 2-200 characters, at least one letter or digit);
 *  anything else - a sentence, a word with an accent - returns no candidates. */
export function bareCodeIds(q: string, supported: ReadonlySet<string>, withheld: Withheld):
    { primary: string[]; shard: string[] } {
  const code = q.trim();
  const out = { primary: [] as string[], shard: [] as string[] };
  if (!/^[A-Za-z0-9_.:-]{2,200}$/.test(code) || !/[A-Za-z0-9]/.test(code)) return out;
  const forms = code.toUpperCase() === code ? [code] : [code, code.toUpperCase()];
  for (const s of [...supported].sort()) {
    if (NON_REDISTRIBUTABLE.has(s) || isSourceClosed(withheld, s)) continue;
    const ids = forms.map((f) => `${s}:${f}`).filter((id) => WITHHELD_ID_RE.test(id));
    (SHARDED_SOURCES.has(s) ? out.shard : out.primary).push(...ids);
  }
  return out;
}

/** The id-prefix count's ceiling: the most PK entries one request walks to count a prefix. */
export const ID_PREFIX_COUNT_CAP = 10_000;

/** The id-prefix search: one source, one database, two PK-range statements (page + capped count). `total` is
 *  the capped count, never below what this page returned, so a page and its total always agree; `lowerBound`
 *  says the cap was reached. */
async function idPrefixSearch(env: Env, idq: IdPrefix, withheld: Withheld, limit: number, offset: number):
    Promise<{ results: CatalogResultRow[]; total: number; lowerBound: boolean }> {
  const source = idq.source;
  const db = dbFor(env, source);
  const res = await db.prepare(searchIdPrefixSql(source, forSource(withheld, source)))
    .bind(idq.lo, idq.hi, limit, offset).all<CatalogResultRow>();
  const results = res.results ?? [];
  const c = await db.prepare(searchIdPrefixCountSql(source, forSource(withheld, source)))
    .bind(idq.lo, idq.hi, ID_PREFIX_COUNT_CAP + 1).first<CountRow>();
  const n = c?.n ?? 0;
  const total = Math.max(Math.min(n, ID_PREFIX_COUNT_CAP), results.length ? offset + results.length : 0);
  return { results, total, lowerBound: n > ID_PREFIX_COUNT_CAP };
}

export async function handleCatalog(url: URL, env: Env, withheld: Withheld): Promise<Response> {
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
    if (promotesToSourceBrowse(cand, supportedSources(env))) {
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
  // True only when a CAPPED count reached its cap (the global prefix retry, the id-prefix seek): `total` is then
  // "at least". Emitted as `total_is_lower_bound: true` in exactly that case, so every other body is unchanged.
  let lowerBound = false;

  // Sharding (util.ts, task #45): source-scoped queries route to the shard that
  // holds that source; GLOBAL queries hit BOTH databases and merge, or sharded
  // sources silently vanish from search/browse. Merge strategy: fetch the first
  // (offset+limit) window from each DB, concatenate primary-then-shard (browse
  // re-sorts by series_id, the unscoped SQL's order), slice the window, and SUM
  // the counts. Deep offsets cost proportionally on both DBs; limit is capped
  // at 500 and real offsets are shallow, so this stays bounded.
  const scopedDb = dbFor(env, src);
  const window = limit + offset;
  // WITHHELD series (src/withheld.ts): the source's own ids for a scoped statement, every id for a global one
  const whAll = [...withheld.ids].sort();
  const whSrc = src ? forSource(withheld, src) : [];

  if (src && isSourceClosed(withheld, src)) {
    // The whole source is withheld (src/withheld.ts: the list is unreadable and this isolate has no good copy, so
    // the floor applies): a scoped browse or search lists nothing and counts nothing; no statement runs.
  } else if (q && q.trim() && src) {
    // SOURCE-SCOPED search: FTS5 first, then a LIKE fallback constrained to the source. D1 raises on a
    // malformed MATCH expression, so we catch and fall back -- mirroring the Python try/except
    // OperationalError. The q + source pair combine (matching the dev shim); without this the Worker silently
    // ignored source whenever q was present. This fallback stays: `source_id = ?` rides idx_series_source, so
    // it reads one source's rows (measured small), never the whole table -- and it carries the infix match
    // FTS5 cannot (`?source=bls&q=onfarm`, tools/cost/smoke_public.py).
    let ftsOk = false;
    try {
      // COUNT FIRST, for the same reason as the global branch below: this SQL carries
      // LIMIT/OFFSET, so an offset past the last page returns zero rows LEGITIMATELY and the
      // old `results.length > 0` test read that as "FTS failed" and re-ran everything through
      // the LIKE fallback - a different engine, a different `total`, for a query that worked.
      // The COUNT is index-backed and was already being run whenever results existed; the
      // only new cost is running it on an empty page, which is far cheaper than the scan it
      // replaces.
      const c = await scopedDb.prepare(searchFtsSourceCountSql(src, whSrc)).bind(q, src).first<CountRow>();
      const n = c?.n ?? 0;
      const res = await scopedDb.prepare(searchFtsSourceSql(src, whSrc)).bind(q, src, limit, offset).all<CatalogResultRow>();
      results = res.results ?? [];
      if (n > 0) {
        total = n;
        ftsOk = true;
      }
    } catch {
      ftsOk = false; // malformed FTS query -> LIKE fallback below
    }
    if (!ftsOk) {
      const like = `%${q}%`;
      const res = await scopedDb.prepare(searchLikeSourceSql(src, whSrc)).bind(like, like, src, limit, offset).all<CatalogResultRow>();
      results = res.results ?? [];
      const c = await scopedDb.prepare(searchLikeSourceCountSql(src, whSrc)).bind(like, like, src).first<CountRow>();
      total = c?.n ?? results.length;
    }
  } else if (q && q.trim()) {
    // GLOBAL search (no source=). EVERY STATEMENT ON THIS PATH IS INDEX-BOUNDED (2026-11-09).
    //
    // Until now a global q that FTS did not match - zero rows, or a MATCH that threw - fell to a
    // `title LIKE '%q%' OR series_id LIKE '%q%'` over the whole `series` table of BOTH databases, plus a COUNT
    // of each: up to four full table scans per request. A 200 is edge-cached per URL, but every DISTINCT no-match
    // q paid the set, and a client that gives up before the scan ends (the MCP server aborts at 25 s and
    // retries once) never gets a cacheable answer at all and pays again on each repeat. `wrangler d1 insights`
    // put that one statement at most of the catalogue's rows read.
    //
    // This SUPERSEDES a deliberate decision (ledger R675, 2026-09-03: the zero-result fallback was left in
    // place for the malformed-MATCH case, for infix matching, and for parity with the local client). What
    // replaces each reason, in the order tried:
    //   1. an id or id prefix (`bls:CUUR0000SA0`, which FTS5 parses as a column filter and throws on) is a
    //      PK-range seek inside that source's database (idPrefixQuery; no FTS at all);
    //   2. FTS5 on q as given; a malformed expression (`GDP (nominal`, `covid-19`, `U.S.`) is retried ONCE as
    //      quoted terms (ftsTermsQuery) - still the index;
    //   3. nothing matched: a bare native code (`CUUR0000SA0`, `NY.GDP.MKTP.CD`) is looked up as an exact id in
    //      every served source - PK equality seeks, about one row each (bareCodeIds);
    //   4. still nothing: the last word as an FTS prefix (`"emplo"*`, at least 3 characters), with a CAPPED
    //      count - a partial word typed into a search box finds its completions;
    //   then total 0. Infix matching inside a source stays on the scoped path above, where it is cheap.
    //   LOST, by decision: a global infix q (`onfarm` without source=) and a case-mangled bare code that
    //   matches no FTS title. The local client keeps its LIKE fallback; the served API does not mirror it for
    //   the unscoped case, because there it cannot be bounded.
    const supported = supportedSources(env);
    const idq = idPrefixQuery(q, supported);
    if (idq) {
      if (!NON_REDISTRIBUTABLE.has(idq.source) && !isSourceClosed(withheld, idq.source)) {
        ({ results, total, lowerBound } = await idPrefixSearch(env, idq, withheld, limit, offset));
      } // a gated or closed source's id: nothing, and no statement (the SQL would exclude it anyway)
    } else {
      const runFts = async (expr: string, capped: boolean) => {
        const [p, sh] = await Promise.all([
          env.CATALOG.prepare(searchFtsSql(whAll)).bind(expr, window, 0).all<CatalogResultRow>(),
          env.CATALOG_CLIMATE.prepare(searchFtsSql(whAll)).bind(expr, window, 0).all<CatalogResultRow>(),
        ]);
        const merged = [...(p.results ?? []), ...(sh.results ?? [])];
        results = merged.slice(offset, offset + limit);
        // JUDGE FTS ON WHAT THE SQL RETURNED, NOT ON WHAT SURVIVED THE SLICE (R675): `merged` is the FTS
        // result set; a zero-length PAGE of a successful search is legitimate and still needs its total.
        if (merged.length > 0) {
          if (capped) {
            const [cp, cs] = await Promise.all([
              env.CATALOG.prepare(searchFtsCountCappedSql(whAll)).bind(expr, PREFIX_COUNT_CAP + 1).first<CountRow>(),
              env.CATALOG_CLIMATE.prepare(searchFtsCountCappedSql(whAll)).bind(expr, PREFIX_COUNT_CAP + 1).first<CountRow>(),
            ]);
            const a = cp?.n ?? 0, b = cs?.n ?? 0;
            lowerBound = a > PREFIX_COUNT_CAP || b > PREFIX_COUNT_CAP;
            total = Math.max(Math.min(a, PREFIX_COUNT_CAP) + Math.min(b, PREFIX_COUNT_CAP), offset + results.length);
          } else {
            const [cp, cs] = await Promise.all([
              env.CATALOG.prepare(searchFtsCountSql(whAll)).bind(expr).first<CountRow>(),
              env.CATALOG_CLIMATE.prepare(searchFtsCountSql(whAll)).bind(expr).first<CountRow>(),
            ]);
            total = (cp?.n ?? 0) + (cs?.n ?? 0);
          }
        } else {
          total = 0;
        }
      };
      try {
        await runFts(q, false);
      } catch {
        // A malformed MATCH expression. Retry ONCE as plain quoted terms, which FTS5 always parses; if q has
        // no word characters at all there is nothing to search. A second failure is not a syntax problem
        // (the retry's expression cannot be malformed), so it propagates as a 500 - which is not edge-cached
        // - instead of becoming a cached empty 200.
        results = [];
        total = 0;
        const terms = ftsTermsQuery(q);
        if (terms) await runFts(terms, false);
      }
      if (total === 0) {
        const byDb = bareCodeIds(q, supported, withheld);
        if (byDb.primary.length || byDb.shard.length) {
          const run = (db: D1Database, ids: string[]) => ids.length
            ? db.prepare(searchBareCodeSql(whAll)).bind(JSON.stringify(ids)).all<CatalogResultRow>()
            : Promise.resolve({ results: [] as CatalogResultRow[] });
          const [p, sh] = await Promise.all([run(env.CATALOG, byDb.primary), run(env.CATALOG_CLIMATE, byDb.shard)]);
          const merged = [...(p.results ?? []), ...(sh.results ?? [])];
          results = merged.slice(offset, offset + limit);
          total = merged.length; // every match was read: the candidate list bounds it
        }
      }
      if (total === 0) {
        const prefix = ftsPrefixQuery(q);
        if (prefix) await runFts(prefix, true);
      }
    }
  } else if (src) {
    // PK-range browse: [src+':', src+';') — ';' is ':'+1, closing the prefix
    // range. Reads offset+limit PK entries instead of sort-scanning the whole
    // source (the 87.3B-rows-read/day incident; rationale in sql.ts).
    const res = await scopedDb.prepare(browseSourceSql(src, whSrc))
      .bind(src + ":", src + ";", limit, offset).all<CatalogResultRow>();
    results = res.results ?? [];
    if (hasCarveouts(src)) {
      // source_counts counts carved rows, so it advertised 692 for worldbank where 262 are
      // reachable. Only the 3 carve-out sources take this bounded PK-range count; every other
      // source keeps the free cached read (this path is why that cache exists).
      const c = await scopedDb.prepare(browseSourceVisibleCountSql(src, whSrc))
        .bind(src + ":", src + ";").first<CountRow>();
      total = c?.n ?? results.length;
    } else {
      const cached = await scopedDb.prepare(BROWSE_SOURCE_COUNT_CACHED).bind(src).first<CountRow>();
      if (cached && typeof cached.n === "number" && !(cached.n === 0 && results.length > 0)) {
        // source_counts counts withheld rows too: lose the ones the catalogue holds (a PK count, ~1 row each)
        total = Math.max(0, cached.n - await presentCount(scopedDb, whSrc, withheld, `src:${src}`));
      } else if (cached && cached.n === 0 && results.length > 0) {
        // THE CACHE IS PROVABLY WRONG, so do not serve it. `total: 0` beside a non-empty page is
        // self-contradictory: this very request just read rows for the source. statcan entered
        // this state on 2026-09-04 when an aborted push (R709) left 63,000 rows in `series` with
        // source_counts still 0 — and since source_counts is written only at the END of a sync,
        // ANY interrupted push lands here. (ECONLIB_COMPLETION_PLAN.md:77 records `ilo` at
        // `total:1157` over `results:[]` — that is the INVERSE shape, a non-zero cache over an
        // empty page, and this branch deliberately does not touch it: a non-zero cached value
        // still goes out verbatim above.)
        //
        // ONLY THE FIRST PAGE RECOUNTS, and that bound is the point. The recount rides the
        // autoindex, but sql.ts:225-228 is explicit that its cheapness is for carve-out sources
        // of 692 and 1,486 rows and that "on a giant ... a full-range count per page view is
        // exactly what source_counts exists to avoid" — and the sources that reach this branch
        // are interrupted big pushes, i.e. giants. Unbounded, a crawler walking statcan's ~4,664
        // offsets would read 466,341 index entries per distinct URL. Restricted to offset 0 it
        // is one URL per source per 6 h edge cache (index.ts:74-86).
        //
        // Deeper pages take a floor instead, which costs nothing and still cannot contradict
        // itself: total is never below the rows this page actually returned.
        if (offset === 0) {
          const c = await scopedDb.prepare(browseSourceVisibleCountSql(src, whSrc))
            .bind(src + ":", src + ";").first<CountRow>();
          total = c?.n ?? results.length;
        } else {
          total = offset + results.length;
        }
      } else {
        // Fallback: a source synced before its source_counts row exists. Live
        // COUNT once — the sync backfills the row and this path goes cold.
        const c = await scopedDb.prepare(BROWSE_SOURCE_COUNT).bind(src).first<CountRow>();
        total = c ? Math.max(0, c.n - await presentCount(scopedDb, whSrc, withheld, `src:${src}`)) : results.length;
      }
    }
  } else {
    const [p, sh] = await Promise.all([
      env.CATALOG.prepare(browseAllSql(whAll)).bind(window, 0).all<CatalogResultRow>(),
      env.CATALOG_CLIMATE.prepare(browseAllSql(whAll)).bind(window, 0).all<CatalogResultRow>(),
    ]);
    // BROWSE_ALL orders by series_id; restore that order across the two windows.
    const merged = [...(p.results ?? []), ...(sh.results ?? [])]
      .sort((a, b) => (a.series_id < b.series_id ? -1 : a.series_id > b.series_id ? 1 : 0));
    results = merged.slice(offset, offset + limit);
    const [cp, cs] = await Promise.all([
      env.CATALOG.prepare(BROWSE_ALL_COUNT).first<CountRow>(),
      env.CATALOG_CLIMATE.prepare(BROWSE_ALL_COUNT).first<CountRow>(),
    ]);
    // the whole-table COUNT keeps its measured form; the withheld rows each database holds are subtracted
    const [wp, ws] = await Promise.all([presentCount(env.CATALOG, whAll, withheld, "CATALOG"),
                                        presentCount(env.CATALOG_CLIMATE, whAll, withheld, "CATALOG_CLIMATE")]);
    total = Math.max(0, (cp?.n ?? 0) - wp) + Math.max(0, (cs?.n ?? 0) - ws);
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
    (m) => !NON_REDISTRIBUTABLE.has(m.source as string) && !isSeriesCarvedOut(m.series_id as string)
      && !isWithheld(withheld, m.series_id as string),
  );

  // For lang=en this body is byte-identical to the pre-i18n contract (no `lang`).
  const body: Record<string, unknown> = {
    total,
    limit,
    offset,
    catalog_coverage: COVERAGE,
    results: visible,
  };
  if (lowerBound) body.total_is_lower_bound = true;
  if (lang !== "en") body.lang = lang;
  return json(body);
}
