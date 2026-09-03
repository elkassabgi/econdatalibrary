// ---------------------------------------------------------------------------
// src/series.ts  --  GET /v1/series/{id}.csv   (the R2 path)
//
// HONEST DESIGN NOTE -- READ THIS, it is load-bearing.
// ===================================================================
// This handler DOES NOT parse parquet. Decoding Apache Parquet inside a Worker
// (parquet-wasm + a predicate pushdown engine) is heavy on CPU/memory and would
// make the per-request hot path fragile; pretending to do it would violate the
// contract's honesty rule. Instead we use the SIMPLEST CORRECT design (one of
// the two the task offers):
//
//   (a) PRE-DERIVED PER-SERIES CSV OBJECTS in R2.
//       The data pipeline materialises, for every resolvable series, exactly the
//       bytes CONTRACT.md specifies for /v1/series/{id}.csv -- a long CSV with the
//       header `series_id,obs_date,value`, sorted by obs_date -- and stores it at
//       the R2 object key:
//           series/<encodeURIComponent(series_id)>.csv
//       The Worker GETs that object and streams it. This is byte-identical to the
//       dev shim's output BECAUSE the pipeline derives it with the same econdl
//       resolver (read_native -> native_to_tidy -> project [series_id,obs_date,
//       value]); the dev shim and Worker therefore cannot disagree.
//
// Why (a) and not (b) "Worker parses per-source parquet with range+predicate":
//   * Correctness: the resolver logic (33 bespoke per-source predicates, dedup,
//     filename-identity stamping -- see econdl/_resolve.py) is intricate and lives
//     in Python. Re-implementing it in TS-over-parquet-wasm would be a second
//     source of truth that WILL drift. Deriving the CSV once, server-side, with
//     the real resolver keeps ONE source of truth.
//   * Cost/limits: serving a flat object is O(1) subrequests and trivial CPU.
//
// CONSEQUENCE / TODO for whoever provisions R2: the per-series CSV objects must
// be produced by the build/migration job. Until a given series' object exists,
// this handler returns an honest status (501 not_migrated if the source has no
// resolver at all; otherwise a 502-style "resolver_empty"/missing is reported as
// described below) -- NEVER an empty 200, NEVER a fabricated series.
// ===================================================================
//
// Honest-status decision tree (CONTRACT.md v1.1 "Status codes (reconciled)"):
//   1. id not in catalog            -> 404 not_found
//   2. source has no resolver       -> 501 not_migrated
//   3. unsupported dimension filter -> 400 unsupported_filter
//   4. R2 object ABSENT             -> 502 data_unavailable (source migrated, but
//                                       the object isn't published yet; loud +
//                                       actionable, never an empty 200)
//   5. object present but 0 data rows after the date window -> 502 resolver_empty
//   6. >=1 row                      -> 200 text/csv
// ---------------------------------------------------------------------------

import type { Env, SeriesRow, SourceRow, LicenseRow } from "./types";
import { SELECT_SERIES, SELECT_SOURCE, SELECT_LICENSE } from "./sql";
import {
  csv, csvStream, csvPassthrough, json, notFound, notMigrated, dataUnavailable, resolverEmpty,
  unsupportedFilter, badRequest, supportedSources, sourceOf, licenseBlock, dbForSeries,
} from "./util";
import {
  CSV_HEADER, FILTER_MAX_STORED_BYTES, FILTER_MAX_TEXT_BYTES, LineFilter, MAX_RATIO, STREAM_MIN_BYTES,
  VerifiedGunzip, completeLine, identityPipe, isGzipMagic, isizeFromTrailer, newStats, peekGzipHeader,
  prefixBytes, primePump, slices,
} from "./csvStream";
import type { FilterOpts, Primed } from "./csvStream";
import { isGated } from "./denylist";
import { GEO_PROJECTION_SOURCES, geoAlias, normalizeGeoParam, filterGeoRows } from "./geoProjection";

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

// The objects are WRITTEN by Python (tools/derive_csv_bulk.csv_key, core/derive_csv),
// which uses urllib.parse.quote(id, safe="") — i.e. RFC 3986 percent-encoding of
// everything outside A-Za-z0-9-_.~. JavaScript's encodeURIComponent leaves FIVE of those
// characters literal: ! ' ( ) * . So for any id containing one, the reader asked for a key
// the writer never created.
//
// MEASURED 2026-07-30, and it was live: 60,993 catalogued series across 12 sources contain
// one of those characters — 54,745 of them in un_wpp alone. Probing R2 directly, 46 of 46
// sampled objects existed under the Python spelling and 0 of 46 under this one. On the live
// API, a within-source control made it unambiguous: gcb / oxcgrt / un_wpp all returned
// HTTP 200 with real CSV for a plain id and HTTP 502 "the at-rest object for this series is
// not published yet" for an id differing only by a parenthesis. The object WAS published.
// We were telling users that data does not exist while holding it.
//
// Aligned to the writer rather than re-deriving 60,993 objects: a bounded scan of 60,000
// keys under series/ found NO literal ! ' ( ) * in any key, so the store is uniformly
// Python-spelled and there is no second convention this would break.

// Dataset slugs IDB has RENAMED since we ingested them. The old name 404s at the publisher with
// no redirect, which breaks condition (3) of IDB's written permission (2026-07-15): "a clear,
// permanent link back to the original dataset page".
//
// Kept as a map rather than re-keying the affected series: a re-key would change public series
// ids and break every URL a user already holds, in order to repair a link in a comment header.
//
// Checked, not assumed — tools/cost/idb_backlink_check.py asks CKAN package_show whether every
// slug in the served catalogue still resolves, and names any that stop. As of 2026-09-02, 20 of
// 21 resolved and this was the one that did not.
const IDB_RENAMED: Record<string, string> = {
  // -> cima-indicators, "Center of Information to Improve Learning (CIMA)", cc-by, 29 series
  "center-for-learning-improvement-information-cima-regional-indicators-2007-2": "cima-indicators",
};

function idbDatasetUrl(seriesId: string): string {
  const slug = seriesId.split(":")[2] ?? "";
  return `https://data.iadb.org/dataset/${IDB_RENAMED[slug] ?? slug}`;
}

function objectKey(seriesId: string): string {
  const rfc3986 = encodeURIComponent(seriesId).replace(
    /[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
  return `series/${rfc3986}.csv`;
}

// Prominent citation header prepended to every .csv (unless ?raw=1). Lines start
// with '#' so pandas (comment='#') / R (comment.char='#') skip them — but anyone
// who OPENS the file (e.g. a data provider verifying attribution) sees the source,
// licence and citation at the very top. This is the primary "attribution travels
// with the bytes" mechanism and it is REQUIRED for the sources we re-host by
// permission (KOF, UN Comtrade, WHR, IEP). The header is built from the same
// catalog source/licence rows the metadata endpoint uses — one source of truth.
async function citationHeader(seriesId: string, series: SeriesRow, env: Env): Promise<string> {
  const source = sourceOf(seriesId);
  const src = await env.CATALOG.prepare(SELECT_SOURCE).bind(source).first<SourceRow>();
  const licId = series.license_id ?? src?.license_id ?? null;
  const lic = licId ? await env.CATALOG.prepare(SELECT_LICENSE).bind(licId).first<LicenseRow>() : null;
  const L = licenseBlock(lic);

  let citation = "";
  if (series.metadata) {
    try {
      const m = JSON.parse(series.metadata) as { citation?: string; citation_long?: string; citation_short?: string };
      citation = m.citation_long || m.citation || m.citation_short || "";
    } catch { /* best-effort */ }
  }

  const row = (label: string, val: string | null | undefined): string =>
    val ? `#  ${(label + ":").padEnd(11)}${String(val).replace(/\s+/g, " ").trim()}\n` : "";

  let licLine = "";
  if (L) {
    let s = L.name || L.id || "";
    if (L.commercial_ok === false) s += " — NON-COMMERCIAL USE ONLY (honor it)";
    if (L.attribution_required) s += "; attribution required";
    // ShareAlike and NoDerivatives were the two obligations this header never
    // mentioned. 2,866,900 served series carry one or the other — WID alone is
    // 2,465,197 of them under CC BY-NC-SA 4.0 — and the downloader was told only
    // "non-commercial; attribution required", which is an incomplete statement of
    // what they just agreed to. Both are enforceable conditions of the very licences
    // we rely on to host this data, so they belong beside the other two.
    //
    // ShareAlike is read from the licence NAME because the schema has no share_alike
    // column; the name is where it is encoded for every CC-SA source here. Anchored
    // so it cannot fire on an unrelated substring — a bare "sa" occurs inside plenty
    // of identifiers, and an unanchored test is how R112 produced three wrong answers.
    const lname = String(L.name || L.id || "").toLowerCase();
    if (/(^|[-_])sa([-_.]|\d|$)/.test(lname) || lname.includes("sharealike")) {
      s += "; SHARE-ALIKE — anything you build from this must carry the same licence";
    }
    if (L.no_modify) s += "; NO DERIVATIVES — redistribute verbatim, unmodified";
    licLine = `#  ${"License:".padEnd(11)}${s}\n`;
  }

  const bar = "# " + "=".repeat(76) + "\n";
  return (
    bar +
    "#  DATA CITATION — please credit the original source in any use or publication.\n" +
    "#  By downloading from the Elkassabgi Data Library you agreed to cite this source.\n" +
    "#\n" +
    row("Series", `${series.title ?? seriesId}  [${seriesId}]`) +
    row("Source", src?.attribution?.replace(/^\s*source:\s*/i, "")) +
    licLine +
    // IDB written permission (2026-07-15) requires "a clear, permanent link
    // back to the original dataset page" — ids are idb:IDB:<dataset-slug>:...
    row("Dataset", source === "idb" ? idbDatasetUrl(seriesId) : null) +
    row("Homepage", src?.homepage) +
    row("Terms", src?.terms_url) +
    row("Cite as", citation) +
    "#  Provided:  Elkassabgi Data Library — econdatalibrary.com\n" +
    "#  (Pipelines: pandas pd.read_csv(url, comment='#'), or append ?raw=1 for bare CSV.)\n" +
    bar
  );
}

export async function handleSeriesCsv(
  seriesId: string, url: URL, env: Env, ctx?: ExecutionContext,
  onDone?: (bytes: number, ok: boolean) => Promise<void>,
): Promise<Response> {
  const requestedId = seriesId;
  let geoFilter: string | null = null;
  // What the CALLER typed, kept beside the resolved code so every error message quotes
  // the user's own words. Without it a request for XD is refused with "no rows for
  // 'HIC'" -- a code they never used, about a request they never made.
  let geoRequested: string | null = null;

  // 1) catalog membership (404 if unknown) — with per-geo projection fallback
  //    (geoProjection.ts): an uncatalogued 3-part alias like
  //    worldbank:DT.DOD.DECT.CD:LMY resolves to the CLEARED grouped
  //    worldbank_wdi:<CODE> object filtered to that economy. index.ts already
  //    gated the ALIAS spelling; the canonical spelling is gated here too (R32).
  let series = await dbForSeries(env, seriesId).prepare(SELECT_SERIES).bind(seriesId).first<SeriesRow>();
  if (!series) {
    const alias = geoAlias(seriesId);
    if (alias && !isGated(alias.canonical)) {
      const canon = await dbForSeries(env, alias.canonical)
        .prepare(SELECT_SERIES).bind(alias.canonical).first<SeriesRow>();
      if (canon) {
        series = canon; seriesId = alias.canonical;
        geoFilter = alias.geo; geoRequested = alias.requested;
      }
    }
    if (!series) return notFound(requestedId);
  }

  // 2) resolver coverage (501 if the source has no at-rest resolver).
  const source = sourceOf(seriesId);
  if (!supportedSources(env).has(source)) return notMigrated(source, seriesId);

  // 3) filters. Date window is server-honoured. `?geo=` is honoured for grouped
  //    projection sources (their row ids end in the economy code); freq/unit are
  //    NOT columns in the derived CSV -> 400 unsupported_filter (never a
  //    silently-unfiltered 200). format=full|filtered accepted (both currently
  //    yield the same long CSV; filtered is reserved).
  for (const dim of ["freq", "unit"]) {
    if (url.searchParams.has(dim)) {
      return unsupportedFilter(
        `filter '${dim}=' is not honored yet: the derived per-series CSV has no ` +
          `${dim} column. Refusing to return a silently-unfiltered series.`,
      );
    }
  }
  const geoParam = url.searchParams.get("geo");
  if (geoParam !== null) {
    const src0 = sourceOf(seriesId);
    if (geoFilter !== null) {
      // Alias id AND ?geo= together must agree — never silently prefer one.
      if (normalizeGeoParam(geoParam) !== geoFilter) {
        return badRequest(
          `conflicting geo: the id names '${geoRequested ?? geoFilter}' but ?geo= says `
          + `'${geoParam}'`);
      }
    } else if (GEO_PROJECTION_SOURCES[src0] === src0) {
      const g = normalizeGeoParam(geoParam);
      if (g === null) {
        return badRequest(
          "geo must be a 2-3 character World Bank economy code, e.g. USA, LMY, WLD");
      }
      geoFilter = g;
      geoRequested = geoParam.trim().toUpperCase();
    } else {
      return unsupportedFilter(
        "filter 'geo=' is not honored yet: the derived per-series CSV has no " +
          "geo column. Refusing to return a silently-unfiltered series.",
      );
    }
  }
  const fmt = url.searchParams.get("format");
  if (fmt !== null && fmt !== "full" && fmt !== "filtered") {
    return badRequest(`format must be 'full' or 'filtered', got '${fmt}'`);
  }
  const from = url.searchParams.get("from");
  const to = url.searchParams.get("to");
  if (from !== null && !DATE_RE.test(from)) return badRequest("from must be YYYY-MM-DD");
  if (to !== null && !DATE_RE.test(to)) return badRequest("to must be YYYY-MM-DD");

  // 4) fetch the pre-derived CSV object from R2.
  const obj = await env.SERIES_BUCKET.get(objectKey(seriesId));
  if (obj === null) {
    // The series is cataloged and its source IS migrated (has a resolver), but
    // the per-series CSV object has not been derived/published yet. Per the
    // CONTRACT.md v1.1 status pin this is 502 data_unavailable (NOT 501
    // not_migrated -- the source is migrated; the object just isn't published).
    // Loud + actionable, never an empty 200. Mirrors the dev shim's
    // resolve()-raised "at-rest file absent" -> data_unavailable branch.
    return dataUnavailable(source, seriesId);
  }

  // Objects may be stored gzip-compressed (cost plan 2026-08-18: numeric CSVs
  // compress 5-10x and R2 storage is the bill's dominant line). The worker
  // always materializes the text anyway (date window + citation header), so
  // stored compression is invisible to clients — the edge re-compresses the
  // RESPONSE per each client's Accept-Encoding as it always has. Detection is
  // by the object's own contentEncoding metadata, set at PUT time by
  // core/derive_csv.py; plain objects keep working, so the fleet can migrate
  // gradually with this reader deployed first.
  const gzipped = obj.httpMetadata?.contentEncoding === "gzip";
  const rawParam0 = url.searchParams.get("raw");
  const bare0 = rawParam0 === "1" || rawParam0 === "true";

  // 4a) LARGE objects are STREAMED, never materialised (csvStream.ts). Measured
  //     2026-09-01: 51 cbs_nl objects exceed 100 MB gzipped (max 554 MB), i.e. GBs
  //     of text; `.text()` of that inside a 128 MB isolate dies with Error 1102 —
  //     every one of them was catalogued and undeliverable. The stream keeps the
  //     honest statuses: it is primed before the Response exists, so 0 rows is
  //     still 502 / the geo 404 names real alternatives.
  if (obj.size >= STREAM_MIN_BYTES) {
    return streamLarge(obj, gzipped, seriesId, requestedId, series, env,
                       { from, to, geo: geoFilter }, geoRequested, bare0, ctx, onDone);
  }

  const text = gzipped
    ? await new Response(
        obj.body.pipeThrough(new DecompressionStream("gzip")),
      ).text()
    : await obj.text();
  // A stored object that does not start with the contract header is malformed: 502, never a
  // 200 whose "rows" are something else (R582 F8: the string path served it; the stream path
  // refuses it — both must agree).
  const firstNl = text.indexOf("\n");
  const firstLine = (firstNl < 0 ? text : text.slice(0, firstNl)).replace(/\r$/, "");
  if (firstLine !== CSV_HEADER) {
    return json({ error: "data_unavailable", source, series_id: seriesId,
                  detail: `the at-rest object is malformed (header '${firstLine.slice(0, 60)}'); refusing to serve it` }, 502);
  }

  // 4b) per-geo projection: keep only this economy's rows. Zero matches is an
  //     honest 404 that names real alternatives, never an empty 200.
  let projected = text;
  if (geoFilter !== null) {
    const r = filterGeoRows(text, geoFilter);
    if (r.rows === 0) {
      return json({
        error: "geo_not_found",
        detail: `no rows for geo '${geoRequested ?? geoFilter}'`
          + (geoRequested && geoRequested !== geoFilter
              ? ` (the store spells it '${geoFilter}')` : "")
          + ` in ${seriesId} — this grouped ` +
          `series holds ${r.geos.length} economies (e.g. ` +
          `${r.geos.slice(0, 8).join(", ")}). Request the full set at ` +
          `/v1/series/${encodeURIComponent(seriesId)}.csv`,
      }, 404);
    }
    projected = r.text;
  }
  const filtered = applyDateWindow(projected, from, to);

  // 5) zero data rows -> 502 resolver_empty (refuse an empty series silently).
  if (countDataRows(filtered) === 0) return resolverEmpty(seriesId);

  // 6) 200 with >=1 row. Prepend the prominent citation header by default;
  //    ?raw=1 (used by the MCP server, econdl clients, and pipelines) returns the
  //    bare series_id,obs_date,value CSV. The R2 ETag describes the bare object, so
  //    only pass it through on the raw path.
  const rawParam = url.searchParams.get("raw");
  const bare = rawParam === "1" || rawParam === "true";
  // The R2 ETag describes the FULL stored object — never attach it to a
  // geo-projected subset.
  // The R2 ETag describes the FULL stored object: never attach it to a projected OR
  // date-windowed body (R582 F8 — the window case was leaking it).
  const whole = geoFilter === null && from === null && to === null;
  if (bare) return csv(filtered, whole ? { etag: obj.httpEtag } : undefined);
  const note = geoFilter !== null
    ? `# Projection: rows for geo=${geoFilter} of grouped series ${seriesId}` +
      (requestedId !== seriesId ? ` (requested as ${requestedId})` : "") + "\n"
    : "";
  return csv((await citationHeader(seriesId, series, env)) + note + filtered);
}

/** Serve a large object without holding it (csvStream.ts; ledger R579/R582/R585).
 *
 *  PASSTHROUGH — no window, no geo, gzipped object: the stored gzip bytes go to the client
 *  untouched (content-encoding: gzip, exact content-length, ~0.7 CPU-s per GB, flat memory).
 *  Primed first: the first stored chunk(s) must carry the gzip magic AND inflate to the
 *  contract header plus a data row (a plain object flagged gzip, or a double-gzipped object,
 *  is 502 - never a 200 the client cannot decode). The in-body citation is OMITTED on this
 *  path (a gzip member prepended to the stored bytes is not decoded by curl --compressed) and
 *  the response says so: `x-econdl-citation-omitted: large-object` + a Link to the metadata.
 *  ?raw=1 additionally passes the R2 ETag (it describes exactly these bytes).
 *
 *  INFLATE — window / geo / plain object: a PULL-DRIVEN pump (never a pipeThrough chain,
 *  which workerd runs ahead of the consumer): one R2 chunk -> fflate gunzip with CRC32/ISIZE
 *  verification -> byte-level line filter -> `await writer.write()` into an identity stream.
 *  Primed before the Response exists so the contract statuses hold (wrong header or 0 rows
 *  -> 502; geo never matched -> 404 naming real economies; malformed -> 502). A mid-run
 *  error ABORTS the transfer (never a clean EOF on a 200). Refused UP FRONT (400,
 *  actionable) when the stored size could wrap the 4 GiB ISIZE or the decompressed size
 *  exceeds FILTER_MAX_TEXT_BYTES. Bytes written to the client are counted and handed to
 *  `onDone` when the transfer ends (the download log records delivered bytes, not produced). */
async function streamLarge(
  obj: R2ObjectBody, gzipped: boolean, seriesId: string, requestedId: string,
  series: SeriesRow, env: Env, opts: FilterOpts, geoRequested: string | null, bare: boolean,
  ctx: ExecutionContext | undefined, onDone: ((bytes: number, ok: boolean) => Promise<void>) | undefined,
): Promise<Response> {
  const filtered = opts.from !== null || opts.to !== null || opts.geo !== null;
  const key = objectKey(seriesId);
  const source = sourceOf(seriesId);
  const malformed = (why: string) => json({ error: "data_unavailable", source, series_id: seriesId,
    detail: `the at-rest object is malformed (${why}); refusing to serve it` }, 502);
  const settle = (p: Promise<number>, written: () => number) => {
    // R599: an aborted transfer still moved bytes - report what was written, not 0
    const done = p.then((n) => onDone?.(n, true), () => onDone?.(written(), false)).catch(() => undefined);
    if (ctx) ctx.waitUntil(done);
  };

  if (!filtered && gzipped) {
    // Prime on THIS body: pull stored chunks until the inflated prefix shows the header and a
    // data row (bounded: peekGzipHeader stops after 64 KB of text), then cancel it and serve a
    // SECOND GET's body untouched - the runtime pumps a native R2 body at ~0.7 CPU-s per GB
    // (R582), whereas re-emitting the bytes through a JS pump measured 2.8 MB/s. One extra
    // class-B GET per large download.
    const held: Uint8Array[] = [];
    let heldBytes = 0;
    let magicChecked = false;
    let peek = { headerOk: false, hasRow: false };
    const reader = obj.body.getReader();
    try {
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        held.push(value);
        heldBytes += value.length;
        if (!magicChecked && heldBytes >= 3) {
          magicChecked = true;
          const first3 = new Uint8Array(3);
          let o = 0;
          for (const c of held) { for (let i = 0; i < c.length && o < 3; i++) first3[o++] = c[i]; if (o >= 3) break; }
          if (!isGzipMagic(first3)) throw new Error("not a gzip member (flagged gzip at rest)");
        }
        peek = peekGzipHeader(held);
        if (peek.headerOk && peek.hasRow) break;
        if (held.reduce((n, c) => n + c.length, 0) > 4 * 1024 * 1024) break;   // 4 MB of gzip with no data row: give up
      }
    } catch (e) {
      await reader.cancel(e).catch(() => undefined);
      return malformed(String((e as Error).message ?? e).slice(0, 120));
    }
    await reader.cancel().catch(() => undefined);
    if (!peek.headerOk) return malformed("stored bytes do not inflate to the contract header");
    if (!peek.hasRow) return resolverEmpty(seriesId);
    // The primed bytes are the ones served: the second GET is conditional on the same ETag; a
    // replace between the two GETs yields a bodyless result (R593 b) and an honest 502.
    const fresh = await env.SERIES_BUCKET.get(key, { onlyIf: { etagMatches: obj.etag } });
    if (fresh === null) return dataUnavailable(source, seriesId);
    if (!("body" in fresh) || fresh.body === null) {
      return json({ error: "data_unavailable", source, series_id: seriesId,
        detail: "the object was replaced while the response was being prepared; retry" }, 502);
    }
    const extra: Record<string, string> = {
      "content-encoding": "gzip",
      "content-length": String(fresh.size),
      // a cacheable, pre-encoded body must vary on the request's encoding, or a shared cache
      // could hand these gzip bytes to a client that did not accept gzip (review round 8)
      "vary": "Accept-Encoding",
      "x-econdl-citation-omitted": "large-object",
      "link": `</v1/series/${encodeURIComponent(seriesId)}.metadata.json>; rel="describedby"`,
    };
    if (bare) extra["etag"] = fresh.httpEtag;
    return csvPassthrough(fresh.body, extra);
  }

  // Inflate path: budget first, never a 200 that dies at the CPU limit.
  if (gzipped) {
    if (obj.size > FILTER_MAX_STORED_BYTES) {
      await obj.body.cancel().catch(() => undefined);
      return unsupportedFilter(
        `this series' object is ${(obj.size / 1e6).toFixed(0)} MB stored, above the size at which its ` +
        `decompressed length is knowable in advance; server-side filtering is not offered for it. ` +
        `Download the full series at /v1/series/${encodeURIComponent(seriesId)}.csv?raw=1 (gzip) and filter locally.`);
    }
    const tail = await env.SERIES_BUCKET.get(key, { range: { offset: Math.max(0, obj.size - 4), length: 4 } });
    const isize = tail === null ? -1 : isizeFromTrailer(new Uint8Array(await tail.arrayBuffer()));
    if (isize > obj.size * MAX_RATIO) {
      // an ISIZE beyond any ratio the fleet has produced is forged, wrapped, or a degenerate
      // object (R593: a 5.96 MB object declaring 100 MB bought 79.6 CPU-s and 2.455 GB of
      // egress). Not a 'malformed' verdict - an honest 344x object exists in theory - but a
      // filter the server will not run (R599).
      await obj.body.cancel().catch(() => undefined);
      return unsupportedFilter(
        `this series' object declares ${(isize / 1e6).toFixed(0)} MB decompressed from ${(obj.size / 1e6).toFixed(1)} MB stored ` +
        `(${(isize / obj.size).toFixed(0)}x); server-side filtering is offered up to ${MAX_RATIO}x. ` +
        `Download the full series at /v1/series/${encodeURIComponent(seriesId)}.csv?raw=1 (gzip) and filter locally.`);
    }
    if (isize < 0 || isize > FILTER_MAX_TEXT_BYTES) {
      await obj.body.cancel().catch(() => undefined);
      return unsupportedFilter(
        `this series' object is ${isize < 0 ? "of unknown decompressed size" : `${(isize / 1e6).toFixed(0)} MB decompressed`}` +
        `, above the server-side filter budget of ${(FILTER_MAX_TEXT_BYTES / 1e6).toFixed(0)} MB. ` +
        `Download the full series at /v1/series/${encodeURIComponent(seriesId)}.csv?raw=1 ` +
        `(gzip) and filter locally.`);
    }
  } else if (obj.size > FILTER_MAX_TEXT_BYTES) {
    await obj.body.cancel().catch(() => undefined);
    return unsupportedFilter(
      `this series' object is ${(obj.size / 1e6).toFixed(0)} MB, above the server-side filter budget ` +
      `of ${(FILTER_MAX_TEXT_BYTES / 1e6).toFixed(0)} MB. Download it at ` +
      `/v1/series/${encodeURIComponent(seriesId)}.csv?raw=1 and filter locally.`);
  }

  const stats = newStats();
  const lf = new LineFilter(opts, stats);
  const vg = gzipped ? new VerifiedGunzip(stats) : null;
  // one piece per INFLATE_SLICE of stored bytes, emitted as produced (R593/R599: the transient
  // is one slice's inflate plus the coalescing buffer, never chunk x ratio)
  // LAZY (R603): a generator - each slice is inflated only when the pump asks for the next
  // piece, i.e. after the previous piece has gone through the coalescing writer. Array.map
  // evaluated every slice before the first write and the transient never moved.
  const step = function* (chunk: Uint8Array): Generator<Uint8Array> {
    if (!vg) { yield lf.push(chunk); return; }
    for (const sl of slices(chunk)) yield lf.push(vg.push(sl));
  };
  const finish = function* (): Generator<Uint8Array> {
    if (vg) yield lf.push(vg.finish());
    yield lf.flush();
    yield completeLine(stats.rows);   // the in-band completeness marker (R593)
  };
  let primed: Primed;
  try {
    primed = await primePump(obj.body, step, finish, stats);
  } catch (e) {
    return malformed((stats.malformed ?? String((e as Error).message ?? e)).slice(0, 120));
  }
  if (primed.first === null || stats.headerOk === false || stats.rows === 0) {
    if (opts.geo !== null && stats.headerOk !== false && !stats.geoMatched) {
      const geos = [...stats.geos].sort();
      return json({
        error: "geo_not_found",
        detail: `no rows for geo '${geoRequested ?? opts.geo}'`
          + ` in ${seriesId} — this grouped series holds ${geos.length} economies (e.g. `
          + `${geos.slice(0, 8).join(", ")}). Request the full set at `
          + `/v1/series/${encodeURIComponent(seriesId)}.csv`,
      }, 404);
    }
    return stats.headerOk === false ? malformed("stored bytes do not start with the contract header") : resolverEmpty(seriesId);
  }
  const note = opts.geo !== null
    ? `# Projection: rows for geo=${opts.geo} of grouped series ${seriesId}` +
      (requestedId !== seriesId ? ` (requested as ${requestedId})` : "") + "\n"
    : "";
  const prefix = (bare ? "" : (await citationHeader(seriesId, series, env)) + note) + CSV_HEADER + "\n";
  const { readable, writable } = identityPipe();
  const writer = writable.getWriter();
  const run = (async () => {
    const pb = prefixBytes(prefix);
    await writer.write(pb); stats.bytesOut += pb.length;
    return primed.run(writer);
  })();
  settle(run, () => stats.bytesOut);
  // The R2 ETag describes the FULL stored object — only on the bare, unfiltered path.
  const extra = bare && !filtered ? { etag: obj.httpEtag } : undefined;
  return csvStream(readable, obj.size, extra);
}

/** Keep the header + rows whose obs_date is within [from, to] (inclusive).
 *  obs_date is ISO (YYYY-MM-DD...), so lexical compare == chronological compare. */
function applyDateWindow(text: string, from: string | null, to: string | null): string {
  if (from === null && to === null) return text;
  const lines = text.split("\n");
  if (lines.length === 0) return text;
  const header = lines[0];
  const out: string[] = [header];
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (line.trim() === "") continue;
    const cols = line.split(",");
    const obsDate = cols[1] ?? ""; // series_id,obs_date,value
    if (from !== null && obsDate < from) continue;
    if (to !== null && obsDate > to) continue;
    out.push(line);
  }
  return out.join("\n") + "\n";
}

/** Count non-empty data rows (excludes the header). */
function countDataRows(text: string): number {
  const lines = text.split("\n");
  let n = 0;
  for (let i = 1; i < lines.length; i++) {
    if (lines[i].trim() !== "") n++;
  }
  return n;
}
