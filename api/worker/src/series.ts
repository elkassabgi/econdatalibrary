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
  csv, json, notFound, notMigrated, dataUnavailable, resolverEmpty, unsupportedFilter,
  badRequest, supportedSources, sourceOf, licenseBlock, dbForSeries,
} from "./util";
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
    row("Dataset", source === "idb" ? `https://data.iadb.org/dataset/${seriesId.split(":")[2] ?? ""}` : null) +
    row("Homepage", src?.homepage) +
    row("Terms", src?.terms_url) +
    row("Cite as", citation) +
    "#  Provided:  Elkassabgi Data Library — econdatalibrary.com\n" +
    "#  (Pipelines: pandas pd.read_csv(url, comment='#'), or append ?raw=1 for bare CSV.)\n" +
    bar
  );
}

export async function handleSeriesCsv(seriesId: string, url: URL, env: Env): Promise<Response> {
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
  const text = gzipped
    ? await new Response(
        obj.body.pipeThrough(new DecompressionStream("gzip")),
      ).text()
    : await obj.text();

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
  if (bare) return csv(filtered, geoFilter === null ? { etag: obj.httpEtag } : undefined);
  const note = geoFilter !== null
    ? `# Projection: rows for geo=${geoFilter} of grouped series ${seriesId}` +
      (requestedId !== seriesId ? ` (requested as ${requestedId})` : "") + "\n"
    : "";
  return csv((await citationHeader(seriesId, series, env)) + note + filtered);
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
