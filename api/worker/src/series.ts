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

import type { Env, SeriesRow } from "./types";
import { SELECT_SERIES } from "./sql";
import {
  csv, notFound, notMigrated, dataUnavailable, resolverEmpty, unsupportedFilter,
  badRequest, supportedSources, sourceOf,
} from "./util";

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function objectKey(seriesId: string): string {
  return `series/${encodeURIComponent(seriesId)}.csv`;
}

export async function handleSeriesCsv(seriesId: string, url: URL, env: Env): Promise<Response> {
  // 1) catalog membership (404 if unknown).
  const series = await env.CATALOG.prepare(SELECT_SERIES).bind(seriesId).first<SeriesRow>();
  if (!series) return notFound(seriesId);

  // 2) resolver coverage (501 if the source has no at-rest resolver).
  const source = sourceOf(seriesId);
  if (!supportedSources(env).has(source)) return notMigrated(source, seriesId);

  // 3) filters. Date window is server-honoured; geo/freq/unit are NOT columns in
  //    the derived CSV yet -> any such filter is 400 unsupported_filter (never a
  //    silently-unfiltered 200). format=full|filtered accepted (both currently
  //    yield the same long CSV; filtered is reserved).
  for (const dim of ["geo", "freq", "unit"]) {
    if (url.searchParams.has(dim)) {
      return unsupportedFilter(
        `filter '${dim}=' is not honored yet: the derived per-series CSV has no ` +
          `${dim} column. Refusing to return a silently-unfiltered series.`,
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

  const text = await obj.text();
  const filtered = applyDateWindow(text, from, to);

  // 5) zero data rows -> 502 resolver_empty (refuse an empty series silently).
  if (countDataRows(filtered) === 0) return resolverEmpty(seriesId);

  // 6) 200 with >=1 row.
  return csv(filtered, { etag: obj.httpEtag });
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
