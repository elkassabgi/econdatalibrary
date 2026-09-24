// node --test test/wideSources.test.ts   (Node >= 22.6 strips types natively)
//
// FOUR SOURCES WERE ADVERTISED AND UNDELIVERABLE FOR ~20 DAYS. The CSV_HEADER guard in
// series.ts refuses any stored object whose first line is not "series_id,obs_date,value".
// wikidata, fhfa, census and treasury are DELIBERATELY stored wide - the resolver marks them
// tidy_ok=False (clients/python/econdl/_resolve.py:1491) because a long projection of a
// relational table would be a lie - so their CORRECT objects were rejected as malformed.
// Measured on the live worker 2026-09-22 with an authenticated key: fhfa 89,706 +
// census 2,993 + wikidata 250 + treasury 14 = 92,963 series answering
//   502 {"error":"data_unavailable","detail":"the at-rest object is malformed
//        (header 'dataset,series_key,obs_date,hpi,...')"}
// while bls and oecd answered 200 in the same minute.
//
// THE RULE THESE TESTS PIN: a wide object is served WHOLE and untouched, and never filtered.
// from/to/geo are refused for it, because applyDateWindow reads cols[1] on the assumption of
// series_id,obs_date,value; on an fhfa row cols[1] is `series_key`. The guard itself is kept:
// a NON-wide object with a wrong header is still refused.
//
// WHY THEY DRIVE THE ROUTE HANDLER. Two earlier versions of this fix passed every test here
// and were broken in production, because the tests called helpers (peekGzipHeader,
// LineFilter, streamLarge) and never the function that decides which of them runs:
//   - the passthrough still called peekGzipHeader without the wide flag (every gzipped wide
//     object over 256 KiB kept answering 502);
//   - the filter path emitted the wide header, the primer stopped on it alone whenever a stored
//     chunk ended between the header's newline and the first row's, and a healthy object
//     answered 502 resolver_empty.
// So every serving test below enters handleSeriesCsv through a fake catalogue and a fake bucket,
// and the large-object ones deliver the stored bytes at several chunk sizes, including the
// split that broke the second version.
import { test } from "node:test";
import assert from "node:assert/strict";
import { register } from "node:module";
import { gzipSync, gunzipSync } from "node:zlib";
import { CSV_HEADER, STREAM_MIN_BYTES, newStats, peekGzipHeader, peekPlainHeader, LineFilter }
  from "../src/csvStream.ts";
import { NATIVE_ONLY_SOURCES, isNativeOnly } from "../src/util.ts";

// series.ts imports its siblings without file extensions, which esbuild and tsc resolve and
// Node's ESM resolver does not - so it is imported dynamically, after the hook is registered.
register(new URL("./tsResolveHooks.mjs", import.meta.url));
const { handleSeriesCsv, streamLarge } = await import("../src/series.ts");

const enc = new TextEncoder();
const dec = new TextDecoder();

// The REAL headers, copied from the live 502 details - not invented shapes.
const FHFA_HEADER = "dataset,series_key,obs_date,hpi,hpi_1990,hpi_2000,annual_change";
const CENSUS_HEADER = "series_key,data_type_code,seasonally_adj,category_code";
const TREASURY_HEADER = "series_key,obs_date,record_date,debt_held_public_amt";
const WIKIDATA_HEADER = "series_key,qid,wikidata_url,name,tickers,primary_ticker";

// =========================================================================================
// The source list and the header peeks
// =========================================================================================

test("the wide-source list is exactly the resolver's _NATIVE_ONLY set", () => {
  // Mirrors clients/python/econdl/_resolve.py:1491. tests/test_worker_native_only.py fails CI
  // if the two ever drift; this one pins the shape on the worker side.
  assert.deepEqual([...NATIVE_ONLY_SOURCES].sort(),
    ["census", "fhfa", "hf_equities", "treasury", "wikidata"]);
});

test("isNativeOnly is true for every wide source and false for a tidy one", () => {
  for (const s of NATIVE_ONLY_SOURCES) assert.equal(isNativeOnly(s), true, s);
  // CONTROL: if this returned true for everything the guard would be gone, not fixed.
  for (const s of ["bls", "oecd", "worldbank", "ilostat", "eurostat"]) {
    assert.equal(isNativeOnly(s), false, s);
  }
});

test("both peeks accept a real wide header ONLY when told the source is wide", () => {
  for (const hdr of [FHFA_HEADER, CENSUS_HEADER, TREASURY_HEADER, WIKIDATA_HEADER]) {
    const text = `${hdr}\nx,y,z\n`;
    const gz = [new Uint8Array(gzipSync(Buffer.from(text)))];
    const plain = [enc.encode(text)];
    assert.equal(peekGzipHeader(gz, false).headerOk, false, `gzip ${hdr} unflagged`);
    assert.equal(peekGzipHeader(gz, true).headerOk, true, `gzip ${hdr} flagged`);
    assert.equal(peekPlainHeader(plain, false).headerOk, false, `plain ${hdr} unflagged`);
    assert.equal(peekPlainHeader(plain, true).headerOk, true, `plain ${hdr} flagged`);
  }
});

test("both peeks accept the canonical header without the flag", () => {
  const text = `${CSV_HEADER}\na,2020-01-01,1\n`;
  assert.equal(peekGzipHeader([new Uint8Array(gzipSync(Buffer.from(text)))], false).headerOk, true);
  assert.equal(peekPlainHeader([enc.encode(text)], false).headerOk, true);
});

test("a wide header may be anything EXCEPT empty", () => {
  // An empty first line would serve the rows labelled with nothing.
  for (const blank of ["\n", "\r\n", "   \n"]) {
    const text = `${blank}a,b,c\n`;
    assert.equal(peekPlainHeader([enc.encode(text)], true).headerOk, false, JSON.stringify(blank));
    assert.equal(peekGzipHeader([new Uint8Array(gzipSync(Buffer.from(text)))], true).headerOk,
      false, JSON.stringify(blank));
  }
});

test("the peeks' wide flag is REQUIRED, so a call site cannot silently omit it", () => {
  // The first fix gave the flag a default of `false`, the passthrough call site was not updated,
  // and every gzipped wide object over 256 KiB kept answering 502. Function.length counts
  // parameters before the first default: 2 while the flag is required, 1 the moment somebody
  // re-adds `= false` - which is what makes `npm run typecheck` fail on a forgetful call site.
  assert.equal(peekGzipHeader.length, 2);
  assert.equal(peekPlainHeader.length, 2);
});

test("the row filter still refuses a wrong header - the guard is kept, not removed", () => {
  // The case the guard was written for (commit e59cce123). A wide object never reaches this
  // filter (it is served whole), so the filter needs no exception for one.
  const stats = newStats();
  const f = new LineFilter({ from: null, to: null, geo: null, allowAnyHeader: true }, stats);
  f.push(enc.encode(`${FHFA_HEADER}\nannual_cbsa,01,2020-12-31,100.0,1,2,3\n`));
  f.flush();
  assert.equal(stats.headerOk, false,
    "the filter consumes and replaces the header, so it must refuse any header but the canonical one");
});

// =========================================================================================
// A fake catalogue and bucket, so the tests enter the ROUTE HANDLER
// =========================================================================================

type Stored = { bytes: Uint8Array; gzip: boolean; etag: string };

function objectKey(id: string): string {       // mirrors series.ts:objectKey
  return "series/" + encodeURIComponent(id)
    .replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase()) + ".csv";
}

/** chunks: a size in bytes, or "header-split" = cut exactly after the first newline. */
function streamOf(b: Uint8Array, chunks: number | "header-split"): ReadableStream<Uint8Array> {
  const parts: Uint8Array[] = [];
  if (chunks === "header-split") {
    const nl = b.indexOf(10);
    parts.push(b.subarray(0, nl + 1), b.subarray(nl + 1));
  } else {
    for (let o = 0; o < b.length; o += chunks) parts.push(b.subarray(o, Math.min(b.length, o + chunks)));
  }
  return new ReadableStream<Uint8Array>({
    start(c) { for (const p of parts) c.enqueue(p); c.close(); },
  });
}

function makeEnv(store: Map<string, Stored>, chunks: number | "header-split",
                 opts: { replaceAfterFirstGet?: string } = {}) {
  let gets = 0;
  const bucket = {
    get: async (key: string, o?: { range?: { offset: number; length: number };
                                    onlyIf?: { etagMatches?: string } }) => {
      const s = store.get(key);
      if (!s) return null;
      gets++;
      if (opts.replaceAfterFirstGet && gets === 2) s.etag = opts.replaceAfterFirstGet;
      const meta = { key, size: s.bytes.length, etag: s.etag, httpEtag: `"${s.etag}"`,
                     httpMetadata: s.gzip ? { contentEncoding: "gzip" } : {} };
      // A failed precondition returns the object WITHOUT a body, as R2 does.
      if (o?.onlyIf?.etagMatches !== undefined && o.onlyIf.etagMatches !== s.etag) return meta;
      const b = o?.range ? s.bytes.subarray(o.range.offset, o.range.offset + o.range.length)
                         : s.bytes;
      return { ...meta, body: streamOf(b, chunks),
               arrayBuffer: async () => b.slice().buffer,
               text: async () => dec.decode(b) };
    },
  };
  const rows: Record<string, unknown> = {};
  for (const key of store.keys()) {
    const id = decodeURIComponent(key.slice("series/".length, -".csv".length));
    rows[id] = { series_id: id, source_id: id.split(":")[0], license_id: "lic-test",
                 metadata: null, title: id };
  }
  const d1 = {
    prepare: (sql: string) => ({
      bind: (...args: unknown[]) => ({
        first: async () => {
          if (/FROM series\b/.test(sql)) return rows[String(args[0])] ?? null;
          if (/FROM source\b/.test(sql)) return { source_id: args[0], license_id: "lic-test",
                                                  name: "Test publisher", url: "https://example.org" };
          if (/FROM license\b/.test(sql)) return { license_id: "lic-test", name: "CC BY 4.0",
                                                   commercial_ok: 1, attribution_required: 1 };
          return null;
        },
      }),
    }),
  };
  return {
    CATALOG: d1, CATALOG_CLIMATE: d1, SERIES_BUCKET: bucket,
    SUPPORTED_SOURCES: "fhfa,census,wikidata,treasury,bls",
  } as never;
}

async function call(id: string, store: Map<string, Stored>, chunks: number | "header-split",
                    query = "raw=1", opts = {}) {
  const url = new URL(`https://econdl.test/v1/series/${encodeURIComponent(id)}.csv?${query}`);
  const res = await handleSeriesCsv(id, url, makeEnv(store, chunks, opts), undefined, undefined);
  const buf = Buffer.from(await res.arrayBuffer());
  const text = res.headers.get("content-encoding") === "gzip"
    ? gunzipSync(buf).toString() : buf.toString();
  return { res, text };
}

function storeOf(id: string, text: string, gzip: boolean): Map<string, Stored> {
  const raw = Buffer.from(text);
  return new Map([[objectKey(id), { bytes: new Uint8Array(gzip ? gzipSync(raw) : raw), gzip,
                                    etag: "etag-1" }]]);
}

// A census-shaped wide body: its own header, ~240 columns, and one QUOTED FIELD HOLDING A
// NEWLINE - the shape a line filter would have cut (review finding 4). Built to clear 256 KiB.
const CENSUS_COLS = ["series_key", ...Array.from({ length: 239 }, (_, i) => `c${i}`)];
const CENSUS_WIDE_HDR = CENSUS_COLS.join(",");
// Values from a fixed linear congruential sequence, so the body does NOT compress away: the
// gzipped copy must ALSO clear 256 KiB STORED, or the gzip cases take the small-object path
// and test nothing (the first draft of these tests did exactly that).
let lcg = 12345;
const rnd = () => (lcg = (lcg * 1103515245 + 12345) % 2147483648);
const censusRow = (i: number) =>
  [`intltrade__imports__hs#84:${i}`, ...Array.from({ length: 239 }, () => String(rnd() % 1000003))].join(",");
const BIG_CENSUS = CENSUS_WIDE_HDR + "\n"
  + `"a quoted, multi-line\nfield",${Array.from({ length: 239 }, () => "0").join(",")}\n`
  + Array.from({ length: 700 }, (_, i) => censusRow(i)).join("\n") + "\n";
const SMALL_FHFA = FHFA_HEADER + "\n" + "annual_cbsa,01,2020-12-31,100.0,1.0,2.0,3.0\n"
  + "annual_cbsa,01,2021-12-31,105.0,1.0,2.0,3.0\n";
const BIG_TIDY = CSV_HEADER + "\n"
  + Array.from({ length: 12000 }, (_, i) => `bls:X,2020-01-${String((i % 28) + 1).padStart(2, "0")},${i}`)
    .join("\n") + "\n";

const CHUNKINGS: (number | "header-split")[] = [64, 1024, 65536, "header-split", 1 << 30];

test("SANITY: the large fixtures really take the streaming path (>= STREAM_MIN_BYTES)", () => {
  // Under the threshold the string path runs instead, and the large-object tests below would
  // pass while exercising nothing.
  assert.ok(Buffer.byteLength(BIG_CENSUS) >= STREAM_MIN_BYTES, `census ${Buffer.byteLength(BIG_CENSUS)}`);
  const gz = gzipSync(Buffer.from(BIG_CENSUS)).length;
  assert.ok(gz >= STREAM_MIN_BYTES,
    `census GZIPPED is ${gz} B stored - under the threshold the gzip cases take the string path`);
  assert.ok(Buffer.byteLength(BIG_TIDY) >= STREAM_MIN_BYTES, `tidy ${Buffer.byteLength(BIG_TIDY)}`);
  assert.ok(Buffer.byteLength(SMALL_FHFA) < STREAM_MIN_BYTES, "the small fixture must stay small");
  // and the multi-line quoted field is really there
  assert.ok(BIG_CENSUS.includes('"a quoted, multi-line\nfield"'));
});

// =========================================================================================
// Serving, through the handler
// =========================================================================================

test("a SMALL wide object is served whole with its own header (string path)", async () => {
  const id = "fhfa:annual_cbsa:01";
  const { res, text } = await call(id, storeOf(id, SMALL_FHFA, false), 1 << 30);
  assert.equal(res.status, 200, text.slice(0, 200));
  assert.equal(text, SMALL_FHFA, "served bytes must equal the stored object exactly");
});

test("a SMALL wide object keeps its own header under the citation too", async () => {
  const id = "fhfa:annual_cbsa:01";
  const { res, text } = await call(id, storeOf(id, SMALL_FHFA, false), 1 << 30, "");
  assert.equal(res.status, 200, text.slice(0, 200));
  const data = text.split("\n").filter((l) => l !== "" && !l.startsWith("#"));
  assert.equal(data[0], FHFA_HEADER);
  assert.equal(text.includes(CSV_HEADER), false, "the canonical header must not appear in a wide body");
});

for (const chunks of CHUNKINGS) {
  test(`a LARGE PLAIN wide object is served byte-identical (chunks=${chunks})`, async () => {
    // 2,255 of census's 2,700 large objects are stored plain (head_object over all of them,
    // 2026-09-23). The second version of this fix answered 502 here when a chunk ended right
    // after the header - "header-split" is that case.
    const id = "census:intltrade__imports__hs#84";
    const { res, text } = await call(id, storeOf(id, BIG_CENSUS, false), chunks);
    assert.equal(res.status, 200, text.slice(0, 200));
    assert.equal(text, BIG_CENSUS, "served bytes must equal the stored object exactly");
    assert.equal(res.headers.get("content-encoding"), null);
    assert.equal(res.headers.get("x-econdl-citation-omitted"), "large-object");
    // WHAT A CLIENT NEEDS, not which helper built it (review round 3). CONTRACT.md: a CSV with
    // no content-length must end with `# econdl-complete`, which a byte-untouched passthrough
    // cannot add - so the EXACT stored length is the completeness check, and no-transform keeps
    // an intermediary from recoding it away. index.ts also logs the download only when it is set.
    assert.equal(res.headers.get("content-length"), String(Buffer.byteLength(BIG_CENSUS)));
    assert.match(res.headers.get("cache-control") ?? "", /no-transform/);
  });

  test(`a LARGE GZIPPED wide object is served byte-identical (chunks=${chunks})`, async () => {
    const id = "census:intltrade__imports__hs#84";
    const { res, text } = await call(id, storeOf(id, BIG_CENSUS, true), chunks);
    assert.equal(res.status, 200, text.slice(0, 200));
    assert.equal(res.headers.get("content-encoding"), "gzip");
    assert.equal(text, BIG_CENSUS, "served bytes must inflate to the stored object exactly");
  });
}

test("a wide request with ?from= is REFUSED, never mis-filtered", async () => {
  const id = "fhfa:annual_cbsa:01";
  for (const q of ["from=2021-01-01", "to=2020-12-31", "geo=USA"]) {
    const { res, text } = await call(id, storeOf(id, SMALL_FHFA, false), 1 << 30, `raw=1&${q}`);
    assert.equal(res.status, 400, `${q}: ${text.slice(0, 200)}`);
    assert.match(text, /unsupported_filter/, q);
  }
});

test("gzip bytes flagged PLAIN at rest are refused, not served as text/csv", async () => {
  const id = "census:intltrade__imports__hs#84";
  const gz = new Uint8Array(gzipSync(Buffer.from(BIG_CENSUS)));
  const store = new Map([[objectKey(id), { bytes: gz, gzip: false, etag: "etag-1" }]]);
  const { res } = await call(id, store, 65536);
  assert.equal(res.status, 502);
});

test("a LARGE wide object with an EMPTY first line is refused", async () => {
  const id = "census:x";
  const { res } = await call(id, storeOf(id, "\n" + BIG_CENSUS.split("\n").slice(1).join("\n"), false), 4096);
  assert.equal(res.status, 502);
});

test("a LARGE wide object with a header and NO rows is refused as empty", async () => {
  const id = "census:x";
  const body = CENSUS_WIDE_HDR + "\n" + "\n".repeat(300_000);   // big enough to stream, no data
  const { res, text } = await call(id, storeOf(id, body, false), 65536);
  assert.equal(res.status, 502, text.slice(0, 200));
});

test("an object REPLACED between the prime and the serve is refused, not served", async () => {
  // The passthrough primes on one GET and serves a second, conditional on the ETag. Without
  // the condition a replaced object would be served unchecked.
  const id = "census:intltrade__imports__hs#84";
  const { res, text } = await call(id, storeOf(id, BIG_CENSUS, false), 65536, "raw=1",
    { replaceAfterFirstGet: "etag-2" });
  assert.equal(res.status, 502, text.slice(0, 200));
  assert.match(text, /replaced/);
});

test("a wide object that ever reaches the row filter is REFUSED, not mislabelled", async () => {
  // Unreachable through the handler by construction: every wide request is unfiltered and
  // served whole. This drives streamLarge directly with the one input the handler never builds
  // - a wide object WITH a filter - so the guard is pinned and cannot be deleted as dead code.
  // Without it, a future change routing a wide object here would have its header consumed and
  // its rows served under `series_id,obs_date,value`.
  const id = "census:intltrade__imports__hs#84";
  const store = storeOf(id, BIG_CENSUS, false);
  const env = makeEnv(store, 65536) as unknown as { SERIES_BUCKET: { get: (k: string) => Promise<unknown> } };
  const obj = await env.SERIES_BUCKET.get(objectKey(id));
  const res = await streamLarge(obj as never, false, id, id, {} as never, env as never,
    { from: "2020-01-01", to: null, geo: null, allowAnyHeader: true }, null, true, undefined, undefined);
  assert.equal(res.status, 502);
  assert.match(await res.text(), /wide-format object reached the row filter/);
});

test("CONTROL: a large TIDY object still goes through the filter, with one canonical header", async () => {
  const id = "bls:X";
  const { res, text } = await call(id, storeOf(id, BIG_TIDY, false), 65536);
  assert.equal(res.status, 200, text.slice(0, 200));
  assert.equal(text.split("\n")[0], CSV_HEADER);
  assert.equal(text.split(CSV_HEADER).length - 1, 1, "exactly one canonical header");
});

test("CONTROL: a large TIDY object with a wrong header is still refused", async () => {
  const id = "bls:X";
  const bad = "id,date,val\n" + BIG_TIDY.split("\n").slice(1).join("\n");
  const { res } = await call(id, storeOf(id, bad, false), 65536);
  assert.equal(res.status, 502);
});
