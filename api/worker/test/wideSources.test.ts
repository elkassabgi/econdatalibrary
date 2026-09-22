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
// THE GUARD ITSELF IS RIGHT and these tests keep it: a NON-wide source with a wrong header is
// still refused. What was missing is that four sources have another correct header.
//
// WHY FILTERS ARE REFUSED RATHER THAN APPLIED. applyDateWindow reads cols[1] because a
// canonical row is series_id,obs_date,value; on an fhfa row cols[1] is `series_key`. Relaxing
// the header check alone would have filtered on the wrong column and still answered 200 -
// worse than the outage. So a wide object is served whole and from/to/geo are refused, which
// is the rule CONTRACT.md already applies to geo/freq/unit.
import { test } from "node:test";
import assert from "node:assert/strict";
import { gzipSync, gunzipSync } from "node:zlib";
import { register } from "node:module";
import { CSV_HEADER, STREAM_MIN_BYTES, contractHeaderPrefix, newStats, peekGzipHeader, LineFilter } from "../src/csvStream.ts";
import { NATIVE_ONLY_SOURCES, isNativeOnly } from "../src/util.ts";

// series.ts imports its siblings without file extensions, which esbuild and tsc resolve and
// Node's ESM resolver does not - so it is imported dynamically, after the hook is registered.
register(new URL("./tsResolveHooks.mjs", import.meta.url));
const { streamLarge } = await import("../src/series.ts");

const enc = new TextEncoder();
const dec = new TextDecoder();

// The REAL headers, copied from the live 502 details - not invented shapes.
const FHFA_HEADER = "dataset,series_key,obs_date,hpi,hpi_1990,hpi_2000,annual_change";
const CENSUS_HEADER = "series_key,data_type_code,seasonally_adj,category_code";
const TREASURY_HEADER = "series_key,obs_date,record_date,debt_held_public_amt";
const WIKIDATA_HEADER = "series_key,qid,wikidata_url,name,tickers,primary_ticker";

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

test("peekGzipHeader accepts a real wide header ONLY when told the source is wide", () => {
  for (const hdr of [FHFA_HEADER, CENSUS_HEADER, TREASURY_HEADER, WIKIDATA_HEADER]) {
    const gz = [new Uint8Array(gzipSync(Buffer.from(`${hdr}\nx,y,z\n`)))];
    // the bug: refused when not flagged
    assert.equal(peekGzipHeader(gz, false).headerOk, false, `${hdr} must be refused unflagged`);
    // the fix: accepted when the caller knows it is a wide source
    assert.equal(peekGzipHeader(gz, true).headerOk, true, `${hdr} must be accepted when wide`);
  }
});

test("peekGzipHeader still accepts the canonical header without any flag", () => {
  const gz = [new Uint8Array(gzipSync(Buffer.from(`${CSV_HEADER}\na,2020-01-01,1\n`)))];
  assert.equal(peekGzipHeader(gz, false).headerOk, true);
});

test("the streaming filter accepts a wide header only under allowAnyHeader", () => {
  const body = `${FHFA_HEADER}\nannual_cbsa,01,2020-12-31,100.0,1,2,3\n`;
  for (const [allow, want] of [[false, false], [true, true]] as [boolean, boolean][]) {
    const stats = newStats();
    const f = new LineFilter({ from: null, to: null, geo: null, allowAnyHeader: allow }, stats);
    f.push(enc.encode(body));
    f.flush();
    assert.equal(stats.headerOk, want,
      `allowAnyHeader=${allow} should give headerOk=${want}`);
  }
});

test("a TIDY source with a wrong header is still refused - the guard is kept, not removed", () => {
  // This is the case the guard was written for (commit e59cce123). If this ever passes,
  // the fix has gone too far and a malformed object would be served as data.
  const stats = newStats();
  const f = new LineFilter({ from: null, to: null, geo: null }, stats);
  f.push(enc.encode("total,rows,whatever\n1,2,3\n"));
  f.flush();
  assert.equal(stats.headerOk, false);
});

// ---------------------------------------------------------------------------------------
// THE LARGE-OBJECT PATHS. Everything above this line tests functions in isolation, and all
// of it passed while BOTH paths a wide object over STREAM_MIN_BYTES (256 KiB) actually takes
// were still broken. These pin the paths, not the helpers.
// ---------------------------------------------------------------------------------------

test("peekGzipHeader's wide flag is REQUIRED, so a call site cannot silently omit it", () => {
  // The passthrough branch in series.ts (`if (!filtered && gzipped)`) called
  // peekGzipHeader(held) with no flag while the parameter still had `= false`. A wide source
  // can NEVER be `filtered` - series.ts refuses from/to/geo for it - so every gzipped wide
  // object at or above STREAM_MIN_BYTES took that branch and kept answering the very 502 the
  // flag was added to end, with the whole suite green.
  //
  // Function.length counts parameters before the first default: 2 while the flag is required,
  // 1 the moment somebody re-adds `= false`. That is what makes `npm run typecheck` - which
  // CI runs - fail on a call site that forgets it, instead of a test that cannot see it.
  assert.equal(peekGzipHeader.length, 2,
    "peekGzipHeader must declare allowAnyHeader with NO default value");
});

test("contractHeaderPrefix adds the canonical header only when the stored one was consumed", () => {
  assert.equal(contractHeaderPrefix(false), CSV_HEADER + "\n");
  assert.equal(contractHeaderPrefix(undefined), CSV_HEADER + "\n");   // the ordinary call
  assert.equal(contractHeaderPrefix(true), "", "a wide body already carries its own header");
});

test("a large WIDE object keeps its OWN header in the body, and the rows match it", () => {
  // THE SECOND BUG. Marking the header ok is not the same as serving it. LineFilter consumed
  // the line and returned, series.ts prepended CSV_HEADER unconditionally, and the result was
  // a 200 whose header read `series_id,obs_date,value` above seven-column fhfa rows - a
  // silently MIS-labelled body, which series.ts:253 itself calls worse than refusing.
  const rows = "annual_cbsa,01,2020-12-31,100.0,1.0,2.0,3.0\n"
             + "annual_cbsa,01,2021-12-31,105.0,1.0,2.0,3.0\n";
  const stats = newStats();
  const f = new LineFilter({ from: null, to: null, geo: null, allowAnyHeader: true }, stats);
  const emitted = dec.decode(f.push(enc.encode(`${FHFA_HEADER}\n${rows}`))) + dec.decode(f.flush());
  const body = contractHeaderPrefix(true) + emitted;      // exactly what streamLarge writes
  const lines = body.split("\n").filter((l) => l !== "");

  assert.equal(stats.headerOk, true);
  assert.equal(lines[0], FHFA_HEADER, "the body must open with the object's own header");
  assert.equal(body.includes(CSV_HEADER), false,
    "the canonical header must not appear anywhere in a wide body");
  assert.equal(lines.length, 3, "header + 2 rows");
  // the point of the header: every row is labelled by it
  const want = FHFA_HEADER.split(",").length;
  for (const l of lines) assert.equal(l.split(",").length, want, `column count of: ${l}`);
});

test("a large TIDY object's header is consumed and re-added exactly once - not zero, not twice", () => {
  // CONTROL for the test above. If the emit branch ever fires for a tidy source the header
  // appears twice; if contractHeaderPrefix ever returns "" for one it appears not at all.
  const stats = newStats();
  const f = new LineFilter({ from: null, to: null, geo: null }, stats);
  const emitted = dec.decode(f.push(enc.encode(`${CSV_HEADER}\nx:1,2020-01-01,1\n`)));
  assert.equal(emitted.includes(CSV_HEADER), false, "LineFilter must CONSUME a canonical header");

  const body = contractHeaderPrefix(undefined) + emitted;
  assert.equal(body.split(CSV_HEADER).length - 1, 1, "exactly one canonical header");
  const lines = body.split("\n").filter((l) => l !== "");
  assert.deepEqual(lines, [CSV_HEADER, "x:1,2020-01-01,1"]);
});

test("every wide source's real header survives the round trip, not just fhfa's", () => {
  for (const hdr of [FHFA_HEADER, CENSUS_HEADER, TREASURY_HEADER, WIKIDATA_HEADER]) {
    const row = hdr.split(",").map((_, i) => `v${i}`).join(",");
    const stats = newStats();
    const f = new LineFilter({ from: null, to: null, geo: null, allowAnyHeader: true }, stats);
    const body = contractHeaderPrefix(true)
      + dec.decode(f.push(enc.encode(`${hdr}\n${row}\n`))) + dec.decode(f.flush());
    assert.equal(body, `${hdr}\n${row}\n`, `${hdr} must pass through unchanged`);
    assert.equal(stats.rows, 1, hdr);
  }
});

// =========================================================================================
// THE FUNCTION ITSELF, through a fake R2.
//
// Everything above calls helpers directly. Both of streamLarge's branches shipped broken for
// wide sources with all of it green, so these drive the real function and assert on the real
// Response. STREAM_MIN_BYTES is 256 KiB: a wide object below it takes the string path (which
// was always correct - which is why a live probe of small fhfa objects looked healthy) and
// only an object at or above it reaches the two paths below.
// =========================================================================================

function bucketOf(bytes: Uint8Array, contentEncoding?: string) {
  const mk = (range?: { offset: number; length: number }) => ({
    size: bytes.length,
    httpEtag: '"etag-1"',
    etag: "etag-1",
    httpMetadata: contentEncoding ? { contentEncoding } : {},
    body: new ReadableStream<Uint8Array>({
      start(c) {
        const b = range ? bytes.subarray(range.offset, range.offset + range.length) : bytes;
        const half = Math.ceil(b.length / 2);          // two chunks, so priming really iterates
        c.enqueue(b.subarray(0, half));
        c.enqueue(b.subarray(half));
        c.close();
      },
    }),
    arrayBuffer: async () => {
      const b = range ? bytes.subarray(range.offset, range.offset + range.length) : bytes;
      return b.slice().buffer;
    },
  });
  return {
    get: async (_k: string, o?: { range?: { offset: number; length: number } }) => mk(o?.range),
  };
}

/** Drive streamLarge exactly as the route does, on a ?raw=1 (bare) request. */
async function serve(body: string, opts: { wide: boolean; gzip: boolean }) {
  const raw = Buffer.from(body);
  const bytes = new Uint8Array(opts.gzip ? gzipSync(raw) : raw);
  const enc2 = opts.gzip ? "gzip" : undefined;
  const bucket = bucketOf(bytes, enc2);
  const obj = await bucket.get("k");
  const res = await streamLarge(
    obj as never, opts.gzip, "fhfa:annual_cbsa:01", "fhfa:annual_cbsa:01",
    {} as never, { SERIES_BUCKET: bucket } as never,
    { from: null, to: null, geo: null, allowAnyHeader: opts.wide },
    null, true, undefined, undefined);
  const buf = Buffer.from(await res.arrayBuffer());
  // the passthrough answers with the STORED gzip bytes and says so
  const text = res.headers.get("content-encoding") === "gzip"
    ? gunzipSync(buf).toString()
    : buf.toString();
  return { status: res.status, text };
}

// Wide and tidy bodies big enough to clear STREAM_MIN_BYTES (256 KiB) - that threshold is the
// whole reason these two paths were never exercised.
const BIG_FHFA = FHFA_HEADER + "\n"
  + Array.from({ length: 9000 },
      (_, i) => "annual_cbsa,01,2020-12-31," + (100 + i) + ".0,1.0,2.0,3.0").join("\n") + "\n";
const BIG_TIDY = CSV_HEADER + "\n"
  + Array.from({ length: 12000 },
      (_, i) => "fhfa:x,2020-01-" + ((i % 28) + 1) + "," + i).join("\n") + "\n";

test("SANITY: the fixtures really are large enough to take the streaming paths", () => {
  // Without this the tests below could pass while testing nothing, because an object under
  // the threshold never reaches streamLarge at all.
  assert.ok(Buffer.byteLength(BIG_FHFA) >= STREAM_MIN_BYTES,
    "wide fixture is " + Buffer.byteLength(BIG_FHFA) + " B, under " + STREAM_MIN_BYTES);
  assert.ok(Buffer.byteLength(BIG_TIDY) >= STREAM_MIN_BYTES,
    "tidy fixture is " + Buffer.byteLength(BIG_TIDY) + " B, under " + STREAM_MIN_BYTES);
});

test("PASSTHROUGH: a large GZIPPED wide object is served, not 502'd", async () => {
  // THE BLOCKER. `filtered` is false for every wide source (series.ts refuses from/to/geo for
  // one), so a gzipped wide object ALWAYS takes the passthrough branch - and that branch
  // called peekGzipHeader without the wide flag, so headerOk stayed false and it answered
  // 502 "stored bytes do not inflate to the contract header": the same outage, after the fix.
  const r = await serve(BIG_FHFA, { wide: true, gzip: true });
  assert.equal(r.status, 200, "expected 200, got " + r.status + ": " + r.text.slice(0, 200));
  assert.equal(r.text.split("\n")[0], FHFA_HEADER);
});

test("PASSTHROUGH keeps refusing a large gzipped TIDY object with a bad header", async () => {
  // CONTROL: the guard must survive the fix on this path too.
  const r = await serve("id,date,val\n" + "a,b,c\n".repeat(60000), { wide: false, gzip: true });
  assert.equal(r.status, 502);
  assert.match(r.text, /malformed/);
});

test("INFLATE: a large PLAIN wide object is labelled by its OWN header, never the canonical one", async () => {
  // THE SECOND BLOCKER. A wide object over the threshold that is NOT gzipped at rest skips the
  // passthrough and is rebuilt line by line. LineFilter swallowed the stored header and the
  // caller prepended CSV_HEADER, so this answered 200 with "series_id,obs_date,value" over
  // seven-column rows - a body that lies about itself, which is worse than the 502.
  const r = await serve(BIG_FHFA, { wide: true, gzip: false });
  assert.equal(r.status, 200, "expected 200, got " + r.status + ": " + r.text.slice(0, 200));
  const lines = r.text.split("\n").filter((l) => l !== "" && !l.startsWith("#"));
  assert.equal(lines[0], FHFA_HEADER, "body must open with the object's own header");
  assert.equal(r.text.includes(CSV_HEADER), false,
    "the canonical header must not appear anywhere in a wide body");
  const want = FHFA_HEADER.split(",").length;
  for (const l of lines) assert.equal(l.split(",").length, want, "column count of: " + l);
});

test("INFLATE still gives a large PLAIN TIDY object exactly one canonical header", async () => {
  // CONTROL for the test above, on the same path.
  const r = await serve(BIG_TIDY, { wide: false, gzip: false });
  assert.equal(r.status, 200, "expected 200, got " + r.status + ": " + r.text.slice(0, 200));
  assert.equal(r.text.split(CSV_HEADER).length - 1, 1, "exactly one canonical header");
  assert.equal(r.text.split("\n")[0], CSV_HEADER);
});
