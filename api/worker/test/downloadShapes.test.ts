// node --test test/downloadShapes.test.ts
//
// THE DOWNLOAD SHAPES, AS THE ROUTE HANDLER ANSWERS THEM. tools/selfhost/soak_keyed.py checks keyed downloads
// through the soak worker by shape, and its Python tests use a stand-in for the worker. The first version of
// that script and its stand-in agreed with each other and were both wrong (review AR-303, ledger R1545): the
// author read `streamLarge` without its branch condition and took a large PLAIN object for a passthrough. It
// is the pump. So the table lives HERE, measured on handleSeriesCsv with a fake catalogue and a fake bucket
// (the pattern of wideSources.test.ts), and tests/test_soak_keyed.py names this file as the source of its
// stand-in. Change the worker's shapes -> this test fails -> change the script and its stand-in with it.
//
//   STRING       stored below 256 KiB: content-length; citation lines first, or the contract header with raw=1;
//                the ETag only for the bare, whole answer.
//   PASSTHROUGH  stored gzip of 256 KiB or more, no window: the stored bytes, content-encoding gzip, exact
//                content-length, `x-econdl-citation-omitted`. (Also: an object of the wide-layout sources, plain
//                or gzip - their own header, never filtered.)
//   PUMP         a large PLAIN object of the contract layout, WITH OR WITHOUT a window, and a large gzip object
//                with a window: no content-length, `x-econdl-bytes-at-rest`, the last line
//                `# econdl-complete rows=N`; citation lines first unless raw=1; the ETag only bare and whole.
import { test } from "node:test";
import assert from "node:assert/strict";
import { register } from "node:module";
import { gzipSync, gunzipSync } from "node:zlib";
import { CSV_HEADER, STREAM_MIN_BYTES } from "../src/csvStream.ts";

register(new URL("./tsResolveHooks.mjs", import.meta.url));
const { handleSeriesCsv } = await import("../src/series.ts");

type Stored = { bytes: Uint8Array; gzip: boolean; etag: string };
const dec = new TextDecoder();
function objectKey(id: string): string {
  return "series/" + encodeURIComponent(id).replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase()) + ".csv";
}
function streamOf(b: Uint8Array, chunk = 65536): ReadableStream<Uint8Array> {
  const parts: Uint8Array[] = [];
  for (let o = 0; o < b.length; o += chunk) parts.push(b.subarray(o, Math.min(b.length, o + chunk)));
  return new ReadableStream<Uint8Array>({ start(c) { for (const p of parts) c.enqueue(p); c.close(); } });
}
function makeEnv(store: Map<string, Stored>) {
  const bucket = {
    get: async (key: string, o?: { range?: { offset: number; length: number }; onlyIf?: { etagMatches?: string } }) => {
      const s = store.get(key);
      if (!s) return null;
      const meta = { key, size: s.bytes.length, etag: s.etag, httpEtag: `"${s.etag}"`,
                     httpMetadata: s.gzip ? { contentEncoding: "gzip" } : {} };
      if (o?.onlyIf?.etagMatches !== undefined && o.onlyIf.etagMatches !== s.etag) return meta;
      const b = o?.range ? s.bytes.subarray(o.range.offset, o.range.offset + o.range.length) : s.bytes;
      return { ...meta, body: streamOf(b), arrayBuffer: async () => b.slice().buffer, text: async () => dec.decode(b) };
    },
  };
  const rows: Record<string, unknown> = {};
  for (const key of store.keys()) {
    const id = decodeURIComponent(key.slice("series/".length, -".csv".length));
    rows[id] = { series_id: id, source_id: id.split(":")[0], license_id: "lic-test", metadata: null, title: id };
  }
  const d1 = { prepare: (sql: string) => ({ bind: (...args: unknown[]) => ({ first: async () => {
    if (/FROM series\b/.test(sql)) return rows[String(args[0])] ?? null;
    if (/FROM source\b/.test(sql)) return { source_id: args[0], license_id: "lic-test", name: "Test publisher", url: "https://example.org" };
    if (/FROM license\b/.test(sql)) return { license_id: "lic-test", name: "CC BY 4.0", commercial_ok: 1, attribution_required: 1 };
    return null;
  } }) }) };
  return { CATALOG: d1, CATALOG_CLIMATE: d1, SERIES_BUCKET: bucket, SUPPORTED_SOURCES: "fhfa,census,wikidata,treasury,bls" } as never;
}

let lcg = 12345;
const rnd = () => (lcg = (lcg * 1103515245 + 12345) % 2147483648);
function canonical(id: string, rows: number): string {
  const lines = [CSV_HEADER];
  for (let i = 0; i < rows; i++) {
    lines.push(`${id},${String(1900 + Math.floor(i / 12)).padStart(4, "0")}-${String(i % 12 + 1).padStart(2, "0")}-01,${rnd()}${rnd()}${rnd()}`);
  }
  return lines.join("\n") + "\n";
}
function wide(rows: number): string {
  const lines = ["dataset,series_key,obs_date,hpi,hpi_1990,hpi_2000,annual_change"];
  for (let i = 0; i < rows; i++) lines.push(`annual_cbsa,k${i},2020-12-31,${rnd()},${rnd()},${rnd()},${rnd()}`);
  return lines.join("\n") + "\n";
}
const store = new Map<string, Stored>();
function put(id: string, text: string, gzip: boolean): number {
  const raw = Buffer.from(text);
  const bytes = new Uint8Array(gzip ? gzipSync(raw) : raw);
  store.set(objectKey(id), { bytes, gzip, etag: "etag-" + id.replace(/\W/g, "") });
  return bytes.length;
}
const SMALL = "bls:SMALL.PLAIN", LPL = "bls:LARGE.PLAIN", LGZ = "bls:LARGE.GZ", WPL = "fhfa:LARGE.WIDE.PLAIN";
const size: Record<string, number> = {
  [SMALL]: put(SMALL, canonical(SMALL, 240), false),
  [LPL]: put(LPL, canonical(LPL, 9000), false),
  [LGZ]: put(LGZ, canonical(LGZ, 40000), true),
  [WPL]: put(WPL, wide(9000), false),
};
const WIN = "from=1000-01-01&to=2999-12-31";

type Answer = { status: number; shape: string; length: string | null; encoding: string | null; etag: boolean; omitted: string | null;
                atRest: string | null; bytes: number; first: string; last: string; error: string | null };
async function ask(id: string, query: string): Promise<Answer> {
  const url = new URL(`https://econdl.test/v1/series/${encodeURIComponent(id)}.csv${query ? "?" + query : ""}`);
  const res: Response = await handleSeriesCsv(id, url, makeEnv(store), undefined, undefined);
  const buf = Buffer.from(await res.arrayBuffer());
  const h = res.headers;
  const text = h.get("content-encoding") === "gzip" && res.status === 200 ? gunzipSync(buf).toString() : buf.toString();
  const lines = text.split("\n").filter((x) => x !== "");
  const shape = res.status !== 200 ? "refused" : h.get("x-econdl-bytes-at-rest") !== null ? "PUMP"
    : h.get("x-econdl-citation-omitted") !== null ? "PASSTHROUGH" : "STRING";
  let error: string | null = null;
  if (res.status !== 200) { try { error = JSON.parse(text).error; } catch { error = "?"; } }
  return { status: res.status, shape, length: h.get("content-length"), encoding: h.get("content-encoding"), etag: h.get("etag") !== null,
           omitted: h.get("x-econdl-citation-omitted"), atRest: h.get("x-econdl-bytes-at-rest"), bytes: buf.length,
           first: lines[0] ?? "", last: lines[lines.length - 1] ?? "", error };
}

test("the stand-in objects are of the sizes the cases need", () => {
  assert.ok(size[SMALL] < STREAM_MIN_BYTES);
  for (const id of [LPL, LGZ, WPL]) assert.ok(size[id] >= STREAM_MIN_BYTES, id);
});

test("STRING: a small object has a content-length; the ETag only bare and whole", async () => {
  const cited = await ask(SMALL, "");
  assert.deepEqual([cited.status, cited.shape, cited.etag, cited.first.startsWith("#")], [200, "STRING", false, true]);
  assert.equal(cited.length, String(cited.bytes));
  const bare = await ask(SMALL, "raw=1");
  assert.deepEqual([bare.status, bare.shape, bare.etag, bare.first, bare.length], [200, "STRING", true, CSV_HEADER, String(bare.bytes)]);
  const win = await ask(SMALL, "raw=1&from=1905-01-01&to=1909-12-01");
  assert.deepEqual([win.status, win.shape, win.etag, win.first, win.length], [200, "STRING", false, CSV_HEADER, String(win.bytes)]);
  assert.ok(win.bytes < bare.bytes);
});

test("PASSTHROUGH: a large stored-gzip object without a window goes out as stored", async () => {
  for (const q of ["", "raw=1"]) {
    const a = await ask(LGZ, q);
    assert.deepEqual([a.status, a.shape, a.encoding, a.omitted, a.atRest, a.length], [200, "PASSTHROUGH", "gzip", "large-object", null, String(size[LGZ])], q);
    assert.equal(a.bytes, size[LGZ]);
    assert.equal(a.etag, q === "raw=1");
  }
});

test("PUMP: a large PLAIN object of the contract layout is the pump WITH OR WITHOUT a window", async () => {
  const whole = await ask(LPL, "");
  assert.deepEqual([whole.status, whole.shape, whole.length, whole.omitted, whole.encoding], [200, "PUMP", null, null, null]);
  assert.equal(whole.atRest, String(size[LPL]));
  assert.ok(whole.first.startsWith("#"), "citation lines first");
  assert.match(whole.last, /^# econdl-complete rows=9000$/);
  const bare = await ask(LPL, "raw=1");
  assert.deepEqual([bare.status, bare.shape, bare.length, bare.omitted, bare.etag, bare.first], [200, "PUMP", null, null, true, CSV_HEADER]);
  assert.match(bare.last, /^# econdl-complete rows=9000$/);
  const win = await ask(LPL, "raw=1&" + WIN);
  assert.deepEqual([win.status, win.shape, win.length, win.omitted, win.etag, win.first], [200, "PUMP", null, null, false, CSV_HEADER]);
  assert.match(win.last, /^# econdl-complete rows=9000$/);
});

test("PUMP: a large stored-gzip object with a window", async () => {
  const a = await ask(LGZ, "raw=1&" + WIN);
  assert.deepEqual([a.status, a.shape, a.length, a.omitted, a.encoding, a.etag, a.first], [200, "PUMP", null, null, null, false, CSV_HEADER]);
  // 40,000 monthly rows from 1900 on: the window to 2999 holds (2999 - 1900 + 1) x 12 = 13,200 of them
  assert.match(a.last, /^# econdl-complete rows=13200$/);
});

test("a wide-layout object is a plain PASSTHROUGH with its length, and is never filtered", async () => {
  const a = await ask(WPL, "");
  assert.deepEqual([a.status, a.shape, a.encoding, a.omitted, a.length], [200, "PASSTHROUGH", null, "large-object", String(size[WPL])]);
  assert.ok(a.first.startsWith("dataset,series_key"));
  const refused = await ask(WPL, "raw=1&" + WIN);
  assert.deepEqual([refused.status, refused.error], [400, "unsupported_filter"]);
});
