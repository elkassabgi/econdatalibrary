// node --test test/csvStream.test.ts   (Node >= 22.6 strips types natively; web streams are global)
import { test } from "node:test";
import assert from "node:assert/strict";
import { gzipSync } from "node:zlib";
import {
  CARRY_MAX, CSV_HEADER, FILTER_MAX_STORED_BYTES, LineFilter, VerifiedGunzip, crc32Update, identityPipe,
  isGzipMagic, isizeFromTrailer, newStats, peekGzipHeader, primePump, rowPasses, trailerOf,
} from "../src/csvStream.ts";

const enc = new TextEncoder();
const dec = new TextDecoder();
const bytes = (s: string) => enc.encode(s);
const NONE = { from: null, to: null, geo: null };
const DATA = `${CSV_HEADER}\ncbs_nl:a:NL,2020-01-01,1.0\ncbs_nl:a:BE,2020-06-01,2.0\ncbs_nl:a:NL,2021-01-01,3.0\n`;

function fromChunks(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({ start(c) { for (const ch of chunks) c.enqueue(ch); c.close(); } });
}
async function drain(rs: ReadableStream<Uint8Array>): Promise<string> {
  const r = rs.getReader(); const parts: Uint8Array[] = [];
  for (;;) { const { value, done } = await r.read(); if (done) break; parts.push(value); }
  const out = new Uint8Array(parts.reduce((n, p) => n + p.length, 0));
  let o = 0; for (const p of parts) { out.set(p, o); o += p.length; }
  return dec.decode(out);
}
function splits(b: Uint8Array, n = 9): Uint8Array[][] {
  const out: Uint8Array[][] = [[b]];
  for (let i = 1; i < b.length; i += Math.max(1, Math.floor(b.length / n))) {
    out.push([b.subarray(0, i), b.subarray(i)]);
    out.push([b.subarray(0, i), new Uint8Array(0), b.subarray(i)]);
  }
  return out;
}
/** Run the pump end to end through the identity pipe. */
async function pumpAll(chunks: Uint8Array[], step: (c: Uint8Array) => Iterable<Uint8Array>, finish: () => Iterable<Uint8Array>, stats = newStats()) {
  const primed = await primePump(fromChunks(chunks), step, finish, stats);
  if (primed.first === null) return { text: "", stats, written: 0 };
  const { readable, writable } = identityPipe();
  const done = primed.run(writable.getWriter());
  const text = await drain(readable);
  const written = await done;
  return { text, stats, written };
}
function filterPipeline(opts = NONE, stats = newStats()) {
  const lf = new LineFilter(opts, stats);
  return { step: (c: Uint8Array) => [lf.push(c)], finish: () => [lf.flush()], stats };
}

test("LineFilter passes every row and consumes the header, for any chunking", async () => {
  for (const chunks of splits(bytes(DATA))) {
    const p = filterPipeline();
    const { text, stats } = await pumpAll(chunks, p.step, p.finish, p.stats);
    assert.equal(text, DATA.slice(CSV_HEADER.length + 1));
    assert.equal(stats.rows, 3); assert.equal(stats.headerOk, true);
    assert.equal(stats.bytesOut, text.length);
  }
});

test("window and geo filters; geo-never-matched vs geo-matched-but-window-empty (R582 F3)", async () => {
  let p = filterPipeline({ from: "2020-06-01", to: "2020-12-31", geo: null });
  assert.equal((await pumpAll([bytes(DATA)], p.step, p.finish, p.stats)).text, "cbs_nl:a:BE,2020-06-01,2.0\n");
  p = filterPipeline({ from: null, to: null, geo: "NL" });
  assert.equal((await pumpAll([bytes(DATA)], p.step, p.finish, p.stats)).text, "cbs_nl:a:NL,2020-01-01,1.0\ncbs_nl:a:NL,2021-01-01,3.0\n");
  assert.equal(p.stats.geoMatched, true);
  const none = filterPipeline({ from: null, to: null, geo: "XX" });
  await pumpAll([bytes(DATA)], none.step, none.finish, none.stats);
  assert.equal(none.stats.geoMatched, false); assert.deepEqual([...none.stats.geos].sort(), ["BE", "NL"]);
  const win = filterPipeline({ from: "2099-01-01", to: null, geo: "NL" });
  await pumpAll([bytes(DATA)], win.step, win.finish, win.stats);
  assert.equal(win.stats.geoMatched, true); assert.equal(win.stats.rows, 0);
});

test("LineFilter keeps a row's CR, flags a wrong header, drops malformed/blank rows, caps the carry (R582 F5)", async () => {
  const p = filterPipeline();
  const { text, stats } = await pumpAll([bytes("id,date,value\r\nnocomma\r\n\r\n  \nx:NL,2020-01-01,1\r\n")], p.step, p.finish, p.stats);
  assert.equal(stats.headerOk, false); assert.equal(text, "x:NL,2020-01-01,1\r\n"); assert.equal(stats.rows, 1);
  const big = new Uint8Array(CARRY_MAX + 10).fill(0x61);
  const q = filterPipeline();
  await assert.rejects(primePump(fromChunks([bytes(CSV_HEADER + "\n"), big]), q.step, q.finish, q.stats), /malformed object/);
  // R599: one over-long line contained in a single push is refused too
  const r = filterPipeline();
  const oneLine = new Uint8Array([...bytes(CSV_HEADER + "\n"), ...new Uint8Array(CARRY_MAX + 10).fill(0x61), 0x0a, ...bytes("k,2020-01-01,1\n")]);
  await assert.rejects(primePump(fromChunks([oneLine]), r.step, r.finish, r.stats), /malformed object/);
});

test("rowPasses handles a row without a third column", () => {
  const s = newStats();
  assert.equal(rowPasses("k,2020-01-01", { from: "2020-01-01", to: null, geo: null }, s), true);
  assert.equal(rowPasses("k,2019-12-31", { from: "2020-01-01", to: null, geo: null }, s), false);
});

test("VerifiedGunzip inflates chunk by chunk and verifies CRC32 + ISIZE, for any chunking", async () => {
  const rows = DATA.slice(CSV_HEADER.length + 1).repeat(2000);
  const text = CSV_HEADER + "\n" + rows;
  const gz = new Uint8Array(gzipSync(Buffer.from(text)));
  assert.equal(isGzipMagic(gz), true);
  assert.equal(isizeFromTrailer(gz.subarray(gz.length - 4)), bytes(text).length);
  const cases: Uint8Array[][] = [[gz], [gz.subarray(0, 10), gz.subarray(10)], [gz.subarray(0, gz.length - 3), gz.subarray(gz.length - 3)]];
  for (let i = 1; i < gz.length; i += Math.max(1, Math.floor(gz.length / 7))) cases.push([gz.subarray(0, i), gz.subarray(i)]);
  for (const chunks of cases) {
    const stats = newStats(); const vg = new VerifiedGunzip(stats); const lf = new LineFilter(NONE, stats);
    const { text: out } = await pumpAll(chunks, (c) => [lf.push(vg.push(c))], () => [lf.push(vg.finish()), lf.flush()], stats);
    assert.equal(out, rows); assert.equal(stats.rows, 6000); assert.equal(stats.bytesIn, gz.length); assert.equal(stats.bytesInflated, bytes(text).length);
  }
});

test("a flipped byte, a truncated member, trailing garbage and a second member are REFUSED, not served (R585)", async () => {
  const text = CSV_HEADER + "\n" + DATA.slice(CSV_HEADER.length + 1).repeat(300);
  const gz = new Uint8Array(gzipSync(Buffer.from(text)));
  const run = async (chunks: Uint8Array[]) => {
    const stats = newStats(); const vg = new VerifiedGunzip(stats); const lf = new LineFilter(NONE, stats);
    let primed;
    try {
      primed = await primePump(fromChunks(chunks), (c) => [lf.push(vg.push(c))], () => [lf.push(vg.finish()), lf.flush()], stats);
    } catch (e) {
      assert.match(String((e as Error).message), /malformed object/);   // refused while priming -> 502, nothing sent
      return "primed-refusal";
    }
    const { readable, writable } = identityPipe();
    const done = primed.run(writable.getWriter());
    let err: unknown = null;
    try { await drain(readable); } catch (e) { err = e; }
    await assert.rejects(done, /malformed object/);                     // refused mid-run -> aborted transfer
    return err;
  };
  const flipped = gz.slice(); flipped[Math.floor(gz.length / 2)] ^= 0x10;
  await run([flipped]);                                                   // CRC mismatch (fflate itself does not check)
  await run([gz.subarray(0, gz.length - 20)]);                           // truncated: trailer missing / mismatch
  const garbage = new Uint8Array([...gz, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
  await run([garbage]);
  const two = new Uint8Array([...gz, ...gz]);                             // second member
  await run([two]);
});

test("a mid-run error ABORTS the client side instead of ending cleanly (R585)", async () => {
  // header + rows, then an over-long line late in the object
  const good = bytes(CSV_HEADER + "\nk,2020-01-01,1\n");
  const bad = new Uint8Array(CARRY_MAX + 5).fill(0x62);
  const p = filterPipeline();
  const primed = await primePump(fromChunks([good, bad]), p.step, p.finish, p.stats);
  assert.ok(primed.first !== null);
  const { readable, writable } = identityPipe();
  const done = primed.run(writable.getWriter());
  await assert.rejects(drain(readable));       // the reader sees an error, not a clean EOF
  await assert.rejects(done, /malformed object/);
});

test("primePump reports EOF-with-nothing as null and counts written bytes", async () => {
  const p = filterPipeline();
  const primed = await primePump(fromChunks([bytes(CSV_HEADER + "\n\n\n")]), p.step, p.finish, p.stats);
  assert.equal(primed.first, null);
  assert.equal(p.stats.rows, 0); assert.equal(p.stats.headerOk, true);
});

test("peekGzipHeader validates the header and sees a data row from the first stored chunk(s)", () => {
  const gz = new Uint8Array(gzipSync(Buffer.from(DATA)));
  assert.deepEqual(peekGzipHeader([gz]), { headerOk: true, hasRow: true });
  assert.deepEqual(peekGzipHeader([gz.subarray(0, 12), gz.subarray(12)]), { headerOk: true, hasRow: true });
  const bad = new Uint8Array(gzipSync(Buffer.from("id,date,val\n1,2,3\n")));
  assert.equal(peekGzipHeader([bad]).headerOk, false);
  const only = new Uint8Array(gzipSync(Buffer.from(CSV_HEADER + "\n")));
  assert.deepEqual(peekGzipHeader([only]), { headerOk: true, hasRow: false });
  assert.equal(peekGzipHeader([bytes("not gzip at all")]).headerOk, false);
});

test("crc32 and trailer helpers agree with zlib", () => {
  const text = "hello, world\n".repeat(100);
  const gz = new Uint8Array(gzipSync(Buffer.from(text)));
  const { crc, isize } = trailerOf(gz.subarray(gz.length - 8));
  assert.equal(isize, text.length);
  assert.equal(crc32Update(0, bytes(text)), crc);
  assert.ok(FILTER_MAX_STORED_BYTES > 100 * 1024 * 1024 && FILTER_MAX_STORED_BYTES < 120 * 1024 * 1024);
});


test("a step that yields several pieces has them emitted in order, priming on the first non-empty one (R599)", async () => {
  const chunk = bytes(CSV_HEADER + "\nk,2020-01-01,1\nk,2020-02-01,2\nk,2020-03-01,3\n");
  const stats = newStats(); const lf = new LineFilter(NONE, stats);
  // split each chunk into 7-byte slices, each slice a separate piece
  const step = (c: Uint8Array) => { const out: Uint8Array[] = []; for (let o = 0; o < c.length; o += 7) out.push(lf.push(c.subarray(o, Math.min(c.length, o + 7)))); return out; };
  const { text } = await pumpAll([chunk], step, () => [lf.flush()], stats);
  assert.equal(text, "k,2020-01-01,1\nk,2020-02-01,2\nk,2020-03-01,3\n");
  assert.equal(stats.rows, 3);
});


test("the step is consumed LAZILY: priming evaluates only up to the first non-empty piece; the rest waits for the writer (R603)", async () => {
  const chunk = bytes(CSV_HEADER + "\nk,2020-01-01,1\nk,2020-02-01,2\nk,2020-03-01,3\n");
  const stats = newStats(); const lf = new LineFilter(NONE, stats);
  let evaluated = 0;
  const step = function* (c: Uint8Array): Generator<Uint8Array> {
    for (let o = 0; o < c.length; o += 7) { evaluated++; yield lf.push(c.subarray(o, Math.min(c.length, o + 7))); }
  };
  const primed = await primePump(fromChunks([chunk]), step, function* () { yield lf.flush(); }, stats);
  assert.ok(primed.first !== null);
  const total = Math.ceil(chunk.length / 7);
  assert.ok(evaluated < total, `priming evaluated ${evaluated} of ${total} slices - must stop at the first non-empty piece`);
  const { readable, writable } = identityPipe();
  const done = primed.run(writable.getWriter());
  const text = await drain(readable);
  await done;
  assert.equal(evaluated, total);
  assert.equal(text, "k,2020-01-01,1\nk,2020-02-01,2\nk,2020-03-01,3\n");
});
