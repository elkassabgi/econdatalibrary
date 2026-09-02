// ---------------------------------------------------------------------------
// src/csvStream.ts  --  serve a large CSV object without ever holding it.
//
// WHY. handleSeriesCsv materialised every object as one string (`.text()`),
// then copied it through the geo/window filters and the citation prepend. A
// Workers isolate has 128 MB; the largest served objects measured 2026-09-01
// are 554 MB GZIPPED (cbs_nl:71493ned, ~2.7-5.4 GB of text) and 51 cbs_nl
// objects exceed 100 MB gzipped. Every one was catalogued and undeliverable
// (Error 1102) - the advertised-but-undeliverable shape. Ledger R579/R582/R585.
//
// WHAT THREE REVIEW ROUNDS MEASURED, which drives every choice here:
//   * workerd's DecompressionStream runs AHEAD of its consumer (R582), and so
//     does ANY pipeThrough chain: the whole object is inflated and filtered
//     into transform queues regardless of the client's pace (R585: 534 MB of
//     output peaked at 752-792 MB, fast or slow client). Web-stream
//     backpressure does not bound it. So there is NO pipeThrough here: a
//     PULL-DRIVEN PUMP reads one R2 chunk, inflates it (fflate, synchronous),
//     filters it, and `await`s the write into an IdentityTransformStream -
//     workerd's native byte pipe, whose write resolves only when the client
//     side has taken the bytes.
//   * inflating costs 15-40 CPU-s per GB of text; passing the STORED GZIP
//     BYTES through untouched costs ~0.7 CPU-s per GB with flat memory. So the
//     unfiltered path never inflates beyond its first chunk (header check).
//   * a mid-stream error must not end as a clean EOF on a 200 (R585): the pump
//     ABORTS the identity stream, so the client sees a broken transfer.
//   * fflate verifies neither CRC32 nor ISIZE (R585): the pump computes CRC32
//     over the inflated bytes and checks both against the gzip trailer; a
//     second gzip member is refused (the derive writes exactly one).
//   * ISIZE is mod 2^32 and forgeable: the filter path is refused outright for
//     objects whose stored size times the fleet's largest measured ratio
//     (37.5x) could wrap 4 GiB, and otherwise budgeted on ISIZE.
//   * a two-member gzip (citation member + object member) is NOT decoded by
//     curl --compressed (stops after the first member, exit 23), so the
//     citation cannot be prepended to the stored bytes: large unfiltered
//     responses omit the in-body citation and say so in headers.
//
// Both shapes are PRIMED before the Response exists (header validated, first
// data row seen), so "0 data rows -> 502" and the geo 404 keep their honest
// status - never an empty 200 (CONTRACT.md).
// ---------------------------------------------------------------------------
import { Gunzip } from "fflate";

export const CSV_HEADER = "series_id,obs_date,value";

/** Objects at or above this AT-REST size use the streaming shapes (~2.5-9.6 MB of
 *  text at the fleet's ratios; the string path makes several copies). */
export const STREAM_MIN_BYTES = 256 * 1024;

/** Largest DECOMPRESSED size the inflate shape will process (~15-40 CPU-s/GB measured;
 *  cpu_ms = 300000 in wrangler.toml). Above it: refused up front with an actionable 400. */
export const FILTER_MAX_TEXT_BYTES = 1_500_000_000;

/** Largest STORED size for which the gzip ISIZE cannot have wrapped 4 GiB at the fleet's
 *  largest measured ratio (37.5x): 4 GiB / 37.5. Above it the inflate shape is refused
 *  without reading the trailer (R585: a forged or wrapped ISIZE defeated the budget). */
export const FILTER_MAX_STORED_BYTES = Math.floor(4 * 1024 * 1024 * 1024 / 37.5);

/** Output bytes accumulated before one awaited write to the client side. */
export const WRITE_COALESCE = 256 * 1024;

/** Stored bytes are fed to the inflater in slices of at most this size: the per-step transient
 *  is slice x ratio (R593: one whole R2 chunk of a forged 412x object drove the working set to
 *  252 MB; 64 KiB x 37.5 = 2.4 MB). */
export const INFLATE_SLICE = 16 * 1024;   // R608 round 7: 16 KiB scales every per-slice buffer 4x down for negligible CPU

/** Largest plausible decompression ratio in the fleet (measured 37.5x). An ISIZE above
 *  stored x this is forged or wrapped and the inflate shape is refused (R593). */
export const MAX_RATIO = 37.5;

/** The last line of every inflate-shape response. workerd delivers writer.abort() to the client
 *  as a clean chunked EOF (R593: every abort mechanism, measured), so a truncated 200 is
 *  indistinguishable from a complete one without an in-band marker. CONTRACT.md makes it
 *  mandatory: a filtered/plain response that does not end with it was cut off. */
export function completeLine(rows: number): Uint8Array {
  return new TextEncoder().encode(`# econdl-complete rows=${rows}\n`);
}

/** Longest line (newline-free run) tolerated before the object is declared malformed. */
export const CARRY_MAX = 1 << 20;

const NL = 0x0a;
const CR = 0x0d;

export interface FilterOpts {
  from: string | null;
  to: string | null;
  geo: string | null;
}

export interface StreamStats {
  rows: number;
  headerOk: boolean | null;
  geoMatched: boolean;
  geos: Set<string>;
  malformed: string | null;
  bytesIn: number;       // stored bytes consumed
  bytesInflated: number; // inflated bytes produced (inflate shape)
  bytesOut: number;      // bytes WRITTEN to the client side (awaited)
}

const GEOS_CAP = 5000;

export function newStats(): StreamStats {
  return { rows: 0, headerOk: null, geoMatched: false, geos: new Set(), malformed: null,
           bytesIn: 0, bytesInflated: 0, bytesOut: 0 };
}

export function rowPasses(line: string, opts: FilterOpts, stats: StreamStats): boolean {
  if (line.length === 0 || line.trim() === "") return false;
  const c1 = line.indexOf(",");
  if (c1 < 0) return false;
  if (opts.geo !== null) {
    const id = line.slice(0, c1);
    const seg = id.slice(id.lastIndexOf(":") + 1);
    if (seg !== opts.geo) {
      if (!stats.geoMatched && stats.geos.size < GEOS_CAP) stats.geos.add(seg);
      return false;
    }
    stats.geoMatched = true;
  }
  if (opts.from !== null || opts.to !== null) {
    const c2 = line.indexOf(",", c1 + 1);
    const obsDate = c2 < 0 ? line.slice(c1 + 1) : line.slice(c1 + 1, c2);
    if (opts.from !== null && obsDate < opts.from) return false;
    if (opts.to !== null && obsDate > opts.to) return false;
  }
  return true;
}

function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  if (a.length === 0) return b;
  const out = new Uint8Array(a.length + b.length);
  out.set(a, 0); out.set(b, a.length);
  return out;
}

function join(parts: Uint8Array[]): Uint8Array {
  let n = 0;
  for (const p of parts) n += p.length;
  const out = new Uint8Array(n);
  let o = 0;
  for (const p of parts) { out.set(p, o); o += p.length; }
  return out;
}

// --- CRC32 (IEEE), table-based --------------------------------------------
const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    t[n] = c >>> 0;
  }
  return t;
})();

export function crc32Update(crc: number, buf: Uint8Array): number {
  let c = ~crc >>> 0;
  for (let i = 0; i < buf.length; i++) c = CRC_TABLE[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
  return ~c >>> 0;
}

/** gzip trailer: CRC32 (LE) then ISIZE (LE, mod 2^32) - the last 8 bytes of a member. */
export function trailerOf(last8: Uint8Array): { crc: number; isize: number } {
  const t = last8.subarray(last8.length - 8);
  const crc = ((t[0] | (t[1] << 8) | (t[2] << 16)) + t[3] * 0x1000000) >>> 0;
  const isize = ((t[4] | (t[5] << 8) | (t[6] << 16)) + t[7] * 0x1000000) >>> 0;
  return { crc, isize };
}

export function isizeFromTrailer(trailer: Uint8Array): number {
  if (trailer.length < 4) return -1;
  const t = trailer.subarray(trailer.length - 4);
  return (t[0] | (t[1] << 8) | (t[2] << 16)) + t[3] * 0x1000000;
}

export function isGzipMagic(first: Uint8Array): boolean {
  return first.length >= 3 && first[0] === 0x1f && first[1] === 0x8b && first[2] === 0x08;
}

/** Split a stored chunk into INFLATE_SLICE-sized pieces so one step never inflates more than
 *  a slice's worth. */
export function slices(chunk: Uint8Array, size = INFLATE_SLICE): Uint8Array[] {
  if (chunk.length <= size) return [chunk];
  const out: Uint8Array[] = [];
  for (let o = 0; o < chunk.length; o += size) out.push(chunk.subarray(o, Math.min(chunk.length, o + size)));
  return out;
}

// --- line filter over bytes, incremental --------------------------------------
/** Feed inflated bytes; returns the passing rows' ORIGINAL bytes ("\n"-terminated). The
 *  header line is consumed and validated. Throws on a carry beyond CARRY_MAX. */
export class LineFilter {
  private dec = new TextDecoder();
  private carry: Uint8Array = new Uint8Array(0);
  private first = true;
  private opts: FilterOpts;
  private stats: StreamStats;
  constructor(opts: FilterOpts, stats: StreamStats) { this.opts = opts; this.stats = stats; }

  /** Decide one line; copy it into `out` at `pos` when it passes. Returns the new pos.
   *  R608: pushing a view object per row and joining afterwards cost 203 B per passing row
   *  (+238 MB on one 26 MB slice); a single preallocated buffer costs the input's size once. */
  private handle(buf: Uint8Array, start: number, endExcl: number, out: Uint8Array, pos: number): number {
    const len = endExcl - start;
    if (len > CARRY_MAX) {   // R599: a whole over-long line inside one push, not only a carry
      this.stats.malformed = `a line longer than ${CARRY_MAX} bytes`;
      throw new Error(`malformed object: ${this.stats.malformed}`);
    }
    let end = endExcl;
    if (end > start && buf[end - 1] === CR) end--;
    const line = this.dec.decode(buf.subarray(start, end));
    if (this.first) {
      this.first = false;
      this.stats.headerOk = line === CSV_HEADER;
      return pos;
    }
    if (rowPasses(line, this.opts, this.stats)) {
      this.stats.rows++;
      out.set(buf.subarray(start, endExcl), pos);   // the row's ORIGINAL bytes (CR kept)
      pos += len;
      out[pos++] = NL;
    }
    return pos;
  }

  push(chunk: Uint8Array): Uint8Array {
    const buf = this.carry.length ? concat(this.carry, chunk) : chunk;   // no copy when nothing is carried
    const out = new Uint8Array(buf.length + 1);   // output never exceeds input (+1 for a final NL)
    let pos = 0;
    let start = 0;
    for (;;) {
      const nl = buf.indexOf(NL, start);
      if (nl < 0) break;
      pos = this.handle(buf, start, nl, out, pos);
      start = nl + 1;
    }
    this.carry = buf.slice(start);   // a COPY, so `buf` is released with this call
    if (this.carry.length > CARRY_MAX) {
      this.stats.malformed = `a line longer than ${CARRY_MAX} bytes`;
      throw new Error(`malformed object: ${this.stats.malformed}`);
    }
    return pos === 0 ? new Uint8Array(0) : out.slice(0, pos);
  }

  flush(): Uint8Array {
    if (!this.carry.length) return new Uint8Array(0);
    const out = new Uint8Array(this.carry.length + 1);
    const pos = this.handle(this.carry, 0, this.carry.length, out, 0);
    this.carry = new Uint8Array(0);
    return pos === 0 ? new Uint8Array(0) : out.slice(0, pos);
  }
}

// --- gunzip under our control, with integrity ----------------------------------
/** Feed stored gzip bytes; returns inflated bytes synchronously. Verifies CRC32 and ISIZE
 *  against the member trailer at finish(); refuses a second member. */
export class VerifiedGunzip {
  private gz: Gunzip;
  private pending: Uint8Array[] = [];
  private crc = 0;
  private size = 0;
  private last8: Uint8Array<ArrayBufferLike> = new Uint8Array(0);
  private members = 0;
  private stats: StreamStats;
  constructor(stats: StreamStats) {
    this.stats = stats;
    this.gz = new Gunzip((data) => {
      if (data.length) { this.pending.push(data as Uint8Array); this.crc = crc32Update(this.crc, data); this.size += data.length; }
    });
    this.gz.onmember = () => {
      this.members++;
      if (this.members > 1) throw new Error("malformed object: a second gzip member (the derive writes exactly one)");
    };
  }
  push(chunk: Uint8Array): Uint8Array {
    this.stats.bytesIn += chunk.length;
    const tail = concat(this.last8, chunk);
    this.last8 = tail.subarray(Math.max(0, tail.length - 8));
    try {
      this.gz.push(chunk, false);
    } catch (e) {
      throw new Error(`malformed object: ${String((e as Error).message ?? e)}`);
    }
    const out = join(this.pending); this.pending = [];
    this.stats.bytesInflated += out.length;
    return out;
  }
  finish(): Uint8Array {
    try {
      this.gz.push(new Uint8Array(0), true);
    } catch (e) {
      throw new Error(`malformed object: ${String((e as Error).message ?? e)}`);
    }
    const out = join(this.pending); this.pending = [];
    this.stats.bytesInflated += out.length;
    if (this.last8.length < 8) throw new Error("malformed object: shorter than a gzip trailer");
    const { crc, isize } = trailerOf(this.last8);
    if (crc !== this.crc) throw new Error(`malformed object: CRC32 mismatch (trailer ${crc.toString(16)}, data ${this.crc.toString(16)})`);
    if (isize !== (this.size % 0x100000000)) throw new Error(`malformed object: ISIZE mismatch (trailer ${isize}, data ${this.size})`);
    return out;
  }
}

// --- the pump --------------------------------------------------------------------
/** A source of stored bytes with a pull interface. */
export type ByteSource = ReadableStream<Uint8Array>;

export interface Primed {
  /** null at EOF with nothing to send */
  first: Uint8Array | null;
  /** continue pumping into `writer`; resolves with the bytes written (incl. `first`) */
  run: (writer: WritableStreamDefaultWriter<Uint8Array>) => Promise<number>;
}

/** Build a primed pump. `step` turns one stored chunk into a LIST of output pieces (one per
 *  inflate slice; pieces may be empty); `finish` is called at EOF and may return trailing
 *  pieces. Priming pulls until the first non-empty piece so the caller can decide the STATUS
 *  before the Response exists; pieces produced after it in the same step are queued, not
 *  concatenated (R599: concatenating a chunk's slices rebuilt the chunk x ratio transient).
 *  The returned `run` writes `first`, the queued pieces, then one piece per coalesced
 *  `await writer.write()` - the backpressure comes from the identity stream, not from any
 *  transform queue. On an error mid-run the writer is ABORTED (a broken transfer, never a
 *  clean EOF on a 200). */
export async function primePump(
  source: ByteSource, step: (chunk: Uint8Array) => Iterable<Uint8Array>, finish: () => Iterable<Uint8Array>, stats: StreamStats,
): Promise<Primed> {
  const reader = source.getReader();
  let first: Uint8Array | null = null;
  let rest: Iterator<Uint8Array> | null = null;   // the SAME iterator, resumed by run (R603: lazy)
  let eof = false;
  // Pull pieces one at a time; stop at the first non-empty one WITHOUT evaluating the rest.
  const take = (pieces: Iterable<Uint8Array>): boolean => {
    const it = pieces[Symbol.iterator]();
    for (;;) {
      const r = it.next();
      if (r.done) return false;
      if (!r.value.length) continue;
      first = r.value;
      rest = it;
      return true;
    }
  };
  try {
    for (;;) {
      const { value, done } = await reader.read();
      if (done) { eof = true; take(finish()); break; }
      if (take(step(value))) break;
    }
  } catch (e) {
    await reader.cancel(e).catch(() => undefined);
    throw e;
  }
  const run = async (writer: WritableStreamDefaultWriter<Uint8Array>): Promise<number> => {
    // Coalesce output to ~WRITE_COALESCE bytes per awaited write: each write is a handoff to
    // the client side (measured 2.8 MB/s at one write per small R2 chunk); memory stays
    // bounded by WRITE_COALESCE plus one chunk's expansion.
    let pending: Uint8Array[] = [];
    let pendingLen = 0;
    const flush = async () => {
      if (!pendingLen) return;
      const buf = pending.length === 1 ? pending[0] : join(pending);
      pending = []; pendingLen = 0;
      await writer.write(buf);
      stats.bytesOut += buf.length;
    };
    const emit = async (out: Uint8Array) => {
      if (!out.length) return;
      pending.push(out); pendingLen += out.length;
      if (pendingLen >= WRITE_COALESCE) await flush();
    };
    try {
      if (first) await emit(first);
      if (rest) { for (;;) { const r = rest.next(); if (r.done) break; await emit(r.value); } }   // resume, lazily
      if (!eof) {
        for (;;) {
          const { value, done } = await reader.read();
          if (done) { for (const piece of finish()) await emit(piece); break; }
          for (const piece of step(value)) await emit(piece);   // one slice inflated per emit (R599/R603)
        }
      }
      await flush();
      await writer.close();
      return stats.bytesOut;
    } catch (e) {
      await reader.cancel(e).catch(() => undefined);
      await writer.abort(e).catch(() => undefined);
      throw e;
    }
  };
  return { first, run };
}

/** workerd's native byte pipe; Node (tests) falls back to a TransformStream. */
export function identityPipe(): { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> } {
  const g = globalThis as unknown as { IdentityTransformStream?: new () => { readable: ReadableStream<Uint8Array>; writable: WritableStream<Uint8Array> } };
  if (g.IdentityTransformStream) return new g.IdentityTransformStream();
  return new TransformStream<Uint8Array, Uint8Array>();
}

/** Prefix text (citation + header) as bytes. */
export function prefixBytes(prefix: string): Uint8Array {
  return new TextEncoder().encode(prefix);
}

/** Peek the first CSV line of a gzipped object from its first stored chunk(s) without
 *  keeping the inflater: used to prime the passthrough (header validated, a data row seen). */
export function peekGzipHeader(chunks: Uint8Array[]): { headerOk: boolean; hasRow: boolean } {
  let text = "";
  const dec = new TextDecoder();
  let done = false;
  const gz = new Gunzip((data) => { if (!done && data.length) { text += dec.decode(data, { stream: true }); if (text.length > 65536) done = true; } });
  try {
    for (const c of chunks) { gz.push(c, false); if (done) break; }
  } catch {
    return { headerOk: false, hasRow: false };
  }
  const nl = text.indexOf("\n");
  if (nl < 0) return { headerOk: text.replace(/\r$/, "") === CSV_HEADER && false, hasRow: false };
  const headerOk = text.slice(0, nl).replace(/\r$/, "") === CSV_HEADER;
  const hasRow = text.slice(nl + 1).split("\n").some((l) => l.trim() !== "");
  return { headerOk, hasRow };
}
