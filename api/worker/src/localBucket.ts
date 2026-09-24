// ---------------------------------------------------------------------------
// src/localBucket.ts  --  SERIES_BUCKET for the self-hosted origin (docs/ECON_SELF_HOSTING_PLAN.md,
// code change 3).
//
// workerd cannot open files, so on the workstation the worker reads objects from the blob sidecar
// (tools/selfhost/blob_sidecar.py) over 127.0.0.1. This class gives series.ts and index.ts the parts
// of the R2Bucket surface they use - and nothing that writes:
//
//   get(key)                         -> null | object with body
//   get(key, { range })              -> object whose body is the slice but whose size is the FULL size
//   get(key, { onlyIf: {etagMatches}}) -> a BODYLESS object when the etag no longer matches (R2's
//                                        behaviour; series.ts:418-423 relies on it), never null
//
// Object fields: key, size, etag, httpEtag ("<etag>" quoted, as R2), httpMetadata.contentEncoding /
// contentType, customMetadata, body, text(), json(), arrayBuffer(). The sidecar sends the stored encoding
// as x-blob-content-encoding and never as Content-Encoding, so fetch does not inflate gzip bytes.
// ---------------------------------------------------------------------------

export interface LocalGetOptions {
  onlyIf?: { etagMatches?: string };
  range?: { offset?: number; length?: number };
}

export interface LocalObject {
  key: string;
  size: number;
  etag: string;
  httpEtag: string;
  httpMetadata: { contentEncoding?: string; contentType?: string };
  customMetadata: Record<string, string>;
}

export interface LocalObjectBody extends LocalObject {
  body: ReadableStream<Uint8Array>;
  text(): Promise<string>;
  json<T = unknown>(): Promise<T>;
  arrayBuffer(): Promise<ArrayBuffer>;
}

function decodeMeta(v: string | null): Record<string, string> {
  if (!v) return {};
  try {
    return JSON.parse(atob(v)) as Record<string, string>;
  } catch {
    return {};
  }
}

export class LocalBucket {
  // A plain field, not a constructor parameter property: node --test strips types and refuses those.
  private readonly base: string;
  constructor(base: string) {
    this.base = base;
  }

  async get(key: string, options?: LocalGetOptions): Promise<LocalObject | LocalObjectBody | null> {
    // REFUSE WHAT IS NOT IMPLEMENTED (R1168 (e)): an unknown option silently ignored returned the whole
    // object for `range: {suffix}`. Only range {offset, length} and onlyIf {etagMatches} are supported.
    const raw = options as Record<string, unknown> | undefined;
    if (raw) {
      for (const k of Object.keys(raw)) {
        if (k !== "range" && k !== "onlyIf") throw new Error(`LocalBucket.get: unsupported option '${k}'`);
      }
      const rng = raw.range as unknown;
      if (rng !== undefined) {
        if (rng instanceof Headers || typeof rng !== "object" || rng === null) {
          throw new Error("LocalBucket.get: range must be {offset, length}");
        }
        for (const k of Object.keys(rng)) {
          if (k !== "offset" && k !== "length") throw new Error(`LocalBucket.get: unsupported range field '${k}'`);
        }
      }
      const oi = raw.onlyIf as unknown;
      if (oi !== undefined) {
        if (oi instanceof Headers || typeof oi !== "object" || oi === null) {
          throw new Error("LocalBucket.get: onlyIf must be {etagMatches}");
        }
        for (const k of Object.keys(oi)) {
          if (k !== "etagMatches") throw new Error(`LocalBucket.get: unsupported onlyIf field '${k}'`);
        }
      }
    }
    const headers: Record<string, string> = {};
    const want = options?.onlyIf?.etagMatches;
    if (want !== undefined) headers["If-Match"] = `"${want.replace(/^"|"$/g, "")}"`;
    if (options?.range) {
      const off = Math.max(0, options.range.offset ?? 0);
      const len = options.range.length;
      headers["Range"] = len !== undefined ? `bytes=${off}-${off + Math.max(0, len) - 1}` : `bytes=${off}-`;
    }
    const r = await fetch(`${this.base.replace(/\/$/, "")}/o/${encodeURIComponent(key)}`, { headers });
    if (r.status === 404) {
      await r.body?.cancel().catch(() => undefined);
      return null;
    }
    const etag = r.headers.get("x-blob-etag") ?? "";
    const meta: LocalObject = {
      key,
      size: Number(r.headers.get("x-blob-size") ?? "0"),
      etag,
      httpEtag: `"${etag}"`,
      httpMetadata: {
        ...(r.headers.get("x-blob-content-encoding") ? { contentEncoding: r.headers.get("x-blob-content-encoding")! } : {}),
        ...(r.headers.get("x-blob-content-type") ? { contentType: r.headers.get("x-blob-content-type")! } : {}),
      },
      customMetadata: decodeMeta(r.headers.get("x-blob-custom-metadata")),
    };
    if (r.status === 412) {
      await r.body?.cancel().catch(() => undefined);
      return meta;                                        // bodyless, like R2 on a failed onlyIf
    }
    if (r.status !== 200 && r.status !== 206) {
      await r.body?.cancel().catch(() => undefined);
      throw new Error(`blob sidecar answered ${r.status} for ${key}`);
    }
    const body = r.body ?? new ReadableStream<Uint8Array>({ start(c) { c.close(); } });
    return {
      ...meta,
      body,
      text: () => new Response(body).text(),
      json: <T = unknown>() => new Response(body).json() as Promise<T>,
      arrayBuffer: () => new Response(body).arrayBuffer(),
    };
  }

  // The origin never writes: every write path belongs to the local updater, not the server.
  async put(): Promise<never> { throw new Error("the self-hosted origin's bucket is read-only"); }
  async delete(): Promise<never> { throw new Error("the self-hosted origin's bucket is read-only"); }
  async head(): Promise<never> { throw new Error("LocalBucket.head is not used by the worker"); }
  async list(): Promise<never> { throw new Error("LocalBucket.list is not used by the worker"); }
}
