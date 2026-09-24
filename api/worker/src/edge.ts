// ---------------------------------------------------------------------------
// src/edge.ts  --  the EDGE worker's forwarding to the self-hosted origin (docs/ECON_SELF_HOSTING_PLAN.md,
// code change 1).
//
// econdl-api keeps its public name for good. With FORWARD = "on" it still does, itself: auth, the licence
// gate (451 before auth), rate limiting and the download log, and it forwards the data work to the
// workstation's origin (src/localMode.ts) with the shared secret. With FORWARD unset or anything but "on"
// NONE of this runs and the worker behaves exactly as before - that is what lets it be deployed before T0
// and flipped by one committed line.
//
// Forwarding rules:
//   * the client's credentials never reach the origin: X-API-Key, Authorization and ?api_key= are
//     stripped; the secret and identity headers are always OVERWRITTEN, never passed through;
//   * a forwarded body is never read unless the origin marked it x-econ-count: 1 (a length-less
//     download): a gzip passthrough body stays unread, so it stays gzip on the wire;
//   * a counted download is logged when the transfer ends - on completion AND on abort (R593/R599);
//   * public answers (catalogue, sources, stats, last-updates, metadata, bundle) may be edge-cached;
//     data (.csv) never is.
// ---------------------------------------------------------------------------

// The two header names shared with src/localMode.ts, by value: node --test (type stripping) cannot
// resolve an extension-less import, and test/edge.test.ts pins these equal to localMode's.
export const ORIGIN_SECRET_HEADER = "x-econ-origin-secret";
export const COUNT_HEADER = "x-econ-count";

export interface EdgeEnv {
  FORWARD?: string;
  ORIGIN_URL?: string;               // the tunnel hostname (fallback) - e.g. https://origin.example
  ORIGIN_SECRET?: string;            // the shared secret the origin requires
  ORIGIN_ACCESS_ID?: string;         // Cloudflare Access service token (fallback path)
  ORIGIN_ACCESS_SECRET?: string;
}

/** The edge forwards only when FORWARD is exactly "on". */
export function isForward(env: { FORWARD?: string }): boolean {
  return env.FORWARD === "on";
}

/** Routes whose 200 answers the edge may cache (per data centre). Never the .csv data route. */
export const CACHEABLE_PATHS: ReadonlySet<string> = new Set([
  "/v1/catalog", "/v1/sources", "/v1/stats", "/v1/last-updates", "/v1/bundle",
]);

export function isCacheable(path: string): boolean {
  return CACHEABLE_PATHS.has(path) || (path.startsWith("/v1/series/") && path.endsWith(".metadata.json"));
}

const STRIPPED_REQUEST_HEADERS = ["x-api-key", "authorization", "cookie", ORIGIN_SECRET_HEADER,
  "cf-access-client-id", "cf-access-client-secret"];

/** The request the origin receives: same path and query minus api_key, credentials stripped, the secret
 *  (and the Access token, when configured) set by the edge. Returns null when the edge is not configured
 *  to forward (no origin URL or no secret) - the caller answers 503, never forwards without the secret. */
export function originRequest(request: Request, env: EdgeEnv): Request | null {
  if (!env.ORIGIN_URL || !env.ORIGIN_SECRET) return null;
  const inbound = new URL(request.url);
  inbound.searchParams.delete("api_key");
  const target = new URL(inbound.pathname + inbound.search, env.ORIGIN_URL);
  const headers = new Headers(request.headers);
  for (const h of STRIPPED_REQUEST_HEADERS) headers.delete(h);
  headers.set(ORIGIN_SECRET_HEADER, env.ORIGIN_SECRET);
  if (env.ORIGIN_ACCESS_ID && env.ORIGIN_ACCESS_SECRET) {
    headers.set("cf-access-client-id", env.ORIGIN_ACCESS_ID);
    headers.set("cf-access-client-secret", env.ORIGIN_ACCESS_SECRET);
  }
  return new Request(target.toString(), { method: "GET", headers });
}

/** A body that counts the bytes the client actually takes and reports them when the transfer ends -
 *  completed or aborted. `onDone(bytes, ok)` runs inside ctx.waitUntil. */
export function countingBody(
  body: ReadableStream<Uint8Array>,
  onDone: (bytes: number, ok: boolean) => Promise<void>,
  ctx: { waitUntil(p: Promise<unknown>): void },
): ReadableStream<Uint8Array> {
  let bytes = 0;
  const counter = new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      bytes += chunk.byteLength;
      controller.enqueue(chunk);
    },
  });
  const done = body.pipeTo(counter.writable).then(
    () => onDone(bytes, true),
    () => onDone(bytes, false),
  );
  ctx.waitUntil(done.catch(() => undefined));
  return counter.readable;
}

/** The origin's answer as the client gets it. The origin marks everything private, no-store; the edge
 *  keeps that for data and errors. `COUNT_HEADER` is internal and is removed. */
export function clientResponse(originResp: Response, body: ReadableStream<Uint8Array> | null): Response {
  const out = new Response(body, originResp);
  out.headers.delete(COUNT_HEADER);
  return out;
}

export function notConfigured(): Response {
  return new Response(JSON.stringify({
    error: "origin_not_configured",
    detail: "FORWARD is on but ORIGIN_URL or ORIGIN_SECRET is unset; nothing is forwarded without them",
  }), { status: 503, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
}
