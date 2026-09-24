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
// Forwarding rules (review AR of e2b2855e1, ledger R1169, is why most of these exist):
//   * only the routes the origin serves are forwarded; anything else is the edge's own 404;
//   * the upstream URL is ORIGIN_URL with the client's path and query SET on it, and must keep
//     ORIGIN_URL's origin - a client path such as `//host/` or `/\host/` can never choose the host the
//     secret is sent to (the blocker of that review: `new URL(path, base)` let it);
//   * redirects are never followed: a 3xx, or any answer without the origin's own mark (a tunnel error
//     page, an Access login page), is a 502 and is never cached or logged as a download;
//   * the origin gets an allowlist of request headers; the client's credentials never reach it, and the
//     secret and Access token are set by the edge, never passed through;
//   * a download 200 without content-length is counted as it streams, whatever the origin's marker says
//     (a hop that drops content-length must not drop the log row, R593); the count is logged on
//     completion AND on abort;
//   * public answers may be edge-cached, keyed without api_key, for a per-route time; data (.csv), the
//     bundle (its default snapshot is "today") and errors never are.
// ---------------------------------------------------------------------------

// The header names shared with src/localMode.ts, by value: node --test (type stripping) cannot resolve
// an extension-less import, and test/edge.test.ts pins these equal to localMode's.
export const ORIGIN_SECRET_HEADER = "x-econ-origin-secret";
export const COUNT_HEADER = "x-econ-count";
export const ORIGIN_MARK_HEADER = "x-econ-origin";

export interface EdgeEnv {
  FORWARD?: string;
  ORIGIN_URL?: string;               // an ORIGIN only (scheme://host[:port]), e.g. https://origin.example
  ORIGIN_SECRET?: string;            // the shared secret the origin requires
  ORIGIN_ACCESS_ID?: string;         // Cloudflare Access service token (fallback path)
  ORIGIN_ACCESS_SECRET?: string;
  ORIGIN_TIMEOUT_MS?: string;        // wait for the origin's response HEADERS; default 30000
}

/** The edge forwards only when FORWARD is exactly "on". */
export function isForward(env: { FORWARD?: string }): boolean {
  return env.FORWARD === "on";
}

/** Where the edge keeps its OWN state (page views, the cost-guard status): the users db once EDGE_STATE
 *  is "users" - set at plan step 5, before T0, so the step-6a freeze proof sees no write from the edge -
 *  or once FORWARD is on. A rollback that turns FORWARD off must keep EDGE_STATE = "users". */
export function edgeStateInUsers(env: { FORWARD?: string; EDGE_STATE?: string }): boolean {
  return isForward(env) || env.EDGE_STATE === "users";
}

/** /v1/edge-status: what the off-machine check (tools/selfhost/watch_edge.py) compares with the committed
 *  wrangler.toml. Public and harmless: two booleans, two raw config values and a public commit id. It
 *  touches no storage, so it can never become a cost path, and never names the origin's address. */
export function edgeStatus(env: EdgeEnv & { EDGE_STATE?: string; GIT_COMMIT?: string }): Response {
  return new Response(JSON.stringify({
    commit: env.GIT_COMMIT || null,
    forward: isForward(env),
    edge_state: edgeStateInUsers(env) ? "users" : "econ",
    forward_raw: env.FORWARD ?? "",
    edge_state_raw: env.EDGE_STATE ?? "",
    origin_configured: Boolean(env.ORIGIN_URL && env.ORIGIN_SECRET),
  }), { headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" } });
}

/** Routes the origin answers. Everything else gets the edge's own 404 and is never forwarded. */
const FORWARDED_PATHS: ReadonlySet<string> = new Set([
  "/", "/v1", "/v1/", "/v1/catalog", "/v1/sources", "/v1/last-updates", "/v1/stats", "/v1/bundle",
]);

export function isForwardable(path: string): boolean {
  return FORWARDED_PATHS.has(path) || path.startsWith("/v1/series/");
}

/** Seconds a 200 may be served from the edge cache, per route; 0 = never cached. catalog and stats keep
 *  the 6 h they had before forwarding; the freshness and listing routes get 5 minutes so a licence-gate
 *  change or a new last-update shows within minutes; metadata 1 hour. */
export function cacheSeconds(path: string): number {
  if (path === "/v1/catalog" || path === "/v1/stats") return 21600;
  if (path === "/v1/sources" || path === "/v1/last-updates") return 300;
  if (path.startsWith("/v1/series/") && path.endsWith(".metadata.json")) return 3600;
  return 0;
}

export function isCacheable(path: string): boolean {
  return cacheSeconds(path) > 0;
}

/** The edge-cache key: the client URL without api_key (a key must never be part of a cache key, and a
 *  keyed and an unkeyed request for public data are the same answer). */
export function cacheKey(request: Request): Request {
  const u = new URL(request.url);
  u.searchParams.delete("api_key");
  return new Request(u.toString(), { method: "GET" });
}

/** Request headers the origin may see. Nothing else crosses: no credentials, no cookies, no cf-* or
 *  x-forwarded-* from the client, no host. */
const FORWARDED_REQUEST_HEADERS = ["accept", "accept-encoding", "accept-language", "user-agent"];

/** The request the origin receives, or null when the edge is not configured to forward (no origin URL,
 *  an ORIGIN_URL that is not a bare origin, or no secret) or when the client's path would move it off
 *  ORIGIN_URL's origin. The caller answers 503 / 400; nothing is ever sent without the secret. */
export function originRequest(request: Request, env: EdgeEnv): Request | null {
  if (!env.ORIGIN_URL || !env.ORIGIN_SECRET) return null;
  let base: URL;
  try {
    base = new URL(env.ORIGIN_URL);
  } catch {
    return null;
  }
  if (base.pathname !== "/" || base.search || base.hash || base.username || base.password) return null;
  const inbound = new URL(request.url);
  inbound.searchParams.delete("api_key");
  const target = new URL(base.href);
  target.pathname = inbound.pathname;
  target.search = inbound.search;
  if (target.origin !== base.origin) return null;
  const headers = new Headers();
  for (const h of FORWARDED_REQUEST_HEADERS) {
    const v = request.headers.get(h);
    if (v !== null) headers.set(h, v);
  }
  headers.set(ORIGIN_SECRET_HEADER, env.ORIGIN_SECRET);
  if (env.ORIGIN_ACCESS_ID && env.ORIGIN_ACCESS_SECRET) {
    headers.set("cf-access-client-id", env.ORIGIN_ACCESS_ID);
    headers.set("cf-access-client-secret", env.ORIGIN_ACCESS_SECRET);
  }
  return new Request(target.toString(), { method: "GET", headers, redirect: "manual" });
}

/** Fetch from the origin. Never follows a redirect; waits at most ORIGIN_TIMEOUT_MS for the response
 *  headers (the body of a long download is not bounded by it). Returns the origin's answer only when it
 *  carries the origin's mark and is not a 3xx; otherwise a JSON 502/504 that is never cached or logged. */
export async function fetchOrigin(req: Request, env: EdgeEnv): Promise<Response> {
  const ms = Number(env.ORIGIN_TIMEOUT_MS) > 0 ? Number(env.ORIGIN_TIMEOUT_MS) : 30000;
  // On timeout the pending fetch is ABORTED, so a late answer is never read or left open (R1175). The
  // timer is cleared once the headers arrive, so a long download's body is never cut by it.
  const ctl = new AbortController();
  let timer: ReturnType<typeof setTimeout> | null = null;
  const timeout = new Promise<"timeout">((ok) => {
    timer = setTimeout(() => { ctl.abort(); ok("timeout"); }, ms);
  });
  let resp: Response | "timeout";
  try {
    resp = await Promise.race([fetch(req, { redirect: "manual", signal: ctl.signal }), timeout]);
  } catch {
    return gatewayError(502, "origin_unreachable", "the data origin did not answer");
  } finally {
    if (timer !== null) clearTimeout(timer);
  }
  if (resp === "timeout") return gatewayError(504, "origin_timeout", "the data origin did not answer in time");
  if ((resp.status >= 300 && resp.status < 400) || resp.headers.get(ORIGIN_MARK_HEADER) !== "1") {
    await resp.body?.cancel().catch(() => undefined);
    return gatewayError(502, "origin_bad_answer", "the data origin's answer could not be verified");
  }
  return resp;
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

/** The origin's answer as the client gets it: the internal headers are removed. */
export function clientResponse(originResp: Response, body: ReadableStream<Uint8Array> | null): Response {
  const out = new Response(body, originResp);
  out.headers.delete(COUNT_HEADER);
  out.headers.delete(ORIGIN_MARK_HEADER);
  return out;
}

function gatewayError(status: number, error: string, detail: string): Response {
  return new Response(JSON.stringify({ error, detail }), {
    status, headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}

export function notConfigured(): Response {
  return gatewayError(503, "origin_not_configured",
    "FORWARD is on but ORIGIN_URL or ORIGIN_SECRET is unset or invalid; nothing is forwarded without them");
}

export function refusedPath(): Response {
  return gatewayError(400, "bad_request", "this path cannot be forwarded");
}
