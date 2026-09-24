// ---------------------------------------------------------------------------
// src/localMode.ts  --  the self-hosted ORIGIN's rules (docs/ECON_SELF_HOSTING_PLAN.md).
//
// Owner decision, 2026-09-23: all of econ is served from the workstation. The public name
// econdl-api.elkassabgi.workers.dev stays on an EDGE worker that does auth, the licence gate,
// rate limiting and the download log, and forwards to this origin over a private tunnel with a
// shared secret. The origin runs the SAME code with LOCAL = "1" (wrangler.origin.toml), and these
// functions are the only differences:
//
//   * refuse EVERY request when ORIGIN_SECRET is unset (a misconfigured origin must fail closed,
//     never serve without the edge's checks), and every request whose secret header is wrong;
//   * mark EVERY answer private, no-store: a cache in front of the origin would otherwise replay one
//     authorised download to anyone asking the same URL (review R1158 (1), R1160);
//   * mark a 200 answer that carries no content-length with x-econ-count: 1, so the edge counts the
//     bytes it forwards and logs the download on completion AND on abort (R593/R599, R1163 (5));
//   * the routes that need the family identity DB (/v1/pv, /v1/pv/report, /v1/public-stats) are
//     answered by the edge, never here (the origin has no USERS binding).
//
// Pure functions over Request/Response, so node --test can pin them without a runtime.
// ---------------------------------------------------------------------------

export const ORIGIN_SECRET_HEADER = "x-econ-origin-secret";
export const COUNT_HEADER = "x-econ-count";

/** Routes the origin never answers: the edge owns them (they read or write USERS). */
export const EDGE_ONLY_PATHS: ReadonlySet<string> = new Set(["/v1/pv", "/v1/pv/report", "/v1/public-stats"]);

export function isLocal(env: { LOCAL?: string }): boolean {
  return env.LOCAL === "1";
}

function jsonResponse(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "private, no-store" },
  });
}

/** Constant-time comparison of two strings: both are hashed first, so the loop always runs over
 *  32 bytes whatever the inputs' lengths, and no early exit reveals where they differ. */
export async function secretsEqual(given: string, expected: string): Promise<boolean> {
  const enc = new TextEncoder();
  const [a, b] = await Promise.all([
    crypto.subtle.digest("SHA-256", enc.encode(given)),
    crypto.subtle.digest("SHA-256", enc.encode(expected)),
  ]);
  const x = new Uint8Array(a);
  const y = new Uint8Array(b);
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= x[i] ^ y[i];
  return diff === 0;
}

/** The gate every origin request passes first. null = let it through; a Response = refuse. */
export async function originGate(request: Request, env: { ORIGIN_SECRET?: string }): Promise<Response | null> {
  const expected = env.ORIGIN_SECRET;
  if (!expected) {
    return jsonResponse({
      error: "origin_not_configured",
      detail: "This origin has no ORIGIN_SECRET, so it refuses every request rather than serve " +
        "without the edge's checks.",
    }, 503);
  }
  const given = request.headers.get(ORIGIN_SECRET_HEADER) ?? "";
  if (!(await secretsEqual(given, expected))) {
    return jsonResponse({ error: "forbidden", detail: "not reachable except through the edge" }, 403);
  }
  const path = new URL(request.url).pathname;
  if (EDGE_ONLY_PATHS.has(path)) {
    return jsonResponse({ error: "edge_only", detail: `${path} is answered by the edge worker` }, 404);
  }
  return null;
}

/** Applied to every answer the origin gives. Returns a response with mutable headers. */
export function finalizeLocal(resp: Response): Response {
  const out = new Response(resp.body, resp);
  out.headers.set("cache-control", "private, no-store");
  if (out.status === 200 && !out.headers.has("content-length")) {
    out.headers.set(COUNT_HEADER, "1");
  } else {
    out.headers.delete(COUNT_HEADER);
  }
  return out;
}
