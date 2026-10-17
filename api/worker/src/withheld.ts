// ---------------------------------------------------------------------------
// src/withheld.ts  --  tables WITHHELD from users until our copy is complete (owner decision, Ahmed,
// 2026-10-17: "hide the incomplete tables until complete, then show them again").
//
// THE LIST is one small JSON object in the store, `_aqueduct/withheld.json`:
//     {"version": 1, "ids": {"insee_melodi:DS_RP_TD_MIGRES_COMP": {"since": "...", "why": "...",
//                                                                    "until": "complete" | "operator"}, ...}}
// It is written by tools/withheld_list.py (operator, conditional writes) and maintained by the fetcher that
// completes a table (insee_melodi removes an `until: "complete"` entry once the flow is complete per INSEE paging
// AND its served CSV holds every row). No deploy is needed to hide or un-hide.
//
// WHERE IT IS READ:
//   * cloud worker (FORWARD off) and the self-hosted origin (LOCAL): from SERIES_BUCKET - R2, or the local store
//     through LocalBucket. The origin also answers GET /v1/withheld (secret-gated like every origin route) with
//     the object's bytes, for the edge;
//   * forwarding edge (FORWARD on): from the ORIGIN's /v1/withheld, because after T0 the store is local and the
//     edge's R2 copy goes stale (AR-339 Part 2 finding 1). The edge gates /v1/series/* with it BEFORE auth.
//
// FAIL CLOSED (AR-339 finding 5): a read that fails, an absent object, or an object that does not validate is
// UNREADABLE: the last good copy is used, else the compiled-in FLOOR (withheldFloor.ts, generated from
// api/worker/withheld.json by tools/gen_withheld_floor.py). Never "nothing withheld" by accident. A failure is
// cached 30 s (an outage must not GET on every request); a good read 300 s. To withhold nothing the operator
// writes `"ids": {}`; the object is never deleted. Disclosed cost of the floor: if the object turns unreadable
// after a table was un-hidden, the floor (as of the last deploy) hides it again until the object is readable -
// the safe direction.
//
// WHAT A WITHHELD ID GETS: 404 {"error":"withheld", ...} on .metadata.json and .csv (before auth, like the
// licence gate), no row in /v1/catalog search or browse (inline literal NOT IN, src/sql.ts), no entry in a
// bundle (enumeration excludes it; a named id is `unresolved` with reason "withheld"), and it is not counted in
// browse totals or /v1/stats catalog_entries (subtracted in the handler, by a primary-key count of the ids
// present). The list's ETag is part of the edge-cache key of /v1/catalog and /v1/stats, so a change shows
// within the 300 s reload instead of the 6 h cache life (finding 6).
// ---------------------------------------------------------------------------

import { WITHHELD_FLOOR, WITHHELD_FLOOR_ETAG } from "./withheldFloor.ts";

export const WITHHELD_KEY = "_aqueduct/withheld.json";
export const WITHHELD_ROUTE = "/v1/withheld";
/** The ids the SQL inlines as literals: no quote, no space, no wildcard can pass. */
export const WITHHELD_ID_RE = /^[a-z0-9_]+:[A-Za-z0-9_.-]+$/;
/** More than this is refused as invalid (an inline NOT IN must stay small; a runaway writer must not grow it). */
export const WITHHELD_MAX = 500;
export const TTL_OK_MS = 300_000;
export const TTL_FAIL_MS = 30_000;

/** What a user reads (owner's wording, 2026-10-17). Per source: the sentence names the publisher. */
const DETAIL_BY_SOURCE: Record<string, string> = {
  insee_melodi: "Temporarily withheld: our copy is incomplete and is being re-downloaded from INSEE; it is " +
    "listed again when the download is complete.",
};
const DETAIL_DEFAULT = "Temporarily withheld: our copy is incomplete and is being re-downloaded from the " +
  "publisher; it is listed again when the download is complete.";

export function withheldDetail(id: string): string {
  const i = id.indexOf(":");
  return DETAIL_BY_SOURCE[i < 0 ? id : id.slice(0, i)] ?? DETAIL_DEFAULT;
}

export interface Withheld {
  /** every withheld id */
  ids: ReadonlySet<string>;
  /** a short tag that changes whenever the set may have changed (the object's ETag, or "floor-<hash>") */
  etag: string;
  /** where this copy came from: "store" | "origin" | "last-good" | "floor" */
  from: string;
}

export function sourceOfId(id: string): string {
  const i = id.indexOf(":");
  return i < 0 ? id : id.slice(0, i);
}

/** The withheld ids of ONE source, sorted (what a source-scoped statement inlines). */
export function forSource(w: Withheld, source: string): string[] {
  return [...w.ids].filter((id) => sourceOfId(id) === source).sort();
}

export function isWithheld(w: Withheld, id: string): boolean {
  return w.ids.has(id);
}

/** The answer for a withheld series route: 404 with its own error code, so a client can tell it from an
 *  unknown id ("not_found") and from a licence refusal (451). */
export function withheldResponse(id: string): Response {
  return new Response(JSON.stringify({ error: "withheld", series_id: id, detail: withheldDetail(id) }), {
    status: 404,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store",
               "access-control-allow-origin": "*" },
  });
}

/** Parse and VALIDATE the object. Anything wrong -> null (unreadable), never a partial set. */
export function parseWithheld(text: string): Set<string> | null {
  let o: unknown;
  try {
    o = JSON.parse(text);
  } catch {
    return null;
  }
  if (!o || typeof o !== "object" || Array.isArray(o)) return null;
  const rec = o as Record<string, unknown>;
  if (rec.version !== 1) return null;
  const ids = rec.ids;
  if (!ids || typeof ids !== "object" || Array.isArray(ids)) return null;
  const keys = Object.keys(ids as Record<string, unknown>);
  if (keys.length > WITHHELD_MAX) return null;
  const out = new Set<string>();
  for (const k of keys) {
    if (!WITHHELD_ID_RE.test(k)) return null;
    const v = (ids as Record<string, unknown>)[k];
    if (!v || typeof v !== "object" || Array.isArray(v)) return null;
    const until = (v as Record<string, unknown>).until;
    if (until !== "complete" && until !== "operator") return null;
    out.add(k);
  }
  return out;
}

export function floorWithheld(): Withheld {
  return { ids: new Set(WITHHELD_FLOOR), etag: WITHHELD_FLOOR_ETAG, from: "floor" };
}

// Per-isolate cache. `lastGood` survives a failed reload; `cached` decides when to reload.
let cached: { at: number; ttl: number; w: Withheld } | null = null;
let lastGood: Withheld | null = null;

/** Tests only: forget the per-isolate state. */
export function resetWithheldCache(): void {
  cached = null;
  lastGood = null;
}

type Reader = () => Promise<{ text: string; etag: string } | null>;

/** The current list through the cache. `read` returns the object's text and tag, null when absent, and may
 *  throw; `from` names it. */
export async function loadWithheld(read: Reader, from: string, now: () => number = Date.now): Promise<Withheld> {
  const t = now();
  if (cached && t - cached.at < cached.ttl) return cached.w;
  let got: { text: string; etag: string } | null = null;
  try {
    got = await read();
  } catch {
    got = null;
  }
  const ids = got ? parseWithheld(got.text) : null;
  if (got && ids) {
    const w: Withheld = { ids, etag: `${from}-${got.etag}`, from };
    lastGood = w;
    cached = { at: t, ttl: TTL_OK_MS, w };
    return w;
  }
  const w = lastGood ? { ...lastGood, from: "last-good" } : floorWithheld();
  cached = { at: t, ttl: TTL_FAIL_MS, w };
  return w;
}

/** The reader over a bucket (R2, or LocalBucket on the origin). */
export function bucketReader(bucket: R2Bucket): Reader {
  return async () => {
    const obj = await bucket.get(WITHHELD_KEY);
    if (!obj) return null;
    return { text: await obj.text(), etag: String(obj.etag || "") };
  };
}

/** The origin's GET /v1/withheld: the object's bytes as stored (the edge validates them itself), or 404 when
 *  absent. Answered ONLY by the origin (LOCAL); the cloud worker and the edge answer 404 to a client. */
export async function originWithheldRoute(bucket: R2Bucket): Promise<Response> {
  const obj = await bucket.get(WITHHELD_KEY);
  if (!obj) return new Response(JSON.stringify({ error: "absent" }), { status: 404,
    headers: { "content-type": "application/json", "cache-control": "no-store" } });
  return new Response(await obj.text(), { status: 200, headers: {
    "content-type": "application/json", "cache-control": "no-store", "x-withheld-etag": String(obj.etag || "") } });
}

/** A cache-key Request for an edge-cached route that also carries the list's tag, so a new list misses the cache.
 *  The tag is a query parameter of the KEY only; it is never forwarded or seen by a handler. */
export function keyWithWithheld(req: Request, w: Withheld): Request {
  const u = new URL(req.url);
  u.searchParams.delete("api_key");
  u.searchParams.set("__withheld", w.etag);
  return new Request(u.toString(), { method: "GET" });
}
