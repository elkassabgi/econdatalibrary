// /v1/guard-heartbeat - the workstation watchdog's beat, for a reader OFF the workstation.
//
// WHY A ROUTE. tools/guard_heartbeat.py --publish stamps `_aqueduct/guard_heartbeat.json` every guard tick.
// Its only reader was updater-daily.yml's `--check`, which reads R2 - and T0 switches that workflow off and
// moves the beat into the self-hosted store, where nothing off the machine could see it (review R1210: a
// watchdog nobody watches). This route serves the beat from wherever SERIES_BUCKET is (R2 before T0, the
// origin's blob store after), so the scheduled check in .github/workflows/selfhost-watch.yml can read it.
// When the workstation is down the edge answers "unavailable", and that fails the check too.
//
// CACHED 60 s AT THE EDGE (R1226): the route is public, and uncached every hit was an R2 read before T0. The
// key is the fixed path (a query string cannot bypass it) and only a 200 is kept; 60 s is nothing against the
// check's 45-minute staleness limit.
//
// ONLY A TIMESTAMP AND COUNTS. The beat names hosts, crawler scripts and the sources the emptiness audit
// looked at; this route is public, so none of that crosses. The full beat stays readable on the machine.

import type { Env } from "./types.ts";
import { json } from "./util.ts";

const KEY = "_aqueduct/guard_heartbeat.json";

/** The route with a 60 s edge cache (never on the origin, which has no Cache API: `local`). */
export async function cachedGuardHeartbeat(url: URL, env: Env, ctx: ExecutionContext, local: boolean): Promise<Response> {
  if (local) return handleGuardHeartbeat(env);
  const cache = caches.default;
  const key = new Request(`${url.origin}/v1/guard-heartbeat`, { method: "GET" });
  const hit = await cache.match(key);
  if (hit) return hit;
  const resp = await handleGuardHeartbeat(env);
  if (resp.status !== 200) return resp;
  const toCache = new Response(resp.body, resp);
  toCache.headers.set("cache-control", "public, max-age=0, s-maxage=60");
  ctx.waitUntil(cache.put(key, toCache.clone()));
  return toCache;
}

export async function handleGuardHeartbeat(env: Env): Promise<Response> {
  const noStore = { "cache-control": "no-store" };
  const obj = await env.SERIES_BUCKET.get(KEY);
  if (obj === null) {
    return json({ error: "heartbeat_absent", detail: "the watchdog has never published a beat" }, 503, noStore);
  }
  let beat: Record<string, unknown>;
  try {
    beat = await obj.json() as Record<string, unknown>;
  } catch {
    return json({ error: "heartbeat_unreadable" }, 503, noStore);
  }
  const alive = Array.isArray(beat.jobs_alive) ? beat.jobs_alive.length : null;
  const tracked = Array.isArray(beat.tracked) ? beat.tracked.length : null;
  const emp = (beat.emptiness && typeof beat.emptiness === "object") ? beat.emptiness as Record<string, unknown> : {};
  return json({
    utc: typeof beat.utc === "string" ? beat.utc : null,
    table_ok: beat.table_ok !== false,
    jobs_alive: alive,
    jobs_tracked: tracked,
    emptiness_ran: emp.ran === true,
    fetch_without_write: typeof emp.fetch_without_write === "number" ? emp.fetch_without_write : null,
  }, 200, noStore);
}
