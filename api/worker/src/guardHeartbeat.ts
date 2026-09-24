// /v1/guard-heartbeat - the workstation watchdog's beat, for a reader OFF the workstation.
//
// WHY A ROUTE. tools/guard_heartbeat.py --publish stamps `_aqueduct/guard_heartbeat.json` every guard tick.
// Its only reader was updater-daily.yml's `--check`, which reads R2 - and T0 switches that workflow off and
// moves the beat into the self-hosted store, where nothing off the machine could see it (review R1210: a
// watchdog nobody watches). This route serves the beat from wherever SERIES_BUCKET is (R2 before T0, the
// origin's blob store after), so a scheduled check elsewhere (.github/workflows/guard-heartbeat.yml) can
// read it. When the workstation is down the edge answers "unavailable", and that fails the check too.
//
// ONLY A TIMESTAMP AND COUNTS. The beat names hosts, crawler scripts and the sources the emptiness audit
// looked at; this route is public, so none of that crosses. The full beat stays readable on the machine.

import type { Env } from "./types.ts";
import { json } from "./util.ts";

const KEY = "_aqueduct/guard_heartbeat.json";

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
