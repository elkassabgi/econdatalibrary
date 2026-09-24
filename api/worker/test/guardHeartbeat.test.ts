// /v1/guard-heartbeat (src/guardHeartbeat.ts): the watchdog's beat for an off-machine reader, reduced to a
// timestamp and counts - no host, crawler or source name crosses (the beat names all three).
import assert from "node:assert/strict";
import { test } from "node:test";

import { handleGuardHeartbeat } from "../src/guardHeartbeat.ts";

const BEAT = {
  utc: "2026-09-24T14:00:00+00:00",
  host: "WORKSTATION-NAME",
  jobs_alive: ["ingest_a.py", "ingest_b.py"],
  jobs_detail: [{ name: "ingest_a.py", pid: 1, age_s: 10 }],
  table_ok: true,
  tracked: ["ingest_a.py", "ingest_b.py", "ingest_c.py"],
  emptiness: { ran: true, fetch_without_write: 0, detail: { some_source: ["unit"] } },
};

function envWith(obj: unknown) {
  return {
    SERIES_BUCKET: {
      async get(key: string) {
        assert.equal(key, "_aqueduct/guard_heartbeat.json");
        return obj === undefined ? null : { json: async () => obj };
      },
    },
  } as never;
}

test("the beat is served as a timestamp and counts only", async () => {
  const r = await handleGuardHeartbeat(envWith(BEAT));
  assert.equal(r.status, 200);
  assert.equal(r.headers.get("cache-control"), "no-store");
  const body = await r.json() as Record<string, unknown>;
  assert.deepEqual(body, { utc: BEAT.utc, table_ok: true, jobs_alive: 2, jobs_tracked: 3,
                           emptiness_ran: true, fetch_without_write: 0 });
  const text = JSON.stringify(body);
  for (const secret of ["WORKSTATION-NAME", "ingest_a", "ingest_c", "some_source"]) {
    assert.equal(text.includes(secret), false, secret);
  }
});

test("an absent beat is a 503, not a pass", async () => {
  const r = await handleGuardHeartbeat(envWith(undefined));
  assert.equal(r.status, 503);
  assert.equal((await r.json() as Record<string, unknown>).error, "heartbeat_absent");
});

test("an unreadable beat is a 503", async () => {
  const env = { SERIES_BUCKET: { async get() { return { json: async () => { throw new Error("bad"); } }; } } } as never;
  const r = await handleGuardHeartbeat(env);
  assert.equal(r.status, 503);
});
