// /v1/guard-heartbeat (src/guardHeartbeat.ts): the watchdog's beat for an off-machine reader, reduced to a
// timestamp and counts - no host, crawler or source name crosses (the beat names all three).
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { cachedGuardHeartbeat, handleGuardHeartbeat } from "../src/guardHeartbeat.ts";

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

test("every field is the beat's own, not a constant (R1226: fixed true/0 mutants survived)", async () => {
  const r = await handleGuardHeartbeat(envWith({ ...BEAT, table_ok: false,
    emptiness: { ran: false, fetch_without_write: 3 } }));
  assert.deepEqual(await r.json(), { utc: BEAT.utc, table_ok: false, jobs_alive: 2, jobs_tracked: 3,
                                     emptiness_ran: false, fetch_without_write: 3 });
});

test("the router sends /v1/guard-heartbeat to this handler (R1226: deleting the line passed)", () => {
  const src = readFileSync(new URL("../src/index.ts", import.meta.url), "utf8");
  assert.match(src, /path === "\/v1\/guard-heartbeat"\) return await cachedGuardHeartbeat\(url, env, ctx, local\)/);
});

test("the edge keeps a 200 for 60 s under a fixed key; the origin never caches", async () => {
  const store = new Map<string, Response>();
  const puts: string[] = [];
  (globalThis as { caches?: unknown }).caches = {
    default: {
      async match(k: Request) { return store.get(k.url)?.clone(); },
      async put(k: Request, v: Response) { puts.push(k.url); store.set(k.url, v); },
    },
  };
  const waits: Promise<unknown>[] = [];
  const ctx = { waitUntil: (p: Promise<unknown>) => waits.push(p) } as never;
  try {
    const r1 = await cachedGuardHeartbeat(new URL("https://e.example/v1/guard-heartbeat?x=1"), envWith(BEAT), ctx, false);
    await Promise.all(waits);
    assert.equal(r1.headers.get("cache-control"), "public, max-age=0, s-maxage=60");
    assert.deepEqual(puts, ["https://e.example/v1/guard-heartbeat"], "the query string is not part of the key");
    const r2 = await cachedGuardHeartbeat(new URL("https://e.example/v1/guard-heartbeat?y=2"), envWith(undefined), ctx, false);
    assert.equal(r2.status, 200, "served from the cache, whatever the query");
    const absent = await cachedGuardHeartbeat(new URL("https://o.example/v1/guard-heartbeat"), envWith(undefined), ctx, false);
    assert.equal(absent.status, 503);
    assert.equal(puts.length, 1, "a 503 is never cached");
    const origin = await cachedGuardHeartbeat(new URL("https://e.example/v1/guard-heartbeat"), envWith(undefined), ctx, true);
    assert.equal(origin.status, 503, "local (the origin): no cache at all");
  } finally {
    delete (globalThis as { caches?: unknown }).caches;
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
