// A BEHAVIOUR pin (reviews R1178, R1179): once the edge keeps its own state in USERS (EDGE_STATE = "users" or
// FORWARD = "on"), the page-view beacon, its report and the cost guard must not touch econ D1 or econ R2 AT
// ALL. `env` itself is a Proxy that records every read of a forbidden binding, whatever the spelling
// (property, computed name, destructuring, `in`, Object.keys) - and planted positives prove the recorder
// sees an access.
//
// R1179: the first version reached only the cost guard's BLIND branch (no token bound) and called the
// handlers directly. It now drives the worker's own fetch() and scheduled() entry points, runs every cost
// guard branch (measured OK, measured breach, measurement failed, blind), makes USERS throw "no such table"
// once so the missing-table branches run, and runs every pending timer and waitUntil before it looks.
// R1184: a fault fires once PER STATEMENT KIND (the report had its own missing-table branch), "broken" D1
// errors, a report cache HIT, an untracked page path and setImmediate are all reached too.
import assert from "node:assert/strict";
import { mock, test } from "node:test";

import worker from "../src/index.ts";

const FORBIDDEN = ["CATALOG", "CATALOG_CLIMATE", "SERIES_BUCKET"];

type Fault = "none" | "missing" | "broken";

/** A D1 stand-in that records SQL and answers empty. A fault fires ONCE PER STATEMENT KIND (the beacon's
 *  INSERT, the report's SELECT, the cost guard's batch), so every entry point meets it (R1184: one shared
 *  "missing once" was used up by the beacon and the report's own branch never ran). "missing" = the table
 *  is not there yet; "broken" = any other D1 error. */
function fakeD1(log: string[], fault: Fault) {
  const pending = new Set(fault === "none" ? [] : ["INSERT", "SELECT", "econ_ops_status"]);
  const exec = (sql: string) => {
    log.push(sql);
    if (/CREATE TABLE/i.test(sql)) return;
    const kind = /econ_ops_status/.test(sql) ? "econ_ops_status" : /^\s*INSERT/i.test(sql) ? "INSERT" : "SELECT";
    if (pending.has(kind) && /econ_pageview|econ_ops_status/.test(sql)) {
      pending.delete(kind);
      throw new Error(fault === "missing" ? "D1_ERROR: no such table: econ_pageview: SQLITE_ERROR"
                                          : "D1_ERROR: database is locked: SQLITE_BUSY");
    }
  };
  const stmt = (sql: string) => ({
    bind: (..._a: unknown[]) => stmt(sql),
    run: async () => { exec(sql); return { success: true, meta: {} }; },
    all: async () => { exec(sql); return { results: [], meta: {} }; },
    first: async () => { exec(sql); return null; },
    raw: async () => { exec(sql); return []; },
  });
  return { prepare: (sql: string) => stmt(sql),
           batch: async (s: { run: () => Promise<unknown> }[]) => { const out = []; for (const x of s) out.push(await x.run()); return out; },
           exec: async (sql: string) => { exec(sql); return { count: 1, duration: 0 }; } };
}

function recordingEnv(vars: Record<string, string>, fault: Fault = "none") {
  const touched = new Set<string>();
  const users: string[] = [];
  const poison = new Proxy({}, { get: () => { throw new Error("econ storage used"); } });
  const base: Record<string, unknown> = { ...vars, USERS: fakeD1(users, fault), CATALOG: poison,
                                           CATALOG_CLIMATE: poison, SERIES_BUCKET: poison };
  const note = (k: string | symbol) => { if (FORBIDDEN.includes(String(k))) touched.add(String(k)); };
  const env = new Proxy(base, {
    get(t, k, r) { note(k); return Reflect.get(t, k, r); },
    has(t, k) { note(k); return Reflect.has(t, k); },
    getOwnPropertyDescriptor(t, k) { note(k); return Reflect.getOwnPropertyDescriptor(t, k); },
    // Object.keys / spread / JSON.stringify(env) enumerate the bindings: that is a read of each of them
    ownKeys(t) { FORBIDDEN.forEach((k) => touched.add(k)); return Reflect.ownKeys(t); },
  });
  return { env: env as never, touched, users };
}

/** The analytics API: "ok" (small totals), "breach" (over every limit), "fail" (HTTP 500). */
function stubAnalytics(mode: "ok" | "breach" | "fail") {
  const n = mode === "breach" ? 10_000_000_000 : 5;
  return mock.method(globalThis, "fetch", async () => {
    if (mode === "fail") return new Response("upstream", { status: 500 });
    return Response.json({ data: { viewer: { accounts: [{
      d1AnalyticsAdaptiveGroups: [{ sum: { rowsRead: n, rowsWritten: n } }],
      r2OperationsAdaptiveGroups: [{ dimensions: { actionType: "PutObject" }, sum: { requests: n } }],
    }] } } });
  });
}

async function exercise(env: never, cacheHit = false) {
  const pending: Promise<unknown>[] = [];
  const ctx = { waitUntil: (p: Promise<unknown>) => { pending.push(p); }, passThroughOnException: () => {} } as never;
  // A cache HIT answers the report from the edge cache (the branch R1184 found unreached); a miss runs it.
  const hit = () => new Response(JSON.stringify({ cached: true }), { headers: { "content-type": "application/json" } });
  const caches = { default: { match: async () => (cacheHit ? hit() : undefined), put: async () => {} } };
  const hadCaches = "caches" in globalThis;
  const savedCaches = (globalThis as any).caches;
  (globalThis as any).caches = caches;
  mock.timers.enable({ apis: ["setTimeout", "setInterval", "setImmediate"] });
  try {
    // Every call may throw or answer an error (the poisoned bindings throw on use; a breach throws by
    // design); the recorder has logged the access before any throw.
    const calls = [
      () => worker.fetch(new Request("https://e.example/v1/pv?p=%2Fabout"), env, ctx),
      () => worker.fetch(new Request("https://e.example/v1/pv?p=%2Fnot-a-tracked-page"), env, ctx),
      () => worker.fetch(new Request("https://e.example/v1/pv/report?days=3"), env, ctx),
      () => worker.scheduled({ cron: "*/30 * * * *", scheduledTime: Date.now() } as never, env, ctx),
    ];
    for (const c of calls) await Promise.resolve().then(c).catch(() => undefined);
    mock.timers.tick(7 * 86_400_000);   // every timer and immediate (runAll() throws on a lone immediate)                          // a read deferred to a timer is still a read
    await Promise.allSettled(pending);
    mock.timers.tick(7 * 86_400_000);   // every timer and immediate (runAll() throws on a lone immediate)
    await Promise.allSettled(pending);
  } finally {
    mock.timers.reset();
    if (hadCaches) (globalThis as any).caches = savedCaches; else delete (globalThis as any).caches;
  }
}

const STATES = [{ EDGE_STATE: "users" }, { FORWARD: "on", ORIGIN_URL: "https://origin.example", ORIGIN_SECRET: "s" }];
const GUARD = [
  ["blind", {}],
  ["measured ok", { CF_ANALYTICS_TOKEN: "t", CF_ACCOUNT_ID: "a" }],
  ["measured breach", { CF_ANALYTICS_TOKEN: "t", CF_ACCOUNT_ID: "a" }],
  ["measurement failed", { CF_ANALYTICS_TOKEN: "t", CF_ACCOUNT_ID: "a" }],
] as const;

for (const vars of STATES) {
  for (const [branch, guardVars] of GUARD) {
    for (const fault of ["none", "missing", "broken"] as Fault[]) {
      for (const cacheHit of [false, true]) {
        test(`${JSON.stringify(vars)}, cost guard ${branch}, USERS fault ${fault}, report cache ${cacheHit ? "hit" : "miss"}: `
             + "no econ D1 or R2", async () => {
          const fetchMock = stubAnalytics(branch === "measured breach" ? "breach" : branch === "measurement failed" ? "fail" : "ok");
          try {
            const { env, touched, users } = recordingEnv({ ...vars, ...guardVars }, fault);
            await exercise(env, cacheHit);
            assert.deepEqual([...touched], [], "no read of CATALOG / CATALOG_CLIMATE / SERIES_BUCKET, in any spelling");
            assert.ok(users.some((s) => /INSERT INTO econ_pageview/.test(s)), "the page view went to USERS");
            if (!cacheHit) assert.ok(users.some((s) => /FROM econ_pageview/.test(s)), "the report read USERS");
            assert.ok(users.some((s) => s.includes("econ_ops_status")), "the cost-guard record went to USERS");
            if (fault === "missing") {
              const ddl = users.filter((s) => /CREATE TABLE IF NOT EXISTS econ_pageview/.test(s)).length;
              assert.ok(ddl >= (cacheHit ? 1 : 1), "the beacon's missing-table branch ran");
            }
            if (branch !== "blind") assert.ok(fetchMock.mock.callCount() > 0, "the measured branch ran");
          } finally {
            fetchMock.mock.restore();
          }
        });
      }
    }
  }
}

test("planted positive: with neither set, the same entry points DO reach econ storage", async () => {
  const fetchMock = stubAnalytics("ok");
  try {
    const { env, touched } = recordingEnv({ CF_ANALYTICS_TOKEN: "t", CF_ACCOUNT_ID: "a" });
    await exercise(env);
    assert.ok(touched.has("CATALOG"), "the beacon used econ D1");
    assert.ok(touched.has("SERIES_BUCKET"), "the cost guard used econ R2");
  } finally {
    fetchMock.mock.restore();
  }
});

test("planted positive: the recorder sees destructuring, `in`, Object.keys and a deferred read", async () => {
  for (const probe of [
    (e: any) => { const { CATALOG_CLIMATE } = e; return CATALOG_CLIMATE; },
    (e: any) => "SERIES_BUCKET" in e,
    (e: any) => Object.keys(e),
    (e: any) => { setTimeout(() => e["CATA" + "LOG"], 60_000); },
    (e: any) => { setImmediate(() => e["SERIES_" + "BUCKET"]); },
  ]) {
    const { env, touched } = recordingEnv({ EDGE_STATE: "users" });
    mock.timers.enable({ apis: ["setTimeout", "setImmediate"] });
    try {
      probe(env);
      mock.timers.tick(7 * 86_400_000);   // every timer and immediate (runAll() throws on a lone immediate)
    } finally {
      mock.timers.reset();
    }
    assert.ok(touched.size > 0, String(probe));
  }
});
