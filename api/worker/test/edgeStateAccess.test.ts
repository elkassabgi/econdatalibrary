// A BEHAVIOUR pin (review R1178): once the edge keeps its own state in USERS (EDGE_STATE = "users" or
// FORWARD = "on"), the page-view beacon, its report and the cost guard must not touch econ D1 or econ R2 AT
// ALL. The earlier pin counted the text `env.CATALOG`, which `(env as any).CATALOG`, `env["CATALOG"]`,
// destructuring or optional chaining all slip past. Here `env` itself is a Proxy that records every read of
// a forbidden binding, whatever the spelling - and a planted positive proves the recorder sees an access.
import assert from "node:assert/strict";
import { test } from "node:test";

import { handlePageview, handlePageviewReport } from "../src/pageview.ts";
import { runCostGuard } from "../src/costGuard.ts";

const FORBIDDEN = ["CATALOG", "CATALOG_CLIMATE", "SERIES_BUCKET"];

/** A tiny D1 stand-in that records SQL and answers empty. */
function fakeD1(log: string[]) {
  const stmt = (sql: string) => ({
    bind: (..._a: unknown[]) => stmt(sql),
    run: async () => { log.push(sql); return { success: true }; },
    all: async () => { log.push(sql); return { results: [] }; },
    first: async () => { log.push(sql); return null; },
  });
  return { prepare: (sql: string) => stmt(sql), batch: async (s: { run: () => Promise<unknown> }[]) => Promise.all(s.map((x) => x.run())) };
}

function recordingEnv(vars: Record<string, string>) {
  const touched = new Set<string>();
  const users: string[] = [];
  const poison = new Proxy({}, { get: () => { throw new Error("econ storage used"); } });
  const base: Record<string, unknown> = { ...vars, USERS: fakeD1(users), CATALOG: poison, CATALOG_CLIMATE: poison,
                                           SERIES_BUCKET: poison };
  const env = new Proxy(base, {
    get(t, k, r) {
      if (FORBIDDEN.includes(String(k))) touched.add(String(k));
      return Reflect.get(t, k, r);
    },
    has(t, k) {
      if (FORBIDDEN.includes(String(k))) touched.add(String(k));
      return Reflect.has(t, k);
    },
    getOwnPropertyDescriptor(t, k) {
      if (FORBIDDEN.includes(String(k))) touched.add(String(k));
      return Reflect.getOwnPropertyDescriptor(t, k);
    },
  });
  return { env: env as never, touched, users };
}

async function exercise(env: never) {
  // Each call is allowed to throw (the poisoned bindings throw on use, and the cost guard always throws its
  // BLIND verdict here); the recorder has logged the access before any throw.
  await handlePageview(new URL("https://e.example/v1/pv?p=%2Fabout"), env).catch(() => undefined);
  await handlePageviewReport(new URL("https://e.example/v1/pv/report?days=3"), env).catch(() => undefined);
  await runCostGuard(env).catch(() => undefined);
}

for (const vars of [{ EDGE_STATE: "users" }, { FORWARD: "on" }]) {
  test(`with ${JSON.stringify(vars)} the edge's own writers never touch econ D1 or R2`, async () => {
    const { env, touched, users } = recordingEnv(vars);
    await exercise(env);
    assert.deepEqual([...touched], [], "no read of CATALOG / CATALOG_CLIMATE / SERIES_BUCKET, in any spelling");
    assert.ok(users.some((s) => s.includes("econ_pageview")), "the page view went to USERS");
    assert.ok(users.some((s) => s.includes("econ_ops_status")), "the cost-guard record went to USERS");
  });
}

test("planted positive: with neither set, the same code DOES reach econ storage (the recorder can see it)", async () => {
  const { env, touched } = recordingEnv({});
  await exercise(env);
  assert.ok(touched.has("CATALOG"), "the beacon used econ D1");
  assert.ok(touched.has("SERIES_BUCKET"), "the cost guard used econ R2");
});
