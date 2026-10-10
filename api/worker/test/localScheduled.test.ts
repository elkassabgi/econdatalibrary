// The self-hosted origin never runs the scheduled handler's work (review AR-268).
//
// miniflare's entry worker answers /cdn-cgi/mf/scheduled before the worker's fetch() and its secret gate, so
// on the origin a caller that reaches an instance on that path could start scheduled() (read in miniflare's
// code; not run on the real `wrangler dev` chain). The router refuses the path
// (tests/test_selfhost_router.py); this pins the second barrier: with LOCAL = "1" the handler returns
// before it reads another binding, starts a task or opens a connection.
// The control beside it makes the same call without LOCAL and DOES see the cost guard run, so a rig that
// could not see the work would fail here.
import assert from "node:assert/strict";
import { mock, test } from "node:test";

import worker from "../src/index.ts";

function rig(vars: Record<string, string>) {
  const sql: string[] = [];
  const stmt = (s: string) => ({
    bind: (..._a: unknown[]) => stmt(s),
    run: async () => { sql.push(s); return { success: true, meta: {} }; },
    all: async () => { sql.push(s); return { results: [], meta: {} }; },
    first: async () => { sql.push(s); return null; },
    raw: async () => { sql.push(s); return []; },
  });
  const users = {
    prepare: (s: string) => stmt(s),
    batch: async (b: { run: () => Promise<unknown> }[]) => { const out = []; for (const x of b) out.push(await x.run()); return out; },
    exec: async (s: string) => { sql.push(s); return { count: 1, duration: 0 }; },
  };
  const read: string[] = [];
  const env = new Proxy({ ...vars, USERS: users } as Record<string, unknown>, {
    get(t, k, r) { read.push(String(k)); return Reflect.get(t, k, r); },
  });
  const pending: Promise<unknown>[] = [];
  const ctx = { waitUntil: (p: Promise<unknown>) => { pending.push(p); }, passThroughOnException: () => {} };
  return { env: env as never, ctx: ctx as never, sql, read, pending };
}

async function runScheduled(vars: Record<string, string>) {
  const r = rig(vars);
  const fetchMock = mock.method(globalThis, "fetch", async () => new Response("upstream", { status: 500 }));
  mock.timers.enable({ apis: ["setTimeout", "setInterval", "setImmediate"] });
  try {
    await Promise.resolve()
      .then(() => worker.scheduled({ cron: "*/30 * * * *", scheduledTime: Date.now() } as never, r.env, r.ctx))
      .catch(() => undefined);
    for (let i = 0; i < 2; i++) {
      mock.timers.tick(7 * 86_400_000);                  // a step deferred to a timer still counts
      await Promise.allSettled(r.pending);
    }
    return { ...r, fetches: fetchMock.mock.callCount() };
  } finally {
    mock.timers.reset();
    fetchMock.mock.restore();
  }
}

const GUARD_VARS = { EDGE_STATE: "users", CF_ANALYTICS_TOKEN: "t", CF_ACCOUNT_ID: "a" };

test("LOCAL = '1': scheduled() starts nothing - no task, no SQL, no connection, no binding read but LOCAL", async () => {
  const r = await runScheduled({ ...GUARD_VARS, LOCAL: "1", ORIGIN_SECRET: "s" });
  assert.equal(r.pending.length, 0, "no waitUntil");
  assert.deepEqual(r.sql, [], "no statement on USERS");
  assert.equal(r.fetches, 0, "no outbound request");
  assert.deepEqual([...new Set(r.read)], ["LOCAL"], "only LOCAL was read");
});

test("control: without LOCAL = '1' the same call runs the cost guard", async () => {
  for (const local of [undefined, "", "0", "true"]) {
    const r = await runScheduled(local === undefined ? GUARD_VARS : { ...GUARD_VARS, LOCAL: local });
    assert.equal(r.pending.length, 1, `one waitUntil (LOCAL = ${JSON.stringify(local)})`);
    assert.ok(r.fetches >= 1, "the measurement was asked for");
    assert.ok(r.sql.some((s) => s.includes("econ_ops_status")), "the cost-guard record went to USERS");
  }
});
