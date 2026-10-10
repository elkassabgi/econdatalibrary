// The SOAK switch (src/edge.ts isSoak; api/worker/wrangler.soak.toml; self-hosting plan step 4).
//
// A soak worker is a public test address bound to the production users database. With SOAK = "1" it
// answers neither page-view route (they would create and fill econ_pageview there) and its scheduled
// handler does nothing (it would write econ_ops_status). Everything else is as on any forwarding edge.
// Every "does nothing" below has a control beside it: the same call WITHOUT the switch runs its
// statements on the same recording database, so a rig that could not see the work would fail.
import assert from "node:assert/strict";
import { mock, test } from "node:test";

import worker from "../src/index.ts";
import { edgeStatus, isSoak } from "../src/edge.ts";

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
  const pending: Promise<unknown>[] = [];
  const ctx = { waitUntil: (p: Promise<unknown>) => { pending.push(p); }, passThroughOnException: () => {} };
  return { env: { ...vars, USERS: users } as never, ctx: ctx as never, sql, pending };
}

/** Runs `call` with the network, the edge cache and the timers replaced; returns what it saw. */
async function withStubs<T>(call: () => Promise<T>, pending: Promise<unknown>[]) {
  const fetchMock = mock.method(globalThis, "fetch", async () => new Response("upstream", { status: 500 }));
  const hadCaches = "caches" in globalThis;
  const savedCaches = (globalThis as any).caches;
  (globalThis as any).caches = { default: { match: async () => undefined, put: async () => {} } };
  mock.timers.enable({ apis: ["setTimeout", "setInterval", "setImmediate"] });
  try {
    const out = await call();
    for (let i = 0; i < 2; i++) {
      mock.timers.tick(7 * 86_400_000);
      await Promise.allSettled(pending);
    }
    return { out, fetches: fetchMock.mock.callCount() };
  } finally {
    mock.timers.reset();
    fetchMock.mock.restore();
    if (hadCaches) (globalThis as any).caches = savedCaches; else delete (globalThis as any).caches;
  }
}

const FORWARDING = { FORWARD: "on", ORIGIN_URL: "https://origin.example", ORIGIN_SECRET: "s" };
const get = (r: ReturnType<typeof rig>, path: string) =>
  withStubs(() => worker.fetch(new Request("https://soak.example" + path), r.env, r.ctx), r.pending);

test("the switch is on only for SOAK = '1'", () => {
  assert.equal(isSoak({ SOAK: "1" }), true);
  for (const v of [undefined, "", "0", "true", "yes", "on", " 1", "1 "]) assert.equal(isSoak({ SOAK: v }), false, String(v));
});

test("a soak worker answers neither page-view route, and runs no statement for them", async () => {
  for (const path of ["/v1/pv?p=%2Fabout", "/v1/pv", "/v1/pv/report?days=3", "/v1/pv/report"]) {
    const r = rig({ ...FORWARDING, SOAK: "1" });
    const { out, fetches } = await get(r, path);
    assert.equal(out.status, 404, path);
    assert.deepEqual(await out.json(), { error: "not_found", detail: `no route for ${path.split("?")[0]}` });
    assert.deepEqual(r.sql, [], `${path}: no statement on USERS`);
    assert.equal(fetches, 0, `${path}: no outbound request`);
    assert.equal(r.pending.length, 0, `${path}: no deferred work`);
  }
});

test("control: without the switch the same two routes do their work on USERS", async () => {
  for (const soak of [undefined, "0", "true"]) {
    const vars = soak === undefined ? FORWARDING : { ...FORWARDING, SOAK: soak };
    let r = rig(vars);
    let res = (await get(r, "/v1/pv?p=%2Fabout")).out;
    assert.notEqual(res.status, 404, `pixel, SOAK = ${JSON.stringify(soak)}`);
    assert.ok(r.sql.some((s) => /INSERT INTO econ_pageview/.test(s)), "the page view was written");
    r = rig(vars);
    res = (await get(r, "/v1/pv/report?days=3")).out;
    assert.notEqual(res.status, 404, `report, SOAK = ${JSON.stringify(soak)}`);
    assert.ok(r.sql.some((s) => /FROM econ_pageview/.test(s)), "the report was read");
  }
});

test("a soak worker's scheduled handler starts nothing; without the switch it runs the cost guard", async () => {
  const guard = { ...FORWARDING, CF_ANALYTICS_TOKEN: "t", CF_ACCOUNT_ID: "a" };
  const run = async (vars: Record<string, string>) => {
    const r = rig(vars);
    const { fetches } = await withStubs(async () => {
      await Promise.resolve()
        .then(() => worker.scheduled({ cron: "*/30 * * * *", scheduledTime: Date.now() } as never, r.env, r.ctx))
        .catch(() => undefined);
    }, r.pending);
    return { ...r, fetches };
  };
  const soak = await run({ ...guard, SOAK: "1" });
  assert.equal(soak.pending.length, 0, "no waitUntil");
  assert.deepEqual(soak.sql, [], "no statement on USERS");
  assert.equal(soak.fetches, 0, "no outbound request");
  const plain = await run(guard);
  assert.equal(plain.pending.length, 1);
  assert.ok(plain.fetches >= 1, "the measurement was asked for");
  assert.ok(plain.sql.some((s) => s.includes("econ_ops_status")), "the cost-guard record went to USERS");
});

test("/v1/edge-status says whether the worker is a soak worker", async () => {
  for (const [vars, want] of [[{ SOAK: "1" }, true], [{}, false], [{ SOAK: "true" }, false]] as const) {
    const body = await edgeStatus({ ...FORWARDING, ...vars }).json() as Record<string, unknown>;
    assert.equal(body.soak, want);
    assert.equal(body.forward, true);
    assert.deepEqual(Object.keys(body).sort(),
      ["commit", "edge_state", "edge_state_raw", "forward", "forward_raw", "origin_configured", "soak"]);
  }
  const r = rig({ ...FORWARDING, SOAK: "1" });
  const res = (await get(r, "/v1/edge-status")).out;
  assert.equal(res.status, 200);
  assert.equal(((await res.json()) as { soak: boolean }).soak, true);
});

test("the other routes of a soak worker are not closed: public-stats answers, a data route is forwarded", async () => {
  let r = rig({ ...FORWARDING, SOAK: "1" });
  const stats = await get(r, "/v1/public-stats");
  assert.equal(stats.out.status, 200, "public-stats stays open on the soak worker (it writes nothing)");
  assert.ok(!r.sql.some((s) => /^\s*(INSERT|UPDATE|DELETE|CREATE)/i.test(s)), "and it wrote nothing");
  r = rig({ ...FORWARDING, SOAK: "1" });
  const fwd = await get(r, "/v1/sources");
  assert.equal(fwd.fetches, 1, "one request went to the origin");
  assert.equal(fwd.out.status, 502, "the stub origin's answer has no origin mark: the edge's 502, not a 404");
});
