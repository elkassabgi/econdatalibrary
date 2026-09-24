// The REAL src/index.ts, run in workerd through wrangler's unstable_dev, in both configurations (AR-150).
//
// The unit tests of src/localMode.ts cannot see how index.ts wires it: four wiring mutants (the gate's
// answer ignored, finalizeLocal not applied, the Cache API used for /v1/catalog in local mode, and the
// .csv local branch inverted - which makes PRODUCTION serve downloads with no auth and no log) passed tsc
// and every other test. These cases run the worker itself:
//   production (wrangler.toml):         a .csv with no key is 401
//   origin (wrangler.origin.toml):      no secret configured -> 503; wrong secret -> 403;
//                                       right secret -> a .csv answer that is NOT 401, no-store;
//                                       /v1/pv -> 404 (edge-only)
// Bindings are simulated locally (empty D1, no sidecar), so the data answers are errors - the point is
// the gate, the auth path and the headers, not the data.
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { unstable_dev } from "wrangler";

const CSV = "/v1/series/ecb%3AEXR%3AD.AUD.EUR.SP00.A.csv";
const QUIET = { disableExperimentalWarning: true, disableDevRegistry: true } as const;

async function start(config: string, vars: Record<string, string>) {
  return unstable_dev("src/index.ts", {
    config, vars, local: true, persistTo: mkdtempSync(join(tmpdir(), "econ-wiring-")),
    logLevel: "none", experimental: QUIET,
  });
}

test("production: a .csv download with no key is refused 401 (auth runs)", { timeout: 120_000 }, async (t) => {
  const w = await start("wrangler.toml", {});
  t.after(() => w.stop());
  const r = await w.fetch(CSV);
  assert.equal(r.status, 401, await r.text());
});

test("origin: no ORIGIN_SECRET configured refuses every request 503", { timeout: 120_000 }, async (t) => {
  // explicitly EMPTY: a workstation's .dev.vars would otherwise supply the real secret (CI has none)
  const w = await start("wrangler.origin.toml", { ORIGIN_SECRET: "" });
  t.after(() => w.stop());
  for (const p of [CSV, "/v1/sources", "/v1/catalog"]) {
    const r = await w.fetch(p, { headers: { "x-econ-origin-secret": "anything" } });
    assert.equal(r.status, 503, p);
    await r.arrayBuffer();
  }
});

test("origin: the gate, the skipped auth, the edge-only routes and no-store", { timeout: 120_000 }, async (t) => {
  const w = await start("wrangler.origin.toml", { ORIGIN_SECRET: "wiring-test-secret" });
  t.after(() => w.stop());
  const noKey = await w.fetch(CSV);
  assert.equal(noKey.status, 403);
  await noKey.arrayBuffer();
  const wrong = await w.fetch(CSV, { headers: { "x-econ-origin-secret": "not-it" } });
  assert.equal(wrong.status, 403);
  await wrong.arrayBuffer();
  const ok = await w.fetch(CSV, { headers: { "x-econ-origin-secret": "wiring-test-secret" } });
  assert.notEqual(ok.status, 401, "the origin must not run download auth (the edge did)");
  assert.notEqual(ok.status, 403);
  assert.equal(ok.headers.get("cache-control"), "private, no-store", "finalizeLocal applied");
  await ok.arrayBuffer();
  const pv = await w.fetch("/v1/pv?p=/", { headers: { "x-econ-origin-secret": "wiring-test-secret" } });
  assert.equal(pv.status, 404, "edge-only route");
  await pv.arrayBuffer();
});

test("the /v1/catalog branch skips the Cache API in local mode before it is touched", () => {
  const src = readFileSync(new URL("../src/index.ts", import.meta.url), "utf8");
  const local = src.indexOf('if (path === "/v1/catalog" && local) return await handleCatalog(url, env);');
  const cache = src.indexOf("const cache = caches.default;");
  assert.ok(local > 0 && cache > 0 && local < cache, "the local short-circuit must come before caches.default");
  assert.match(src, /const statsCache = local \? null : caches\.default;/);
});
