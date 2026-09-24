// The forwarding edge (src/edge.ts, plan code change 1): unit rules, then the REAL index.ts in workerd
// (unstable_dev) forwarding to a stand-in origin that records what it receives, with a second server
// standing for any host a client might try to steer the secret to (review R1169).
//
// The FORWARD-on worker runs with the EDGE-ONLY config (test/_harness.ts): the econ D1 and econ R2
// bindings are removed, so any path that still touches them fails instead of reading an empty simulation
// (review R1172). A planted positive proves the removal bites.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createServer, request as httpRequest, type IncomingHttpHeaders, type Server } from "node:http";
import { test } from "node:test";

import * as edge from "../src/edge.ts";
import * as localMode from "../src/localMode.ts";
import { NON_REDISTRIBUTABLE } from "../src/denylist.ts";
import { all, at, EDGE_CONFIG, edgeOnlyConfig, execSql, newPersist, start, withBindings, type Dev } from "./_harness.ts";

test("the header names match the origin's", () => {
  assert.equal(edge.ORIGIN_SECRET_HEADER, localMode.ORIGIN_SECRET_HEADER);
  assert.equal(edge.COUNT_HEADER, localMode.COUNT_HEADER);
  assert.equal(edge.ORIGIN_MARK_HEADER, localMode.ORIGIN_MARK_HEADER);
});

test("the origin marks every answer it gives", () => {
  for (const r of [new Response("x"), new Response("e", { status: 500 }), new Response(null, { status: 404 })]) {
    assert.equal(localMode.finalizeLocal(r).headers.get(localMode.ORIGIN_MARK_HEADER), "1");
  }
});

test("forwarding is on only for FORWARD = 'on'", () => {
  assert.equal(edge.isForward({ FORWARD: "on" }), true);
  for (const v of [undefined, "", "off", "1", "true", "ON"]) assert.equal(edge.isForward({ FORWARD: v }), false, String(v));
});

const CONFIGURED = { ORIGIN_URL: "https://o.example", ORIGIN_SECRET: "real" };

test("the origin request carries only allowlisted headers, never the client's credentials", () => {
  const client = new Request("https://edge.example/v1/series/x.csv?api_key=CLIENTKEY&from=2020-01-01", {
    headers: { "x-api-key": "CLIENTKEY", authorization: "Bearer T", cookie: "c=1",
               "x-econ-origin-secret": "forged", "cf-access-client-id": "forged", "cf-connecting-ip": "1.2.3.4",
               "x-forwarded-for": "1.2.3.4", "x-elkassabgi-client": "py", referer: "https://r.example/",
               "user-agent": "ua", accept: "text/csv", "accept-language": "fr" },
  });
  assert.equal(edge.originRequest(client, {}), null, "no origin configured -> nothing forwarded");
  assert.equal(edge.originRequest(client, { ORIGIN_URL: "https://o.example" }), null, "no secret -> nothing forwarded");
  assert.equal(edge.originRequest(client, { ...CONFIGURED, ORIGIN_URL: "https://o.example/base" }), null,
               "ORIGIN_URL must be a bare origin");
  assert.equal(edge.originRequest(client, { ...CONFIGURED, ORIGIN_URL: "not a url" }), null);
  const r = edge.originRequest(client, { ...CONFIGURED, ORIGIN_ACCESS_ID: "id", ORIGIN_ACCESS_SECRET: "sec" })!;
  const u = new URL(r.url);
  assert.equal(u.origin + u.pathname, "https://o.example/v1/series/x.csv");
  assert.equal(u.searchParams.get("api_key"), null);
  assert.equal(u.searchParams.get("from"), "2020-01-01");
  assert.deepEqual([...r.headers.keys()].sort(),
    ["accept", "accept-language", "cf-access-client-id", "cf-access-client-secret", "user-agent", "x-econ-origin-secret"]);
  assert.equal(r.headers.get("x-econ-origin-secret"), "real", "overwritten, never passed through");
  assert.equal(r.headers.get("cf-access-client-id"), "id");
  assert.equal(r.redirect, "manual");
});

test("no client path can move the request off ORIGIN_URL's host", () => {
  for (const p of ["//evil.example/v1/sources", "/\\evil.example/v1/sources", "//evil.example:8443/x",
                   "/%2F%2Fevil.example/x", "/v1/series/..%2F..%2F@evil.example.csv"]) {
    const r = edge.originRequest(new Request("https://edge.example" + p), CONFIGURED);
    // NOT `if (r)`: that passed vacuously on null (R1175). The path is SET on ORIGIN_URL, so the request
    // exists and stays on the origin's host; the origin check behind it is a second, separate layer.
    assert.ok(r, p);
    assert.equal(new URL(r.url).origin, "https://o.example", p);
  }
});

test("only the origin's own routes are forwarded", () => {
  for (const p of ["/", "/v1", "/v1/", "/v1/catalog", "/v1/sources", "/v1/last-updates", "/v1/stats", "/v1/bundle",
                   "/v1/series/a%3Ab.csv", "/v1/series/a%3Ab.metadata.json"]) assert.equal(edge.isForwardable(p), true, p);
  for (const p of ["//evil.example/v1/sources", "/v1/pv", "/v1/public-stats", "/v1/nope", "/admin", "/v1/catalogX"]) {
    assert.equal(edge.isForwardable(p), false, p);
  }
});

test("cache times per route: data, bundle and the rest are never cached", () => {
  const want: Record<string, number> = {
    "/v1/catalog": 21600, "/v1/stats": 21600, "/v1/sources": 300, "/v1/last-updates": 300,
    "/v1/series/a%3Ab.metadata.json": 3600, "/v1/series/a%3Ab.csv": 0, "/v1/bundle": 0, "/": 0, "/v1/pv": 0,
  };
  for (const [p, s] of Object.entries(want)) assert.equal(edge.cacheSeconds(p), s, p);
  assert.equal(new URL(edge.cacheKey(new Request("https://e.example/v1/catalog?q=a&api_key=K")).url).search, "?q=a");
});

test("a counted body reports the bytes on completion and on abort", async () => {
  const waits: Promise<unknown>[] = [];
  const ctx = { waitUntil: (p: Promise<unknown>) => { waits.push(p); } };
  const seen: [number, boolean][] = [];
  const src = () => new ReadableStream<Uint8Array>({
    start(c) { c.enqueue(new Uint8Array(10)); c.enqueue(new Uint8Array(5)); c.close(); },
  });
  const full = edge.countingBody(src(), async (b, ok) => { seen.push([b, ok]); }, ctx);
  assert.equal((await new Response(full).arrayBuffer()).byteLength, 15);
  await Promise.all(waits);
  assert.deepEqual(seen[0], [15, true]);

  const endless = new ReadableStream<Uint8Array>({ pull(c) { c.enqueue(new Uint8Array(4)); } });
  const cut = edge.countingBody(endless, async (b, ok) => { seen.push([b, ok]); }, ctx);
  const reader = cut.getReader();
  await reader.read();
  await reader.cancel("client went away");
  await Promise.all(waits);
  assert.equal(seen[1][1], false, "an aborted transfer is still reported");
  assert.ok(seen[1][0] > 0, "with the bytes that were taken");
});

test("a header timeout ABORTS the pending request to the origin", async (t) => {
  // In workerd the end of a request cancels its subrequests anyway, so the integration test below cannot
  // see a missing abort (R1175 follow-up: that mutant survived there). Under node, fetch keeps a pending
  // request open until the server answers - so here only a real abort closes it early.
  let closedEarly = 0;
  const server = createServer((_req, res) => {
    let answered = false;
    res.on("close", () => { if (!answered) closedEarly++; });
    setTimeout(() => { if (!res.destroyed) { answered = true; res.end("{}"); } }, 5000);
  });
  const host = await listen(server);
  t.after(() => { server.closeAllConnections(); server.close(); });
  const env = { ORIGIN_URL: `http://${host}`, ORIGIN_SECRET: "s", ORIGIN_TIMEOUT_MS: "200" };
  const resp = await edge.fetchOrigin(edge.originRequest(new Request("https://e.example/v1/sources"), env)!, env);
  assert.equal(resp.status, 504);
  const t0 = Date.now();
  while (closedEarly === 0 && Date.now() - t0 < 3000) await new Promise((r) => setTimeout(r, 50));
  assert.equal(closedEarly, 1, "the origin saw the request closed long before it answered");
});

test("the edge's own writers touch econ storage in exactly ONE place each (AR-151 finding 3)", () => {
  // A guarded access (`if (env.CATALOG) ...`) slips past the edge-only config test and the table checks,
  // so the sources are pinned: the store choice in pageview.ts, and the R2 branch of the cost guard.
  const src = (f: string) => readFileSync(at("src", f), "utf8").replace(/^\s*\/\/.*$/gm, "").replace(/\/\*[\s\S]*?\*\//g, "");
  const count = (s: string, needle: string) => s.split(needle).length - 1;
  assert.equal(count(src("pageview.ts"), "env.CATALOG"), 1, "pageview.ts reaches econ D1 only through store()");
  assert.equal(count(src("pageview.ts"), "env.CATALOG_CLIMATE"), 0);
  assert.equal(count(src("pageview.ts"), "env.SERIES_BUCKET"), 0);
  assert.equal(count(src("costGuard.ts"), "env.SERIES_BUCKET"), 1, "costGuard.ts reaches econ R2 only in writeStatus");
  assert.equal(count(src("costGuard.ts"), "env.CATALOG"), 0);
});

test("the client never sees the internal headers", () => {
  const o = new Response("x", { headers: { "x-econ-count": "1", "x-econ-origin": "1", "content-type": "text/csv" } });
  const c = edge.clientResponse(o, o.body);
  assert.equal(c.headers.get("x-econ-count"), null);
  assert.equal(c.headers.get("x-econ-origin"), null);
  assert.equal(c.headers.get("content-type"), "text/csv");
});

// ---- the real worker ----------------------------------------------------------------------------------
type Seen = { url: string; headers: IncomingHttpHeaders };
const CSV = "series_id,obs_date,value\nx,2020-01-01,1\n";
const MIGRATION = readFileSync(at("migrations", "users_selfhost.sql"), "utf8");
const OLD_PAGEVIEW = "CREATE TABLE pageview (path TEXT NOT NULL, day TEXT NOT NULL, " +
                     "hits INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (path, day))";
const USERS_SCHEMA = `
  CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, is_vip INTEGER, api_key TEXT, is_active INTEGER,
    api_key_expires_at TEXT, country TEXT DEFAULT '', institution TEXT DEFAULT '', hide_institution INTEGER);
  INSERT INTO users (id, email, is_vip, api_key, is_active) VALUES (7, 'u@example.org', 0, 'GOODKEY', 1);
  CREATE TABLE login_history (user_id INTEGER, country TEXT);
  CREATE TABLE rate_limits (key TEXT PRIMARY KEY, count INTEGER, window_start TEXT);
  CREATE TABLE econ_download_log (user_id INTEGER, series_id TEXT, ip TEXT, channel TEXT, bytes INTEGER,
    ts TEXT DEFAULT CURRENT_TIMESTAMP);`;

/** A persist folder whose users db has a real key, the tables auth and public-stats read, and (unless
 *  told otherwise) the migration; econ D1 gets today's pageview table. */
async function seeded(migrate = true): Promise<string> {
  const persist = newPersist("econ-edge-");
  await withBindings(EDGE_CONFIG, persist, async (env) => {
    await execSql(env.USERS, USERS_SCHEMA);
    if (migrate) await execSql(env.USERS, MIGRATION);
    await execSql(env.CATALOG, OLD_PAGEVIEW);
  });
  return persist;
}

async function listen(server: Server): Promise<string> {
  await new Promise<void>((ok) => server.listen(0, "127.0.0.1", () => ok()));
  const a = server.address();
  return `127.0.0.1:${typeof a === "object" && a ? a.port : 0}`;
}

/** Any host a client might try to aim the edge at. It must never receive a request. */
async function collector() {
  const seen: Seen[] = [];
  const server = createServer((req, res) => { seen.push({ url: req.url ?? "", headers: req.headers }); res.end("stolen"); });
  return { server, seen, host: await listen(server) };
}

/** The stand-in origin. Each path/mode stands for one thing a real origin or tunnel can answer. */
async function standInOrigin(collectorHost: string) {
  const seen: Seen[] = [];
  let sourcesDown = false;
  let slowAborted = 0;
  const mark = { "x-econ-origin": "1", "cache-control": "private, no-store" };
  const server = createServer((req, res) => {
    const url = req.url ?? "";
    seen.push({ url, headers: req.headers });
    const u = new URL(url, "http://x");
    const mode = u.searchParams.get("mode");
    if (mode === "redirect") {
      res.writeHead(302, { ...mark, location: `http://${collectorHost}/stolen` }); res.end(); return;
    }
    if (mode === "unmarked") {                                    // a tunnel error page, an Access login page
      res.writeHead(200, { "content-type": "text/html" }); res.end("<html>login</html>"); return;
    }
    if (mode === "slow") {
      let answered = false;
      res.on("close", () => { if (!answered) slowAborted++; });   // the edge gave up and closed it
      setTimeout(() => { if (!res.destroyed) { answered = true; res.writeHead(200, mark); res.end("{}"); } }, 8000);
      return;
    }
    if (u.pathname === "/v1/series/zz%3Alen.csv") {
      res.writeHead(200, { ...mark, "content-type": "text/csv", "content-length": String(Buffer.byteLength(CSV)) });
      res.end(CSV); return;
    }
    if (u.pathname === "/v1/series/zz%3Achunk.csv") {             // no content-length AND no count marker
      res.writeHead(200, { ...mark, "content-type": "text/csv" });
      res.write(CSV); res.end(CSV); return;
    }
    if (u.pathname === "/v1/series/zz%3Aerr.csv") {
      res.writeHead(500, { ...mark, "content-type": "application/json" }); res.end('{"error":"x"}'); return;
    }
    if (u.pathname === "/v1/sources" && sourcesDown) {
      res.writeHead(500, { ...mark, "content-type": "application/json" }); res.end('{"error":"down"}'); return;
    }
    res.writeHead(200, { ...mark, "content-type": "application/json" });
    res.end(JSON.stringify({ total: 1, sources: [{ source: "zz", name: "ZZ" }] }));
  });
  return { server, seen, host: await listen(server), setSourcesDown: (v: boolean) => { sourcesDown = v; },
           slowAborted: () => slowAborted };
}

function raw(w: Dev, path: string): Promise<number> {
  return new Promise((ok, bad) => {
    const r = httpRequest({ host: w.address, port: w.port, path, method: "GET" }, (res) => {
      res.resume(); res.on("end", () => ok(res.statusCode ?? 0));
    });
    r.on("error", bad); r.end();
  });
}

async function get(w: Dev, path: string) {
  const r = await w.fetch(path);
  const text = await r.text();
  return { status: r.status, text, headers: r.headers };
}

/** Poll `read` until `done` holds (rows are written in waitUntil, after the response), up to 15 s; returns
 *  the last value read either way, so the caller's assertion reports what was actually there. */
async function eventually<T>(read: () => Promise<T>, done: (v: T) => boolean): Promise<T> {
  const until = Date.now() + 15000;
  let v = await read();
  while (!done(v) && Date.now() < until) {
    await new Promise((r) => setTimeout(r, 200));
    v = await read();
  }
  return v;
}

test("the edge-only config really has no econ D1 (planted positive that can fail)", { timeout: 180_000 }, async () => {
  // With FORWARD and EDGE_STATE unset the beacon and its report use econ D1's `pageview` table, which
  // seeded() creates. So the FULL config answers 200 / counted, and a config WITHOUT the binding must
  // answer 500 / failed. If edgeOnlyConfig() ever kept the binding, the second half would pass as 200
  // and this test would fail (R1175: the old planted positive got 500 from both configs).
  const off = { FORWARD: "", EDGE_STATE: "", ORIGIN_URL: "", ORIGIN_SECRET: "" };
  const outcome = async (config: string) => {
    const w = await start(config, off, await seeded());
    try {
      const pv = await get(w, "/v1/pv?p=%2Fabout");
      return { report: (await get(w, "/v1/pv/report?days=3")).status, pv: pv.headers.get("x-econ-pv") };
    } finally {
      await w.stop();
    }
  };
  assert.deepEqual(await outcome(EDGE_CONFIG), { report: 200, pv: "counted" }, "control: the full config has econ D1");
  assert.deepEqual(await outcome(edgeOnlyConfig()), { report: 500, pv: "failed" }, "the edge-only config does not");
});

test("FORWARD on: the real edge, with no econ D1 or R2 binding, against a stand-in origin",
     { timeout: 300_000 }, async (t) => {
  const evil = await collector();
  const origin = await standInOrigin(evil.host);
  t.after(() => { origin.server.close(); evil.server.close(); });
  const persist = await seeded();
  const w = await start(edgeOnlyConfig(), {
    FORWARD: "on", ORIGIN_URL: `http://${origin.host}`, ORIGIN_SECRET: "edge-test-secret", ORIGIN_TIMEOUT_MS: "1000", SOURCE_NAMES_MAX_AGE_S: "0", SOURCE_NAMES_BACKOFF_S: "1",
  }, persist);
  t.after(() => w.stop());
  const calls = () => origin.seen.length;
  const downloads = () => withBindings(EDGE_CONFIG, persist, (env) =>
    all(env.USERS, "SELECT series_id, bytes FROM econ_download_log ORDER BY series_id"));

  await t.test("a client path can never reach another host (R1169 B1)", async () => {
    assert.equal(await raw(w, `//${evil.host}/v1/sources`), 404);
    assert.equal(await raw(w, `/\\${evil.host}/v1/sources`), 404);
    assert.equal(await raw(w, `//${evil.host}/v1/series/zz%3Alen.csv?api_key=GOODKEY`), 404);
    assert.equal(evil.seen.length, 0, "the collector received nothing");
    assert.equal(calls(), 0, "and nothing was forwarded");
  });

  await t.test("unknown paths are the edge's own 404", async () => {
    assert.equal((await get(w, "/v1/nope")).status, 404);
    assert.equal(calls(), 0);
  });

  await t.test("the licence gate answers 451 before auth and before the origin", async () => {
    const src = [...NON_REDISTRIBUTABLE][0];
    assert.ok(src, "the gate list is not empty");
    const id = encodeURIComponent(`${src}:x`);
    assert.equal((await get(w, `/v1/series/${id}.csv`)).status, 451);
    assert.equal((await get(w, `/v1/series/${id}.metadata.json`)).status, 451);
    assert.equal(calls(), 0);
  });

  await t.test("a download with no key is refused at the edge", async () => {
    assert.equal((await get(w, "/v1/series/zz%3Alen.csv")).status, 401);
    assert.equal(calls(), 0);
  });

  await t.test("keyed downloads: stripped, marked answers only, logged by length or by counting", async () => {
    const a = await get(w, "/v1/series/zz%3Alen.csv?api_key=GOODKEY");
    assert.equal(a.status, 200);
    assert.equal(a.text, CSV);
    assert.equal(a.headers.get("x-econ-origin"), null);
    const got = origin.seen.at(-1)!;
    assert.equal(new URL(got.url, "http://x").searchParams.get("api_key"), null);
    assert.equal(got.headers["x-api-key"], undefined);
    assert.equal(got.headers["x-econ-origin-secret"], "edge-test-secret");

    const b = await get(w, "/v1/series/zz%3Achunk.csv?api_key=GOODKEY");
    assert.equal(b.status, 200);
    assert.equal(b.text, CSV + CSV);
    assert.equal((await get(w, "/v1/series/zz%3Aerr.csv?api_key=GOODKEY")).status, 500);
    assert.deepEqual(await eventually(downloads, (rows) => rows.length >= 2), [
      { series_id: "zz:chunk", bytes: Buffer.byteLength(CSV) * 2 },   // no length, no marker: still logged
      { series_id: "zz:len", bytes: Buffer.byteLength(CSV) },         // from content-length
    ], "one row per successful download, none for the 500");
  });

  await t.test("redirects and unmarked answers are 502, never followed, cached or served", async () => {
    const before = calls();
    for (let i = 0; i < 2; i++) {
      const r = await get(w, "/v1/sources?mode=redirect");
      assert.equal(r.status, 502);
      assert.doesNotMatch(r.text, /stolen/);
      const u = await get(w, "/v1/catalog?mode=unmarked");
      assert.equal(u.status, 502);
      assert.doesNotMatch(u.text, /login/);
    }
    assert.equal(calls() - before, 4, "a refused answer is never served from the cache");
    assert.equal(evil.seen.length, 0, "the redirect target was never contacted");
    assert.equal((await get(w, "/v1/series/zz%3Alen.csv?api_key=GOODKEY&mode=redirect")).status, 502);
    assert.equal((await get(w, "/v1/series/zz%3Alen.csv?api_key=GOODKEY&mode=unmarked")).status, 502);
    // a negative can only be checked after a wait; one known-good download after the refused ones, and
    // waiting for ITS row, proves the log is caught up before counting
    assert.equal((await get(w, "/v1/series/zz%3Alen.csv?api_key=GOODKEY")).status, 200);
    const rows = await eventually(downloads, (r) => r.length >= 3);
    assert.equal(rows.length, 3, "a refused download is not logged (only the good one after it is)");
  });

  await t.test("a slow origin is a 504 after ORIGIN_TIMEOUT_MS", async () => {
    const t0 = Date.now();
    assert.equal((await get(w, "/v1/last-updates?mode=slow")).status, 504);
    assert.ok(Date.now() - t0 < 6000, "answered well before the origin would have (8 s)");
    const aborted = await eventually(async () => origin.slowAborted(), (n) => n >= 1);
    assert.equal(aborted, 1, "the pending request to the origin was aborted, not left open (R1175)");
  });

  await t.test("public answers are cached without api_key in the key; bundle never", async () => {
    const before = calls();
    assert.equal((await get(w, "/v1/sources?api_key=A")).status, 200);
    assert.equal((await get(w, "/v1/sources?api_key=B")).status, 200);
    assert.equal((await get(w, "/v1/sources")).status, 200);
    assert.equal(calls() - before, 1, "one origin call for three requests differing only in api_key");
    const b0 = calls();
    await get(w, "/v1/bundle");
    await get(w, "/v1/bundle");
    assert.equal(calls() - b0, 2, "the bundle is never cached");
  });

  await t.test("public-stats, the beacon and its report work with no econ D1", async () => {
    // SOURCE_NAMES_MAX_AGE_S = 0 (vars above): the origin is asked every time, so the second call REALLY
    // falls back to the kept copy (R1175 item 4). Downloads of zz:* were logged by the subtests above.
    const topNames = (text: string) =>
      (JSON.parse(text).top_sources as { source_id: string; name: string }[]).map((s) => [s.source_id, s.name]);
    const up = await get(w, "/v1/public-stats");
    assert.equal(up.status, 200, up.text.slice(0, 200));
    assert.deepEqual(topNames(up.text), [["zz", "ZZ"]], "names from the origin");
    origin.setSourcesDown(true);
    const before = calls();
    const down = await get(w, "/v1/public-stats");
    assert.equal(down.status, 200, down.text.slice(0, 200));
    assert.ok(calls() > before, "the origin was asked (and failed), so this is the fallback, not a cache hit");
    assert.deepEqual(topNames(down.text), [["zz", "ZZ"]], "names from the kept copy while the origin is down");
    await new Promise((r) => setTimeout(r, 300));                  // the back-off record is put in waitUntil
    const asked = calls();
    const again = await get(w, "/v1/public-stats");
    assert.equal(again.status, 200);
    assert.equal(calls(), asked, "during the 60 s back-off the origin is not asked again (AR-151 finding 8)");
    assert.deepEqual(topNames(again.text), [["zz", "ZZ"]]);
    await new Promise((r) => setTimeout(r, 1500));               // past the 1 s back-off (SOURCE_NAMES_BACKOFF_S)
    const beforeRetry = calls();
    await get(w, "/v1/public-stats");
    assert.ok(calls() > beforeRetry, "once the back-off ends the origin is asked again (a longer back-off would not)");
    origin.setSourcesDown(false);
    const pv = await get(w, "/v1/pv?p=%2Fabout");
    assert.equal(pv.status, 200);
    assert.equal(pv.headers.get("x-econ-pv"), "counted", "counted with no econ D1 binding at all (R1175)");
    const rep = await get(w, "/v1/pv/report?days=3");
    assert.equal(rep.status, 200, rep.text.slice(0, 200));
    assert.deepEqual(JSON.parse(rep.text).by_path.map((x: { path: string; hits: number }) => [x.path, x.hits]),
                     [["/about", 1]]);
  });
});

test("FORWARD on, cold, origin unreachable: public-stats still answers", { timeout: 120_000 }, async (t) => {
  const w = await start(edgeOnlyConfig(), { FORWARD: "on", ORIGIN_URL: "http://127.0.0.1:9", ORIGIN_SECRET: "s" },
                        await seeded());
  t.after(() => w.stop());
  const r = await get(w, "/v1/public-stats");
  assert.equal(r.status, 200, r.text.slice(0, 200));
  assert.deepEqual(JSON.parse(r.text).top_sources, [], "no kept names: no top-sources list, never a 500");
  assert.equal((await get(w, "/v1/sources")).status, 502, "and a data route says the origin is unreachable");
});

test("FORWARD unset: nothing is forwarded even with an origin configured", { timeout: 120_000 }, async (t) => {
  const evil = await collector();
  const origin = await standInOrigin(evil.host);
  t.after(() => { origin.server.close(); evil.server.close(); });
  const w = await start(EDGE_CONFIG, { ORIGIN_URL: `http://${origin.host}`, ORIGIN_SECRET: "s" });
  t.after(() => w.stop());
  for (const p of ["/v1/sources", "/v1/catalog", "/v1/series/zz%3Alen.metadata.json", "/", "/v1/nope"]) await get(w, p);
  assert.equal(origin.seen.length, 0);
});

test("FORWARD on without an origin configured answers 503 and forwards nothing", { timeout: 120_000 }, async (t) => {
  const w = await start(edgeOnlyConfig(), { FORWARD: "on", ORIGIN_URL: "", ORIGIN_SECRET: "" });
  t.after(() => w.stop());
  assert.equal((await get(w, "/v1/sources")).status, 503);
});

// ---- the edge's own writes: users db with FORWARD on, econ D1 / R2 unchanged without ------------------
const MODES = [
  { name: "FORWARD on -> users db only", forward: true,
    vars: { FORWARD: "on", EDGE_STATE: "", ORIGIN_URL: "http://127.0.0.1:9", ORIGIN_SECRET: "s", GIT_COMMIT: "c0ffee" },
    status: { commit: "c0ffee", forward: true, edge_state: "users", forward_raw: "on", edge_state_raw: "", origin_configured: true } },
  { name: "EDGE_STATE users, FORWARD off (plan step 5) -> users db only", forward: true,
    vars: { FORWARD: "", EDGE_STATE: "users", ORIGIN_URL: "", ORIGIN_SECRET: "" },
    status: { commit: null, forward: false, edge_state: "users", forward_raw: "", edge_state_raw: "users", origin_configured: false } },
  { name: "neither -> econ D1 / R2 only (as before)", forward: false,
    vars: { FORWARD: "", EDGE_STATE: "", ORIGIN_URL: "", ORIGIN_SECRET: "" },
    status: { commit: null, forward: false, edge_state: "econ", forward_raw: "", edge_state_raw: "", origin_configured: false } },
];

for (const mode of MODES) {
  const forward = mode.forward;                                   // "the edge's own state is in USERS"
  test(`page views, the cost-guard status and /v1/edge-status: ${mode.name}`, { timeout: 300_000 }, async (t) => {
    const persist = await seeded();
    // the FULL config here, on purpose: econ D1 and R2 exist, so "nowhere else" is measured, not assumed
    const w = await start(EDGE_CONFIG, mode.vars, persist, true);
    t.after(() => w.stop());

    const st = await get(w, "/v1/edge-status");
    assert.equal(st.status, 200);
    assert.equal(st.headers.get("cache-control"), "no-store");
    assert.deepEqual(JSON.parse(st.text), mode.status);

    for (const [p, want] of [["/about", "counted"], ["/about", "counted"], ["/not-a-tracked-path", "ignored"]]) {
      const r = await get(w, `/v1/pv?p=${encodeURIComponent(p)}`);
      assert.equal(r.status, 200);
      assert.equal(r.headers.get("x-econ-pv"), want, p);
    }
    const rep = await get(w, "/v1/pv/report?days=3&junk=1");
    assert.equal(rep.status, 200);
    assert.deepEqual(JSON.parse(rep.text).by_path.map((x: { path: string; hits: number }) => [x.path, x.hits]),
                     [["/about", 2]], "the report reads the same db");
    // cached 5 min, keyed only on days: a new hit and a different junk parameter still read the cached copy
    assert.equal((await get(w, "/v1/pv?p=%2Fabout")).status, 200);
    const again = await get(w, "/v1/pv/report?days=3&junk=2");
    assert.deepEqual(JSON.parse(again.text).by_path.map((x: { path: string; hits: number }) => [x.path, x.hits]),
                     [["/about", 2]], "served from the edge cache (the key ignores other parameters)");
    const odd = await get(w, "/v1/pv/report?days=abc");
    assert.equal(odd.status, 200, "days=abc is the default window, not a 500");
    assert.equal(JSON.parse(odd.text).window_days, 90);
    await get(w, "/__scheduled?cron=*/30+*+*+*+*");               // no analytics token: a BLIND record, then throw

    await withBindings(EDGE_CONFIG, persist, async (env) => {
      const users = await all(env.USERS, "SELECT path, hits FROM econ_pageview ORDER BY path");
      const econ = await all(env.CATALOG, "SELECT path, hits FROM pageview ORDER BY path");
      assert.deepEqual(forward ? users : econ, [{ path: "/about", hits: 3 }], "counted in the right db (3: the report above was the cached one)");
      assert.deepEqual(forward ? econ : users, [], "and nothing in the other");
      const status = await all(env.USERS, "SELECT key, body FROM econ_ops_status");
      const r2 = await env.SERIES_BUCKET.get("_aqueduct/cost_status.json");
      if (forward) {
        assert.equal(status.length, 1, "the status record is in the users db");
        assert.equal(status[0].key, "_aqueduct/cost_status.json");
        assert.equal(JSON.parse(String(status[0].body)).blind, true);
        assert.equal(r2, null, "and NOT in econ R2");
      } else {
        assert.deepEqual(status, [], "the users db is untouched");
        assert.ok(r2, "the record went to R2, as before");
        assert.equal(JSON.parse(await r2.text()).blind, true);
      }
    });
  });
}

test("FORWARD on before the migration: the tables heal themselves, same columns as the migration",
     { timeout: 300_000 }, async (t) => {
  const persist = await seeded(false);
  const w = await start(edgeOnlyConfig(), { FORWARD: "on", ORIGIN_URL: "http://127.0.0.1:9", ORIGIN_SECRET: "s" },
                        persist, true);
  t.after(() => w.stop());
  const rep = await get(w, "/v1/pv/report?days=3");
  assert.equal(rep.status, 200, "the report does not 500 on a missing table");
  assert.deepEqual(JSON.parse(rep.text).by_path, []);
  const created = await withBindings(EDGE_CONFIG, persist, (env) =>
    all(env.USERS, "SELECT name FROM sqlite_master WHERE name = 'econ_pageview'"));
  assert.deepEqual(created, [], "an unauthenticated GET of the report never creates a table (R1174 B7)");
  await get(w, "/v1/pv?p=%2Fabout");
  await get(w, "/__scheduled?cron=*/30+*+*+*+*");
  const healed = await withBindings(EDGE_CONFIG, persist, async (env) => ({
    hits: await all(env.USERS, "SELECT path, hits FROM econ_pageview"),
    status: await all(env.USERS, "SELECT key FROM econ_ops_status"),
    cols: [await all(env.USERS, "PRAGMA table_info(econ_pageview)"), await all(env.USERS, "PRAGMA table_info(econ_ops_status)")],
  }));
  assert.deepEqual(healed.hits, [{ path: "/about", hits: 1 }], "the first hit after the flip is counted");
  assert.deepEqual(healed.status, [{ key: "_aqueduct/cost_status.json" }], "the first tick is recorded");
  const fromMigration = await withBindings(EDGE_CONFIG, newPersist("econ-mig-"), async (env) => {
    await execSql(env.USERS, MIGRATION);
    return [await all(env.USERS, "PRAGMA table_info(econ_pageview)"), await all(env.USERS, "PRAGMA table_info(econ_ops_status)")];
  });
  assert.deepEqual(healed.cols, fromMigration, "the worker's DDL and the migration's are the same tables");
});

test("the one-time page-view merge in the migration header is safe to run twice", { timeout: 120_000 }, async () => {
  await withBindings(EDGE_CONFIG, newPersist("econ-merge-"), async (env) => {
    await execSql(env.USERS, MIGRATION);
    await execSql(env.USERS, `
      CREATE TABLE econ_pageview_import (path TEXT NOT NULL, day TEXT NOT NULL, hits INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (path, day));
      INSERT INTO econ_pageview_import VALUES ('/about', '2026-09-01', 5), ('/', '2026-09-02', 3);
      INSERT INTO econ_pageview VALUES ('/about', '2026-09-01', 2)`);
    // The statements are taken FROM THE MIGRATION HEADER'S step 2, so the recipe Ahmed runs and the test
    // cannot drift apart (R1175 item 10).
    const lines = MIGRATION.split(/\r?\n/);
    const from = lines.findIndex((l) => /^--\s+2\. /.test(l));
    const to = lines.findIndex((l) => /^--\s+3\. /.test(l));
    assert.ok(from > 0 && to > from, "the migration header still has its numbered merge recipe");
    const recipe = lines.slice(from + 1, to).map((l) => l.replace(/^--/, "").replace(/\s--\s.*$/, "")).join(" ");
    const stmts = recipe.split(";").map((s) => s.trim()).filter(Boolean);
    assert.equal(stmts.length, 2, recipe);
    const merge = () => env.USERS.batch(stmts.map((s) => env.USERS.prepare(s)));
    await merge();
    await assert.rejects(merge(), "a second run is refused");
    assert.deepEqual(await all(env.USERS, "SELECT path, day, hits FROM econ_pageview ORDER BY path"),
                     [{ path: "/", day: "2026-09-02", hits: 3 }, { path: "/about", day: "2026-09-01", hits: 7 }]);
  });
});
