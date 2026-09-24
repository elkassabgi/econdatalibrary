// The forwarding edge (src/edge.ts, plan code change 1): unit rules, then the REAL index.ts in workerd
// (unstable_dev) forwarding to a stand-in origin that records what it receives.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer, type IncomingHttpHeaders } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { unstable_dev } from "wrangler";

import * as edge from "../src/edge.ts";
import * as localMode from "../src/localMode.ts";

test("the header names match the origin's", () => {
  assert.equal(edge.ORIGIN_SECRET_HEADER, localMode.ORIGIN_SECRET_HEADER);
  assert.equal(edge.COUNT_HEADER, localMode.COUNT_HEADER);
});

test("forwarding is on only for FORWARD = 'on'", () => {
  assert.equal(edge.isForward({ FORWARD: "on" }), true);
  for (const v of [undefined, "", "off", "1", "true", "ON"]) assert.equal(edge.isForward({ FORWARD: v }), false, String(v));
});

test("the origin request strips the client's credentials and sets the secret", () => {
  const client = new Request("https://edge.example/v1/series/x.csv?api_key=CLIENTKEY&from=2020-01-01", {
    headers: { "x-api-key": "CLIENTKEY", authorization: "Bearer T", cookie: "c=1",
               "x-econ-origin-secret": "forged", "cf-access-client-id": "forged", "user-agent": "ua" },
  });
  assert.equal(edge.originRequest(client, {}), null, "no origin configured -> nothing forwarded");
  assert.equal(edge.originRequest(client, { ORIGIN_URL: "https://o.example" }), null, "no secret -> nothing forwarded");
  const r = edge.originRequest(client, { ORIGIN_URL: "https://o.example", ORIGIN_SECRET: "real",
                                          ORIGIN_ACCESS_ID: "id", ORIGIN_ACCESS_SECRET: "sec" })!;
  const u = new URL(r.url);
  assert.equal(u.origin + u.pathname, "https://o.example/v1/series/x.csv");
  assert.equal(u.searchParams.get("api_key"), null);
  assert.equal(u.searchParams.get("from"), "2020-01-01");
  for (const h of ["x-api-key", "authorization", "cookie"]) assert.equal(r.headers.get(h), null, h);
  assert.equal(r.headers.get("x-econ-origin-secret"), "real", "overwritten, never passed through");
  assert.equal(r.headers.get("cf-access-client-id"), "id");
  assert.equal(r.headers.get("user-agent"), "ua");
});

test("public answers are cacheable; data never is", () => {
  for (const p of ["/v1/catalog", "/v1/sources", "/v1/stats", "/v1/last-updates", "/v1/bundle",
                   "/v1/series/a%3Ab.metadata.json"]) assert.equal(edge.isCacheable(p), true, p);
  for (const p of ["/v1/series/a%3Ab.csv", "/v1/pv", "/v1/public-stats"]) assert.equal(edge.isCacheable(p), false, p);
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

// ---- the real worker, forwarding to a recording stand-in origin ---------------------------------------
type Seen = { url: string; headers: IncomingHttpHeaders };

async function standInOrigin() {
  const seen: Seen[] = [];
  const server = createServer((req, res) => {
    seen.push({ url: req.url ?? "", headers: req.headers });
    if ((req.url ?? "").startsWith("/v1/series/")) {
      res.writeHead(200, { "content-type": "text/csv", "cache-control": "private, no-store", "x-econ-count": "1" });
      res.end("series_id,obs_date,value\nx,2020-01-01,1\n");
      return;
    }
    res.writeHead(200, { "content-type": "application/json", "cache-control": "private, no-store" });
    res.end(JSON.stringify({ total: 1, sources: [{ source: "zz", name: "ZZ" }] }));
  });
  await new Promise<void>((ok) => server.listen(0, "127.0.0.1", () => ok()));
  const a = server.address();
  return { server, seen, url: `http://127.0.0.1:${typeof a === "object" && a ? a.port : 0}` };
}

// wrangler is run as node + its own entry script, with the SQL in a file: npx.cmd under a shell mangles
// the quotes and parentheses of an inline --command on Windows (it crashed, 0xC0000409).
const WRANGLER = join("node_modules", "wrangler", "bin", "wrangler.js");
let sqlFiles = 0;

function d1Local(persist: string, sql: string, json = false): string {
  const file = join(persist, `q${sqlFiles++}.sql`);
  writeFileSync(file, sql);
  return execFileSync(process.execPath,
    [WRANGLER, "d1", "execute", "hfdatalibrary-db", "--local", "--persist-to", persist, "--file", file,
     ...(json ? ["--json"] : [])], { encoding: "utf8" });
}

function seedUsers(persist: string) {
  // A real key in the LOCAL simulation of hfdatalibrary-db, plus the two tables auth writes.
  d1Local(persist, [
    "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT, is_vip INTEGER, api_key TEXT, is_active INTEGER, api_key_expires_at TEXT);",
    "INSERT INTO users VALUES (7, 'u@example.org', 0, 'GOODKEY', 1, NULL);",
    "CREATE TABLE rate_limits (key TEXT PRIMARY KEY, count INTEGER, window_start TEXT);",
    "CREATE TABLE econ_download_log (user_id INTEGER, series_id TEXT, ip TEXT, channel TEXT, bytes INTEGER, ts TEXT DEFAULT CURRENT_TIMESTAMP);",
  ].join(" "));
}

function countDownloads(persist: string): string {
  return d1Local(persist, "SELECT COUNT(*) AS n, COALESCE(SUM(bytes),0) AS b FROM econ_download_log;", true);
}

test("FORWARD on: the edge gates, strips, forwards, caches public answers and logs downloads",
     { timeout: 300_000 }, async (t) => {
  const origin = await standInOrigin();
  t.after(() => origin.server.close());
  const persist = mkdtempSync(join(tmpdir(), "econ-edge-"));
  seedUsers(persist);
  const w = await unstable_dev("src/index.ts", {
    config: "wrangler.toml", local: true, persistTo: persist, logLevel: "none",
    vars: { FORWARD: "on", ORIGIN_URL: origin.url, ORIGIN_SECRET: "edge-test-secret" },
    experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
  });
  t.after(() => w.stop());

  // a download with no key: refused at the edge, the origin never asked
  const noKey = await w.fetch("/v1/series/zz%3Aa.csv");
  assert.equal(noKey.status, 401);
  await noKey.arrayBuffer();
  assert.equal(origin.seen.length, 0);

  // a keyed download: forwarded without the key, with the secret; counted and logged
  const ok = await w.fetch("/v1/series/zz%3Aa.csv?api_key=GOODKEY");
  assert.equal(ok.status, 200);
  const body = await ok.text();
  assert.match(body, /x,2020-01-01,1/);
  assert.equal(ok.headers.get("x-econ-count"), null, "the internal marker never reaches the client");
  const got = origin.seen.at(-1)!;
  assert.equal(new URL(got.url, "http://x").searchParams.get("api_key"), null);
  assert.equal(got.headers["x-api-key"], undefined);
  assert.equal(got.headers["x-econ-origin-secret"], "edge-test-secret");
  await new Promise((r) => setTimeout(r, 500));                   // the log is written in waitUntil
  const logged = JSON.parse(countDownloads(persist))[0].results[0];
  assert.equal(logged.n, 1);
  assert.equal(logged.b, Buffer.byteLength(body));

  // a public answer: forwarded once, then served from the edge cache
  const before = origin.seen.length;
  for (let i = 0; i < 2; i++) {
    const r = await w.fetch("/v1/sources");
    assert.equal(r.status, 200);
    await r.arrayBuffer();
  }
  assert.equal(origin.seen.length - before, 1, "the second /v1/sources came from the edge cache");
});

test("FORWARD on without an origin configured answers 503 and forwards nothing", { timeout: 120_000 }, async (t) => {
  const w = await unstable_dev("src/index.ts", {
    config: "wrangler.toml", local: true, persistTo: mkdtempSync(join(tmpdir(), "econ-edge-")), logLevel: "none",
    vars: { FORWARD: "on", ORIGIN_URL: "", ORIGIN_SECRET: "" },
    experimental: { disableExperimentalWarning: true, disableDevRegistry: true },
  });
  t.after(() => w.stop());
  const r = await w.fetch("/v1/sources");
  assert.equal(r.status, 503);
  await r.arrayBuffer();
});
