// The REAL src/index.ts, run in workerd through wrangler's unstable_dev, in both configurations (AR-150).
//
// The unit tests of src/localMode.ts cannot see how index.ts wires it: four wiring mutants (the gate's
// answer ignored, finalizeLocal not applied, the Cache API used for /v1/catalog in local mode, and the
// .csv local branch inverted - which makes PRODUCTION serve downloads with no auth and no log) passed tsc
// and every other test. These cases run the worker itself:
//   production (wrangler.toml):         a .csv with no key is 401
//   origin (wrangler.origin.toml):      no secret configured -> 503; wrong secret -> 403; /v1/pv -> 404;
//                                       with a seeded catalogue and a stand-in blob sidecar, real 200s:
//                                       a JSON answer is NOT marked for counting, a string-path .csv
//                                       carries content-length and no mark, a streamed .csv carries the
//                                       mark and no length (R1171: every answer here used to be a 500,
//                                       so three mutants of the download flag survived)
// Paths come from test/_harness.ts, so this runs from the repo root the way CI runs it (R1171).
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import { test } from "node:test";

import { at, EDGE_CONFIG, execSql, newPersist, ORIGIN_CONFIG, start, withBindings } from "./_harness.ts";

const ID = "ecb:EXR:D.AUD.EUR.SP00.A";
const BIG = "ecb:EXR:D.USD.EUR.SP00.A";
const CSV = `/v1/series/${encodeURIComponent(ID)}.csv`;
const HEADER = "series_id,obs_date,value";

test("production: a .csv download with no key is refused 401 (auth runs)", { timeout: 120_000 }, async (t) => {
  const w = await start(EDGE_CONFIG, {});
  t.after(() => w.stop());
  const r = await w.fetch(CSV);
  assert.equal(r.status, 401, await r.text());
});

test("origin: no ORIGIN_SECRET configured refuses every request 503", { timeout: 120_000 }, async (t) => {
  // explicitly EMPTY: a workstation's .dev.vars would otherwise supply the real secret (CI has none)
  const w = await start(ORIGIN_CONFIG, { ORIGIN_SECRET: "" });
  t.after(() => w.stop());
  for (const p of [CSV, "/v1/sources", "/v1/catalog"]) {
    const r = await w.fetch(p, { headers: { "x-econ-origin-secret": "anything" } });
    assert.equal(r.status, 503, p);
    await r.arrayBuffer();
  }
});

// ---- a stand-in blob sidecar (the protocol of tools/selfhost/blob_sidecar.py) ------------------------
function csvBody(id: string, rows: number): Buffer {
  const lines = [HEADER];
  for (let i = 0; i < rows; i++) lines.push(`${id},${new Date(Date.UTC(1990, 0, 1 + i)).toISOString().slice(0, 10)},${i}.5`);
  return Buffer.from(lines.join("\n") + "\n");
}
const key = (id: string) => "series/" + encodeURIComponent(id) + ".csv";
const BLOBS: Record<string, Buffer> = { [key(ID)]: csvBody(ID, 3), [key(BIG)]: csvBody(BIG, 9000) };

async function sidecar(): Promise<{ server: Server; base: string }> {
  const server = createServer((req, res) => {
    const k = decodeURIComponent((req.url ?? "").replace(/^\/o\//, ""));
    const body = BLOBS[k];
    if (!body) { res.writeHead(404, { "content-length": "0" }); res.end(); return; }
    const h = { "content-type": "text/csv", "x-blob-size": String(body.length), "x-blob-etag": `e-${body.length}`,
                "x-blob-custom-metadata": Buffer.from("{}").toString("base64") };
    const m = /^bytes=(\d+)-(\d+)$/.exec(String(req.headers["range"] ?? ""));
    const part = m ? body.subarray(Number(m[1]), Number(m[2]) + 1) : body;
    res.writeHead(m ? 206 : 200, { ...h, "content-length": String(part.length) });
    res.end(part);
  });
  await new Promise<void>((ok) => server.listen(0, "127.0.0.1", () => ok()));
  const a = server.address();
  return { server, base: `http://127.0.0.1:${typeof a === "object" && a ? a.port : 0}` };
}

const CATALOG_SEED = `
  CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
    geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, last_updated TEXT, metadata TEXT);
  INSERT INTO series (series_id, source_id, title, license_id) VALUES ('${ID}', 'ecb', 'AUD per EUR', 'ecb-terms');
  INSERT INTO series (series_id, source_id, title, license_id) VALUES ('${BIG}', 'ecb', 'USD per EUR', 'ecb-terms');
  CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT, attribution TEXT, terms_url TEXT);
  INSERT INTO source VALUES ('ecb', 'European Central Bank', 'https://www.ecb.europa.eu', 'ecb-terms', 'Source: ECB', NULL);
  CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, reservable INTEGER, commercial_ok INTEGER,
    attribution_required INTEGER, no_modify INTEGER, url TEXT);
  INSERT INTO license VALUES ('ecb-terms', 'ECB terms', 1, 1, 1, 0, NULL)`;

test("origin: the gate, the skipped auth, the edge-only routes, no-store, and the download mark on real 200s",
     { timeout: 300_000 }, async (t) => {
  const blobs = await sidecar();
  t.after(() => blobs.server.close());
  const persist = newPersist("econ-wiring-");
  await withBindings(ORIGIN_CONFIG, persist, (env) => execSql(env.CATALOG, CATALOG_SEED));
  const w = await start(ORIGIN_CONFIG, { ORIGIN_SECRET: "wiring-test-secret", BLOB_SIDECAR_URL: blobs.base, INSTANCE_ID: "gen-test-1" }, persist);
  t.after(() => w.stop());
  const secret = { headers: { "x-econ-origin-secret": "wiring-test-secret" } };

  for (const h of [undefined, { headers: { "x-econ-origin-secret": "not-it" } }]) {
    const r = await w.fetch(CSV, h);
    assert.equal(r.status, 403);
    assert.equal(r.headers.get("x-econ-origin"), "1", "the gate's answer is marked, so the edge passes an honest 403");
    await r.arrayBuffer();
  }
  for (const p of ["/v1/pv?p=/", "/v1/edge-status"]) {
    const r = await w.fetch(p, secret);
    assert.equal(r.status, 404, `edge-only route ${p}`);
    await r.arrayBuffer();
  }

  const index = await w.fetch("/", secret);
  assert.equal(index.status, 200);
  assert.equal(index.headers.get("x-econ-count"), null, "a JSON 200 is never marked for counting (AR-150)");
  assert.equal(index.headers.get("x-econ-origin"), "1");
  assert.equal(index.headers.get("x-econ-instance"), "gen-test-1", "the instance names itself (R1180: the swap refuses any other answer)");
  assert.equal(index.headers.get("cache-control"), "private, no-store");
  await index.arrayBuffer();

  const small = await w.fetch(CSV, secret);
  const smallText = await small.text();
  assert.equal(small.status, 200, "the origin serves without auth - the edge did it: " + smallText.slice(0, 200));
  assert.match(smallText, new RegExp(`${HEADER}\\n${ID.replace(/\./g, "\\.")},1990-01-01,0\\.5`));
  assert.ok(small.headers.has("content-length"), "the string path declares its length");
  assert.equal(small.headers.get("x-econ-count"), null, "and so is not marked");
  assert.match(small.headers.get("cache-control") ?? "", /^private, no-store/);

  const big = await w.fetch(`/v1/series/${encodeURIComponent(BIG)}.csv`, secret);
  const bigText = await big.text();
  assert.equal(big.status, 200, bigText.slice(0, 200));
  assert.ok(BLOBS[key(BIG)].length >= 256 * 1024, "the object is large enough to be streamed");
  assert.equal(big.headers.get("content-length"), null, "a streamed answer has no length");
  assert.equal(big.headers.get("x-econ-count"), "1", "so the origin marks it for the edge to count");
  assert.match(bigText, /# econdl-complete/);
});

test("the /v1/catalog branch skips the Cache API in local mode before it is touched", () => {
  const src = readFileSync(at("src", "index.ts"), "utf8");
  const local = src.indexOf('if (path === "/v1/catalog" && local) return await handleCatalog(url, env);');
  const cache = src.indexOf("const cache = caches.default;");
  assert.ok(local > 0 && cache > 0 && local < cache, "the local short-circuit must come before caches.default");
  assert.match(src, /const statsCache = local \? null : caches\.default;/);
});
