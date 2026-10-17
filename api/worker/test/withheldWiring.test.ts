// The withheld list through the REAL src/index.ts in workerd (unstable_dev), in both configurations:
//   production (wrangler.toml, FORWARD off): the list from R2; a withheld id answers 404 "withheld" on metadata
//     and on .csv BEFORE auth; it is in no catalogue browse, search, total, stats count or bundle; a control id of
//     the same source is untouched;
//   origin (wrangler.origin.toml): GET /v1/withheld serves the local store's object behind the secret.
// (Forward mode is in test/edge.test.ts, against a stand-in origin.)
import assert from "node:assert/strict";
import { createServer, type Server } from "node:http";
import { test } from "node:test";

import { EDGE_CONFIG, ORIGIN_CONFIG, execSql, newPersist, start, withBindings } from "./_harness.ts";

const HELD = "insee_melodi:DS_RP_TD_MIGRES_COMP";
const KEPT = "insee_melodi:DD_CNA_APU";
const OTHER = "ecb:EXR:D.AUD.EUR.SP00.A";
const LIST = JSON.stringify({ version: 1, ids: { [HELD]: { since: "2026-10-17", why: "t", until: "complete" } } });
const enc = encodeURIComponent;

const SEED = `
  CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
    geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, last_updated TEXT, metadata TEXT);
  INSERT INTO series (series_id, source_id, title, license_id) VALUES ('${HELD}', 'insee_melodi', 'Residential migration MIG2', 'etalab-2.0');
  INSERT INTO series (series_id, source_id, title, license_id) VALUES ('${KEPT}', 'insee_melodi', 'Public administrations APU', 'etalab-2.0');
  INSERT INTO series (series_id, source_id, title, license_id) VALUES ('${OTHER}', 'ecb', 'AUD per EUR', 'ecb-terms');
  CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT, attribution TEXT, terms_url TEXT);
  INSERT INTO source VALUES ('insee_melodi', 'INSEE Melodi', 'https://www.insee.fr', 'etalab-2.0', 'Source: INSEE', NULL);
  INSERT INTO source VALUES ('ecb', 'European Central Bank', 'https://www.ecb.europa.eu', 'ecb-terms', 'Source: ECB', NULL);
  CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, reservable INTEGER, commercial_ok INTEGER,
    attribution_required INTEGER, no_modify INTEGER, url TEXT);
  INSERT INTO license VALUES ('etalab-2.0', 'Etalab 2.0', 1, 1, 1, 0, NULL);
  INSERT INTO license VALUES ('ecb-terms', 'ECB terms', 1, 1, 1, 0, NULL);
  CREATE TABLE source_counts (source_id TEXT PRIMARY KEY, n INTEGER);
  INSERT INTO source_counts VALUES ('insee_melodi', 2);
  INSERT INTO source_counts VALUES ('ecb', 1);
  CREATE TABLE source_state (source_id TEXT PRIMARY KEY, strategy TEXT, cadence TEXT, status TEXT,
    last_success_utc TEXT, last_attempt_utc TEXT, owner TEXT, enabled INTEGER DEFAULT 1, note TEXT);
  CREATE TABLE unit_state (source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT,
    last_success_utc TEXT, last_attempt_utc TEXT, status TEXT, last_obs_date TEXT, obs_count INTEGER DEFAULT 0,
    attempt_count INTEGER DEFAULT 0, last_error TEXT, PRIMARY KEY(source_id, unit_id));
  CREATE TABLE source_data_through (source_id TEXT PRIMARY KEY, data_through TEXT)`;
const SHARD = `
  CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
    geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, last_updated TEXT, metadata TEXT);
  CREATE TABLE source_counts (source_id TEXT PRIMARY KEY, n INTEGER)`;

async function getJson(w: Awaited<ReturnType<typeof start>>, path: string) {
  const r = await w.fetch(path);
  const text = await r.text();
  let body: Record<string, unknown> = {};
  try { body = JSON.parse(text); } catch { /* a CSV */ }
  return { status: r.status, body, text };
}

test("production: a withheld table is unreachable on every route; a control id is untouched",
     { timeout: 300_000 }, async (t) => {
  const persist = newPersist("econ-withheld-");
  await withBindings(EDGE_CONFIG, persist, async (env) => {
    await execSql(env.CATALOG, SEED);
    await execSql(env.CATALOG_CLIMATE, SHARD);
    await env.SERIES_BUCKET.put("_aqueduct/withheld.json", LIST);
    await env.SERIES_BUCKET.put("_aqueduct/stats.json", JSON.stringify({ individual_series: 3 }));
  });
  const w = await start(EDGE_CONFIG, {}, persist);
  t.after(() => w.stop());

  const m = await getJson(w, `/v1/series/${enc(HELD)}.metadata.json`);
  assert.equal(m.status, 404, m.text);
  assert.equal(m.body.error, "withheld");
  assert.match(String(m.body.detail), /re-downloaded from INSEE/);
  const c = await getJson(w, `/v1/series/${enc(HELD)}.csv`);
  assert.equal(c.status, 404, "withheld BEFORE auth: no key, still the honest 404 (not 401)");
  assert.equal(c.body.error, "withheld");
  const km = await getJson(w, `/v1/series/${enc(KEPT)}.metadata.json`);
  assert.equal(km.status, 200, "control of the same source: " + km.text.slice(0, 300));
  assert.equal((await getJson(w, `/v1/series/${enc(KEPT)}.csv`)).status, 401, "control: auth runs as before");
  assert.equal((await getJson(w, `/v1/series/${enc("insee_melodi:NOPE")}.metadata.json`)).body.error, "not_found",
               "an unknown id is still not_found, a different error");

  const browse = await getJson(w, "/v1/catalog?source=insee_melodi");
  assert.equal(browse.status, 200, browse.text);
  assert.deepEqual((browse.body.results as { series_id: string }[]).map((r) => r.series_id), [KEPT]);
  assert.equal(browse.body.total, 1, "source_counts (2) minus the withheld row the catalogue holds (1)");
  const all = await getJson(w, "/v1/catalog");
  assert.deepEqual((all.body.results as { series_id: string }[]).map((r) => r.series_id).sort(), [KEPT, OTHER].sort());
  assert.equal(all.body.total, 2, "the whole-table count loses the withheld row");
  const q = await getJson(w, "/v1/catalog?q=MIG2");           // no series_fts here: the LIKE fallback runs
  assert.equal(q.status, 200, q.text);
  assert.deepEqual(q.body.results, [], "a search never finds a withheld table");
  assert.equal(q.body.total, 0);
  const qc = await getJson(w, "/v1/catalog?q=APU");
  assert.deepEqual((qc.body.results as { series_id: string }[]).map((r) => r.series_id), [KEPT], "search control");

  const st = await getJson(w, "/v1/stats");
  assert.equal(st.status, 200, st.text);
  assert.equal(st.body.catalog_entries, 2, "3 catalogued - 1 withheld");

  const b = await getJson(w, "/v1/bundle?source=insee_melodi");
  assert.equal(b.status, 200, b.text);
  assert.deepEqual((b.body["econdl:series_requested"] as string[]), [KEPT], "the enumeration never reads it");
  const bn = await getJson(w, `/v1/bundle?ids=${enc(HELD)},${enc(KEPT)}`);
  const un = bn.body["econdl:unresolved"] as { id: string; reason: string }[];
  assert.equal(un.length, 1);
  assert.equal(un[0].id, HELD);
  assert.match(un[0].reason, /^withheld:/);

  assert.equal((await getJson(w, "/v1/withheld")).status, 404, "the list route is the origin's only");
});

test("production: no list in R2 -> the compiled-in FLOOR applies (fail closed)", { timeout: 300_000 }, async (t) => {
  const persist = newPersist("econ-withheld-floor-");
  await withBindings(EDGE_CONFIG, persist, async (env) => {
    await execSql(env.CATALOG, SEED);
    await execSql(env.CATALOG_CLIMATE, SHARD);
  });
  const w = await start(EDGE_CONFIG, {}, persist);
  t.after(() => w.stop());
  // HELD is in the committed list (api/worker/withheld.json), so the floor withholds it
  assert.equal((await getJson(w, `/v1/series/${enc(HELD)}.metadata.json`)).body.error, "withheld");
  assert.equal((await getJson(w, `/v1/series/${enc(KEPT)}.metadata.json`)).status, 200);
});

test("origin: GET /v1/withheld serves the local store's object behind the secret", { timeout: 300_000 }, async (t) => {
  const server: Server = createServer((req, res) => {
    const k = decodeURIComponent((req.url ?? "").replace(/^\/o\//, ""));
    if (k !== "_aqueduct/withheld.json") { res.writeHead(404, { "content-length": "0" }); res.end(); return; }
    const body = Buffer.from(LIST);
    res.writeHead(200, { "content-type": "application/json", "x-blob-size": String(body.length), "x-blob-etag": "e-list",
                         "x-blob-custom-metadata": Buffer.from("{}").toString("base64"),
                         "content-length": String(body.length) });
    res.end(body);
  });
  await new Promise<void>((ok) => server.listen(0, "127.0.0.1", () => ok()));
  t.after(() => server.close());
  const a = server.address();
  const base = `http://127.0.0.1:${typeof a === "object" && a ? a.port : 0}`;
  const w = await start(ORIGIN_CONFIG, { ORIGIN_SECRET: "withheld-secret", BLOB_SIDECAR_URL: base, INSTANCE_ID: "g" });
  t.after(() => w.stop());
  const no = await w.fetch("/v1/withheld");
  assert.equal(no.status, 403, "no secret, no list");
  await no.arrayBuffer();
  const r = await w.fetch("/v1/withheld", { headers: { "x-econ-origin-secret": "withheld-secret" } });
  const text = await r.text();
  assert.equal(r.status, 200, text);
  assert.equal(text, LIST, "the object's bytes, unchanged");
  assert.equal(r.headers.get("x-withheld-etag"), "e-list");
});
