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
  const hm = await getJson(w, `/v1/series/${enc(HELD)}.metadata.json`);
  assert.equal(hm.body.error, "withheld");
  assert.match(String(hm.body.detail), /our copy is incomplete/, "a floor id: the owner's sentence");
  // AR-341 finding 2: the floor alone fails OPEN for an id withheld after it was committed, so while it applies
  // the floor's sources are withheld WHOLE. KEPT is not in the floor; it is withheld too, and the other source is not.
  const km = await getJson(w, `/v1/series/${enc(KEPT)}.metadata.json`);
  assert.equal(km.status, 404, km.text);
  assert.equal(km.body.error, "withheld");
  // AR-341 re-review N2: KEPT is complete - it is closed only because the list could not be read
  const UNREADABLE = "Temporarily unavailable: the withheld-tables list could not be read; try again in a minute.";
  assert.equal(km.body.detail, UNREADABLE);
  const kc = await getJson(w, `/v1/series/${enc(KEPT)}.csv`);
  assert.equal(kc.body.error, "withheld", "before auth");
  assert.equal(kc.body.detail, UNREADABLE);
  assert.equal((await getJson(w, `/v1/series/${enc(OTHER)}.metadata.json`)).status, 200, "another source is open");
  const browse = await getJson(w, "/v1/catalog?source=insee_melodi");
  assert.equal(browse.status, 200, browse.text);
  assert.deepEqual(browse.body.results, []);
  assert.equal(browse.body.total, 0);
  const all = await getJson(w, "/v1/catalog");
  assert.deepEqual((all.body.results as { series_id: string }[]).map((r) => r.series_id), [OTHER]);
  const b = await getJson(w, "/v1/bundle?source=insee_melodi");
  assert.equal(b.status, 404, b.text);
  assert.equal(b.body.error, "withheld");
  assert.equal(b.body.detail, UNREADABLE);
  const bn = await getJson(w, `/v1/bundle?ids=${enc(KEPT)},${enc(HELD)}`);
  const ur = bn.body["econdl:unresolved"] as { id: string; reason: string }[];
  assert.match(ur.find((u) => u.id === KEPT)!.reason, /^withheld: the withheld-tables list could not be read/);
  assert.match(ur.find((u) => u.id === HELD)!.reason, /^withheld: our copy is incomplete/);
});

const FIRST = "insee_melodi:AAA_HELD_FIRST";               // sorts before every other id of the seed but ecb's
const LIST2 = JSON.stringify({ version: 1, ids: { [HELD]: { since: "2026-10-17", why: "t", until: "complete" },
                                                  [FIRST]: { since: "2026-10-17", why: "t", until: "complete" } } });
const FTS = `
  CREATE VIRTUAL TABLE series_fts USING fts5(series_id UNINDEXED, title)`;

test("limit=1: the SQL excludes withheld rows, so the first page is the control row (AR-341 finding 6)",
     { timeout: 300_000 }, async (t) => {
  // Without the in-SQL exclusion the handler's filter still drops the row, but the page comes back EMPTY next to
  // a non-zero total. A withheld id sorts first in each window here, so an argument of [] shows.
  const persist = newPersist("econ-withheld-limit1-");
  await withBindings(EDGE_CONFIG, persist, async (env) => {
    await execSql(env.CATALOG, SEED + `;
      INSERT INTO series (series_id, source_id, title, license_id) VALUES ('${FIRST}', 'insee_melodi', 'Zebrafish held first', 'etalab-2.0');
      UPDATE series SET title = 'Public administrations APU zebrafish' WHERE series_id = '${KEPT}';
      UPDATE source_counts SET n = 3 WHERE source_id = 'insee_melodi';` + FTS + `;
      INSERT INTO series_fts (series_id, title) VALUES ('${FIRST}', 'Zebrafish held first');
      INSERT INTO series_fts (series_id, title) VALUES ('${KEPT}', 'Public administrations APU zebrafish')`);
    await execSql(env.CATALOG_CLIMATE, SHARD + ";" + FTS);
    await env.SERIES_BUCKET.put("_aqueduct/withheld.json", LIST2);
  });
  const w = await start(EDGE_CONFIG, {}, persist);
  t.after(() => w.stop());
  const ids = (r: { body: Record<string, unknown> }) => (r.body.results as { series_id: string }[]).map((x) => x.series_id);

  const src = await getJson(w, "/v1/catalog?source=insee_melodi&limit=1");
  assert.equal(src.status, 200, src.text);
  assert.deepEqual(ids(src), [KEPT], "browseSourceSql must carry the source's withheld ids");
  assert.equal(src.body.total, 1);
  const all = await getJson(w, "/v1/catalog?limit=1&offset=1");     // ecb first, then FIRST, then KEPT
  assert.equal(all.status, 200, all.text);
  assert.deepEqual(ids(all), [KEPT], "browseAllSql must carry every withheld id");
  const q = await getJson(w, "/v1/catalog?q=zebrafish&limit=1");
  assert.equal(q.status, 200, q.text);
  assert.deepEqual(ids(q), [KEPT], "the global searchFtsSql must carry every withheld id: ");
  assert.equal(q.body.total, 1);
  const qs = await getJson(w, "/v1/catalog?q=zebrafish&source=insee_melodi&limit=1");
  assert.deepEqual(ids(qs), [KEPT], "the scoped FTS search too");
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

// dst (owner decision 2026-10-19): the zero-collapse tables. A dst catalogue id has a SECOND colon.
const DST_HELD = "dst:DST:DNINTVAL";
const DST_KEPT = "dst:DST:FOLK1A";
const DST_SEED = SEED + `;
  INSERT INTO series (series_id, source_id, title, license_id) VALUES ('${DST_HELD}', 'dst', 'International reserve assets (IMF compilation)', 'cc-by-4.0');
  INSERT INTO series (series_id, source_id, title, license_id) VALUES ('${DST_KEPT}', 'dst', 'Population at the first day of the quarter', 'cc-by-4.0');
  INSERT INTO source VALUES ('dst', 'Statistics Denmark', 'https://www.dst.dk', 'cc-by-4.0', 'Source: Statistics Denmark', NULL);
  INSERT INTO license VALUES ('cc-by-4.0', 'CC BY 4.0', 1, 1, 1, 0, NULL);
  INSERT INTO source_counts VALUES ('dst', 2)`;
const DENMARK = "Temporarily withheld: our copy of this table shows only zeros because it was downloaded at the wrong " +
  "level of detail; it is listed again when it is downloaded again correctly from Statistics Denmark.";

test("production: a withheld dst table answers 404 with the Denmark sentence; insee keeps its own; controls open",
     { timeout: 300_000 }, async (t) => {
  const persist = newPersist("econ-withheld-dst-");
  const list = JSON.stringify({ version: 1, ids: {
    [HELD]: { since: "2026-10-17", why: "t", until: "complete" },
    [DST_HELD]: { since: "2026-10-19", why: "zero collapse", until: "operator" } } });
  await withBindings(EDGE_CONFIG, persist, async (env) => {
    await execSql(env.CATALOG, DST_SEED);
    await execSql(env.CATALOG_CLIMATE, SHARD);
    await env.SERIES_BUCKET.put("_aqueduct/withheld.json", list);
    await env.SERIES_BUCKET.put("_aqueduct/stats.json", JSON.stringify({ individual_series: 5 }));
  });
  const w = await start(EDGE_CONFIG, {}, persist);
  t.after(() => w.stop());
  const m = await getJson(w, `/v1/series/${enc(DST_HELD)}.metadata.json`);
  assert.equal(m.status, 404, m.text);
  assert.equal(m.body.error, "withheld");
  assert.equal(m.body.detail, DENMARK);
  const c = await getJson(w, `/v1/series/${enc(DST_HELD)}.csv`);
  assert.equal(c.status, 404, "before auth");
  assert.equal(c.body.detail, DENMARK);
  assert.equal((await getJson(w, `/v1/series/${enc(DST_KEPT)}.metadata.json`)).status, 200, "a dst control is open");
  assert.equal((await getJson(w, `/v1/series/${enc(DST_KEPT)}.csv`)).status, 401, "control: auth runs as before");
  const im = await getJson(w, `/v1/series/${enc(HELD)}.metadata.json`);
  assert.equal(im.body.detail, "Temporarily withheld: our copy is incomplete and is being re-downloaded from INSEE; " +
    "it is listed again when the download is complete.", "the insee sentence is unchanged");
  const browse = await getJson(w, "/v1/catalog?source=dst");
  assert.deepEqual((browse.body.results as { series_id: string }[]).map((r) => r.series_id), [DST_KEPT]);
  assert.equal(browse.body.total, 1);
  const bn = await getJson(w, `/v1/bundle?ids=${enc(DST_HELD)},${enc(HELD)}`);
  const un = bn.body["econdl:unresolved"] as { id: string; reason: string }[];
  assert.match(un.find((u) => u.id === DST_HELD)!.reason, /^withheld: our copy of this table shows only zeros .* from Statistics Denmark \(HTTP 404 'withheld' on direct fetch\)$/);
  assert.equal(un.find((u) => u.id === HELD)!.reason, "withheld: our copy is incomplete and is being re-downloaded; " +
    "it is listed again when the download is complete (HTTP 404 'withheld' on direct fetch)", "insee: unchanged");
});

test("production: no list -> the floor closes dst WHOLE; a complete dst table gets the unreadable-list sentence",
     { timeout: 300_000 }, async (t) => {
  const persist = newPersist("econ-withheld-dst-floor-");
  await withBindings(EDGE_CONFIG, persist, async (env) => {
    await execSql(env.CATALOG, DST_SEED);
    await execSql(env.CATALOG_CLIMATE, SHARD);
  });
  const w = await start(EDGE_CONFIG, {}, persist);
  t.after(() => w.stop());
  const UNREADABLE = "Temporarily unavailable: the withheld-tables list could not be read; try again in a minute.";
  const km = await getJson(w, `/v1/series/${enc(DST_KEPT)}.metadata.json`);
  assert.equal(km.status, 404, km.text);
  assert.equal(km.body.error, "withheld");
  assert.equal(km.body.detail, UNREADABLE);
  assert.equal((await getJson(w, `/v1/series/${enc(DST_HELD)}.metadata.json`)).body.detail, DENMARK,
               "a dst id in the committed floor keeps the Denmark sentence");
  assert.equal((await getJson(w, `/v1/series/${enc(OTHER)}.metadata.json`)).status, 200, "another source is open");
  const browse = await getJson(w, "/v1/catalog?source=dst");
  assert.deepEqual(browse.body.results, []);
  const b = await getJson(w, "/v1/bundle?source=dst");
  assert.equal(b.status, 404, b.text);
  assert.equal(b.body.detail, UNREADABLE);
});
