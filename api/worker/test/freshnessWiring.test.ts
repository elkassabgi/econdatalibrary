// The REAL worker (src/index.ts in workerd through unstable_dev) prints the freshness vocabulary on
// /v1/sources and /v1/last-updates - in BOTH configurations, the cloud edge (wrangler.toml, D1) and the
// self-hosted origin (wrangler.origin.toml, its copy of the same tables). The unit tests of src/freshness.ts
// cannot see the handlers' wiring (a label computed and then not printed, the wrong row fed in, a note
// leaking into the JSON); these can. Every label has a control beside it that must NOT change.
import assert from "node:assert/strict";
import { test } from "node:test";

import { EDGE_CONFIG, execSql, newPersist, ORIGIN_CONFIG, start, withBindings } from "./_harness.ts";

const DAY = 86_400_000;
const ago = (d: number) => new Date(Date.now() - d * DAY).toISOString();
const NOTE_ECB = "164 sub-unit(s) attempted, none failed; 376 deferred by budget, not reached in this pass [A.parquet]";
const NOTE_LANE = "statcan: 4 cube(s) QUARANTINED (each failed 3x on one release): 1; 0 cube(s) owe a merge, 0 owe serving, 0 released cube(s) not held (catalogue debt)";
const NOTE_ISTAT = "istat: 1/12 sub-unit(s) returned 200 but parsed 0 rows from a non-trivial body";

function seed(): string {
  const src = (id: string) => `INSERT INTO source VALUES ('${id}', '${id} name', NULL, 'lic', NULL, NULL);
    INSERT INTO series (series_id, source_id, title, license_id) VALUES ('${id}:X', '${id}', 'x', 'lic')`;
  const ss = (id: string, cad: string, st: string, d: number) =>
    `INSERT INTO source_state (source_id, cadence, status, last_success_utc, last_attempt_utc) VALUES ('${id}', '${cad}', '${st}', '${ago(d)}', '${ago(d)}')`;
  return [
    `CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT, title TEXT, frequency TEXT, unit TEXT,
       geography TEXT, category TEXT, license_id TEXT, start_date TEXT, end_date TEXT, last_updated TEXT, metadata TEXT)`,
    `CREATE TABLE source (source_id TEXT PRIMARY KEY, name TEXT, homepage TEXT, license_id TEXT, attribution TEXT, terms_url TEXT)`,
    `CREATE TABLE license (license_id TEXT PRIMARY KEY, name TEXT, reservable INTEGER, commercial_ok INTEGER,
       attribution_required INTEGER, no_modify INTEGER, url TEXT)`,
    `INSERT INTO license VALUES ('lic', 'L', 1, 1, 1, 0, NULL)`,
    `CREATE TABLE source_state (source_id TEXT PRIMARY KEY, strategy TEXT, cadence TEXT, status TEXT,
       last_success_utc TEXT, last_attempt_utc TEXT, owner TEXT, enabled INTEGER DEFAULT 1, note TEXT)`,
    `CREATE TABLE unit_state (source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT,
       last_success_utc TEXT, last_attempt_utc TEXT, status TEXT, last_obs_date TEXT, obs_count INTEGER DEFAULT 0,
       attempt_count INTEGER DEFAULT 0, last_error TEXT, PRIMARY KEY(source_id, unit_id))`,
    `CREATE TABLE source_data_through (source_id TEXT PRIMARY KEY, data_through TEXT)`,
    ...["istat", "ecb", "statcan", "bcrp", "insee_melodi"].map(src),
    ss("istat", "monthly", "ok", 98), ss("ecb", "daily", "ok", 95), ss("statcan", "weekly", "ok", 46),
    ss("bcrp", "daily", "ok", 0.5), ss("ksh", "annual", "ok", 118),
  ].join(";\n");
}

// Unit rows carry notes with "; " in them, which execSql's statement split would cut: bound, one by one.
const UNITS: [string, string, number | null, number, string | null][] = [
  ["istat", "partial", 98, 0.1, NOTE_ISTAT], ["ecb", "partial", 95, 0.2, NOTE_ECB],
  ["statcan", "partial", 46, 0.1, NOTE_LANE], ["bcrp", "ok", 0.5, 0.5, null],
  ["insee_melodi", "transient_fail", null, 0.1, "boom"], ["ksh", "ok", 118, 118, null],
];
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function seedUnits(db: any): Promise<void> {
  for (const [id, st, succ, att, note] of UNITS) {
    await db.prepare("INSERT INTO unit_state (source_id, unit_id, status, last_success_utc, last_attempt_utc, " +
                     "last_error, obs_count) VALUES (?, '_all', ?, ?, ?, ?, 1)")
      .bind(id, st, succ === null ? null : ago(succ), ago(att), note).run();
  }
}

for (const [label, config, vars, headers] of [
  ["edge (wrangler.toml)", EDGE_CONFIG, {}, {}],
  ["origin (wrangler.origin.toml)", ORIGIN_CONFIG, { ORIGIN_SECRET: "fresh-test-secret" },
   { "x-econ-origin-secret": "fresh-test-secret" }],
] as const) {
  test(`${label}: /v1/sources and /v1/last-updates print late / paused / retired, controls unchanged`,
       { timeout: 300_000 }, async (t) => {
    const persist = newPersist("econ-fresh-");
    await withBindings(config, persist, async (env) => {
      await execSql(env.CATALOG, seed());
      await seedUnits(env.CATALOG);
      // /v1/sources probes the climate shard for its sources (sources.ts): an empty table, so none is present
      await execSql(env.CATALOG_CLIMATE, "CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)");
    });
    const w = await start(config, vars as Record<string, string>, persist);
    t.after(() => w.stop());

    const rs = await w.fetch("/v1/sources", { headers });
    const sText = await rs.text();
    assert.equal(rs.status, 200, sText.slice(0, 300));
    const s = Object.fromEntries(JSON.parse(sText).sources.map((x: { source: string; freshness: unknown }) =>
      [x.source, x.freshness]));
    assert.equal(s.istat.status, "late");
    assert.equal(s.ecb.status, "ok", "a declared rotator whose latest pass is a clean deferral is not late");
    assert.equal(s.statcan.status, "ok", "a lane whose reporter says current is not late");
    assert.equal(s.bcrp.status, "ok", "control: fresh");
    assert.equal(s.insee_melodi.status, "paused", "paused from the registry, with no source_state row");
    assert.deepEqual(Object.keys(s.istat).sort(), ["cadence", "data_through", "last_updated", "status"],
                     "the freshness shape is unchanged");

    const rl = await w.fetch("/v1/last-updates", { headers });
    const lText = await rl.text();
    assert.equal(rl.status, 200, lText.slice(0, 300));
    const l = Object.fromEntries(JSON.parse(lText).datasets.map((d: { source: string }) => [d.source, d]));
    assert.equal(l.istat.status, "late");
    assert.notEqual(l.istat.next_update_expected, null, "late keeps its (past) expected date");
    assert.equal(l.ecb.status, "partial");
    assert.equal(l.statcan.status, "partial");
    assert.equal(l.bcrp.status, "ok");
    assert.equal(l.insee_melodi.status, "paused");
    assert.equal(l.insee_melodi.next_update_expected, null);
    assert.equal(l.ksh.status, "retired");
    assert.equal(l.ksh.next_update_expected, null, "retired promises no next update");
    assert.deepEqual(Object.keys(l.bcrp).sort(), ["last_obs_date", "last_updated", "next_update_expected", "obs_count",
      "source", "source_date_accessed", "source_version", "status", "unit"], "the dataset shape is unchanged");

    for (const body of [sText, lText]) {
      for (const leak of ["sub-unit(s)", "QUARANTINED", "last_error", "last_attempt", "boom"]) {
        assert.ok(!body.includes(leak), `a note or attempt stamp reached the response: ${leak}`);
      }
    }
  });
}
