// /v1/public-stats publishes NO download count or volume (owner's decision, 2026-10-03), and the
// most-downloaded list is at most 3 ranked names. A receipt that can fail: the fake users db plants
// distinctive count values (987654 total, 13579 per source, 24680246 bytes) and the test asserts that
// none of them reaches the body, while a control asserts the handler really read those rows (the
// ranking is non-empty and in count order) - so an always-empty list cannot pass it (AR-214 round 1).
//
// PS_DIR=<dir holding publicStats.ts> runs it against another copy of src (used to prove it fails on the
// parent commit).
import assert from "node:assert/strict";
import { test } from "node:test";
import { pathToFileURL } from "node:url";
import { join } from "node:path";

const dir = process.env.PS_DIR ?? join(import.meta.dirname, "..", "src");
const { handlePublicStats } = await import(pathToFileURL(join(dir, "publicStats.ts")).href);

const PLANTED_TOTAL = 987654, PLANTED_BYTES = 24680246;
// Five catalogued sources with distinct per-source counts; ranking must be by these counts.
const BY_SOURCE = [
  { source: "aa", downloads: 13579 }, { source: "bb", downloads: 8642 }, { source: "cc", downloads: 7531 },
  { source: "dd", downloads: 6420 }, { source: "ee", downloads: 5319 },
];
const NAMES: Record<string, string> = { aa: "Alpha Source", bb: "Beta Source", cc: "Gamma Source",
                                       dd: "Delta Source", ee: "Epsilon Source" };

function fakeUsers(seen: string[]) {
  const stmt = (sql: string) => ({
    bind: () => stmt(sql),
    first: async () => {
      seen.push(sql);
      if (/SUM\(bytes\)/i.test(sql)) return { b: PLANTED_BYTES };
      if (/econ_download_log/i.test(sql)) return { c: PLANTED_TOTAL };
      return { c: 3 };                                   // users
    },
    all: async () => {
      seen.push(sql);
      if (/econ_download_log/i.test(sql)) return { results: BY_SOURCE };
      return { results: [] };
    },
  });
  return { prepare: (sql: string) => stmt(sql) };
}

test("public-stats carries no download count or volume, and top_sources is <= 3 ranked names", async () => {
  const seen: string[] = [];
  const env = { USERS: fakeUsers(seen) } as unknown;
  const res: Response = await handlePublicStats(env, async () => NAMES);
  assert.equal(res.status, 200);
  const text = await res.text();
  const body = JSON.parse(text);

  // CONTROL: the per-source rows were read and ranked, so the assertions below measure something.
  assert.ok(seen.some((s) => /GROUP BY source/i.test(s)), "the per-source query ran against the fake");
  assert.deepEqual(body.top_sources.map((t: { name: string }) => t.name),
                   ["Alpha Source", "Beta Source", "Gamma Source"], "top 3, in download order");

  // No count field anywhere at the top level...
  for (const k of Object.keys(body)) assert.ok(!/download|bytes|served/i.test(k), `top-level field ${k}`);
  // ...no field on a ranking entry beyond its id and name...
  for (const t of body.top_sources) assert.deepEqual(Object.keys(t).sort(), ["name", "source_id"]);
  // ...and none of the planted numbers anywhere in the body, under any key.
  for (const n of [PLANTED_TOTAL, PLANTED_BYTES, ...BY_SOURCE.map((r) => r.downloads)]) {
    assert.ok(!text.includes(String(n)), `planted count ${n} leaked into the body`);
  }
});
