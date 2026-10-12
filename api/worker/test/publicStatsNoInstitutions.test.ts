// /v1/public-stats carries NO institutions list of its own (2026-10-11). The family has one allow-list (the hf
// API's feed, which the stats page reads). This route used to return a list of its own from the raw answers of
// the institution field, so it showed names the allow-list does not show.
//
// A receipt that can fail: the fake users db answers EVERY query it does not know with a planted name (so a
// list read by another SQL text, for example `SELECT * FROM users`, still reaches the body), keys are searched at
// every depth, and the control asserts that the handler really read the users db (the per-country query ran).
//
// PS_DIR=<dir holding publicStats.ts> runs it against another copy of src (to see it fail on the parent commit).
import assert from "node:assert/strict";
import { test } from "node:test";
import { pathToFileURL } from "node:url";
import { join } from "node:path";

const dir = process.env.PS_DIR ?? join(import.meta.dirname, "..", "src");
const { handlePublicStats } = await import(pathToFileURL(join(dir, "publicStats.ts")).href);

const PLANTED = "Planted Trading Firm LLC";
const PLANTED_ROW = { institution: PLANTED, name: PLANTED, org: PLANTED, label: PLANTED, users: 7, c: 7 };

function fakeUsers(seen: string[]) {
  const stmt = (sql: string) => ({
    bind: () => stmt(sql),
    first: async () => { seen.push(sql); return { c: 3, b: 0, institution: PLANTED }; },
    all: async () => {
      seen.push(sql);
      if (/user_countries/i.test(sql)) return { results: [{ country: "US", users: 2 }] };
      if (/econ_download_log/i.test(sql)) return { results: [] };
      return { results: [PLANTED_ROW] };   // every other query: a row that names an institution
    },
  });
  return { prepare: (sql: string) => stmt(sql) };
}

function keysAtEveryDepth(v: unknown, path = ""): string[] {
  if (Array.isArray(v)) return v.flatMap((x, i) => keysAtEveryDepth(x, `${path}[${i}]`));
  if (v && typeof v === "object") {
    return Object.entries(v).flatMap(([k, x]) => [`${path}.${k}`, ...keysAtEveryDepth(x, `${path}.${k}`)]);
  }
  return [];
}

test("public-stats has no institutions field and asks the users db for no institution", async () => {
  const seen: string[] = [];
  const res: Response = await handlePublicStats({ USERS: fakeUsers(seen) } as unknown, async () => ({}));
  assert.equal(res.status, 200);
  const text = await res.text();
  const body = JSON.parse(text);

  // CONTROL: the handler read the fake (the per-country query ran and its row is in the body).
  assert.ok(seen.some((s) => /user_countries/i.test(s)), "the per-country query ran against the fake");
  assert.equal(body.countries.US, 2);

  for (const k of keysAtEveryDepth(body)) assert.ok(!/institution/i.test(k), `field ${k}`);
  assert.ok(!seen.some((s) => /institution/i.test(s)), "a query on the institution field ran");
  assert.ok(!text.includes(PLANTED), "a planted institution name reached the body");
});

test("the key search reaches nested fields (a control of the instrument)", () => {
  const keys = keysAtEveryDepth({ community: { institutions: [{ n: 1 }] }, a: [{ institution_count: 0 }] });
  assert.ok(keys.includes(".community.institutions"));
  assert.ok(keys.includes(".a[0].institution_count"));
});
