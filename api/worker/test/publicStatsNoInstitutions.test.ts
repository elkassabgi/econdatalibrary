// /v1/public-stats carries NO institutions list of its own (2026-10-11). The family has one allow-list (the hf
// API's feed, which the stats page reads). This route used to return the top 50 raw answers of the institution
// field with a block-list only, so it showed names the allow-list does not show.
//
// A receipt that can fail: the fake users db would answer an institutions query with a planted name, and the
// control asserts that the handler really read the users db (the per-country query ran).
//
// PS_DIR=<dir holding publicStats.ts> runs it against another copy of src (to see it fail on the parent commit).
import assert from "node:assert/strict";
import { test } from "node:test";
import { pathToFileURL } from "node:url";
import { join } from "node:path";

const dir = process.env.PS_DIR ?? join(import.meta.dirname, "..", "src");
const { handlePublicStats } = await import(pathToFileURL(join(dir, "publicStats.ts")).href);

const PLANTED = "Planted Trading Firm LLC";

function fakeUsers(seen: string[]) {
  const stmt = (sql: string) => ({
    bind: () => stmt(sql),
    first: async () => { seen.push(sql); return { c: 3, b: 0 }; },
    all: async () => {
      seen.push(sql);
      if (/institution/i.test(sql)) return { results: [{ institution: PLANTED, users: 7 }] };
      if (/user_countries/i.test(sql)) return { results: [{ country: "US", users: 2 }] };
      return { results: [] };
    },
  });
  return { prepare: (sql: string) => stmt(sql) };
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

  for (const k of Object.keys(body)) assert.ok(!/institution/i.test(k), `top-level field ${k}`);
  assert.ok(!seen.some((s) => /institution/i.test(s)), "a query on the institution field ran");
  assert.ok(!text.includes(PLANTED), "a planted institution name reached the body");
});
