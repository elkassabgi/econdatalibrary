// The source-names timings as pure functions (R1179 finding 2): production sets neither variable, so the
// DEFAULTS are the values that run and are pinned here, with every malformed value.
import assert from "node:assert/strict";
import { test } from "node:test";

import { sourceNamesBackoffMs, sourceNamesMaxAgeMs } from "../src/edge.ts";

test("the back-off: 60 s unless a number >= 1 is set", () => {
  assert.equal(sourceNamesBackoffMs({}), 60_000, "production sets nothing");
  for (const bad of ["", "  ", "abc", "NaN", "-5", "0", "0.5", "Infinity"]) {
    assert.equal(sourceNamesBackoffMs({ SOURCE_NAMES_BACKOFF_S: bad }), 60_000, JSON.stringify(bad));
  }
  assert.equal(sourceNamesBackoffMs({ SOURCE_NAMES_BACKOFF_S: "3" }), 3_000);
  assert.equal(sourceNamesBackoffMs({ SOURCE_NAMES_BACKOFF_S: " 120 " }), 120_000);
});

test("the max age: 1 h unless a number >= 0 is set; 0 is a real setting", () => {
  assert.equal(sourceNamesMaxAgeMs({}), 3_600_000, "production sets nothing");
  for (const bad of ["", "abc", "-1", "Infinity"]) {
    assert.equal(sourceNamesMaxAgeMs({ SOURCE_NAMES_MAX_AGE_S: bad }), 3_600_000, JSON.stringify(bad));
  }
  assert.equal(sourceNamesMaxAgeMs({ SOURCE_NAMES_MAX_AGE_S: "0" }), 0);
  assert.equal(sourceNamesMaxAgeMs({ SOURCE_NAMES_MAX_AGE_S: "600" }), 600_000);
});

test("production's wrangler.toml sets neither, so the defaults above are what runs", async () => {
  const { readFileSync } = await import("node:fs");
  const toml = readFileSync(new URL("../wrangler.toml", import.meta.url), "utf-8");
  assert.ok(!/SOURCE_NAMES_(BACKOFF|MAX_AGE)_S/.test(toml.replace(/^\s*#.*$/gm, "")));
});
