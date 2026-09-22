// A bare `?q=<source id>` for a GATED source must reach the 451, not the LIKE fallback.
//
// `/v1/catalog` promotes a bare token to a source browse when it recognises the token as a
// source id, and the redistribution gate immediately after answers 451 for a denylisted one.
// While the promotion tested only SUPPORTED_SOURCES, an id that is gated AND absent from that
// list never promoted, never reached the gate, and fell into the search path: `series_fts
// MATCH` finds nothing for it, so `ftsOk` stays false and the `%token%` LIKE fallback runs
// across both catalogue databases plus two COUNT(*) scans — ~24.2M rows read, 40-65 s, for a
// 200 with an empty body. And because it is a 200 the edge caches it, so the honest, free,
// uncached 451 is replaced by an expensive, cached, misleading success.
//
// These cases name no source: the gated id is taken from the denylist itself at run time, so
// the test cannot drift out of step with it and carries none of its contents in the file.
import assert from "node:assert/strict";
import { test } from "node:test";

import { NON_REDISTRIBUTABLE, promotesToSourceBrowse } from "../src/denylist.ts";

test("a supported source id promotes to the source browse", () => {
  const supported = new Set(["zz_supported_fixture"]);
  assert.equal(promotesToSourceBrowse("zz_supported_fixture", supported), true);
});

test("a GATED source id promotes too, so the request reaches the 451 instead of the LIKE scan", () => {
  const gated = [...NON_REDISTRIBUTABLE][0];
  assert.ok(gated, "the denylist is empty, so this property cannot be tested here");
  // Deliberately an EMPTY supported set: the point is that denylist membership alone suffices.
  assert.equal(promotesToSourceBrowse(gated, new Set()), true);
});

test("an ordinary search term does NOT promote — the search path still runs for real queries", () => {
  const gated = [...NON_REDISTRIBUTABLE][0];
  const term = "inflation";
  assert.ok(!NON_REDISTRIBUTABLE.has(term) && term !== gated,
            "fixture term must not be a source id for this control to mean anything");
  assert.equal(promotesToSourceBrowse(term, new Set(["zz_supported_fixture"])), false);
});
