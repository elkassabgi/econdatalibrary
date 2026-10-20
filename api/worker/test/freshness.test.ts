// The freshness vocabulary as pure functions (src/freshness.ts; owner decisions 2026-10-20).
// Cases are shared with tests/test_freshness_vocab.py (test/freshness_cases.json): planted late / not-late /
// paused / retired / rotator / lane cases, each with a negative control beside it. The statcan grammar is
// checked against test/statcan_verdicts.json, which pytest regenerates from statcan.verdict() itself.
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import { CLASSES, LATE, needsUnits, promisesNextUpdate, sourceLabel, unitStatus } from "../src/freshness.ts";
import { DISCONTINUED, LANE, PAUSED, ROTATING, RUN_BY } from "../src/freshnessClasses.ts";
import { runCases } from "./_freshness_runner.ts";

const doc = JSON.parse(readFileSync(new URL("./freshness_cases.json", import.meta.url), "utf-8"));
const verdicts = JSON.parse(readFileSync(new URL("./statcan_verdicts.json", import.meta.url), "utf-8"));

test("every shared case gives its expected source label, unit statuses and next-update promise", () => {
  assert.ok(doc.cases.length >= 30);
  const got = runCases(doc);
  doc.cases.forEach((k: { name: string; expect_source: string | null; expect_units: string[]; expect_next: boolean },
                    i: number) => {
    assert.deepEqual([got[i].source, got[i].units, got[i].next], [k.expect_source, k.expect_units, k.expect_next],
                     k.name);
  });
});

test("the lane grammar matches exactly the verdicts that say the lane is current (real verdict() output)", () => {
  assert.ok(verdicts.verdicts.length >= 10);
  for (const v of verdicts.verdicts) assert.equal(CLASSES.laneCurrent.test(v.note), v.current, v.branch);
  assert.ok(verdicts.verdicts.some((v: { current: boolean }) => v.current), "control: some are current");
  assert.ok(verdicts.verdicts.some((v: { current: boolean }) => !v.current), "control: some are not");
});

test("the deferral grammar is health's: clean deferral yes, a transient pass or a zero-attempt pass no", () => {
  assert.ok(CLASSES.deferral.test("164 sub-unit(s) attempted, none failed; 376 deferred by budget, not reached in this pass"));
  assert.ok(CLASSES.deferral.test("1 sub-unit(s) attempted, none failed; 2 deferred by budget and taken next tick [a, b]"));
  assert.ok(!CLASSES.deferral.test("0 sub-unit(s) attempted, none failed; 540 deferred by budget, not reached in this pass"));
  assert.ok(!CLASSES.deferral.test("2/68 sub-unit(s) transient-failed; will retry"));
  assert.ok(!CLASSES.deferral.test("164 sub-unit(s) attempted, none failed; 376 deferred by budget, not reached in this pass; csv_derive failed"));
});

test("today's production classes: ecb rotates, statcan is a lane, insee_melodi is paused, ksh is retired", () => {
  assert.ok(ROTATING.has("ecb") && !ROTATING.has("cso"));
  assert.ok(LANE.has("statcan"));
  assert.ok(Object.prototype.hasOwnProperty.call(PAUSED, "insee_melodi"));
  for (const s of ["ksh", "imf_hpdd", "imf_fiscaldecentralization"]) assert.ok(!RUN_BY.has(s), s);
  for (const s of ["ecb", "istat", "cso", "bls", "eia", "statcan", "sec_edgar", "cbs_nl"]) assert.ok(RUN_BY.has(s), s);
  assert.ok(DISCONTINUED.has("unctad_bopcaba"));
  assert.ok(needsUnits("ecb") && needsUnits("statcan") && !needsUnits("istat"));
});

test("today's data (2026-10-20): the five late sources read late; ecb and statcan do not", () => {
  const now = Date.parse("2026-10-20T15:33:02Z");
  const late = (sid: string, cadence: string, last: string, unit = "partial", note: string | null = null,
                attempt = "2026-10-20T11:00:00Z") =>
    sourceLabel(sid, { status: "ok", last_success_utc: last, cadence },
                [{ status: unit, last_error: note, last_attempt_utc: attempt, last_success_utc: last }], now);
  assert.equal(late("istat", "monthly", "2026-07-14T11:23:06+00:00",
                    "partial", "istat: 1/12 sub-unit(s) returned 200 but parsed 0 rows"), LATE);
  assert.equal(late("cso", "irregular", "2026-09-06T11:55:27+00:00", "partial",
                    "4 sub-unit(s) attempted, none failed; 10740 deferred by budget, not reached in this pass [EOSA03]"), LATE);
  assert.equal(late("bls", "weekly", "2026-10-02T11:28:13+00:00", "partial",
                    "6/12 sub-unit(s) transient-failed; will retry"), LATE);
  assert.equal(late("eia", "daily", "2026-10-02T11:44:07+00:00", "ok"), LATE);
  assert.equal(late("unctad_creativegoodsvalue", "weekly", "2026-10-04T13:12:49+00:00", "no_change"), LATE);
  assert.equal(late("ecb", "daily", "2026-07-16T18:57:46+00:00", "partial",
                    "164 sub-unit(s) attempted, none failed; 376 deferred by budget, not reached in this pass [A]; csv coverage note: x - served ids coherent",
                    "2026-10-19T23:26:26+00:00"), null);
  assert.equal(late("statcan", "weekly", "2026-09-04T01:02:02+00:00", "partial",
                    "statcan: 4 cube(s) QUARANTINED (each failed 3x on one release): 10100145, 18100212, 18100275, 25100045; 0 cube(s) owe a merge, 0 owe serving, 2 released cube(s) not held (catalogue debt)",
                    "2026-10-20T12:27:54+00:00"), null);
  // controls: a fresh source is not late; a retired one promises nothing
  assert.equal(late("bcrp", "daily", "2026-10-19T22:40:49+00:00", "ok"), null);
  assert.equal(sourceLabel("ksh", { status: "ok", last_success_utc: "2026-06-24T04:09:50+00:00", cadence: "annual" },
                           [{ status: "ok" }], now), "retired");
  assert.equal(promisesNextUpdate("retired"), false);
  assert.equal(promisesNextUpdate(LATE), true);
  assert.equal(unitStatus("discontinued", LATE), "discontinued");
});
