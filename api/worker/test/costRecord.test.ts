// src/costRecord.ts - how a cost-guard tick ends (review R1175: the rule had code and no test).
import assert from "node:assert/strict";
import { test } from "node:test";

import { record } from "../src/costRecord.ts";

const ok = async () => {};
const broken = async () => { throw new Error("D1_ERROR: no such table: econ_ops_status"); };

test("a quiet tick with a good write returns", async () => {
  await record(ok, null);
});

test("a verdict is raised after a good write, unchanged", async () => {
  const breach = new Error("COST BREACH: D1 reads 9 today > 1");
  await assert.rejects(record(ok, breach), (e) => e === breach);
});

test("a failed write during a BREACH still leads with the breach", async () => {
  await assert.rejects(record(broken, new Error("COST BREACH: R2 class-A 2,000,000 today > 1,000,000")), (e: Error) => {
    assert.match(e.message, /^COST BREACH: R2 class-A/, "the verdict comes first");
    assert.match(e.message, /could not be written: .*no such table/, "and the write failure after it");
    return true;
  });
});

test("a failed write on a quiet tick is still an error", async () => {
  await assert.rejects(record(broken, null), /^Error: cost guard OK \| AND the status record could not be written/);
});
