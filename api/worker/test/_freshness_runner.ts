// Not a test file (CI runs test/*.test.ts only). tests/test_freshness_vocab.py runs it with node to hold the
// TypeScript rule (src/freshness.ts) to the SAME answers as the Python one (core/freshness_vocab.py):
//   node test/_freshness_runner.ts <cases.json> <verdicts.json>
// prints {"cases": [{source, units, next}], "lane": [bool per verdict note]} on stdout.
import { readFileSync } from "node:fs";

import { CLASSES, type FreshnessClasses, promisesNextUpdate, sourceLabel, unitStatus } from "../src/freshness.ts";

export function plantedClasses(p: {
  paused: Record<string, string>; run_by: string[]; discontinued: string[]; rotating: string[]; lane: string[];
}): FreshnessClasses {
  return { ...CLASSES, paused: p.paused, runBy: new Set(p.run_by), discontinued: new Set(p.discontinued),
           rotating: new Set(p.rotating), lane: new Set(p.lane) };
}

// deno-lint-ignore no-explicit-any
export function runCases(doc: any): { source: string | null; units: (string | null)[]; next: boolean }[] {
  const c = plantedClasses(doc.planted);
  const now = Date.parse(doc.now);
  // deno-lint-ignore no-explicit-any
  return doc.cases.map((k: any) => {
    const label = sourceLabel(k.sid, k.src, k.units, now, c);
    // deno-lint-ignore no-explicit-any
    return { source: label, units: k.units.map((u: any) => unitStatus(u.status, label)), next: promisesNextUpdate(label) };
  });
}

if (process.argv[1] && process.argv[1].endsWith("_freshness_runner.ts") && process.argv.length >= 4) {
  const cases = JSON.parse(readFileSync(process.argv[2], "utf-8"));
  const verdicts = JSON.parse(readFileSync(process.argv[3], "utf-8"));
  // deno-lint-ignore no-explicit-any
  const lane = verdicts.verdicts.map((v: any) => CLASSES.laneCurrent.test(v.note));
  process.stdout.write(JSON.stringify({ cases: runCases(cases), lane }));
}
