// ---------------------------------------------------------------------------
// src/costRecord.ts -- how the cost guard (src/costGuard.ts) finishes a tick: write the status record,
// then raise the verdict. Its own file, with no imports, so test/costRecord.test.ts can run it under
// node --test (review R1175: the rule had code and no test that could catch it breaking).
//
// A failed write never REPLACES the verdict (R1172: a missing table turned every breach into the same
// generic D1 error). The one error thrown carries the verdict FIRST and the write failure after it, so a
// breach still reads as a breach in the Worker's error log and in Cloudflare's error notification.
// ---------------------------------------------------------------------------

export async function record(write: () => Promise<void>, verdict: Error | null): Promise<void> {
  try {
    await write();
  } catch (w) {
    const lead = verdict ? verdict.message : "cost guard OK";
    throw new Error(`${lead} | AND the status record could not be written: ${String(w).slice(0, 200)}`);
  }
  if (verdict) throw verdict;
}
