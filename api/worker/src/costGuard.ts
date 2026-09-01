// ---------------------------------------------------------------------------
// src/costGuard.ts -- the cost meter, on a schedule Cloudflare actually honours.
//
// WHY THIS EXISTS AND NOT JUST THE GITHUB WORKFLOW. `tools/billing_guard.py` runs in
// .github/workflows/billing-guard.yml, and GitHub's scheduled workflows are best-effort.
// Measured on that exact workflow, 15 scheduled runs against a `23 13 * * *` cron: median
// 0.7 h late, worst 9.7 h, and the five most recent were 3.8, 4.2, 6.1, 9.6 and 9.7 hours
// late. A `*/30` cron there produces mostly DROPPED events, not 48 runs a day. Ahmed asked
// for a permanent 30-minute check after the 2026-08-31 spike reached him through his invoice
// (~$27 in a day: D1 11,412,906 writes and 2,805,188,474 reads, R2 class-A 2,880,378 ops);
// GitHub cannot promise that cadence, Cloudflare Cron Triggers can, and this account already
// runs on Cloudflare.
//
// WHAT IT DOES. Every tick it asks the GraphQL analytics API for TODAY's D1 rows read/written
// and R2 class-A operations, compares them against daily limits, and writes the result to R2
// at `_aqueduct/cost_status.json` -- a durable record that exists whether or not anyone is
// watching, and which the next tick, a human, or the Python guard can read.
//
// On a breach it ALSO throws, so the run is recorded as a Worker error: Cloudflare's own
// notification on Worker errors is then a delivery path that needs no third-party email
// service. A quiet tick writes the same file and returns normally.
//
// DELIBERATELY NOT EXPOSED OVER HTTP. Account spend is not public data, and this Worker
// serves an unauthenticated API.
// ---------------------------------------------------------------------------

export interface CostGuardEnv {
  SERIES_BUCKET: R2Bucket;
  // A read-only "Account Analytics: Read" token. Same value as CF_ANALYTICS_TOKEN in the
  // repo .env; set with: npx wrangler secret put CF_ANALYTICS_TOKEN
  CF_ANALYTICS_TOKEN?: string;
  CF_ACCOUNT_ID?: string;
}

// Daily limits, from measured steady state rather than guesses. The clean day before the
// spike (2026-08-30) ran 373,588,605 D1 reads, 678,127 D1 writes and 84,876 class-A ops.
// Monthly included allowances are 25B reads, 50M writes and 1M class-A operations, so a
// single day above ALERT_R2_A has spent the entire month's operation allowance.
export const LIMITS = {
  d1ReadsDay: 2_000_000_000,
  d1WritesDay: 5_000_000,
  r2ClassADay: 1_000_000,
};

const GQL = "https://api.cloudflare.com/client/v4/graphql";
const STATUS_KEY = "_aqueduct/cost_status.json";

interface DayTotals { d1Reads: number; d1Writes: number; r2ClassA: number; }

// R2 prices class-B (reads) at $0.36/M and class-A (writes, lists) at $4.50/M. Everything
// else here is counted, not priced -- the Python guard does the full invoice arithmetic.
const CLASS_B = new Set([
  "ListBuckets", "GetBucket", "HeadBucket", "GetObject", "HeadObject", "UsageSummary",
  "ListParts",
]);

async function gql(token: string, query: string, variables: Record<string, unknown>) {
  const r = await fetch(GQL, {
    method: "POST",
    headers: { "Authorization": `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  if (!r.ok) throw new Error(`analytics HTTP ${r.status}`);
  const j = await r.json() as { data?: unknown; errors?: unknown };
  if (j.errors) throw new Error(`analytics errors: ${JSON.stringify(j.errors).slice(0, 200)}`);
  return j.data as Record<string, unknown>;
}

export async function measureToday(token: string, acct: string): Promise<DayTotals> {
  // UTC, because the GraphQL `date` dimension is UTC. Using a local date here would read the
  // wrong day for several hours every evening (the Python guard carries the same note).
  const today = new Date().toISOString().slice(0, 10);
  const vars = { acct, start: today, end: today };

  const d1 = await gql(token, `
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    d1AnalyticsAdaptiveGroups(limit: 100, filter: {date_geq: $start, date_leq: $end}) {
      sum { rowsRead rowsWritten } } } } }`, vars);
  const r2 = await gql(token, `
query($acct: String!, $start: Date!, $end: Date!) {
  viewer { accounts(filter: {accountTag: $acct}) {
    r2OperationsAdaptiveGroups(limit: 500, filter: {date_geq: $start, date_leq: $end}) {
      dimensions { actionType } sum { requests } } } } }`, vars);

  const accounts = (d1 as any)?.viewer?.accounts?.[0];
  let reads = 0, writes = 0;
  for (const g of accounts?.d1AnalyticsAdaptiveGroups ?? []) {
    reads += g.sum?.rowsRead ?? 0;
    writes += g.sum?.rowsWritten ?? 0;
  }
  let classA = 0;
  for (const g of (r2 as any)?.viewer?.accounts?.[0]?.r2OperationsAdaptiveGroups ?? []) {
    if (!CLASS_B.has(g.dimensions?.actionType)) classA += g.sum?.requests ?? 0;
  }
  return { d1Reads: reads, d1Writes: writes, r2ClassA: classA };
}

export function breachesOf(t: DayTotals): string[] {
  const out: string[] = [];
  if (t.d1Reads > LIMITS.d1ReadsDay)
    out.push(`D1 reads ${t.d1Reads.toLocaleString()} today > ${LIMITS.d1ReadsDay.toLocaleString()}`);
  if (t.d1Writes > LIMITS.d1WritesDay)
    out.push(`D1 writes ${t.d1Writes.toLocaleString()} today > ${LIMITS.d1WritesDay.toLocaleString()}`);
  if (t.r2ClassA > LIMITS.r2ClassADay)
    out.push(`R2 class-A ${t.r2ClassA.toLocaleString()} today > ${LIMITS.r2ClassADay.toLocaleString()} `
             + `(~$${(t.r2ClassA / 1e6 * 4.5).toFixed(2)}, and the monthly included allowance is 1M)`);
  return out;
}

export async function runCostGuard(env: CostGuardEnv): Promise<void> {
  const at = new Date().toISOString();
  const token = env.CF_ANALYTICS_TOKEN;
  const acct = env.CF_ACCOUNT_ID;

  // BLIND IS A FAILURE, NOT A QUIET PASS. The Python guard spent months green in CI purely
  // because its token was never passed, reporting "$13/mo" against a real $328. A guard that
  // cannot measure has to be louder than one that can, not quieter.
  if (!token || !acct) {
    const body = { at, ok: false, blind: true,
                   note: "CF_ANALYTICS_TOKEN / CF_ACCOUNT_ID not bound; nothing was measured" };
    await env.SERIES_BUCKET.put(STATUS_KEY, JSON.stringify(body, null, 2),
                                { httpMetadata: { contentType: "application/json" } });
    throw new Error("cost guard is BLIND: CF_ANALYTICS_TOKEN / CF_ACCOUNT_ID not bound");
  }

  let totals: DayTotals;
  try {
    totals = await measureToday(token, acct);
  } catch (e) {
    const body = { at, ok: false, blind: true, note: `measurement failed: ${String(e).slice(0, 200)}` };
    await env.SERIES_BUCKET.put(STATUS_KEY, JSON.stringify(body, null, 2),
                                { httpMetadata: { contentType: "application/json" } });
    throw e;
  }

  const breaches = breachesOf(totals);
  const body = { at, ok: breaches.length === 0, blind: false, totals, limits: LIMITS, breaches };
  await env.SERIES_BUCKET.put(STATUS_KEY, JSON.stringify(body, null, 2),
                              { httpMetadata: { contentType: "application/json" } });
  if (breaches.length) {
    throw new Error("COST BREACH: " + breaches.join(" | "));
  }
}
