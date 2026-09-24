// Hidden page-view counter.
//
// WHY THIS EXISTS: elkassabgidata.com carries no analytics beacon of any kind, so
// "how many people viewed my about page?" currently has no answer and no historical
// data to recover. Cloudflare Web Analytics would be the fuller tool, but it needs a
// dashboard step and a site token; this needs neither and starts counting the moment
// the snippet ships.
//
// WHAT IT STORES: a path, a UTC day, a count. Nothing else. No IP, no user agent, no
// cookie, no identifier. That is deliberate — it keeps the site clear of consent
// banners (there is no personal data to consent to), and a leak of this table would
// tell an attacker only how many times a public page was loaded.
//
// WHAT IT IS NOT: this counts REQUESTS TO THE BEACON, which is not the same as
// people. Anything that runs the snippet is counted once per load, including repeat
// visits by the same person, and anyone who curls the endpoint can inflate a number.
// It is a floor-quality signal for "is this page getting traffic", not an audience
// measurement, and the report endpoint says so in its own payload rather than
// letting a caller assume otherwise.
//
// WHERE IT LIVES (docs/ECON_SELF_HOSTING_PLAN.md): today, table `pageview` in econ D1 (CATALOG). With
// FORWARD = "on" the econ catalogue lives on the workstation and econ D1 is retired, so the edge counts
// into `econ_pageview` in the shared users db (USERS) instead - the same db that already holds
// econ_download_log. Same columns, same allowlist; migrations/users_selfhost.sql creates it, and so does
// the first hit if the flip came before the migration (a missing table must not drop every page view
// silently, R1172). The one-time merge of the old rows is in that migration file's header.
import type { Env } from "./types";
import { edgeStateInUsers } from "./edge";

const CORS = { "Access-Control-Allow-Origin": "*" };

type Store = { db: D1Database; table: "econ_pageview" | "pageview" };

/** The db and table page views are counted in: econ D1 until FORWARD is on, the users db after. */
function store(env: Env): Store {
  return edgeStateInUsers(env) ? { db: env.USERS, table: "econ_pageview" } : { db: env.CATALOG, table: "pageview" };
}

// Identical columns to econ D1's `pageview` and to migrations/users_selfhost.sql (pinned by a test).
export function pageviewDdl(table: Store["table"]): string {
  return `CREATE TABLE IF NOT EXISTS ${table} (path TEXT NOT NULL, day TEXT NOT NULL, ` +
    "hits INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (path, day))";
}

/** Run `fn` on the store. If its table does not exist yet: the BEACON creates it and runs `fn` once more;
 *  the REPORT (an unauthenticated GET) never creates anything and answers `empty` instead (R1174 B7). */
async function onStore<T>(env: Env, fn: (s: Store) => Promise<T>, empty?: T): Promise<T> {
  const s = store(env);
  try {
    return await fn(s);
  } catch (e) {
    if (!/no such table/i.test(String(e))) throw e;
    if (empty !== undefined) return empty;
    await s.db.prepare(pageviewDdl(s.table)).run();
    return await fn(s);
  }
}

/** The report's window in days: 1..3650, 90 when absent or not a number (NaN used to reach toISOString
 *  and answer 500). Also the only part of the URL the report's edge-cache key keeps. */
export function reportDays(url: URL): number {
  const n = Math.floor(Number(url.searchParams.get("days") ?? 90));
  return Number.isFinite(n) ? Math.min(Math.max(n, 1), 3650) : 90;
}

// Only paths we actually publish. An open counter keyed on caller-supplied text
// would let anyone create unbounded rows in D1 — cheap vandalism that costs storage
// and buries the real paths in noise. An allowlist makes the write surface finite.
const TRACKED = new Set([
  "/", "/index.html", "/about", "/about.html", "/data", "/data.html",
  "/download", "/download.html", "/sources", "/sources.html",
  "/stats", "/stats.html", "/docs", "/docs.html", "/license", "/license.html",
  "/faq", "/faq.html", "/contact", "/contact.html", "/cite", "/cite.html",
]);

function normalise(raw: string | null): string | null {
  if (!raw) return null;
  let p = raw.trim();
  if (p.length > 128) return null;             // nothing legitimate is this long
  const q = p.indexOf("?");
  if (q >= 0) p = p.slice(0, q);               // drop query strings entirely
  const h = p.indexOf("#");
  if (h >= 0) p = p.slice(0, h);
  if (!p.startsWith("/")) p = "/" + p;
  return TRACKED.has(p) ? p : null;
}

// 1x1 transparent GIF. Returned regardless of outcome so the page never shows a
// broken image and a counting failure can never be visible to a visitor.
const PIXEL = Uint8Array.from([
  0x47, 0x49, 0x46, 0x38, 0x39, 0x61, 0x01, 0x00, 0x01, 0x00, 0x80, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x21, 0xf9, 0x04, 0x01, 0x00, 0x00, 0x00,
  0x00, 0x2c, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x02, 0x02,
  0x44, 0x01, 0x00, 0x3b,
]);

function pixel(): Response {
  return new Response(PIXEL, {
    status: 200,
    headers: {
      ...CORS,
      "Content-Type": "image/gif",
      // Never let a CDN or browser serve this from cache — a cached beacon stops
      // counting, silently, which is the failure mode hardest to notice.
      "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
    },
  });
}

export async function handlePageview(url: URL, env: Env): Promise<Response> {
  const path = normalise(url.searchParams.get("p"));
  // x-econ-pv says what happened, so a failure the pixel must hide is still visible to a test or an
  // operator (R1175: a stray econ-D1 read after the insert was swallowed and nothing could see it).
  let outcome = "ignored";
  if (path) {
    const day = new Date().toISOString().slice(0, 10);
    try {
      await onStore(env, ({ db, table }) => db.prepare(
        `INSERT INTO ${table} (path, day, hits) VALUES (?1, ?2, 1) ` +
        "ON CONFLICT(path, day) DO UPDATE SET hits = hits + 1",
      ).bind(path, day).run());
      outcome = "counted";
    } catch {
      // A counter must never break a page. Swallow and still return the pixel.
      outcome = "failed";
    }
  }
  const out = pixel();
  out.headers.set("x-econ-pv", outcome);
  return out;
}

export async function handlePageviewReport(url: URL, env: Env): Promise<Response> {
  const days = reportDays(url);
  const since = new Date(Date.now() - days * 86400_000).toISOString().slice(0, 10);
  type PathRow = { path: string; hits: number; first_day: string; last_day: string };
  type DayRow = { day: string; hits: number };
  const empty: { byPath: PathRow[]; byDay: DayRow[] } = { byPath: [], byDay: [] };
  const { byPath, byDay } = await onStore(env, async ({ db, table }) => ({
    byPath: (await db.prepare(
      "SELECT path, SUM(hits) AS hits, MIN(day) AS first_day, MAX(day) AS last_day " +
      `FROM ${table} WHERE day >= ?1 GROUP BY path ORDER BY hits DESC`,
    ).bind(since).all<PathRow>()).results ?? [],
    byDay: (await db.prepare(
      `SELECT day, SUM(hits) AS hits FROM ${table} WHERE day >= ?1 ` +
      "GROUP BY day ORDER BY day DESC LIMIT 90",
    ).bind(since).all<DayRow>()).results ?? [],
  }), empty);

  return new Response(JSON.stringify({
    window_days: days,
    since,
    // Stated in the payload so a number lifted from this endpoint carries its own
    // caveat: these are beacon loads, not distinct people.
    counts: "page loads that executed the beacon — NOT unique visitors; repeat " +
            "visits count each time, and clients that block scripts or images are " +
            "not counted at all",
    by_path: byPath,
    by_day: byDay,
  }, null, 1), {
    headers: { ...CORS, "Content-Type": "application/json; charset=utf-8" },
  });
}
