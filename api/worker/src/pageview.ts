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
import type { Env } from "./types";

const CORS = { "Access-Control-Allow-Origin": "*" };

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
  if (path) {
    const day = new Date().toISOString().slice(0, 10);
    try {
      await env.CATALOG.prepare(
        "INSERT INTO pageview (path, day, hits) VALUES (?1, ?2, 1) " +
        "ON CONFLICT(path, day) DO UPDATE SET hits = hits + 1",
      ).bind(path, day).run();
    } catch {
      // A counter must never break a page. Swallow and still return the pixel.
    }
  }
  return pixel();
}

export async function handlePageviewReport(url: URL, env: Env): Promise<Response> {
  const days = Math.min(Math.max(Number(url.searchParams.get("days") ?? 90), 1), 3650);
  const since = new Date(Date.now() - days * 86400_000).toISOString().slice(0, 10);
  const rows = await env.CATALOG.prepare(
    "SELECT path, SUM(hits) AS hits, MIN(day) AS first_day, MAX(day) AS last_day " +
    "FROM pageview WHERE day >= ?1 GROUP BY path ORDER BY hits DESC",
  ).bind(since).all<{ path: string; hits: number; first_day: string; last_day: string }>();
  const daily = await env.CATALOG.prepare(
    "SELECT day, SUM(hits) AS hits FROM pageview WHERE day >= ?1 " +
    "GROUP BY day ORDER BY day DESC LIMIT 90",
  ).bind(since).all<{ day: string; hits: number }>();

  return new Response(JSON.stringify({
    window_days: days,
    since,
    // Stated in the payload so a number lifted from this endpoint carries its own
    // caveat: these are beacon loads, not distinct people.
    counts: "page loads that executed the beacon — NOT unique visitors; repeat " +
            "visits count each time, and clients that block scripts or images are " +
            "not counted at all",
    by_path: rows.results ?? [],
    by_day: daily.results ?? [],
  }, null, 1), {
    headers: { ...CORS, "Content-Type": "application/json; charset=utf-8" },
  });
}
