// ---------------------------------------------------------------------------
// src/index.ts  --  Cloudflare Worker entrypoint. Routes the /v1 contract.
//
// Same contract, same SQL, same honest-status codes as the Python dev shim.
// Backends: D1 (catalog/license/series/freshness) + R2 (per-series CSV objects).
//
// Endpoint status (see api/worker/README.md for the full matrix):
//   FULLY LIVE from D1 now:
//     GET /v1/catalog                       (search + browse, FTS5 + LIKE)
//     GET /v1/sources                       (309 sources + license + freshness)
//     GET /v1/last-updates                  (canonical SQL + cadence math)
//     GET /v1/series/{id}.metadata.json     (series + source + license + freshness)
//     GET /v1/bundle                        (manifest; client fans out)
//   NEEDS the pre-derived R2 per-series CSV objects (see src/series.ts header):
//     GET /v1/series/{id}.csv               (streams series/<id>.csv from R2)
// ---------------------------------------------------------------------------

import type { Env } from "./types";
import { runCostGuard, type CostGuardEnv } from "./costGuard";
import { handlePageview, handlePageviewReport } from "./pageview";
import { handleCatalog } from "./catalog";
import { handleSources } from "./sources";
import { handleLastUpdates } from "./lastUpdates";
import { handleMetadata } from "./metadata";
import { handleSeriesCsv } from "./series";
import { handleBundle } from "./bundle";
import { requireDownloadAuth, logDownload } from "./auth";
import { isGated } from "./denylist";
import { handlePublicStats } from "./publicStats";
import { json, reqLang } from "./util";
import { isLocal, originGate, finalizeLocal, isDownloadPath } from "./localMode";
import { LocalBucket } from "./localBucket";
import { isForward, isForwardable, cacheSeconds, cacheKey, originRequest, fetchOrigin, countingBody,
  clientResponse, notConfigured, refusedPath } from "./edge";

const CORS_PREFLIGHT: Record<string, string> = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, OPTIONS",
  "access-control-allow-headers": "*",
  "access-control-max-age": "86400",
};

export default {
  // COST GUARD, on a schedule Cloudflare honours. GitHub's scheduled workflows are
  // best-effort: measured on billing-guard.yml, 15 runs against a daily cron came in a
  // median 0.7 h late and as much as 9.7 h, so a */30 cron there is mostly dropped events.
  // Ahmed asked for a permanent 30-minute check after 2026-08-31 cost ~$27 in a day and
  // reached him through his invoice. Cron Triggers run on Cloudflare's own infrastructure,
  // independent of any workstation. See src/costGuard.ts for what it measures and why a
  // blind run is treated as a failure.
  async scheduled(_c: ScheduledController, env: Env, ctx: ExecutionContext): Promise<void> {
    ctx.waitUntil(runCostGuard(env satisfies CostGuardEnv));
  },

  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    // SELF-HOSTED ORIGIN (src/localMode.ts): the secret gate comes before anything else, and every
    // answer - errors included - leaves private, no-store. The production worker never sets LOCAL.
    const local = isLocal(env);
    if (local) {
      const refused = await originGate(request, env);
      if (refused) return refused;
      if (!env.BLOB_SIDECAR_URL) {
        return finalizeLocal(json({ error: "origin_not_configured",
          detail: "BLOB_SIDECAR_URL is unset, so this origin has no store to serve from" }, 503));
      }
      // SERIES_BUCKET becomes the blob sidecar (src/localBucket.ts); the route code is unchanged.
      const lenv: Env = { ...env, SERIES_BUCKET: new LocalBucket(env.BLOB_SIDECAR_URL) as unknown as R2Bucket };
      const download = isDownloadPath(new URL(request.url).pathname);
      return finalizeLocal(await route(request, lenv, ctx, true), { download });
    }
    return route(request, env, ctx, false);
  },
} satisfies ExportedHandler<Env>;

async function route(request: Request, env: Env, ctx: ExecutionContext, local: boolean): Promise<Response> {
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_PREFLIGHT });
    }
    if (request.method !== "GET") {
      return json({ error: "method_not_allowed", detail: "only GET is supported" }, 405);
    }

    const url = new URL(request.url);
    const path = url.pathname;

    try {
      // Fixed routes first.
      // Hidden page-view beacon. Public and unauthenticated BY NECESSITY — it is
      // fired by a static site with no credentials — which is why the write surface
      // is an allowlist of known paths and the row holds no personal data at all.
      if (path === "/v1/pv") return await handlePageview(url, env);
      if (path === "/v1/pv/report") return await handlePageviewReport(url, env);

      // THE FORWARDING EDGE (src/edge.ts, plan code change 1): every other route is answered by the
      // workstation's origin; public-stats stays here (it reads USERS) with its source names taken from
      // the origin, not econ D1. Off unless FORWARD = "on"; the origin itself never forwards.
      if (!local && isForward(env)) {
        if (path === "/v1/public-stats") {
          return await handlePublicStats(env, () => originSourceNames(request, env, ctx));
        }
        return await forwardRoute(request, env, ctx, path);
      }

      // /v1/catalog is edge-cached (2026-08-15 cost incident): a crawler paging
      // one source drove 130B D1 rows read in a day. The catalog changes only at
      // sync time, so a 6h same-URL cache makes re-crawls free without staleness
      // anyone can observe. Only 200s are cached; the cap-400s and errors are not.
      // The origin never uses the Cache API: behind the edge it would hide a catalogue swap for up to
      // 6 h (s-maxage 21600), and the edge does the caching (review R1164).
      if (path === "/v1/catalog" && local) return await handleCatalog(url, env);
      if (path === "/v1/catalog") {
        const cache = caches.default;
        const cacheKey = new Request(url.toString(), { method: "GET" });
        const hit = await cache.match(cacheKey);
        if (hit) return hit;
        const fresh = await handleCatalog(url, env);
        if (fresh.status === 200) {
          const toCache = new Response(fresh.clone().body, fresh);
          toCache.headers.set("cache-control", "public, max-age=300, s-maxage=21600");
          ctx.waitUntil(cache.put(cacheKey, toCache.clone()));
          return toCache;
        }
        return fresh;
      }
      if (path === "/v1/sources") return await handleSources(env);
      if (path === "/v1/last-updates") return await handleLastUpdates(env);
      if (path === "/v1/bundle") return await handleBundle(url, env);

      // Family usage stats for the stats page. USER figures come from the SHARED
      // identity DB (env.USERS) with hf's exact aggregation, so users/map/
      // institutions are identical across libraries; DOWNLOAD figures are this
      // library's own (econ_download_log). Read-only, no auth, no PII.
      if (path === "/v1/public-stats") return await handlePublicStats(env);

      // Headline stats. individual_series/observations are MEASURED on the full
      // data store (census 2026-07-02, D:\...\_series_census_hll.json): global
      // distinct series keys per source via HyperLogLog (~1% error; a floor,
      // since keys that repeat across datasets dedupe), observations = exact
      // parquet row counts. catalog_entries is SUM(n) over the sync-maintained
      // source_counts cache — NOT a live count. The live COUNT(*) below is only
      // the fallback when that SUM is null. Said "counted live" here until
      // 2026-09-04, when noaa's cache row was found 42 high (R709): a cache can
      // drift from `series`, so tools/audit_d1_source_counts.py now checks it.
      if (path === "/v1/stats") {
        // NO hardcoded headline numbers (owner rule: counts must never go stale
        // in code). The measured census results live in R2 at _aqueduct/stats.json
        // — a fresh census re-uploads that object and every consumer (this
        // endpoint, the sites, the MCP server) updates with zero deploys.
        // Catalogue entries = PRIMARY + CLIMATE SHARD (task #45): noaa's series
        // rows live in CATALOG_CLIMATE, so a primary-only count silently drops
        // 3.1M entries the moment the shard migration lands.
        //
        // Same-URL edge cache (2026-08-16, cost incident follow-up): this endpoint
        // ran a full COUNT(*) over 12.3M rows PER HIT — 267M rows read/day, the
        // same billing class as the browse incident, just smaller. The count now
        // reads source_counts (1 row/source, sync-maintained) with the live
        // COUNT(*) kept as fallback, and 200s are cached 6h.
        const statsCache = local ? null : caches.default;
        const statsKey = new Request(url.toString(), { method: "GET" });
        const statsHit = statsCache ? await statsCache.match(statsKey) : undefined;
        if (statsHit) return statsHit;
        const SUM_COUNTS = "SELECT SUM(n) AS c FROM source_counts";
        let catTotal: number | null = null;
        // Set only when one catalogue database answered and the other did not, so a knowingly
        // incomplete total is labelled rather than served as if it were the whole fleet.
        let catPartial = false;
        try {
          const [sp, ss] = await Promise.all([
            env.CATALOG.prepare(SUM_COUNTS).first<{ c: number | null }>(),
            env.CATALOG_CLIMATE.prepare(SUM_COUNTS).first<{ c: number | null }>(),
          ]);
          if (sp?.c != null && ss?.c != null) catTotal = sp.c + ss.c;
        } catch {
          catTotal = null; // table missing -> live COUNT fallback below
        }
        if (catTotal === null) {
          // THE FALLBACK MUST NOT BE ABLE TO 500 THE WHOLE ENDPOINT. This used to be a bare
          // Promise.all over BOTH databases: a failure on EITHER threw to the outer handler and
          // /v1/stats returned 500 — which every site renders as zero, so a D1 incident showed up
          // to users as an empty library rather than an honest error (R50's silent-zero on the
          // serving surface, and exactly the R709 shape). It also defeated most of the point of
          // sharding: an incident confined to one database still took the front-page number down.
          //
          // Settled per database instead. A partial count from the database that IS answering is
          // strictly better than failing outright, and `catalog_partial` says so rather than
          // letting a quietly-low number pass as complete.
          const [catP, catS] = await Promise.allSettled([
            env.CATALOG.prepare("SELECT COUNT(*) AS c FROM series").first<{ c: number }>(),
            env.CATALOG_CLIMATE.prepare("SELECT COUNT(*) AS c FROM series").first<{ c: number }>(),
          ]);
          const okP = catP.status === "fulfilled" ? (catP.value?.c ?? null) : null;
          const okS = catS.status === "fulfilled" ? (catS.value?.c ?? null) : null;
          if (okP === null && okS === null) {
            // Neither answered: say so explicitly. A 503 with a detail is honest; a 500 that the
            // client turns into `0` is not.
            return json({
              error: "catalog_unavailable",
              detail: "Both catalogue databases failed to answer a count. This is a temporary " +
                "backend condition, NOT a statement that the catalogue is empty.",
            }, 503);
          }
          catTotal = (okP ?? 0) + (okS ?? 0);
          if (okP === null || okS === null) catPartial = true;
        }
        const cat = { c: catTotal };
        const obj = await env.SERIES_BUCKET.get("_aqueduct/stats.json");
        if (obj === null) {
          return json({
            error: "stats_unavailable",
            detail: "_aqueduct/stats.json is absent from the store — re-run the " +
              "series census and upload its results. Refusing to serve stale " +
              "compiled-in numbers.",
            catalog_entries: cat?.c ?? null,
          }, 503);
        }
        const measured = await obj.json() as Record<string, unknown>;

        // HEADLINE TOTALS ARE UNDER RECALCULATION — Ahmed's instruction, 2026-08-30: publish
        // the flag now, and show it beside the numbers on the home page, rather than let a
        // reader assume a figure is settled while the database is still being completed.
        //
        // The history is why this is not merely cautious. On 2026-08-11 this endpoint served
        // individual_series = 36.56B, wrong in BOTH directions at once: 32.85B of it was
        // Canadian census one-observation coordinate cells counted as "series", while the US
        // Census store — the library's largest — was missed entirely by the scanning tool
        // (R420). The numbers below are a later census (as_of in the payload), but the
        // question of what should count as a "series" is exactly what is being resolved.
        //
        // REMOVE BOTH FIELDS when the recalculation lands, in the same commit that publishes
        // the new census. A stale "being recalculated" notice is its own lie.
        const statsResp = json({
          ...measured,
          catalog_entries: cat?.c ?? null,
          // Present ONLY when one catalogue database failed to answer and the other did. Absent
          // means the total covers both. Without this a partial count is indistinguishable from a
          // complete one, which is the failure this whole branch exists to avoid.
          ...(catPartial ? { catalog_entries_partial: true } : {}),
          recalculating: true,
          recalculating_note:
            "These headline totals are being recalculated while the database is completed. " +
            "The figures shown are from the census dated in as_of and may change. " +
            "catalog_entries is maintained by the catalogue sync and is not affected.",
        });
        if (!statsCache) return statsResp;
        const statsToCache = new Response(statsResp.clone().body, statsResp);
        statsToCache.headers.set("cache-control", "public, max-age=300, s-maxage=21600");
        ctx.waitUntil(statsCache.put(statsKey, statsToCache.clone()));
        return statsToCache;
      }

      // /v1/series/{id}.csv  and  /v1/series/{id}.metadata.json
      //   {id} is the EXACT catalog series_id, URL-encoded (it contains ':').
      //   It is NOT a provider/dataset/series path split.
      const seriesPrefix = "/v1/series/";
      if (path.startsWith(seriesPrefix)) {
        const tail = path.slice(seriesPrefix.length);
        if (tail.endsWith(".metadata.json")) {
          const enc = tail.slice(0, -".metadata.json".length);
          const id = decodeURIComponent(enc);
          if (!id) return json({ error: "bad_request", detail: "empty series id" }, 400);
          // THE REDISTRIBUTION GATE BELONGS HERE TOO, and did not exist until 2026-09-17.
          //
          // Everywhere else a gated source is not merely undownloadable, it is not REACHABLE:
          // `/v1/sources` hides it (sources.ts filters on the same set), `/v1/catalog?source=`
          // answers 451 rather than an empty result (catalog.ts), and `.csv` below answers 451
          // before auth. This branch answered 200 — title, geography, dates, the licence block
          // and the producer citation, unauthenticated, for any row still in D1. Verified live
          // on a PUBLISHED carve-out rather than a protected id, so the probe names nothing:
          //
          //     /v1/series/worldbank:FP.CPI.TOTL.ZG:AGO.metadata.json -> 200
          //     /v1/series/worldbank:FP.CPI.TOTL.ZG:AGO.csv           -> 451
          //
          // It matters more than a listing inconsistency: rows for gated sources still sit in
          // D1 because the catalogue sync is frozen, so this was the one live path serving
          // them. Closing it covers the whole class and keeps covering it for any row that
          // lands in D1 later, which deleting today's rows would not.
          if (isGated(id)) {
            // `series_id` echoes the id the CALLER sent - nothing it did not already hold - so a client refused on
            // several ids at once can tell which one this answer is about (DeepSeek review, 2026-09-17).
            return json({ error: "not_redistributable", series_id: id, detail: "This source's licence does not permit third-party redistribution. Please obtain it directly from the original provider." }, 451);
          }
          const { lang, error } = reqLang(url);
          if (error) return error; // unsupported ?lang= -> honest 400
          return await handleMetadata(id, env, lang);
        }
        if (tail.endsWith(".csv")) {
          const enc = tail.slice(0, -".csv".length);
          const id = decodeURIComponent(enc);
          if (!id) return json({ error: "bad_request", detail: "empty series id" }, 400);
          // Redistribution gate (denylist.ts): some sources' licences forbid
          // third-party re-hosting, and some individual series are third-party
          // carve-outs of an otherwise-served source. Hard-block the DATA with 451.
          if (isGated(id)) {
            return json({ error: "not_redistributable", series_id: id, detail: "This source's licence does not permit third-party redistribution of the data. Please obtain it directly from the original provider." }, 451);
          }
          // The origin serves what the EDGE already authorised and will log: no auth and no
          // download log here (the edge counts length-less answers via x-econ-count).
          if (local) return await handleSeriesCsv(id, url, env, ctx, async () => {});
          // Shared-login gate (auth.ts): data downloads need the free family
          // key (hf keys work as-is); catalog/metadata/freshness stay open.
          const auth = await requireDownloadAuth(request, env);
          if (auth instanceof Response) return auth;
          // Streamed responses (csvStream.ts) report the bytes actually WRITTEN to the client
          // when the transfer ends (via ctx.waitUntil) - never the at-rest size and never the
          // bytes produced ahead of the client (R582/R585). The string path still logs its
          // exact UTF-8 length up front.
          const userId = auth.user.id;
          // An aborted transfer still moved bytes (R593: 2.455 GB of egress left no row): log
          // what was written whether or not the transfer completed. Completeness is visible to
          // the client through the in-band `# econdl-complete` line, not through this log.
          const onDone = async (bytes: number, _ok: boolean) => {
            if (bytes > 0) await logDownload(env, userId, id, request, bytes);
          };
          const resp = await handleSeriesCsv(id, url, env, ctx, onDone);
          // String path and gzip passthrough declare content-length (exact wire bytes); the
          // inflate shape declares none and reports delivered bytes through onDone instead.
          if (resp.status === 200 && resp.headers.has("content-length")) {
            const bytes = Number(resp.headers.get("content-length")) || 0;
            await logDownload(env, userId, id, request, bytes);
          }
          return resp;
        }
        return json(
          { error: "not_found", detail: "use /v1/series/{id}.csv or /v1/series/{id}.metadata.json" },
          404,
        );
      }

      if (path === "/" || path === "/v1" || path === "/v1/") {
        return json({
          name: "Econ Data Library API",
          version: "v1",
          endpoints: [
            "/v1/catalog", "/v1/sources", "/v1/last-updates", "/v1/stats",
            "/v1/public-stats",
            "/v1/series/{id}.csv", "/v1/series/{id}.metadata.json", "/v1/bundle",
          ],
          contract: "api/CONTRACT.md",
        });
      }

      return json({ error: "not_found", detail: `no route for ${path}` }, 404);
    } catch (err) {
      // Never leak a stack as a 200. Honest 500 with a machine code.
      const detail = err instanceof Error ? err.message : "unknown error";
      return json({ error: "internal_error", detail }, 500);
    }
}

// The gate's two messages, exactly as the non-forwarding routes above word them.
const NOT_REDISTRIBUTABLE_DATA = "This source's licence does not permit third-party redistribution of the data. " +
  "Please obtain it directly from the original provider.";
const NOT_REDISTRIBUTABLE_META = "This source's licence does not permit third-party redistribution. " +
  "Please obtain it directly from the original provider.";

/** The forwarding edge's answer for every route the origin serves (src/edge.ts). Only known routes are
 *  forwarded; the licence gate and download auth run HERE, before anything is forwarded; the download log
 *  is written here - from content-length when the answer has one, otherwise from the bytes the client
 *  actually takes (whatever the origin's own marker says: a hop can drop content-length, R1169 M2). */
async function forwardRoute(request: Request, env: Env, ctx: ExecutionContext, path: string): Promise<Response> {
  if (!isForwardable(path)) return json({ error: "not_found", detail: `no route for ${path}` }, 404);
  if (!env.ORIGIN_URL || !env.ORIGIN_SECRET) return notConfigured();
  const seriesPrefix = "/v1/series/";
  if (path.startsWith(seriesPrefix) && (path.endsWith(".csv") || path.endsWith(".metadata.json"))) {
    const csv = path.endsWith(".csv");
    const enc = path.slice(seriesPrefix.length, -(csv ? ".csv" : ".metadata.json").length);
    const id = decodeURIComponent(enc);
    if (!id) return json({ error: "bad_request", detail: "empty series id" }, 400);
    if (isGated(id)) {
      return json({ error: "not_redistributable", series_id: id,
                    detail: csv ? NOT_REDISTRIBUTABLE_DATA : NOT_REDISTRIBUTABLE_META }, 451);
    }
    if (csv) {
      const auth = await requireDownloadAuth(request, env);
      if (auth instanceof Response) return auth;
      const oreq = originRequest(request, env);
      if (!oreq) return refusedPath();
      const oresp = await fetchOrigin(oreq, env);
      if (oresp.status !== 200 || !oresp.body) return clientResponse(oresp, oresp.body);
      const userId = auth.user.id;
      if (oresp.headers.has("content-length")) {
        await logDownload(env, userId, id, request, Number(oresp.headers.get("content-length")) || 0);
        return clientResponse(oresp, oresp.body);               // body never read: gzip stays gzip
      }
      const body = countingBody(oresp.body, async (bytes) => {
        if (bytes > 0) await logDownload(env, userId, id, request, bytes);
      }, ctx);
      return clientResponse(oresp, body);
    }
  }
  const ttl = cacheSeconds(path);
  const key = cacheKey(request);
  if (ttl > 0) {
    const hit = await caches.default.match(key);
    if (hit) return hit;
  }
  const oreq = originRequest(request, env);
  if (!oreq) return refusedPath();
  const oresp = await fetchOrigin(oreq, env);
  const out = clientResponse(oresp, oresp.body);
  if (ttl > 0 && oresp.status === 200) {
    const toCache = new Response(out.body, out);
    toCache.headers.set("cache-control", `public, max-age=300, s-maxage=${ttl}`);
    ctx.waitUntil(caches.default.put(key, toCache.clone()));
    return toCache;
  }
  return out;
}

/** Source names for /v1/public-stats from the origin's /v1/sources, so the route needs no econ D1 once
 *  the catalogue lives on the workstation. Each good answer is also kept for 30 days under an edge-only
 *  key; when the origin is down the last good names are used, and with none at all the route still
 *  answers - with no top-sources list (the whitelist then admits nothing), never a 500 (R1169 M4). */
async function originSourceNames(request: Request, env: Env, ctx: ExecutionContext): Promise<Record<string, string>> {
  const u = new URL(request.url);
  const staleKey = new Request(new URL("/__edge/source-names", u).toString(), { method: "GET" });
  try {
    const resp = await forwardRoute(new Request(new URL("/v1/sources", u).toString(), { method: "GET" }),
                                    env, ctx, "/v1/sources");
    if (resp.status !== 200) throw new Error(`origin /v1/sources answered ${resp.status}`);
    const body = await resp.json() as { sources?: { source?: string; name?: string | null }[] };
    const out: Record<string, string> = {};
    for (const s of body.sources ?? []) if (s.source) out[s.source] = s.name || s.source;
    ctx.waitUntil(caches.default.put(staleKey, new Response(JSON.stringify(out), {
      headers: { "content-type": "application/json", "cache-control": "public, s-maxage=2592000" },
    })));
    return out;
  } catch (e) {
    console.log("public-stats: origin source names unavailable, using the last good copy:", String(e));
    const stale = await caches.default.match(staleKey);
    return stale ? await stale.json() as Record<string, string> : {};
  }
}
