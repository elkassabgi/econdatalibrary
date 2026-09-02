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

      // /v1/catalog is edge-cached (2026-08-15 cost incident): a crawler paging
      // one source drove 130B D1 rows read in a day. The catalog changes only at
      // sync time, so a 6h same-URL cache makes re-crawls free without staleness
      // anyone can observe. Only 200s are cached; the cap-400s and errors are not.
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
      // parquet row counts. catalog_entries is counted live from D1.
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
        const statsCache = caches.default;
        const statsKey = new Request(url.toString(), { method: "GET" });
        const statsHit = await statsCache.match(statsKey);
        if (statsHit) return statsHit;
        const SUM_COUNTS = "SELECT SUM(n) AS c FROM source_counts";
        let catTotal: number | null = null;
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
          const [catP, catS] = await Promise.all([
            env.CATALOG.prepare("SELECT COUNT(*) AS c FROM series").first<{ c: number }>(),
            env.CATALOG_CLIMATE.prepare("SELECT COUNT(*) AS c FROM series").first<{ c: number }>(),
          ]);
          catTotal = (catP?.c ?? 0) + (catS?.c ?? 0);
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
          recalculating: true,
          recalculating_note:
            "These headline totals are being recalculated while the database is completed. " +
            "The figures shown are from the census dated in as_of and may change. " +
            "catalog_entries is counted live and is not affected.",
        });
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
            return json({ error: "not_redistributable", detail: "This source's licence does not permit third-party redistribution of the data. Please obtain it directly from the original provider." }, 451);
          }
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
  },
} satisfies ExportedHandler<Env>;
