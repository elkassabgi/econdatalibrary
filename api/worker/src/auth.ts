// ---------------------------------------------------------------------------
// src/auth.ts — shared-login download gate (the Data Library family account).
//
// Owner directive + PLAN.md §6 ("API keys + rate limit (echo your 300/min)"):
// hfdatalibrary's users database is THE identity provider for both libraries.
// This module validates the SAME api_keys against the shared `USERS` binding
// (hfdatalibrary-db), so every existing hf account downloads econ data with
// its current key — no separate registration, no migration.
//
// Mirrors hf's mechanics exactly (api/src/index.js):
//   * key extraction: `X-API-Key` header, fallback `?api_key=` query param
//     (same curl/browser ergonomics on both APIs);
//   * validation: is_active = 1 AND key not expired;
//   * rate limit: fixed window in the SHARED `rate_limits` table under the
//     `econ:download` namespace — 300/min per user, echoing hf's download rule;
//   * logging: `econ_download_log` — a SEPARATE table in the shared db, so hf's
//     download counters are never inflated by econ traffic.
//
// Scope: only DATA downloads are gated (/v1/series/{id}.csv). Catalog, search,
// metadata, freshness, stats and the status page stay open — browse free,
// download with the (free) family key.
// ---------------------------------------------------------------------------

import type { Env } from "./types";
import { json } from "./util";

const ACCOUNT_URL = "https://hfdatalibrary.com/pages/download";
const LIMIT_MAX = 300;      // echo hf: 300 downloads/min per account
const LIMIT_WINDOW_S = 60;

export interface AuthedUser {
  id: number;
  email: string | null;
}

interface UserRow { id: number; email: string | null; }
interface RateRow { count: number; window_start: string; }

function extractKey(request: Request): string | null {
  const h = request.headers.get("X-API-Key");
  if (h) return h.trim();
  const q = new URL(request.url).searchParams.get("api_key");
  return q ? q.trim() : null;
}

/** Gate a data download. Returns the authed user, or a ready 401/429 Response.
 *  Failure messages are specific and actionable (missing vs invalid/expired),
 *  matching hf's explainAuthFailure philosophy — a programmatic user must know
 *  whether to add, fix, or regenerate a key, never guess at a bare 401. */
export async function requireDownloadAuth(
  request: Request, env: Env,
): Promise<{ user: AuthedUser } | Response> {
  const key = extractKey(request);
  if (!key) {
    return json({
      error: "auth_required",
      detail:
        "Data downloads use the free Data Library family account — one login for " +
        "hfdatalibrary.com and econdatalibrary.com. If you already have an " +
        "hfdatalibrary API key it works here as-is. Pass it as an `X-API-Key` " +
        `header or `.concat("`?api_key=` query parameter. Get a free key at ", ACCOUNT_URL),
    }, 401);
  }

  const user = await env.USERS.prepare(
    'SELECT id, email FROM users WHERE api_key = ? AND is_active = 1 ' +
    'AND (api_key_expires_at IS NULL OR api_key_expires_at > datetime("now"))',
  ).bind(key).first<UserRow>();
  if (!user) {
    return json({
      error: "invalid_key",
      detail:
        "This API key is unknown, deactivated, or expired. Keys are shared across " +
        `the Data Library family — check or regenerate yours at ${ACCOUNT_URL}.`,
    }, 401);
  }

  // Fixed-window limit in the shared rate_limits table (hf's own mechanism,
  // own namespace so the two libraries' windows never collide).
  const fullKey = `econ:download:${user.id}`;
  const row = await env.USERS.prepare(
    "SELECT count, window_start FROM rate_limits WHERE key = ?",
  ).bind(fullKey).first<RateRow>();
  const now = Date.now();
  if (!row || now - Date.parse(row.window_start + "Z") > LIMIT_WINDOW_S * 1000) {
    await env.USERS.prepare(
      'INSERT OR REPLACE INTO rate_limits (key, count, window_start) ' +
      'VALUES (?, 1, datetime("now"))',
    ).bind(fullKey).run();
    return { user };
  }
  if (row.count >= LIMIT_MAX) {
    return new Response(JSON.stringify({
      error: "rate_limited",
      detail: `Limit is ${LIMIT_MAX} downloads per minute per account (same as ` +
        "hfdatalibrary). Slow down and retry.",
    }), {
      status: 429,
      headers: {
        "content-type": "application/json; charset=utf-8",
        "access-control-allow-origin": "*",
        "retry-after": String(LIMIT_WINDOW_S),
        "cache-control": "no-store",
      },
    });
  }
  await env.USERS.prepare(
    "UPDATE rate_limits SET count = count + 1 WHERE key = ?",
  ).bind(fullKey).run();
  return { user };
}

/** Record a served download in econ's OWN log table (shared db, separate
 *  counters). Never throws — logging must not break a download. */
export async function logDownload(
  env: Env, userId: number, seriesId: string, request: Request,
): Promise<void> {
  try {
    const ip = request.headers.get("cf-connecting-ip") || "";
    await env.USERS.prepare(
      "INSERT INTO econ_download_log (user_id, series_id, ip) VALUES (?, ?, ?)",
    ).bind(userId, seriesId, ip).run();
  } catch (e) {
    console.log("econ_download_log insert failed:", String(e));
  }
}
