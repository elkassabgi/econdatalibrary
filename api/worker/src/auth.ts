// ---------------------------------------------------------------------------
// src/auth.ts — shared-login download gate (the Data Library family account).
//
// Owner directive + PLAN.md §6 ("API keys + rate limit, echoing hf's"):
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
//     `econ:download` namespace — 100/min per user, echoing hf's ENFORCED
//     download rule (api:download, max 100/60s — the canonical family limit);
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
const LIMIT_MAX = 100;      // canonical family limit: 100 downloads/min per account
const LIMIT_MAX_VIP = 500;  // VIP: 5x, matching hf's 'api:download-vip' (bounded, unadvertised)
const LIMIT_WINDOW_S = 60;

export interface AuthedUser {
  id: number;
  email: string | null;
}

interface UserRow { id: number; email: string | null; is_vip: number | null; }
interface RateRow { count: number; window_start: string; }

function extractKey(request: Request): string | null {
  const h = request.headers.get("X-API-Key");
  if (h) return h.trim();
  const q = new URL(request.url).searchParams.get("api_key");
  return q ? q.trim() : null;
}

// ── M2c: family-token (edl_at) validation — ADDITIVE. The api_key path below is
// unchanged. A family_access edl_at (Bearer, stored HASHED as sessions.id by the
// IdP at accounts.elkassabgidata.com) authorizes downloads for the SAME shared
// account. env.USERS is the shared hfdatalibrary-db, so `sessions`/`sso_clients`
// are readable directly. Reduced scope: bound to the request Origin (audience),
// which must be an ACTIVE registered client. Mirrors hf validateFamilyToken.
async function sha256Hex(raw: string): Promise<string> {
  const buf = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(raw));
  return [...new Uint8Array(buf)].map((b) => b.toString(16).padStart(2, "0")).join("");
}
function extractBearer(request: Request): string | null {
  const a = request.headers.get("Authorization") || "";
  return a.startsWith("Bearer ") ? a.slice(7).trim() : null;
}
async function validateFamilyToken(request: Request, env: Env): Promise<UserRow | null> {
  const raw = extractBearer(request);
  if (!raw) return null;
  const idHash = await sha256Hex(raw);
  const row = await env.USERS.prepare(
    "SELECT u.id, u.email, u.is_vip, u.is_active, s.audience AS audience " +
    "FROM sessions s JOIN users u ON s.user_id = u.id " +
    "WHERE s.id = ? AND s.kind = 'family_access' AND s.expires_at > datetime('now')",
  ).bind(idHash).first<{ id: number; email: string | null; is_vip: number | null; is_active: number | null; audience: string | null }>();
  if (!row || !row.is_active) return null;
  const origin = request.headers.get("Origin") || "";
  if (!row.audience || row.audience !== origin) return null;
  const client = await env.USERS.prepare(
    "SELECT status FROM sso_clients WHERE origin = ?",
  ).bind(origin).first<{ status: string }>();
  if (!client || client.status !== "active") return null;
  return { id: row.id, email: row.email, is_vip: row.is_vip };
}

// Shared fixed-window download limit (econ:download namespace, keyed on user.id).
// Used by BOTH the family-token and api_key paths — extracted verbatim from the
// original api_key tail so behaviour is byte-identical.
async function applyDownloadLimit(user: UserRow, env: Env): Promise<{ user: AuthedUser } | Response> {
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
    return { user: { id: user.id, email: user.email } };
  }
  const limitMax = user.is_vip ? LIMIT_MAX_VIP : LIMIT_MAX;
  if (row.count >= limitMax) {
    return new Response(JSON.stringify({
      error: "rate_limited",
      detail: `Limit is ${limitMax} downloads per minute per account (same as ` +
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
  return { user: { id: user.id, email: user.email } };
}

/** Gate a data download. Returns the authed user, or a ready 401/429 Response.
 *  Failure messages are specific and actionable (missing vs invalid/expired),
 *  matching hf's explainAuthFailure philosophy — a programmatic user must know
 *  whether to add, fix, or regenerate a key, never guess at a bare 401. */
export async function requireDownloadAuth(
  request: Request, env: Env,
): Promise<{ user: AuthedUser } | Response> {
  // M2c: a family token (edl_at) authorizes the SAME shared account. Additive —
  // tried first; the api_key path below is byte-for-byte unchanged.
  const fam = await validateFamilyToken(request, env);
  if (fam) return applyDownloadLimit(fam, env);

  const key = extractKey(request);
  if (!key) {
    return json({
      error: "auth_required",
      detail:
        "Data downloads use the free ElkassabgiData account — ONE login for every " +
        "Elkassabgi data library (hfdatalibrary.com, econdatalibrary.com, and any " +
        "future database). If you already registered on either site, your existing " +
        "API key works here as-is. Pass it as an `X-API-Key` " +
        `header or `.concat("`?api_key=` query parameter. Get a free key at ", ACCOUNT_URL),
    }, 401);
  }

  const user = await env.USERS.prepare(
    'SELECT id, email, is_vip FROM users WHERE api_key = ? AND is_active = 1 ' +
    'AND (api_key_expires_at IS NULL OR api_key_expires_at > datetime("now"))',
  ).bind(key).first<UserRow>();
  if (!user) {
    return json({
      error: "invalid_key",
      detail:
        "This API key is unknown, deactivated, or expired. Keys are shared across " +
        `all ElkassabgiData libraries — check or regenerate yours at ${ACCOUNT_URL}.`,
    }, 401);
  }

  // Shared fixed-window limit (same helper the family-token path uses).
  return applyDownloadLimit(user, env);
}

/** Record a served download in econ's OWN log table (shared db, separate
 *  counters). Never throws — logging must not break a download. */
export async function logDownload(
  env: Env, userId: number, seriesId: string, request: Request, bytes = 0,
): Promise<void> {
  try {
    const ip = request.headers.get("cf-connecting-ip") || "";
    // Channel attribution (mirrors hf's downloadChannel): mcp relays carry the
    // X-Elkassabgi-Client header / elkassabgidata-mcp UA or a ?via=mcp link tag;
    // our own sites' browsers send a family Referer; everything else is 'api'.
    const client = (request.headers.get("x-elkassabgi-client") || "").toLowerCase();
    const ua = (request.headers.get("user-agent") || "").toLowerCase();
    const via = new URL(request.url).searchParams.get("via");
    const ref = request.headers.get("referer") || "";
    const channel =
      client === "mcp" || ua.includes("elkassabgidata-mcp") || via === "mcp" ? "mcp" :
      (ref.includes("econdatalibrary.com") || ref.includes("elkassabgidata.com") || ref.includes("hfdatalibrary.com")) ? "web" : "api";
    await env.USERS.prepare(
      "INSERT INTO econ_download_log (user_id, series_id, ip, channel, bytes) VALUES (?, ?, ?, ?, ?)",
    ).bind(userId, seriesId, ip, channel, bytes).run();
  } catch (e) {
    console.log("econ_download_log insert failed:", String(e));
  }
}
