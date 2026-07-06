// ---------------------------------------------------------------------------
// src/turnstile.ts — Cloudflare Turnstile verification + short-lived anon pass.
//
// Anonymous visitors solve Turnstile ONCE; on success we mint an HMAC-signed
// "anon pass" (bound to their IP, ~30 min TTL) that the widget replays on later
// messages via the X-Anon-Pass header — so the bot check gates session start,
// not every message. Registered users skip Turnstile entirely.
//
// Mirrors the verified helper in hfdatalibrary/api/src/index.js: POST the
// token to siteverify, trust only { success: true }. Fail-closed on network
// error; fail-open only when no secret is configured (local/dev).
// ---------------------------------------------------------------------------

import type { Env } from "./types";

const SITEVERIFY = "https://challenges.cloudflare.com/turnstile/v0/siteverify";
const PASS_TTL_S = 30 * 60;

export async function verifyTurnstile(env: Env, token: string, ip: string): Promise<boolean> {
  if (!env.TURNSTILE_SECRET) return true; // not configured -> skip (dev only)
  if (!token) return false;
  try {
    const r = await fetch(SITEVERIFY, {
      method: "POST",
      headers: { "Content-Type": "application/x-www-form-urlencoded" },
      body:
        `secret=${encodeURIComponent(env.TURNSTILE_SECRET)}` +
        `&response=${encodeURIComponent(token)}` +
        `&remoteip=${encodeURIComponent(ip)}`,
    });
    const data = (await r.json()) as { success?: boolean };
    return data.success === true;
  } catch {
    return false;
  }
}

// --- HMAC anon pass ---------------------------------------------------------

function b64url(bytes: ArrayBuffer): string {
  const b = String.fromCharCode(...new Uint8Array(bytes));
  return btoa(b).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

async function hmac(secret: string, msg: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw", new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" }, false, ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(msg));
  return b64url(sig);
}

export async function issueAnonPass(env: Env, ip: string): Promise<string> {
  const secret = env.ANON_PASS_SECRET || env.TURNSTILE_SECRET || "dev-anon-secret";
  const exp = Math.floor(Date.now() / 1000) + PASS_TTL_S;
  const payload = `${ip}.${exp}`;
  const sig = await hmac(secret, payload);
  return `${exp}.${sig}`;
}

export async function verifyAnonPass(env: Env, pass: string, ip: string): Promise<boolean> {
  if (!pass) return false;
  const dot = pass.indexOf(".");
  if (dot < 0) return false;
  const expStr = pass.slice(0, dot);
  const sig = pass.slice(dot + 1);
  const exp = Number(expStr);
  if (!Number.isFinite(exp) || exp < Math.floor(Date.now() / 1000)) return false;
  const secret = env.ANON_PASS_SECRET || env.TURNSTILE_SECRET || "dev-anon-secret";
  const expect = await hmac(secret, `${ip}.${exp}`);
  // constant-time-ish compare
  if (expect.length !== sig.length) return false;
  let diff = 0;
  for (let i = 0; i < expect.length; i++) diff |= expect.charCodeAt(i) ^ sig.charCodeAt(i);
  return diff === 0;
}
