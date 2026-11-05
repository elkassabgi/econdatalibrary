// ---------------------------------------------------------------------------
// src/types.ts — Env bindings + shared shapes for elkassabgidata-assistant.
//
// The assistant is a PROXY over the existing free endpoints (like the MCP): it
// binds no D1/R2 for data. Its only stateful piece is one Durable Object that
// holds rate-limit windows + the monthly spend ledger (atomic counters). All
// data access is HTTP to the already-live workers.
// ---------------------------------------------------------------------------

import { priceCardFor } from "./pricing.ts";

export interface Env {
  // Durable Object holding rate-limit windows + monthly budget ledger.
  ASSISTANT_STATE: DurableObjectNamespace;

  // --- secrets (wrangler secret put) ---------------------------------------
  // DeepSeek API key. When ABSENT, the Worker runs in MOCK mode (deterministic
  // scripted loop) so the whole flow + UI can be demoed with no key/spend.
  DEEPSEEK_API_KEY?: string;
  // Anthropic (Claude) API key. Read ONLY when PROVIDER="anthropic"; when that
  // provider is selected and this key is ABSENT, the Worker runs in MOCK mode.
  ANTHROPIC_API_KEY?: string;
  // Cloudflare Turnstile secret. When ABSENT, Turnstile verification is skipped
  // (dev/local only) — set it in production so anonymous chat is bot-gated.
  TURNSTILE_SECRET?: string;
  // HMAC secret used to sign short-lived "anon passes" so a visitor solves
  // Turnstile once per session, not every message. Any random string.
  ANON_PASS_SECRET?: string;
  // Turnstile SITEKEY (NON-secret) — kept in Worker vars alongside the secret
  // so the widget reads it from /health and can never desync into a lockout.
  TURNSTILE_SITEKEY?: string;

  // --- non-secret vars (wrangler.jsonc "vars", all optional w/ safe defaults)
  PROVIDER?: string;                 // "deepseek" (default) | "anthropic"
  MODEL?: string;                    // DeepSeek model, default "deepseek-v4-flash"
  ANTHROPIC_MODEL?: string;          // Claude model, default "claude-haiku-5-5"
  // Claude counts its (adaptive) thinking toward max_tokens, so it gets its own
  // cap; MAX_OUTPUT_TOKENS stays the DeepSeek cap. Default "2000".
  ANTHROPIC_MAX_OUTPUT_TOKENS?: string;
  MONTHLY_USD_CAP?: string;          // default "30"
  ANON_PER_HOUR?: string;            // default "5"
  ANON_PER_DAY?: string;             // default "15"
  USER_PER_HOUR?: string;            // default "20"
  USER_PER_DAY?: string;             // default "60"
  MAX_TOOL_ROUNDS?: string;          // default "6"
  MAX_OUTPUT_TOKENS?: string;        // default "800"
}

export type Provider = "deepseek" | "anthropic";

export const DEFAULT_PROVIDER: Provider = "deepseek";
export const DEFAULT_DEEPSEEK_MODEL = "deepseek-v4-flash";
export const DEFAULT_ANTHROPIC_MODEL = "claude-haiku-5-5";

// PROVIDER is matched exactly (after trim + lower-case). Anything other than
// "anthropic" — unset, empty, or a typo — resolves to the DeepSeek default, and
// /health reports the provider actually in use so a typo is visible there.
export function resolveProvider(v: string | undefined): Provider {
  return (v ?? "").trim().toLowerCase() === "anthropic" ? "anthropic" : DEFAULT_PROVIDER;
}

// Resolved runtime config (env vars parsed once, with defaults).
export interface Config {
  provider: Provider;
  model: string; // the selected provider's model
  monthlyCapUsd: number;
  anonPerHour: number;
  anonPerDay: number;
  userPerHour: number;
  userPerDay: number;
  maxToolRounds: number;
  maxOutputTokens: number; // the selected provider's per-reply cap
  // true -> scripted mock LLM, no model call. mockReason says why (shown on /health):
  //   "no_key"          the SELECTED provider's key is absent
  //   "unpriced_model"  PROVIDER=anthropic with a model that has no price card
  //                     (pricing.ts). Fails closed: the cap cannot charge it right.
  mock: boolean;
  mockReason: "no_key" | "unpriced_model" | null;
}

export function loadConfig(env: Env): Config {
  const num = (v: string | undefined, d: number) => {
    const n = v == null ? NaN : Number(v);
    return Number.isFinite(n) ? n : d;
  };
  const provider = resolveProvider(env.PROVIDER);
  const anthropic = provider === "anthropic";
  const model = anthropic
    ? (env.ANTHROPIC_MODEL || DEFAULT_ANTHROPIC_MODEL).trim()
    : env.MODEL || DEFAULT_DEEPSEEK_MODEL;
  const hasKey = anthropic ? !!env.ANTHROPIC_API_KEY : !!env.DEEPSEEK_API_KEY;
  const mockReason: Config["mockReason"] =
    anthropic && !priceCardFor(model) ? "unpriced_model" : !hasKey ? "no_key" : null;
  return {
    provider,
    model,
    monthlyCapUsd: num(env.MONTHLY_USD_CAP, 30),
    anonPerHour: num(env.ANON_PER_HOUR, 5),
    anonPerDay: num(env.ANON_PER_DAY, 15),
    userPerHour: num(env.USER_PER_HOUR, 20),
    userPerDay: num(env.USER_PER_DAY, 60),
    maxToolRounds: num(env.MAX_TOOL_ROUNDS, 6),
    maxOutputTokens: anthropic
      ? num(env.ANTHROPIC_MAX_OUTPUT_TOKENS, 2000)
      : num(env.MAX_OUTPUT_TOKENS, 800),
    mock: mockReason !== null,
    mockReason,
  };
}

// Who is asking. Resolved from the forwarded hfd_session bearer token.
export interface Visitor {
  registered: boolean;
  userId: number | null;
  email: string | null;
  name: string | null;
  // The visitor's OWN ElkassabgiData api_key, resolved server-side from their
  // session. NEVER sent to the LLM or echoed into chat — used only to build a
  // ready download URL the visitor's own browser/tools can use.
  apiKey: string | null;
}

// OpenAI-/DeepSeek-compatible chat message shape.
export interface ChatMessage {
  role: "system" | "user" | "assistant" | "tool";
  content: string | null;
  tool_calls?: ToolCall[];
  tool_call_id?: string;
  name?: string;
}

export interface ToolCall {
  id: string;
  type: "function";
  function: { name: string; arguments: string };
}

// Token usage returned by DeepSeek (OpenAI-compatible + cache fields).
export interface Usage {
  prompt_tokens?: number;
  completion_tokens?: number;
  prompt_cache_hit_tokens?: number;
  prompt_cache_miss_tokens?: number;
}
