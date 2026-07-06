// ---------------------------------------------------------------------------
// src/types.ts — Env bindings + shared shapes for elkassabgidata-assistant.
//
// The assistant is a PROXY over the existing free endpoints (like the MCP): it
// binds no D1/R2 for data. Its only stateful piece is one Durable Object that
// holds rate-limit windows + the monthly spend ledger (atomic counters). All
// data access is HTTP to the already-live workers.
// ---------------------------------------------------------------------------

export interface Env {
  // Durable Object holding rate-limit windows + monthly budget ledger.
  ASSISTANT_STATE: DurableObjectNamespace;

  // --- secrets (wrangler secret put) ---------------------------------------
  // DeepSeek API key. When ABSENT, the Worker runs in MOCK mode (deterministic
  // scripted loop) so the whole flow + UI can be demoed with no key/spend.
  DEEPSEEK_API_KEY?: string;
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
  MODEL?: string;                    // default "deepseek-v4-flash"
  MONTHLY_USD_CAP?: string;          // default "30"
  ANON_PER_HOUR?: string;            // default "5"
  ANON_PER_DAY?: string;             // default "15"
  USER_PER_HOUR?: string;            // default "20"
  USER_PER_DAY?: string;             // default "60"
  MAX_TOOL_ROUNDS?: string;          // default "6"
  MAX_OUTPUT_TOKENS?: string;        // default "800"
}

// Resolved runtime config (env vars parsed once, with defaults).
export interface Config {
  model: string;
  monthlyCapUsd: number;
  anonPerHour: number;
  anonPerDay: number;
  userPerHour: number;
  userPerDay: number;
  maxToolRounds: number;
  maxOutputTokens: number;
  mock: boolean; // true when no DeepSeek key -> scripted mock LLM
}

export function loadConfig(env: Env): Config {
  const num = (v: string | undefined, d: number) => {
    const n = v == null ? NaN : Number(v);
    return Number.isFinite(n) ? n : d;
  };
  return {
    model: env.MODEL || "deepseek-v4-flash",
    monthlyCapUsd: num(env.MONTHLY_USD_CAP, 30),
    anonPerHour: num(env.ANON_PER_HOUR, 5),
    anonPerDay: num(env.ANON_PER_DAY, 15),
    userPerHour: num(env.USER_PER_HOUR, 20),
    userPerDay: num(env.USER_PER_DAY, 60),
    maxToolRounds: num(env.MAX_TOOL_ROUNDS, 6),
    maxOutputTokens: num(env.MAX_OUTPUT_TOKENS, 800),
    mock: !env.DEEPSEEK_API_KEY,
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
