// ---------------------------------------------------------------------------
// src/index.ts — elkassabgidata-assistant entry point.
//
// POST /chat: resolve visitor (forward hfd_session bearer -> hf /v1/auth/me),
// bot-gate anonymous users (Turnstile once -> signed anon pass), enforce
// rate-limit + monthly-budget via the AssistantState DO, then run the tool loop
// and stream the answer over SSE. Anonymous users can search & preview; the
// download tools only ever return links, gated behind free registration.
// ---------------------------------------------------------------------------

import { loadConfig, type Env, type Visitor, type ChatMessage } from "./types";
import { runAgent } from "./agent";
import { verifyTurnstile, verifyAnonPass, issueAnonPass } from "./turnstile";

export { AssistantState } from "./state";

const HF_ME = "https://api.hfdatalibrary.com/v1/auth/me";
// Conservative upper-bound per-request cost, reserved at gate time so the
// monthly budget cap holds under concurrency (reconciled to actual at charge).
const ESTIMATE_USD = 0.02;

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const origin = request.headers.get("Origin") || "";
    const cors = corsHeaders(origin);

    if (request.method === "OPTIONS") return new Response(null, { status: 204, headers: cors });

    const url = new URL(request.url);
    if (request.method === "GET" && (url.pathname === "/" || url.pathname === "/health")) {
      const cfg = loadConfig(env);
      // Turnstile config is advertised so the widget stays in lockstep with the
      // Worker (one source of truth) — never a secret-set-but-no-sitekey lockout.
      return json({
        ok: true, service: "elkassabgidata-assistant", mock: cfg.mock, model: cfg.model,
        turnstile: { required: !!env.TURNSTILE_SECRET, sitekey: env.TURNSTILE_SITEKEY || null },
      }, 200, cors);
    }
    if (request.method === "POST" && url.pathname === "/chat") {
      return handleChat(request, env, ctx, cors);
    }
    return json({ error: "not_found" }, 404, cors);
  },
};

async function handleChat(request: Request, env: Env, ctx: ExecutionContext, cors: Record<string, string>): Promise<Response> {
  const cfg = loadConfig(env);

  // --- parse + sanitize -----------------------------------------------------
  let body: any;
  try { body = await request.json(); } catch { return json({ error: "bad_json" }, 400, cors); }
  const history = sanitizeHistory(body?.messages);
  if (!history) return json({ error: "bad_request", detail: "messages must end with a user turn" }, 400, cors);

  // --- who is asking? (forward the session bearer to hf /v1/auth/me) --------
  const visitor = await resolveVisitor(request);
  const ip = request.headers.get("cf-connecting-ip") || "0.0.0.0";

  // --- bot gate for anonymous visitors --------------------------------------
  let issuedPass: string | null = null;
  if (!visitor.registered) {
    const pass = request.headers.get("X-Anon-Pass") || "";
    const passOk = pass ? await verifyAnonPass(env, pass, ip) : false;
    if (!passOk) {
      const token = String(body?.turnstile_token || "");
      const human = await verifyTurnstile(env, token, ip);
      if (!human) return json({ error: "turnstile_required" }, 403, cors);
      issuedPass = await issueAnonPass(env, ip); // returned in the response headers
    }
  }

  // --- rate-limit + budget gate (atomic, in the DO) -------------------------
  // Registered users are keyed by their numeric id; if the id is ever missing
  // (contract drift), fall back to a stable hash of their key so they don't all
  // collapse into one shared "user:null" bucket.
  const subject = visitor.registered
    ? (visitor.userId != null ? String(visitor.userId) : "k:" + hashStr(visitor.apiKey || ""))
    : ip;
  const rlKeys = visitor.registered
    ? [
        { key: `user:h:${subject}`, max: cfg.userPerHour, windowS: 3600 },
        { key: `user:d:${subject}`, max: cfg.userPerDay, windowS: 86400 },
      ]
    : [
        { key: `anon:h:${subject}`, max: cfg.anonPerHour, windowS: 3600 },
        { key: `anon:d:${subject}`, max: cfg.anonPerDay, windowS: 86400 },
      ];
  const stateStub = env.ASSISTANT_STATE.get(env.ASSISTANT_STATE.idFromName("main"));
  const gateRes = await stateStub.fetch("https://do/gate", {
    method: "POST",
    body: JSON.stringify({ rlKeys, capUsd: cfg.monthlyCapUsd, reserveUsd: ESTIMATE_USD }),
  });
  const gate = (await gateRes.json()) as { ok: boolean; reason?: string; retryAfter?: number };
  if (!gate.ok) {
    if (gate.reason === "budget") {
      return json({ error: "at_capacity", detail: "The assistant is at capacity for now. You can still search the catalog directly or connect the MCP server." }, 503, cors);
    }
    const nudge = visitor.registered
      ? "You've hit the usage limit — please try again shortly."
      : "You've hit the free anonymous limit — sign in (free) for a higher limit, or try again shortly.";
    return json({ error: "rate_limited", detail: nudge, retryAfter: gate.retryAfter ?? 60 }, 429, {
      ...cors, "retry-after": String(gate.retryAfter ?? 60),
    });
  }

  // --- run the agent + stream the answer over SSE ---------------------------
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    async start(controller) {
      const send = (event: string, data: unknown) =>
        controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
      // Reconcile the gate's reservation exactly once (delta = actual - reserved
      // on success; 0 on error, keeping the reservation as a conservative charge).
      let settled = false;
      const settle = (delta: number) => {
        if (settled) return; settled = true;
        ctx.waitUntil(stateStub.fetch("https://do/charge", {
          method: "POST", body: JSON.stringify({ delta }),
        }).then(() => {}).catch(() => {}));
      };
      try {
        const result = await runAgent(env, cfg, visitor, history, (label) => send("status", { label }));

        // stream the final text progressively for a live feel
        const text = result.text;
        for (let i = 0; i < text.length; i += 24) send("delta", { t: text.slice(i, i + 24) });

        send("done", {
          registered: visitor.registered,
          registerNeeded: result.registerNeeded,
          offers: result.offers,
          registerUrl: "https://hfdatalibrary.com/pages/download#register",
        });
        settle(result.costUsd - ESTIMATE_USD);
      } catch (e) {
        send("error", { detail: "The assistant hit an error. Please try again." });
        console.log("assistant error:", (e as Error).message);
        settle(0);
      } finally {
        controller.close();
      }
    },
  });

  const headers: Record<string, string> = {
    ...cors,
    "content-type": "text/event-stream; charset=utf-8",
    "cache-control": "no-store",
    "connection": "keep-alive",
  };
  if (issuedPass) headers["X-Anon-Pass"] = issuedPass;
  return new Response(stream, { headers });
}

// --- helpers ----------------------------------------------------------------

function sanitizeHistory(raw: unknown): ChatMessage[] | null {
  if (!Array.isArray(raw)) return null;
  const msgs = raw
    .filter((m: any) => m && (m.role === "user" || m.role === "assistant") && typeof m.content === "string")
    .map((m: any) => ({ role: m.role as "user" | "assistant", content: String(m.content).slice(0, 4000) }))
    .filter((m) => m.content.trim().length > 0)
    .slice(-20);
  if (!msgs.length || msgs[msgs.length - 1].role !== "user") return null;
  return msgs;
}

async function resolveVisitor(request: Request): Promise<Visitor> {
  const auth = request.headers.get("Authorization") || "";
  const token = auth.startsWith("Bearer ") ? auth.slice(7).trim() : "";
  const anon: Visitor = { registered: false, userId: null, email: null, name: null, apiKey: null };
  if (!token) return anon;
  try {
    const r = await fetch(HF_ME, { headers: { Authorization: `Bearer ${token}` } });
    if (!r.ok) return anon;
    const u = (await r.json()) as any;
    if (!u || !u.api_key) return anon;
    return {
      registered: true,
      userId: (u.id ?? u.user_id ?? null) as number | null,
      email: u.email ?? null,
      name: u.name ?? null,
      apiKey: u.api_key,
    };
  } catch {
    return anon;
  }
}

const ALLOWED_ORIGIN = [
  /^https?:\/\/localhost(:\d+)?$/,
  /^https?:\/\/127\.0\.0\.1(:\d+)?$/,
  /^https:\/\/(www\.)?hfdatalibrary\.com$/,
  /^https:\/\/(www\.)?econdatalibrary\.com$/,
  /^https:\/\/(www\.)?elkassabgidata\.com$/,
  // ONLY our own Pages projects' preview subdomains — not any *.pages.dev
  // (those are free/self-service, so a wildcard would trust attacker pages).
  /^https:\/\/([a-z0-9-]+\.)?(hfdatalibrary|econdatalibrary|econfindatalibrary|elkassabgidata)\.pages\.dev$/,
];

// Cheap, stable non-crypto hash (FNV-1a) for a rate-limit bucket key. Not a
// secret — only used to avoid keying on a raw api_key or colliding on null ids.
function hashStr(s: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193); }
  return (h >>> 0).toString(16);
}

function corsHeaders(origin: string): Record<string, string> {
  const ok = origin && ALLOWED_ORIGIN.some((re) => re.test(origin));
  return {
    "access-control-allow-origin": ok ? origin : "*",
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "Content-Type, Authorization, X-Anon-Pass",
    "access-control-expose-headers": "X-Anon-Pass",
    "access-control-max-age": "86400",
    "vary": "Origin",
  };
}

function json(o: unknown, status: number, cors: Record<string, string>): Response {
  return new Response(JSON.stringify(o), {
    status,
    headers: { ...cors, "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });
}
