// ---------------------------------------------------------------------------
// src/llm.ts — model adapter (DeepSeek, OpenAI-compatible) + a MOCK adapter.
//
// The Worker is model-agnostic behind llmComplete(). When DEEPSEEK_API_KEY is
// absent (config.mock), a deterministic scripted adapter drives the same tool
// loop so the entire flow + UI can be demoed locally with no key and no spend.
// ---------------------------------------------------------------------------

import type { ChatMessage, Config, Env, ToolCall, Usage } from "./types";

const DEEPSEEK_URL = "https://api.deepseek.com/chat/completions";

// DeepSeek v4-flash published prices (USD per token). Verify before launch.
const PRICE_IN_MISS = 0.14 / 1_000_000;
const PRICE_IN_HIT = 0.0028 / 1_000_000;
const PRICE_OUT = 0.28 / 1_000_000;

export interface LlmResult {
  message: ChatMessage;
  usage: Usage;
}

export function costUsd(u: Usage): number {
  const hit = u.prompt_cache_hit_tokens ?? 0;
  const miss = u.prompt_cache_miss_tokens ?? Math.max(0, (u.prompt_tokens ?? 0) - hit);
  const out = u.completion_tokens ?? 0;
  return hit * PRICE_IN_HIT + miss * PRICE_IN_MISS + out * PRICE_OUT;
}

export async function llmComplete(
  env: Env,
  config: Config,
  messages: ChatMessage[],
  tools: readonly unknown[],
): Promise<LlmResult> {
  if (config.mock) return mockComplete(messages);

  const body = {
    model: config.model,
    messages,
    tools,
    tool_choice: "auto",
    temperature: 0.2,
    max_tokens: config.maxOutputTokens,
    stream: false,
  };
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), 60_000);
  let r: Response;
  try {
    r = await fetch(DEEPSEEK_URL, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${env.DEEPSEEK_API_KEY}`,
      },
      body: JSON.stringify(body),
      signal: ctl.signal,
    });
  } finally {
    clearTimeout(t);
  }
  if (!r.ok) {
    const detail = await r.text().catch(() => "");
    throw new Error(`LLM HTTP ${r.status}: ${detail.slice(0, 300)}`);
  }
  const d = (await r.json()) as {
    choices?: { message?: ChatMessage }[];
    usage?: Usage;
  };
  const message = d.choices?.[0]?.message;
  if (!message) throw new Error("LLM returned no message");
  return { message, usage: d.usage ?? {} };
}

// --- MOCK adapter (no key): search -> prepare_download -> grounded answer ---
// A deterministic 2-step script that exercises the real tool loop (real catalog
// search, the download gate + register CTA) so the whole flow + UI demos with
// no key/spend. A real model does far better; this only needs to be honest.

const STOP = new Set([
  "how", "many", "much", "the", "a", "an", "of", "in", "on", "for", "to", "is", "are", "was", "were",
  "do", "does", "did", "what", "which", "me", "i", "my", "we", "need", "want", "know", "get", "show",
  "give", "find", "below", "under", "line", "lines", "level", "levels", "living", "live", "lives",
  "people", "persons", "individuals", "number", "amount", "rate", "going", "into", "each", "per",
  "year", "annual", "annually", "and", "or", "that", "this", "there", "their", "country", "data",
  "series", "please", "s",
]);

function keywords(text: string): string {
  const kw = text.toLowerCase().replace(/[^a-z0-9\s]/g, " ").split(/\s+/)
    .filter((w) => w.length > 1 && !STOP.has(w)).slice(0, 4).join(" ");
  return kw || text.slice(0, 60);
}

function mockCall(name: string, args: unknown): ToolCall {
  return { id: `mock_${name}`, type: "function", function: { name, arguments: JSON.stringify(args) } };
}

function mockComplete(messages: ChatMessage[]): LlmResult {
  const last = messages[messages.length - 1];

  if (last?.role === "tool") {
    // Step 2: after a search, prepare a download of the top hit (exercises the gate).
    if (last.name === "search_series") {
      try {
        const results: any[] = JSON.parse(last.content ?? "{}").results ?? [];
        if (results.length) {
          return { message: { role: "assistant", content: null, tool_calls: [mockCall("prepare_download", { series_id: results[0].series_id })] }, usage: {} };
        }
      } catch { /* fall through */ }
      return { message: { role: "assistant", content: "I searched the catalog but didn't find a clear match — try naming the country, indicator, and units. _(demo mode: no DeepSeek key configured.)_" }, usage: {} };
    }
    // Step 3: after prepare_download, give a grounded answer from the real search hits.
    const searchMsg = [...messages].reverse().find((m) => m.role === "tool" && m.name === "search_series");
    let lines = "";
    try {
      const results: any[] = JSON.parse(searchMsg?.content ?? "{}").results ?? [];
      lines = results.slice(0, 3).map((x) => `• ${x.title}  —  \`${x.series_id}\``).join("\n");
    } catch { /* ignore */ }
    const gated = (last.content ?? "").startsWith("gate:");
    const tail = gated
      ? "The data is available to download — you'll just need a free account (instant, no cost)."
      : "I've prepared your download — click the button below.";
    return { message: { role: "assistant", content: `Here's what I found in the Econ Data Library:\n\n${lines}\n\n${tail}\n\n_(demo mode: connect a DeepSeek key for full reasoning, disambiguation, and citations.)_` }, usage: {} };
  }

  // Step 1: search the catalog with keywords pulled from the latest question.
  const lastUser = [...messages].reverse().find((m) => m.role === "user");
  return { message: { role: "assistant", content: null, tool_calls: [mockCall("search_series", { query: keywords(lastUser?.content ?? "data"), limit: 8 })] }, usage: {} };
}
