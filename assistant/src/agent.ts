// ---------------------------------------------------------------------------
// src/agent.ts — the tool-calling loop.
//
// Runs the model with the read-only tools until it produces a final answer (no
// tool call) or the round budget is hit. Grounding is structural: the only way
// to surface data is via tools, and the download tools return links, not rows.
// Emits status lines during tool rounds; returns the final text + any download
// offers + accumulated cost.
// ---------------------------------------------------------------------------

import { llmComplete, costUsd } from "./llm.ts";
import { TOOL_SCHEMAS, executeTool, type ToolCtx, type DownloadOffer } from "./tools.ts";
import { systemPrompt } from "./prompt.ts";
import { makeAnthropicClient, requirePriceCard, runAnthropicLoop, type MessagesClient } from "./anthropic.ts";
import { DEEPSEEK_RESERVE_USD } from "./pricing.ts";
import type { ChatMessage, Config, Env, Visitor } from "./types.ts";

export interface AgentResult {
  text: string;
  offers: DownloadOffer[];
  registerNeeded: boolean;
  costUsd: number;
}

// Per-request reserve held at gate time (index.ts), from the same price table
// that charges the reply. Claude: the model's card (a model without a card is
// mock and never called). DeepSeek and the mock: the existing reserve.
export function reserveUsdFor(config: Config): number {
  if (config.provider === "anthropic" && !config.mock) return requirePriceCard(config.model).reserveUsd;
  return DEEPSEEK_RESERVE_USD;
}

// Injection point for tests: build the Claude client from the key. The default
// is the official SDK client; tests pass a fake (no network, no key).
export interface AgentDeps {
  anthropicClient?: (apiKey: string) => MessagesClient;
}

export async function runAgent(
  env: Env,
  config: Config,
  visitor: Visitor,
  history: ChatMessage[],
  onStatus: (label: string) => void,
  deps: AgentDeps = {},
): Promise<AgentResult> {
  const system = systemPrompt(visitor.registered, visitor.name);
  const ctx: ToolCtx = { visitor, offers: [], register: { needed: false } };

  // Claude only when it is selected AND its key is present; with the key absent
  // config.mock is true and the keyless mock below runs instead.
  if (config.provider === "anthropic" && !config.mock) {
    const client = (deps.anthropicClient ?? makeAnthropicClient)(env.ANTHROPIC_API_KEY as string);
    const r = await runAnthropicLoop(client, config, system, history, ctx, (name) => onStatus(statusFor(name)));
    return { text: r.text, offers: ctx.offers, registerNeeded: ctx.register.needed, costUsd: r.costUsd };
  }

  // DeepSeek (default) and the keyless mock: the OpenAI-compatible loop.
  const messages: ChatMessage[] = [
    { role: "system", content: system },
    ...history,
  ];
  let cost = 0;

  for (let round = 0; round < config.maxToolRounds; round++) {
    const { message, usage } = await llmComplete(env, config, messages, TOOL_SCHEMAS);
    cost += costUsd(usage);

    const calls = message.tool_calls ?? [];
    if (calls.length === 0) {
      return {
        text: (message.content ?? "").trim() || "I'm not sure how to answer that yet — try rephrasing.",
        offers: ctx.offers,
        registerNeeded: ctx.register.needed,
        costUsd: cost,
      };
    }

    // Record the assistant's tool-call turn, then execute each call in order.
    messages.push({ role: "assistant", content: message.content ?? null, tool_calls: calls });
    for (const call of calls) {
      const name = call.function?.name ?? "unknown";
      onStatus(statusFor(name));
      let args: Record<string, unknown> = {};
      let parseOk = true;
      try { args = JSON.parse(call.function?.arguments || "{}"); } catch { parseOk = false; }
      const result = parseOk
        ? await executeTool(name, args, ctx)
        : `error: could not parse the arguments for "${name}" as JSON — resend valid JSON.`;
      messages.push({ role: "tool", tool_call_id: call.id, name, content: result });
    }
  }

  // Ran out of rounds — ask the model for a final answer with tools disabled.
  const { message, usage } = await llmComplete(env, config, [
    ...messages,
    { role: "user", content: "Give your best final answer now using what you've gathered. Do not call more tools." },
  ], []);
  cost += costUsd(usage);
  return {
    text: (message.content ?? "").trim() || "I gathered some results but ran long — please narrow the question.",
    offers: ctx.offers,
    registerNeeded: ctx.register.needed,
    costUsd: cost,
  };
}

function statusFor(tool: string): string {
  switch (tool) {
    case "search_series": return "Searching the catalog…";
    case "series_details": return "Reading the series metadata…";
    case "data_freshness": return "Checking data freshness…";
    case "prepare_download": return "Preparing your download…";
    case "hf_download_link": return "Preparing the equity-data download…";
    default: return "Working…";
  }
}
