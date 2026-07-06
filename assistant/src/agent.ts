// ---------------------------------------------------------------------------
// src/agent.ts — the tool-calling loop.
//
// Runs the model with the read-only tools until it produces a final answer (no
// tool call) or the round budget is hit. Grounding is structural: the only way
// to surface data is via tools, and the download tools return links, not rows.
// Emits status lines during tool rounds; returns the final text + any download
// offers + accumulated cost.
// ---------------------------------------------------------------------------

import { llmComplete, costUsd } from "./llm";
import { TOOL_SCHEMAS, executeTool, type ToolCtx, type DownloadOffer } from "./tools";
import { systemPrompt } from "./prompt";
import type { ChatMessage, Config, Env, Visitor } from "./types";

export interface AgentResult {
  text: string;
  offers: DownloadOffer[];
  registerNeeded: boolean;
  costUsd: number;
}

export async function runAgent(
  env: Env,
  config: Config,
  visitor: Visitor,
  history: ChatMessage[],
  onStatus: (label: string) => void,
): Promise<AgentResult> {
  const messages: ChatMessage[] = [
    { role: "system", content: systemPrompt(visitor.registered, visitor.name) },
    ...history,
  ];
  const ctx: ToolCtx = { visitor, offers: [], register: { needed: false } };
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
