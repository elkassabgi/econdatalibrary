// ---------------------------------------------------------------------------
// src/anthropic.ts — OPTIONAL Claude adapter (official @anthropic-ai/sdk).
//
// Selected only when PROVIDER="anthropic" AND ANTHROPIC_API_KEY is set; DeepSeek
// stays the default (see types.ts loadConfig). The loop works on native
// Anthropic messages rather than translating the OpenAI-shaped history each
// round, because the API needs its own blocks back unchanged: the assistant
// turn is appended as response.content (thinking blocks included), and every
// tool_result of a turn goes back in ONE user message.
//
// The tools are the SAME read-only tools as the DeepSeek path: the schemas are
// translated once from tools.ts TOOL_SCHEMAS and executed by tools.ts
// executeTool, so the anti-exfiltration rules (download tools return links
// only; the gate is server-side) are unchanged.
// ---------------------------------------------------------------------------

import Anthropic from "@anthropic-ai/sdk";
import { executeTool, TOOL_SCHEMAS, type ToolCtx } from "./tools.ts";
import { priceCardFor, type ClaudePriceCard } from "./pricing.ts";
import type { ChatMessage, Config } from "./types.ts";

// Prices are per MODEL, from the cards in pricing.ts (verify before launch). A
// model without a card is refused before any API call (fail closed).
export class UnpricedModelError extends Error {
  constructor(model: string) {
    super(`Claude model "${model}" has no price card in pricing.ts; refusing to call it`);
    this.name = "UnpricedModelError";
  }
}

export function requirePriceCard(model: string): Readonly<ClaudePriceCard> {
  const card = priceCardFor(model);
  if (!card) throw new UnpricedModelError(model);
  return card;
}

// Effort for chat. Claude Haiku 5.5 defaults to "medium"; chat routes do well
// at "low" (fewer, more consolidated tool calls, terser replies).
export const CLAUDE_EFFORT = "low" as const;

export const REFUSAL_TEXT =
  "I can't help with that request. I can help you find, understand, and download data from the ElkassabgiData libraries.";
// Shown alone when a reply hit max_tokens with no text, and APPENDED to any
// partial text (the cut can remove the licence / caveat lines).
export const TRUNCATED_TEXT =
  "_(Answer cut short: it reached the length limit, so the end — including any licence or caveat lines — may be missing. Please narrow the question.)_";
const EMPTY_TEXT = "I'm not sure how to answer that yet — try rephrasing.";
const FINAL_ASK = "Give your best final answer now using what you've gathered. Do not call more tools.";

const nz = (v: number | null | undefined): number =>
  typeof v === "number" && Number.isFinite(v) && v > 0 ? v : 0;

// Price one response.usage with the card of the model that served it.
// input_tokens is the UNCACHED remainder only; the full prompt is input +
// cache_creation + cache_read (and selects the rate card). Thinking tokens are
// billed as output and are already inside usage.output_tokens. An unpriced
// model throws (UnpricedModelError) rather than being charged at a guess.
export function anthropicCostUsd(model: string, u: Partial<Anthropic.Usage> | null | undefined): number {
  const card = requirePriceCard(model);
  if (!u) return 0;
  const input = nz(u.input_tokens);
  const output = nz(u.output_tokens);
  const read = nz(u.cache_read_input_tokens);
  const write = nz(u.cache_creation_input_tokens);
  const write1h = Math.min(write, nz(u.cache_creation?.ephemeral_1h_input_tokens));
  const write5m = write - write1h;
  const long = input + read + write > card.longAbove;
  const pin = long ? card.inLongPerToken : card.inPerToken;
  const pout = long ? card.outLongPerToken : card.outPerToken;
  return (
    input * pin +
    read * pin * card.cacheReadMult +
    write5m * pin * card.cacheWrite5mMult +
    write1h * pin * card.cacheWrite1hMult +
    output * pout
  );
}

// --- tool schema translation (once, from the single OpenAI-style list) -------

interface OpenAiFunctionSchema {
  type: "function";
  function: { name: string; description: string; parameters: unknown };
}

export function toAnthropicTools(schemas: readonly OpenAiFunctionSchema[]): Anthropic.Tool[] {
  const tools: Anthropic.Tool[] = schemas.map((s) => ({
    name: s.function.name,
    description: s.function.description,
    input_schema: s.function.parameters as Anthropic.Tool.InputSchema,
  }));
  // Cache breakpoint on the last tool: tools never change, so this prefix is
  // shared by every visitor (the system prompt after it varies by sign-in state).
  if (tools.length) {
    tools[tools.length - 1] = { ...tools[tools.length - 1], cache_control: { type: "ephemeral" } };
  }
  return tools;
}

export const ANTHROPIC_TOOLS: Anthropic.Tool[] = toAnthropicTools(TOOL_SCHEMAS);

// --- client ------------------------------------------------------------------

// The one SDK method the loop uses; tests pass a fake with this shape.
export interface MessagesClient {
  messages: {
    create(body: Anthropic.MessageCreateParamsNonStreaming): Promise<Anthropic.Message>;
  };
}

export function makeAnthropicClient(apiKey: string): MessagesClient {
  // timeout is in milliseconds in the TypeScript SDK.
  return new Anthropic({ apiKey, timeout: 60_000, maxRetries: 2 });
}

// --- loop --------------------------------------------------------------------

export interface AnthropicLoopResult {
  text: string;
  costUsd: number;
}

// The widget's history is plain user/assistant text. The API requires the
// first message to be a user turn, so leading assistant turns are dropped.
export function toAnthropicHistory(history: ChatMessage[]): Anthropic.MessageParam[] {
  const out: Anthropic.MessageParam[] = [];
  for (const m of history) {
    if ((m.role !== "user" && m.role !== "assistant") || !m.content) continue;
    if (!out.length && m.role !== "user") continue;
    out.push({ role: m.role, content: m.content });
  }
  return out;
}

function textOf(msg: Anthropic.Message): string {
  return msg.content
    .filter((b): b is Anthropic.TextBlock => b.type === "text")
    .map((b) => b.text)
    .join("")
    .trim();
}

// Tool inputs arrive parsed in a non-streaming response; a string is parsed
// with JSON.parse (never matched as raw text). Anything but a plain object fails.
export function parseToolInput(input: unknown): Record<string, unknown> | null {
  let v = input;
  if (typeof v === "string") {
    try { v = JSON.parse(v); } catch { return null; }
  }
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
}

// executeTool reports failures as text ("error: ..." / "<x> failed: HTTP n").
// Those go back with is_error: true. "not_found:" and "gate:" are answers, not errors.
export function isToolError(result: string): boolean {
  return /^error:/i.test(result) || /^\w+ failed: /i.test(result);
}

// A turn that stopped for any of these reasons is final: its tool_use blocks
// (if any) may be cut off, so they are never executed.
function terminalText(msg: Anthropic.Message): string | null {
  switch (msg.stop_reason) {
    case "refusal":
      return REFUSAL_TEXT;
    case "max_tokens":
    case "model_context_window_exceeded": {
      const partial = textOf(msg);
      return partial ? `${partial}\n\n${TRUNCATED_TEXT}` : TRUNCATED_TEXT;
    }
    default:
      return null;
  }
}

export async function runAnthropicLoop(
  client: MessagesClient,
  config: Config,
  system: string,
  history: ChatMessage[],
  ctx: ToolCtx,
  onToolCall: (name: string) => void,
): Promise<AnthropicLoopResult> {
  // Fail closed BEFORE any call: a model the cap cannot price is never called.
  requirePriceCard(config.model);
  const messages = toAnthropicHistory(history);
  // Cache breakpoints (all 5-minute, 3 of the 4 allowed), following
  // shared/prompt-caching.md "robust combination for agent loops":
  //   1. last tool        — static, shared by every visitor
  //   2. system block     — per sign-in state / name
  //   3. top-level cache_control — automatic breakpoint on the last block of the
  //      growing conversation, so each tool round re-reads the history from
  //      cache instead of paying the full input rate for it again.
  const base = {
    model: config.model,
    max_tokens: config.maxOutputTokens,
    system: [{ type: "text" as const, text: system, cache_control: { type: "ephemeral" as const } }],
    tools: ANTHROPIC_TOOLS,
    output_config: { effort: CLAUDE_EFFORT },
    cache_control: { type: "ephemeral" as const },
  };
  let cost = 0;

  for (let round = 0; round < config.maxToolRounds; round++) {
    const resp = await client.messages.create({ ...base, tool_choice: { type: "auto" }, messages });
    cost += anthropicCostUsd(config.model, resp.usage);

    // Check the stop reason BEFORE running any tool.
    const terminal = terminalText(resp);
    if (terminal !== null) return { text: terminal, costUsd: cost };

    if (resp.stop_reason === "pause_turn") {
      messages.push({ role: "assistant", content: resp.content });
      continue;
    }

    const toolUses = resp.content.filter((b): b is Anthropic.ToolUseBlock => b.type === "tool_use");
    if (resp.stop_reason !== "tool_use" || toolUses.length === 0) {
      return { text: textOf(resp) || EMPTY_TEXT, costUsd: cost };
    }

    // Append the FULL assistant content (thinking blocks unchanged), then answer
    // every tool_use of this turn in ONE user message.
    messages.push({ role: "assistant", content: resp.content });
    const results: Anthropic.ToolResultBlockParam[] = [];
    for (const tu of toolUses) {
      onToolCall(tu.name);
      const args = parseToolInput(tu.input);
      if (!args) {
        results.push({
          type: "tool_result", tool_use_id: tu.id, is_error: true,
          content: `error: could not parse the arguments for "${tu.name}" as a JSON object — resend valid JSON.`,
        });
        continue;
      }
      const out = await executeTool(tu.name, args, ctx);
      results.push(isToolError(out)
        ? { type: "tool_result", tool_use_id: tu.id, is_error: true, content: out }
        : { type: "tool_result", tool_use_id: tu.id, content: out });
    }
    messages.push({ role: "user", content: results });
  }

  // Out of rounds: ask for a final answer with tools disabled. The tool list
  // stays (history holds tool_use blocks), tool_choice "none" stops new calls.
  // The ask is added to the pending (never-sent) tool_result turn, so no sent
  // turn is edited.
  const last = messages[messages.length - 1];
  if (last && last.role === "user" && Array.isArray(last.content)) {
    messages[messages.length - 1] = { role: "user", content: [...last.content, { type: "text", text: FINAL_ASK }] };
  } else {
    messages.push({ role: "user", content: FINAL_ASK });
  }
  const resp = await client.messages.create({ ...base, tool_choice: { type: "none" }, messages });
  cost += anthropicCostUsd(config.model, resp.usage);
  const terminal = terminalText(resp);
  if (terminal !== null) return { text: terminal, costUsd: cost };
  return {
    text: textOf(resp) || "I gathered some results but ran long — please narrow the question.",
    costUsd: cost,
  };
}
