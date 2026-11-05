// ---------------------------------------------------------------------------
// src/pricing.ts — price cards for the Claude models the assistant may use.
//
// FAIL CLOSED: a Claude model with no card here cannot be selected. loadConfig
// marks it mock (reason "unpriced_model", shown on /health) and the Claude loop
// refuses it before any API call. The monthly cap is only as good as the price
// used to charge it, so pricing every model at one model's rates would let a
// more expensive model run past the cap. To allow another model, add its card
// here first (from the published prices) and a test for it.
//
// No imports: types.ts (config) and anthropic.ts (adapter) both read this file.
// ---------------------------------------------------------------------------

export interface ClaudePriceCard {
  // USD per token, for prompts (input + cache read + cache write) up to longAbove tokens.
  inPerToken: number;
  outPerToken: number;
  // USD per token, for prompts above longAbove tokens.
  inLongPerToken: number;
  outLongPerToken: number;
  longAbove: number;
  // Multipliers on the input rate in effect.
  cacheReadMult: number;
  cacheWrite5mMult: number;
  cacheWrite1hMult: number;
  // Reserved per request at gate time (index.ts), reconciled to actual at charge.
  reserveUsd: number;
}

// Claude Haiku 5.5 published prices. Verify before launch.
// reserveUsd: worst single turn measured by the reviewer (AR-428) for this
// Worker's limits (7 calls, 20 x 4000-char history, 3 x 6000-char tool results
// per round, uncached, output at the 2000 cap): about 306,600 prompt tokens and
// 14,000 output tokens = 0.0377, rounded up.
export const CLAUDE_PRICES: Readonly<Record<string, Readonly<ClaudePriceCard>>> = Object.freeze({
  "claude-haiku-5-5": Object.freeze({
    inPerToken: 0.10 / 1_000_000,
    outPerToken: 0.50 / 1_000_000,
    inLongPerToken: 0.50 / 1_000_000,
    outLongPerToken: 2.50 / 1_000_000,
    longAbove: 100_000,
    cacheReadMult: 0.1,
    cacheWrite5mMult: 1.25,
    cacheWrite1hMult: 2,
    reserveUsd: 0.04,
  }),
});

// Own-property lookup only (no prototype keys such as "toString").
export function priceCardFor(model: string): Readonly<ClaudePriceCard> | null {
  return Object.prototype.hasOwnProperty.call(CLAUDE_PRICES, model) ? CLAUDE_PRICES[model] : null;
}

export function pricedModels(): string[] {
  return Object.keys(CLAUDE_PRICES);
}

// The DeepSeek path keeps its existing per-request reserve (unchanged).
export const DEEPSEEK_RESERVE_USD = 0.02;
