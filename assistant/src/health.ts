// ---------------------------------------------------------------------------
// src/health.ts — the GET /health body. Kept out of index.ts (which imports the
// Durable Object and so cannot load under node:test) so it can be tested.
// ---------------------------------------------------------------------------

import { pricedModels } from "./pricing.ts";
import type { Config, Env } from "./types.ts";

export function healthBody(cfg: Config, env: Env): Record<string, unknown> {
  return {
    ok: true, service: "elkassabgidata-assistant",
    provider: cfg.provider, mock: cfg.mock, mock_reason: cfg.mockReason, model: cfg.model,
    // with Claude selected, list the models that can be selected (have a price card)
    ...(cfg.provider === "anthropic" ? { priced_models: pricedModels() } : {}),
    // Turnstile config is advertised so the widget stays in lockstep with the
    // Worker (one source of truth) — never a secret-set-but-no-sitekey lockout.
    turnstile: { required: !!env.TURNSTILE_SECRET, sitekey: env.TURNSTILE_SITEKEY || null },
  };
}
