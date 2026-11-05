# elkassabgidata-assistant

Hosted **"ask the data"** chat for the ElkassabgiData family. A proxy Worker that
grounds a model (DeepSeek by default; Claude optional, see *Switching to Claude*)
on the *existing* free endpoints (catalog search +
series metadata + freshness) and gates the actual data download behind a free
account. Anonymous visitors can search & preview; downloading requires
registration (the conversion bait). See `../ASSISTANT_PLAN.md` for the design.

## How it works
```
widget (portal/ask.html)  ──POST /chat (SSE)──►  this Worker
   │ Authorization: Bearer <hfd_session>            │ resolve visitor via hf /v1/auth/me
   │ X-Anon-Pass (after Turnstile)                  │ Turnstile + anon-pass gate (anon only)
   │                                                │ rate-limit + monthly-budget (AssistantState DO)
   │                                                │ tool loop (DeepSeek) over read-only tools:
   │                                                │   search_series, series_details, data_freshness,
   │                                                │   prepare_download (link only), hf_download_link
   ▼                                                ▼
 renders answer + client-side authenticated download buttons
```

**Security by construction:** every tool is read-only over a public endpoint;
the download tools return a *link*, never data rows — so a jailbroken model can
leak nothing it wasn't already free to search. Secrets live in the Worker env,
never in the model context. Budget is capped by an atomic DO counter.

## Local demo (NO key, no spend)
With `DEEPSEEK_API_KEY` unset the Worker runs a deterministic **mock** model that
drives the real tool loop (user → `search_series` on the live catalog → grounded
answer). This exercises the whole flow + UI end-to-end.

```bash
cd assistant
npm install
npm run typecheck        # optional
npm run dev              # wrangler dev on http://localhost:8787
# then open the widget pointed at the local worker:
#   portal/ask.html?api=http://localhost:8787
```

## Deploy (production)
```bash
cd assistant
npm install
wrangler secret put DEEPSEEK_API_KEY     # from platform.deepseek.com
wrangler secret put ANON_PASS_SECRET     # any long random string
wrangler secret put TURNSTILE_SECRET     # optional but recommended (bot-gate)
npm run deploy                           # -> https://elkassabgidata-assistant.elkassabgi.workers.dev
```
Then in `portal/ask.html` set `TURNSTILE_SITEKEY` (mint one scoped to
elkassabgidata.com / econdatalibrary.com / hfdatalibrary.com) if you set
`TURNSTILE_SECRET`, and deploy the portal (`wrangler pages deploy portal
--project-name=elkassabgidata`). To embed on the hf site, add the Worker origin
to `hfdatalibrary/_headers` `connect-src`.

## Config (wrangler.jsonc `vars`, all overridable)
| var | default | meaning |
|---|---|---|
| `PROVIDER` | `deepseek` | `deepseek` or `anthropic`; any other value resolves to `deepseek` (`/health` reports the provider in use) |
| `MODEL` | `deepseek-v4-flash` | DeepSeek model |
| `ANTHROPIC_MODEL` | `claude-haiku-5-5` | Claude model (used only when `PROVIDER=anthropic`). Only a model with a price card in `src/pricing.ts` can be selected; today that is `claude-haiku-5-5` only |
| `ANTHROPIC_MAX_OUTPUT_TOKENS` | `2000` | max tokens per Claude reply; Claude's thinking counts toward it |
| `MONTHLY_USD_CAP` | `30` | hard monthly spend kill-switch |
| `ANON_PER_HOUR` / `ANON_PER_DAY` | `5` / `15` | anonymous message limits (per IP) |
| `USER_PER_HOUR` / `USER_PER_DAY` | `20` / `60` | signed-in limits (per user) |
| `MAX_TOOL_ROUNDS` | `6` | tool-call rounds per turn |
| `MAX_OUTPUT_TOKENS` | `800` | max tokens per DeepSeek reply |

## Switching to Claude (optional; DeepSeek stays the default)
The Worker has a second adapter, `src/anthropic.ts`, built on the official
`@anthropic-ai/sdk`. It uses the same read-only tools (the schemas are translated
from `src/tools.ts`, not copied), the same system prompt, and the same monthly
cap. Nothing changes until you do these steps yourself:

1. In the Claude Console, make a **separate workspace** for the assistant, set a
   **monthly spend limit** on that workspace at or below `MONTHLY_USD_CAP`, and
   create the API key inside that workspace. The Worker's own cap (the
   AssistantState Durable Object) is the first stop; the workspace limit is a
   second, independent stop that holds even if the Worker's estimate is wrong.
2. Store the key as a Worker secret. The command prompts for the value; never put
   the key in a file or in `wrangler.jsonc`:
   ```bash
   cd assistant
   npx wrangler secret put ANTHROPIC_API_KEY
   ```
3. In `wrangler.jsonc` set `"PROVIDER": "anthropic"` (and optionally
   `ANTHROPIC_MAX_OUTPUT_TOKENS`), then `npm run deploy`.
4. Check `GET /health`: it must show `"provider": "anthropic"`, `"mock": false`
   and `"mock_reason": null`. With `PROVIDER=anthropic` and no
   `ANTHROPIC_API_KEY`, the Worker runs the keyless mock (`"mock_reason":
   "no_key"`), the same as DeepSeek without its key.

**Only priced models can be selected.** The monthly cap charges each reply with
the price card of its model, in `src/pricing.ts`, and the per-request reserve
comes from the same card. If `ANTHROPIC_MODEL` names a model with no card, the
Worker refuses it: no Claude call is made, the keyless mock answers, and
`/health` shows `"mock_reason": "unpriced_model"` with the list of
`priced_models`. To use another Claude model, first add its card (from the
published prices, with a reserve that covers a worst-case turn) and a test, then
set `ANTHROPIC_MODEL`.

To go back, set `"PROVIDER": "deepseek"` and deploy again (the Claude secret can
stay; it is not read). Before launch, verify the price card in
`src/pricing.ts` (per-token constants and cache multipliers) against the
current published prices, as for the DeepSeek constants in `src/llm.ts`.

How the Claude path calls the API: model `claude-haiku-5-5`,
`output_config.effort = "low"`, adaptive thinking left at its default (on),
`tool_choice: auto`, no sampling parameters, prompt caching with one breakpoint
on the last tool, one on the system prompt, and top-level automatic caching for
the growing conversation. The stop reason is checked before any tool runs
(`refusal` and `max_tokens` end the turn and no tool runs; a reply cut at
`max_tokens` carries a visible "Answer cut short" note), the full
`response.content` (thinking blocks included) goes back
unchanged, and all tool results of a turn go back in one user message, with
`is_error: true` on failures. Each reply is priced from `response.usage` (input,
output including thinking, cache read, cache write) and charged to the same
monthly ledger.

## Tests
```bash
cd assistant
npm ci
npm test            # node --test; no network, no key (fake Claude client + stubbed fetch)
npm run typecheck
```

## Not built yet (Phase 2)
Latest-value preview (a keyless `/v1/series/{id}.preview.json` on the econ API),
embed-everywhere floating widget, conversation memory, usage line in the daily
digest.
