# elkassabgidata-assistant

Hosted **"ask the data"** chat for the ElkassabgiData family. A proxy Worker that
grounds a DeepSeek model on the *existing* free endpoints (catalog search +
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
| `MODEL` | `deepseek-v4-flash` | DeepSeek model |
| `MONTHLY_USD_CAP` | `30` | hard monthly spend kill-switch |
| `ANON_PER_HOUR` / `ANON_PER_DAY` | `5` / `15` | anonymous message limits (per IP) |
| `USER_PER_HOUR` / `USER_PER_DAY` | `20` / `60` | signed-in limits (per user) |
| `MAX_TOOL_ROUNDS` | `6` | tool-call rounds per turn |
| `MAX_OUTPUT_TOKENS` | `800` | max tokens per model reply |

## Not built yet (Phase 2)
Latest-value preview (a keyless `/v1/series/{id}.preview.json` on the econ API),
embed-everywhere floating widget, conversation memory, usage line in the daily
digest.
