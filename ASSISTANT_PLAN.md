# ElkassabgiData Assistant — plan & assessment

**Status:** proposed (2026-07-05). Awaiting go/no-go + model + access decisions from Ahmed.
**One-liner:** a hosted "ask the data" chat on the ElkassabgiData sites that answers questions by
calling the *same* data tools the MCP server exposes — no install, no AI subscription required by the user.

---

## 1. Verdict — is it a good idea?

**Yes, as a complement to MCP, not a replacement.** They solve different halves of the same problem:

| | MCP server (shipped) | Hosted assistant (proposed) |
|---|---|---|
| Who pays for the AI | the **user** (their Claude/Cursor/ChatGPT sub) | **Ahmed** (per-token API cost) |
| Setup friction | must install/connect an MCP client | **zero** — works in any browser on the site |
| Audience | developers, power users | **everyone** — students, professors, journalists |
| Our marginal cost | ~$0 | cents per conversation, capped |

Most of the target audience (finance/econ faculty and students) will never set up an MCP client. The
hosted assistant is the **last mile**: it removes *all* friction and doubles as a live demo that funnels
power users toward MCP for heavy work. It is also a strong differentiator — "a data library you can talk
to" — and good for SEO/marketing.

**The build is cheap because the hard part already exists.** The catalog search API, series-fetch API, HF
download links, freshness ledger, shared auth, rate limiting, logging, *and* the exact tool schemas +
honesty charter are all live (they power the MCP server). The assistant is just: **chat UI → agent-loop
Worker → cheap LLM with tool-calling → existing tools.** A few hundred lines plus a widget.

**The one real change vs MCP: cost and abuse move onto us.** That is entirely manageable with (a)
login-required, (b) a hard monthly budget kill-switch, (c) per-user daily caps. See §4.

---

## 2. Non-negotiable: grounding (research integrity)

The assistant must **never answer a data question from the model's memory.** It answers *only* from real
tool calls (`search_econ_series` → `get_econ_series`), and:

- cites the source + license on every figure it returns;
- surfaces the honesty-charter caveats (survivorship, the 2022 HF source break, per-source licensing,
  staleness) the same way the MCP prompts do;
- says "I couldn't find that" instead of inventing a series or a number — a wrong id returns an honest
  404, never fabricated data.

This is enforced by the system prompt **and** by a guard: the final answer must reference at least one
tool result, or it is re-prompted / refused. This is the same standard the rest of the platform holds.

---

## 3. Architecture

```
 Browser chat widget  (elkassabgidata.com/ask, embeddable on hf + econ)
        │  POST /chat  (session cookie / API key)
        ▼
 assistant Worker  (Cloudflare, new)
   1. auth        → shared hfdatalibrary-db identity (same key as everything else)
   2. rate/budget → per-user daily message cap + global monthly $ kill-switch
   3. agent loop  → LLM(system=honesty charter + tool schemas, messages)
                     ├─ tool call: search_econ_series  → GET /v1/catalog?q=
                     ├─ tool call: get_econ_series      → GET /v1/series/{id}.csv (+ license)
                     ├─ tool call: get_hf_download_link → hf signed-URL flow
                     └─ tool call: get_data_freshness   → freshness ledger
                     …loop until final grounded answer, then stream back
   4. log         → assistant_log table (tokens, $, tools used) for the morning digest
        ▼
 LLM API  (model-agnostic adapter: DeepSeek or Claude Haiku, OpenAI-style tool calling)
```

Tools are the **same functions** the MCP server already implements — reused, not rebuilt. The LLM backend
is an adapter so we can swap models without touching the agent loop.

---

## 4. Cost & controls (grounded in current published pricing)

Per-conversation estimate assumes a realistic grounded turn: ~15K input tokens (system prompt + tool
results, much of it cacheable) and ~1.5K output tokens across a short multi-turn chat. **Token counts are
assumptions; per-token rates are current published prices (sourced below).**

| Model | Input $/M | Output $/M | ~ per conversation | per 1,000 convos |
|---|---|---|---|---|
| **DeepSeek** (v4-flash / `deepseek-chat`) | $0.14 (cache-miss), $0.0028 (cache-hit) | $0.28 | **~$0.0025** (¼ cent) | **~$2.50** |
| **Claude Haiku 4.5** | $1 (‑90% w/ cache) | $5 | **~$0.02** (2 cents) | **~$22.50** |

Sources: DeepSeek API pricing docs (api-docs.deepseek.com/quick_start/pricing); Claude pricing
(platform.claude.com/docs/en/about-claude/pricing). *Verify both again immediately before launch — rates
move, and DeepSeek is deprecating the `deepseek-chat`/`deepseek-reasoner` aliases on 2026-07-24 in favor of
explicit `deepseek-v4-flash`.*

Both are cheap in absolute terms. Controls that make cost structurally bounded:

1. **Login required** (recommended) — kills anonymous abuse and ties every message to a known account.
2. **Per-user daily message cap** (e.g. 30/day) — one heavy user can't drain the budget.
3. **Global monthly kill-switch** — an env `ASSISTANT_MONTHLY_USD_CAP`; when spend crosses it the widget
   degrades gracefully ("assistant is at capacity today — use the catalog or connect MCP") rather than
   billing without limit.
4. **Max tokens/turn** + **prompt-cache the system prompt** (big discount on the fixed charter/schemas).
5. **Scope guard** — refuse off-topic "use me as a free ChatGPT" requests; keep it a *data* assistant.

**Model recommendation:** start on **DeepSeek** to validate cheaply (Ahmed already leans this way), but
build the adapter model-agnostic so we can flip to **Claude Haiku** if grounding/tool-use quality needs it.
Two caveats to weigh on DeepSeek: (a) tool-calling is less reliable than Claude's, so the grounding guard
matters more; (b) it's a non-US API — the queries are public-data questions (nothing sensitive), but some
institutions have blanket data-governance rules, so this is worth a conscious call.

---

## 5. Phasing

- **Phase 0 — decide** (Ahmed): go/no-go, model, access policy, monthly cap.
- **Phase 1 — MVP:** assistant Worker + minimal chat widget on elkassabgidata.com; 3 tools (search, fetch,
  hf-link); login-gated; daily cap; streaming; grounded system prompt + guard; usage logging.
- **Phase 2 — polish:** nicer UI, embed on hf + econ sites, "download as file" actions, short conversation
  memory, freshness/sources tools, assistant usage line in the morning digest.
- **Phase 3 — harden:** caching, budget kill-switch tuning, optional selective model upgrade, abuse
  monitoring.

---

## 6. Open decisions (blocking Phase 1)

1. **Model:** DeepSeek (cheapest, swappable) — *recommended start* — vs Claude Haiku (best grounding).
2. **Access:** logged-in users only (*recommended*) vs open-to-all rate-limited.
3. **Budget:** monthly USD kill-switch value (a low starter cap, e.g. $20–50, is plenty given the math).

Nothing here is built yet — this is the plan for Ahmed's go-ahead.
