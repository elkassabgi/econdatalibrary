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

Both are cheap in absolute terms. Cost controls are detailed in §7 alongside abuse defense.

**Model — DECIDED: DeepSeek** (`deepseek-v4-flash`), validated cheaply, behind a model-agnostic adapter so
we can flip to Claude Haiku if grounding/tool-use quality needs it. Two caveats we accept: (a) tool-calling
is less reliable than Claude's, so the §2 grounding guard matters more; (b) it's a non-US API — queries are
public-data questions (nothing sensitive), flagged for institutional data-governance awareness.

---

## 6b. Access model — DECIDED: anonymous chat, gated download (registration bait)

Anonymous visitors **can chat** — the assistant searches, shows what it found, previews the latest value,
and explains the series. But **downloading the actual data requires a free account.** The assistant says so
explicitly: *"I found it — [series + latest value]. Create a free account to download the full series."*
This turns the assistant into a conversion funnel while never handing bulk data to the unregistered.

This maps 1:1 onto the existing tool design: `search` + metadata + freshness are already keyless; the actual
data (`get_econ_series`) already requires a key, **enforced server-side**. Anonymous users get metadata +
a tiny preview (latest N observations / a summary), never the full file.

---

## 7. Security & abuse mitigation (public, free LLM endpoint)

A free LLM open to anonymous users is the real risk surface. Layered defense, strongest first:

**Tier 1 — structural (a jailbreak still can't hurt you):**
- **Least privilege.** The assistant can call *only* read-only data tools (search / metadata / preview /
  freshness). No write tools, no code exec, no email, no spend, no secrets. Even a total jailbreak can do no
  more than search public data.
- **No secrets in the model's context.** The DeepSeek key and all creds live in the Worker env, never in the
  system prompt — so "leak your prompt" reveals nothing sensitive.
- **Download gate enforced by the Worker, not the model.** The actual data bytes come from the key-checked
  `get_econ_series` path. Even if a user jailbreaks the model into *offering* the full dataset, the model
  never *received* it — the Worker refuses without a valid account. The LLM can't give away what it never got.
- **Global monthly kill-switch** — env `ASSISTANT_MONTHLY_USD_CAP`; on breach, anonymous chat degrades to
  "register to keep going" (which also serves conversion). Hard ceiling on total spend.

**Tier 2 — bot / script defense (the main cost-drain vector):**
- **Cloudflare Turnstile** (already in our stack) on the first anonymous message (or after 1–2). This is the
  single biggest lever against scripted abuse — it stops the "curl-loop it as a free GPT" attack cold.
- **Layered rate limits:** anonymous keyed on IP + a signed session cookie → small quota (starter: ~5/hour,
  ~15/day — enough to get hooked, not to abuse); registered users get more. Plus **per-turn caps**: max
  output tokens, max tool calls, max context — so no single request is expensive.

**Tier 3 — scope & hygiene:**
- **Topic guard.** System prompt + a cheap pre-check refuses non-data requests ("I only help with
  ElkassabgiData data"), so it can't be repurposed as a general free chatbot. Short max output reinforces this.
- **Tool-call budget per session** — prevents scripting the assistant to page through thousands of series.
- **Transparent, minimal logging** (tokens, cost, tools used; not sensitive content); privacy-page note; no
  user identifiers sent to DeepSeek beyond the query text.

Net: cost is bounded by the kill-switch; automated abuse is blocked by Turnstile + rate limits; and because
the assistant is least-privilege with a server-side download gate, the worst a determined jailbreaker
achieves is… searching public data they could already search for free.

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

## 8. Decisions & what's needed to start Phase 1

- **Model:** DeepSeek `deepseek-v4-flash` — DECIDED (2026-07-05).
- **Access:** anonymous chat, download gated behind free registration (bait) — DECIDED (2026-07-05).
- **Budget:** proposed starter `ASSISTANT_MONTHLY_USD_CAP` = $30/mo kill-switch (revisit up once traffic is
  known; the math says this covers ~12k DeepSeek conversations). Confirm value.
- **Blocker to deploy:** a **DeepSeek API key** (Ahmed creates it at platform.deepseek.com → stored in the
  Worker env, never in chat). Code can be built before the key exists; only deployment needs it.

Code is not written yet — this is the agreed design, ready to build on Ahmed's go.
