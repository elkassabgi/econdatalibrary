// node --test test/*.test.ts   (Node >= 22.6 strips types natively)
//
// The Claude adapter and the provider switch. NO test reaches a real API: the
// Claude client is a fake injected through runAgent's deps, and globalThis.fetch
// is replaced by a stub that answers the public catalog endpoints and records
// every URL, so a call to any model host would be visible (and is asserted on).
import { test, beforeEach, afterEach } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { loadConfig, resolveProvider, DEFAULT_DEEPSEEK_MODEL, DEFAULT_ANTHROPIC_MODEL } from "../src/types.ts";
import { TOOL_SCHEMAS } from "../src/tools.ts";
import { reserveUsdFor, runAgent } from "../src/agent.ts";
import { CLAUDE_PRICES, DEEPSEEK_RESERVE_USD, priceCardFor, pricedModels } from "../src/pricing.ts";
import { mockNote } from "../src/llm.ts";
import { healthBody } from "../src/health.ts";
import {
  ANTHROPIC_TOOLS, CLAUDE_EFFORT, REFUSAL_TEXT, TRUNCATED_TEXT,
  UnpricedModelError, anthropicCostUsd, isToolError, parseToolInput, runAnthropicLoop, toAnthropicHistory, toAnthropicTools,
} from "../src/anthropic.ts";

const ANON = { registered: false, userId: null, email: null, name: null, apiKey: null };
const DUMMY = "dummy-value-for-tests"; // not a key; the fake client never sends it anywhere
const H = "claude-haiku-5-5";

// --- fetch stub ---------------------------------------------------------------
let fetched: string[] = [];
let deepseekReplies: unknown[] = [];
const realFetch = globalThis.fetch;
beforeEach(() => {
  fetched = [];
  deepseekReplies = [];
  globalThis.fetch = (async (input: string | URL | Request) => {
    const url = typeof input === "string" ? input : input instanceof URL ? input.toString() : input.url;
    fetched.push(url);
    const json = (o: unknown) => new Response(JSON.stringify(o), { status: 200, headers: { "content-type": "application/json" } });
    if (url.includes("/v1/catalog")) {
      return json({ total: 1, results: [{ series_id: "wb_wdi:poverty:PL", title: "Poverty headcount, Poland" }] });
    }
    if (url.includes(".metadata.json")) return json({ series_id: "wb_wdi:poverty:PL", title: "Poverty headcount, Poland" });
    if (url.startsWith("https://api.deepseek.com/")) {
      const next = deepseekReplies.shift();
      if (!next) throw new Error("unexpected DeepSeek call");
      return json(next);
    }
    throw new Error(`unexpected fetch in test: ${url}`);
  }) as typeof fetch;
});
afterEach(() => { globalThis.fetch = realFetch; });

// --- fake Claude client -------------------------------------------------------
function msg(content: unknown[], stop_reason: string, usage: Record<string, unknown> = {}) {
  return {
    id: "msg_test", type: "message", role: "assistant", model: DEFAULT_ANTHROPIC_MODEL,
    content, stop_reason, stop_sequence: null,
    usage: { input_tokens: 0, output_tokens: 0, cache_creation_input_tokens: 0, cache_read_input_tokens: 0, ...usage },
  };
}
function fakeClient(script: unknown[]) {
  const requests: any[] = [];
  const keys: string[] = [];
  const factory = (apiKey: string) => {
    keys.push(apiKey);
    return {
      messages: {
        async create(body: unknown) {
          requests.push(structuredClone(body)); // snapshot: the loop keeps appending to its array
          const next = script.shift();
          if (!next) throw new Error("fake client: script exhausted");
          return next as any;
        },
      },
    };
  };
  return { factory, requests, keys };
}
const claudeConfig = (extra: Record<string, string> = {}) =>
  loadConfig({ PROVIDER: "anthropic", ANTHROPIC_API_KEY: DUMMY, ...extra } as any);

// --- DeepSeek stays the default -------------------------------------------------
test("default provider is DeepSeek with its default model and caps", () => {
  const c = loadConfig({} as any);
  assert.equal(c.provider, "deepseek");
  assert.equal(c.model, DEFAULT_DEEPSEEK_MODEL);
  assert.equal(c.model, "deepseek-v4-flash");
  assert.equal(c.maxOutputTokens, 800);
  assert.equal(c.mock, true); // no DeepSeek key -> mock
  assert.equal(loadConfig({ DEEPSEEK_API_KEY: DUMMY } as any).mock, false);
});

test("an Anthropic key alone does not switch the provider", () => {
  const c = loadConfig({ ANTHROPIC_API_KEY: DUMMY } as any);
  assert.equal(c.provider, "deepseek");
  assert.equal(c.mock, true); // the SELECTED provider's (DeepSeek's) key is absent
});

test("wrangler.jsonc keeps PROVIDER = deepseek and holds no Anthropic key", () => {
  const w = readFileSync(new URL("../wrangler.jsonc", import.meta.url), "utf8");
  const m = w.match(/"PROVIDER"\s*:\s*"([^"]*)"/);
  assert.ok(m, "PROVIDER var present");
  assert.equal(m[1], "deepseek");
  assert.equal(resolveProvider(m[1]), "deepseek");
  assert.doesNotMatch(w, /"ANTHROPIC_API_KEY"\s*:/);
});

test("provider selection: exact match after trim/lower-case; anything else is DeepSeek", () => {
  assert.equal(resolveProvider(undefined), "deepseek");
  assert.equal(resolveProvider(""), "deepseek");
  assert.equal(resolveProvider("deepseek"), "deepseek");
  assert.equal(resolveProvider(" Anthropic "), "anthropic");
  assert.equal(resolveProvider("antropic"), "deepseek");
  assert.equal(resolveProvider("claude"), "deepseek");
});

test("PROVIDER=anthropic: Claude model + own cap; mock exactly when ITS key is absent", () => {
  const noKey = loadConfig({ PROVIDER: "anthropic", DEEPSEEK_API_KEY: DUMMY } as any);
  assert.equal(noKey.provider, "anthropic");
  assert.equal(noKey.model, "claude-haiku-5-5");
  assert.equal(noKey.maxOutputTokens, 2000);
  assert.equal(noKey.mock, true);
  assert.equal(noKey.mockReason, "no_key");
  const withKey = claudeConfig();
  assert.equal(withKey.mock, false);
  assert.equal(withKey.mockReason, null);
  assert.equal(claudeConfig({ ANTHROPIC_MAX_OUTPUT_TOKENS: "3000" }).maxOutputTokens, 3000);
  // MODEL (the DeepSeek var) never leaks into the Claude model id
  assert.equal(claudeConfig({ MODEL: "deepseek-v4-flash" }).model, "claude-haiku-5-5");
});

// --- price table: fail closed ----------------------------------------------------
test("price table holds claude-haiku-5-5 with the reference prices, and nothing else", () => {
  assert.deepEqual(pricedModels(), ["claude-haiku-5-5"]);
  const c = CLAUDE_PRICES["claude-haiku-5-5"];
  close(c.inPerToken, 0.10e-6);
  close(c.outPerToken, 0.50e-6);
  close(c.inLongPerToken, 0.50e-6);
  close(c.outLongPerToken, 2.50e-6);
  assert.equal(c.longAbove, 100_000);
  assert.equal(c.cacheReadMult, 0.1);
  assert.equal(c.cacheWrite5mMult, 1.25);
  assert.equal(c.cacheWrite1hMult, 2);
  assert.ok(c.reserveUsd >= 0.0377, "reserve covers the reviewer's worst turn (0.0377)");
  assert.equal(priceCardFor("toString"), null); // own keys only
  assert.equal(priceCardFor("claude-sonnet-5-5"), null);
  assert.ok(Object.isFrozen(CLAUDE_PRICES) && Object.isFrozen(c));
});

test("an unpriced ANTHROPIC_MODEL is refused: mock with reason unpriced_model, even with a key", () => {
  for (const model of ["claude-sonnet-5-5", "claude-opus-5-5", "claude-haiku-5-5-20990101", "nonsense"]) {
    const c = claudeConfig({ ANTHROPIC_MODEL: model });
    assert.equal(c.model, model);
    assert.equal(c.mock, true, model);
    assert.equal(c.mockReason, "unpriced_model", model);
  }
  // the reason wins over a missing key too, so /health names the real blocker
  assert.equal(loadConfig({ PROVIDER: "anthropic", ANTHROPIC_MODEL: "claude-sonnet-5-5" } as any).mockReason, "unpriced_model");
  // DeepSeek is not affected by the Claude table
  assert.equal(loadConfig({ DEEPSEEK_API_KEY: DUMMY, MODEL: "deepseek-v9" } as any).mock, false);
});

test("an unpriced model never reaches messages.create (runAgent and the loop itself)", async () => {
  const { factory, requests, keys } = fakeClient([msg([{ type: "text", text: "should not happen" }], "end_turn")]);
  const cfg = claudeConfig({ ANTHROPIC_MODEL: "claude-sonnet-5-5" });
  const r = await runAgent({ ANTHROPIC_API_KEY: DUMMY } as any, cfg, ANON, [{ role: "user", content: "poverty Poland" }], () => {}, { anthropicClient: factory });
  assert.deepEqual(keys, []);
  assert.equal(requests.length, 0);
  assert.match(r.text, /has no price card/);
  assert.equal(r.costUsd, 0);
  for (const u of fetched) assert.doesNotMatch(u, /anthropic|deepseek/);

  // defence in depth: even a config forced to mock=false is refused before the call
  const forced = { ...cfg, mock: false, mockReason: null };
  const client = factory(DUMMY);
  await assert.rejects(runAnthropicLoop(client, forced, "sys", [{ role: "user", content: "q" }], { visitor: ANON, offers: [], register: { needed: false } }, () => {}), UnpricedModelError);
  assert.equal(requests.length, 0);
  assert.throws(() => anthropicCostUsd("claude-sonnet-5-5", { input_tokens: 1 }), UnpricedModelError);
  assert.throws(() => reserveUsdFor(forced as any), UnpricedModelError);
});

test("per-request reserve comes from the same table, per model", () => {
  assert.equal(reserveUsdFor(claudeConfig()), CLAUDE_PRICES["claude-haiku-5-5"].reserveUsd);
  assert.equal(reserveUsdFor(loadConfig({ DEEPSEEK_API_KEY: DUMMY } as any)), DEEPSEEK_RESERVE_USD);
  assert.equal(DEEPSEEK_RESERVE_USD, 0.02); // DeepSeek's reserve is unchanged
  assert.equal(reserveUsdFor(loadConfig({} as any)), DEEPSEEK_RESERVE_USD); // mock
});

test("/health shows the provider, mock and the mock reason (incl. unpriced_model)", () => {
  const ds = healthBody(loadConfig({} as any), {} as any);
  assert.equal(ds.provider, "deepseek");
  assert.equal(ds.mock, true);
  assert.equal(ds.mock_reason, "no_key");
  assert.equal("priced_models" in ds, false);
  const live = healthBody(claudeConfig(), {} as any);
  assert.deepEqual([live.provider, live.mock, live.mock_reason, live.model], ["anthropic", false, null, "claude-haiku-5-5"]);
  assert.deepEqual(live.priced_models, ["claude-haiku-5-5"]);
  const refused = healthBody(claudeConfig({ ANTHROPIC_MODEL: "claude-sonnet-5-5" }), {} as any);
  assert.deepEqual([refused.mock, refused.mock_reason, refused.model], [true, "unpriced_model", "claude-sonnet-5-5"]);
});

test("index.ts gates and settles with reserveUsdFor(cfg), not a fixed reserve", () => {
  // index.ts imports the Durable Object (cloudflare:workers), so it cannot load
  // under node; this pins the wiring at the source level instead.
  const src = readFileSync(new URL("../src/index.ts", import.meta.url), "utf8");
  assert.match(src, /const ESTIMATE_USD = reserveUsdFor\(cfg\);/);
  assert.match(src, /reserveUsd: ESTIMATE_USD/);
  assert.match(src, /settle\(result\.costUsd - ESTIMATE_USD\)/);
  assert.doesNotMatch(src, /ESTIMATE_USD\s*=\s*[0-9.]/);
  assert.match(src, /json\(healthBody\(loadConfig\(env\), env\), 200, cors\)/);
});

test("mock reply names the selected provider and the reason", () => {
  assert.match(mockNote(loadConfig({} as any)), /no DeepSeek key.*Provider: deepseek/);
  assert.match(mockNote(loadConfig({ PROVIDER: "anthropic" } as any)), /no Claude key.*Provider: anthropic/);
  assert.match(mockNote(claudeConfig({ ANTHROPIC_MODEL: "claude-sonnet-5-5" })), /"claude-sonnet-5-5" has no price card.*Provider: anthropic/);
});

// --- schema translation ---------------------------------------------------------
test("tool schemas are translated once from TOOL_SCHEMAS, field for field", () => {
  assert.equal(ANTHROPIC_TOOLS.length, TOOL_SCHEMAS.length);
  TOOL_SCHEMAS.forEach((s, i) => {
    const t = ANTHROPIC_TOOLS[i] as any;
    assert.equal(t.name, s.function.name);
    assert.equal(t.description, s.function.description);
    assert.deepEqual(t.input_schema, s.function.parameters);
    assert.equal(t.type, undefined);
    assert.equal(t.function, undefined);
  });
  // one cache breakpoint, on the last tool only
  assert.deepEqual(ANTHROPIC_TOOLS.map((t: any) => !!t.cache_control), TOOL_SCHEMAS.map((_, i) => i === TOOL_SCHEMAS.length - 1));
  assert.deepEqual((ANTHROPIC_TOOLS[ANTHROPIC_TOOLS.length - 1] as any).cache_control, { type: "ephemeral" });
  // translating does not mutate the source list
  const before = JSON.stringify(TOOL_SCHEMAS);
  toAnthropicTools(TOOL_SCHEMAS);
  assert.equal(JSON.stringify(TOOL_SCHEMAS), before);
});

// --- usage -> cost ---------------------------------------------------------------
const close = (a: number, b: number) => assert.ok(Math.abs(a - b) < 1e-12, `${a} != ${b}`);

test("usage -> cost: per-token constants, cache read/write, 1h writes", () => {
  close(anthropicCostUsd(H, null), 0);
  close(anthropicCostUsd(H, undefined), 0);
  close(anthropicCostUsd(H, { input_tokens: 0, output_tokens: 0 }), 0);
  close(anthropicCostUsd(H, { input_tokens: 50_000 }), 50_000 * 0.10e-6);
  close(anthropicCostUsd(H, { output_tokens: 1_000 }), 1_000 * 0.50e-6); // thinking is inside output_tokens
  close(anthropicCostUsd(H, { cache_read_input_tokens: 50_000 }), 50_000 * 0.10e-6 * 0.1);
  close(anthropicCostUsd(H, { cache_creation_input_tokens: 40_000 }), 40_000 * 0.10e-6 * 1.25);
  close(
    anthropicCostUsd(H, { cache_creation_input_tokens: 40_000, cache_creation: { ephemeral_5m_input_tokens: 30_000, ephemeral_1h_input_tokens: 10_000 } } as any),
    30_000 * 0.10e-6 * 1.25 + 10_000 * 0.10e-6 * 2,
  );
  close(
    anthropicCostUsd(H, { input_tokens: 1_200, output_tokens: 300, cache_read_input_tokens: 2_000, cache_creation_input_tokens: 500 }),
    1_200 * 0.10e-6 + 300 * 0.50e-6 + 2_000 * 0.10e-6 * 0.1 + 500 * 0.10e-6 * 1.25,
  );
  // nulls (the SDK types allow them) and junk count as zero
  close(anthropicCostUsd(H, { input_tokens: 10, output_tokens: 10, cache_read_input_tokens: null, cache_creation_input_tokens: null } as any), 10 * 0.10e-6 + 10 * 0.50e-6);
  close(anthropicCostUsd(H, { input_tokens: -5, output_tokens: Number.NaN } as any), 0);
});

test("usage -> cost: the long-prompt rate card applies above 100K prompt tokens", () => {
  // exactly 100K (input + cache read + cache write) is still the standard card
  close(anthropicCostUsd(H, { input_tokens: 60_000, cache_read_input_tokens: 40_000, output_tokens: 100 }),
    60_000 * 0.10e-6 + 40_000 * 0.10e-6 * 0.1 + 100 * 0.50e-6);
  close(anthropicCostUsd(H, { input_tokens: 60_001, cache_read_input_tokens: 40_000, output_tokens: 100 }),
    60_001 * 0.50e-6 + 40_000 * 0.50e-6 * 0.1 + 100 * 2.50e-6);
});

// --- helpers ----------------------------------------------------------------------
test("tool input parsing and error classification", () => {
  assert.deepEqual(parseToolInput({ query: "x" }), { query: "x" });
  assert.deepEqual(parseToolInput('{"query":"x\\/y"}'), { query: "x/y" });
  assert.equal(parseToolInput("{not json"), null);
  assert.equal(parseToolInput([1, 2]), null);
  assert.equal(parseToolInput(null), null);
  assert.equal(isToolError('error: unknown tool "x"'), true);
  assert.equal(isToolError("search failed: HTTP 503"), true);
  assert.equal(isToolError('not_found: no series "x" in the catalog.'), false);
  assert.equal(isToolError("gate: available, register first"), false);
  assert.equal(isToolError('{"total":1}'), false);
});

test("history: leading assistant turns are dropped so the first turn is the user's", () => {
  assert.deepEqual(
    toAnthropicHistory([
      { role: "assistant", content: "Hi! Ask me anything." },
      { role: "user", content: "poverty Poland" },
      { role: "assistant", content: "Found one." },
      { role: "user", content: "download it" },
    ]),
    [
      { role: "user", content: "poverty Poland" },
      { role: "assistant", content: "Found one." },
      { role: "user", content: "download it" },
    ],
  );
});

// --- the tool loop with a fake client ---------------------------------------------
test("tool loop: full content appended (thinking kept), ONE user message of tool_results, is_error on failure", async () => {
  const round1 = [
    { type: "thinking", thinking: "", signature: "sig-abc" },
    { type: "text", text: "Let me look." },
    { type: "tool_use", id: "tu_1", name: "search_series", input: { query: "poverty Poland" } },
    { type: "tool_use", id: "tu_2", name: "hf_download_link", input: { ticker: "AAPL" } },
    { type: "tool_use", id: "tu_3", name: "no_such_tool", input: {} },
  ];
  const { factory, requests, keys } = fakeClient([
    msg(round1, "tool_use", { input_tokens: 1_000, output_tokens: 200, cache_creation_input_tokens: 3_000 }),
    msg([{ type: "text", text: "Here is the series." }], "end_turn", { input_tokens: 1_500, output_tokens: 100, cache_read_input_tokens: 3_000 }),
  ]);
  const status: string[] = [];
  const cfg = claudeConfig();
  const r = await runAgent({ ANTHROPIC_API_KEY: DUMMY } as any, cfg, ANON,
    [{ role: "user", content: "How many people are below the poverty line in Poland?" }],
    (s) => status.push(s), { anthropicClient: factory });

  assert.equal(r.text, "Here is the series.");
  assert.deepEqual(keys, [DUMMY]);
  assert.equal(requests.length, 2);
  assert.equal(r.registerNeeded, true); // anonymous hf download hit the server-side gate
  assert.deepEqual(r.offers, []);       // ...and so no download link was handed out
  assert.deepEqual(status, ["Searching the catalog…", "Preparing the equity-data download…", "Working…"]);
  close(r.costUsd,
    anthropicCostUsd(H, { input_tokens: 1_000, output_tokens: 200, cache_creation_input_tokens: 3_000 }) +
    anthropicCostUsd(H, { input_tokens: 1_500, output_tokens: 100, cache_read_input_tokens: 3_000 }));

  // request shape
  for (const q of requests) {
    assert.equal(q.model, "claude-haiku-5-5");
    assert.equal(q.max_tokens, 2000);
    assert.deepEqual(q.output_config, { effort: CLAUDE_EFFORT });
    assert.equal(CLAUDE_EFFORT, "low");
    assert.deepEqual(q.tool_choice, { type: "auto" });
    assert.deepEqual(q.cache_control, { type: "ephemeral" }); // automatic breakpoint on the conversation tail
    for (const k of ["temperature", "top_p", "top_k", "thinking", "stream"]) assert.equal(k in q, false, `${k} must not be sent`);
    assert.deepEqual(q.tools, JSON.parse(JSON.stringify(ANTHROPIC_TOOLS)));
    assert.equal(q.system.length, 1);
    assert.deepEqual(q.system[0].cache_control, { type: "ephemeral" });
    assert.match(q.system[0].text, /ElkassabgiData assistant/);
    assert.equal(q.messages.at(-1).role, "user"); // never an assistant prefill
  }

  const m = requests[1].messages;
  assert.equal(m.length, 3);
  assert.deepEqual(m[1], { role: "assistant", content: round1 }); // response.content unchanged
  assert.equal(m[2].role, "user");
  assert.equal(m[2].content.length, 3);
  assert.deepEqual(m[2].content.map((b: any) => [b.type, b.tool_use_id, b.is_error === true]), [
    ["tool_result", "tu_1", false],
    ["tool_result", "tu_2", false],
    ["tool_result", "tu_3", true],
  ]);
  assert.match(m[2].content[0].content, /wb_wdi:poverty:PL/);
  assert.match(m[2].content[1].content, /^gate:/);
  assert.match(m[2].content[2].content, /unknown tool/);

  // no model host was ever fetched; only the public catalog
  assert.ok(fetched.length > 0);
  for (const u of fetched) assert.match(u, /^https:\/\/econdl-api\.elkassabgi\.workers\.dev\//);
});

test("tool loop: unparseable string input goes back as is_error without running the tool", async () => {
  const { factory, requests } = fakeClient([
    msg([{ type: "tool_use", id: "tu_1", name: "search_series", input: "{oops" }], "tool_use"),
    msg([{ type: "text", text: "ok" }], "end_turn"),
  ]);
  const r = await runAgent({} as any, claudeConfig(), ANON, [{ role: "user", content: "q" }], () => {}, { anthropicClient: factory });
  assert.equal(r.text, "ok");
  const res = requests[1].messages[2].content[0];
  assert.equal(res.is_error, true);
  assert.match(res.content, /could not parse/);
  assert.equal(fetched.length, 0);
});

test("refusal: stop before any tool runs", async () => {
  const { factory, requests } = fakeClient([
    msg([{ type: "tool_use", id: "tu_1", name: "search_series", input: { query: "x" } }], "refusal"),
  ]);
  const status: string[] = [];
  const r = await runAgent({} as any, claudeConfig(), ANON, [{ role: "user", content: "q" }], (s) => status.push(s), { anthropicClient: factory });
  assert.equal(r.text, REFUSAL_TEXT);
  assert.equal(requests.length, 1);
  assert.deepEqual(status, []);
  assert.equal(fetched.length, 0);
});

test("max_tokens: a possibly truncated tool_use is never executed", async () => {
  const { factory, requests } = fakeClient([
    msg([{ type: "tool_use", id: "tu_1", name: "search_series", input: { query: "pov" } }], "max_tokens"),
  ]);
  const r = await runAgent({} as any, claudeConfig(), ANON, [{ role: "user", content: "q" }], () => {}, { anthropicClient: factory });
  assert.equal(r.text, TRUNCATED_TEXT);
  assert.equal(requests.length, 1);
  assert.equal(fetched.length, 0);

  const partial = fakeClient([msg([{ type: "text", text: "Partial answer" }], "max_tokens")]);
  const r2 = await runAgent({} as any, claudeConfig(), ANON, [{ role: "user", content: "q" }], () => {}, { anthropicClient: partial.factory });
  assert.equal(r2.text, `Partial answer

${TRUNCATED_TEXT}`);
  assert.match(r2.text, /Answer cut short/);
});

test("out of rounds: final call keeps tools, sets tool_choice none, asks inside the pending tool_result turn", async () => {
  const { factory, requests } = fakeClient([
    msg([{ type: "tool_use", id: "tu_1", name: "search_series", input: { query: "poverty" } }], "tool_use"),
    msg([{ type: "text", text: "Final." }], "end_turn"),
  ]);
  const r = await runAgent({} as any, claudeConfig({ MAX_TOOL_ROUNDS: "1" }), ANON, [{ role: "user", content: "q" }], () => {}, { anthropicClient: factory });
  assert.equal(r.text, "Final.");
  assert.equal(requests.length, 2);
  const last = requests[1];
  assert.deepEqual(last.tool_choice, { type: "none" });
  assert.equal(last.tools.length, TOOL_SCHEMAS.length);
  assert.equal(last.messages.length, 3);
  const turn = last.messages[2];
  assert.equal(turn.role, "user");
  assert.deepEqual(turn.content.map((b: any) => b.type), ["tool_result", "text"]);
});

// --- provider switch at runtime -------------------------------------------------
test("PROVIDER=anthropic without its key runs the keyless mock and never builds a Claude client", async () => {
  const { factory, keys } = fakeClient([]);
  const cfg = loadConfig({ PROVIDER: "anthropic" } as any);
  assert.equal(cfg.mock, true);
  const r = await runAgent({} as any, cfg, ANON, [{ role: "user", content: "poverty Poland" }], () => {}, { anthropicClient: factory });
  assert.deepEqual(keys, []);
  assert.match(r.text, /demo mode: no Claude key configured. Provider: anthropic/);
  assert.equal(r.costUsd, 0);
  for (const u of fetched) assert.doesNotMatch(u, /anthropic|deepseek/);
});

test("default (DeepSeek) config uses the DeepSeek endpoint, not the Claude client", async () => {
  const { factory, keys } = fakeClient([]);
  deepseekReplies.push({
    choices: [{ message: { role: "assistant", content: "DeepSeek answer" } }],
    usage: { prompt_tokens: 10, completion_tokens: 5 },
  });
  const cfg = loadConfig({ DEEPSEEK_API_KEY: DUMMY, ANTHROPIC_API_KEY: DUMMY } as any);
  const r = await runAgent({ DEEPSEEK_API_KEY: DUMMY } as any, cfg, ANON, [{ role: "user", content: "hi" }], () => {}, { anthropicClient: factory });
  assert.equal(r.text, "DeepSeek answer");
  assert.deepEqual(keys, []);
  assert.deepEqual(fetched, ["https://api.deepseek.com/chat/completions"]);
});
