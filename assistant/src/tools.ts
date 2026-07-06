// ---------------------------------------------------------------------------
// src/tools.ts — the read-only data tools the assistant can call.
//
// LEAST PRIVILEGE: every tool is read-only and hits an ALREADY-PUBLIC endpoint,
// except the two "download" tools which return a link/instructions ONLY — they
// never return bulk data rows. Anonymous callers get metadata previews + a
// registration CTA; the actual data download requires the caller's own key,
// performed client-side by the widget. So a jailbroken model can leak nothing
// it was not already free to search.
// ---------------------------------------------------------------------------

import type { Visitor } from "./types";

const ECON = "https://econdl-api.elkassabgi.workers.dev";
const HF_API = "https://api.hfdatalibrary.com";
const REGISTER_URL = "https://hfdatalibrary.com/pages/download#register";
const UPSTREAM_TIMEOUT_MS = 20_000;
const MAX_TOOL_CHARS = 6_000; // cap any single tool result fed back to the LLM

// A download the widget should render as a client-side, key-authenticated button.
export interface DownloadOffer {
  kind: "econ" | "hf";
  label: string;
  url: string;        // the gated URL; widget adds the user's X-API-Key client-side
  series_id?: string;
  ticker?: string;
}

// Mutable per-request context threaded into tool execution.
export interface ToolCtx {
  visitor: Visitor;
  offers: DownloadOffer[];   // tools push here; agent surfaces them to the widget
  register: { needed: boolean }; // set true when an anon user hit a download gate
}

async function upstream(u: string): Promise<Response> {
  const ctl = new AbortController();
  const t = setTimeout(() => ctl.abort(), UPSTREAM_TIMEOUT_MS);
  try {
    return await fetch(u, {
      headers: { "User-Agent": "elkassabgidata-assistant" },
      signal: ctl.signal,
    });
  } finally {
    clearTimeout(t);
  }
}

function clip(s: string): string {
  return s.length > MAX_TOOL_CHARS
    ? s.slice(0, MAX_TOOL_CHARS) + "\n…[truncated]"
    : s;
}

// --- OpenAI/DeepSeek function schemas --------------------------------------

export const TOOL_SCHEMAS = [
  {
    type: "function",
    function: {
      name: "search_series",
      description:
        "Search the Econ Data Library catalog (billions of economic/financial series from 300+ official sources) for series matching a free-text query. Free, no account. Returns candidate series ids + titles to use with series_details / prepare_download.",
      parameters: {
        type: "object",
        properties: {
          query: { type: "string", description: "Free-text, e.g. 'poverty Poland' or 'Australia imports'." },
          source: { type: "string", description: "Optional: restrict to one source id, e.g. 'worldbank_wdi', 'imf_weo'." },
          limit: { type: "integer", description: "Max results (1-25).", minimum: 1, maximum: 25 },
        },
        required: ["query"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "series_details",
      description:
        "Get full metadata for one economic series: title, unit, geography, frequency, coverage dates, license, attribution/citation, and last-updated. Free, no account. This is the preview you show the user.",
      parameters: {
        type: "object",
        properties: { series_id: { type: "string", description: "Exact catalog id from search_series." } },
        required: ["series_id"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "data_freshness",
      description:
        "Live per-source update status from the automated updater's ledger: last successful update and honest stale/failure flags (dates are never fabricated). Free.",
      parameters: {
        type: "object",
        properties: { source: { type: "string", description: "Optional source id; omit for a broad view." } },
      },
    },
  },
  {
    type: "function",
    function: {
      name: "prepare_download",
      description:
        "Prepare a download of one economic series' full data (CSV). Returns a download link/instructions — never the data rows themselves. If the user is signed in they get a ready one-click download; if not, they get a free-registration prompt.",
      parameters: {
        type: "object",
        properties: { series_id: { type: "string", description: "Exact catalog id to download." } },
        required: ["series_id"],
      },
    },
  },
  {
    type: "function",
    function: {
      name: "hf_download_link",
      description:
        "Prepare a download of HF Data Library 1-minute equity data (per-ticker full history) or the 25 academic variables. Returns a link/instructions only. Signed-in users get a ready download; others are prompted to register free.",
      parameters: {
        type: "object",
        properties: {
          ticker: { type: "string", description: "e.g. AAPL, SPY (1-10 alphanumerics/dots)." },
          dataset: { type: "string", enum: ["bars", "variables", "quality"], description: "Default bars." },
          version: { type: "string", enum: ["clean", "raw"], description: "Default clean." },
          format: { type: "string", enum: ["parquet", "csv"], description: "csv only applies to bars. Default parquet." },
        },
        required: ["ticker"],
      },
    },
  },
] as const;

// --- executor ---------------------------------------------------------------

export async function executeTool(
  name: string,
  args: Record<string, unknown>,
  ctx: ToolCtx,
): Promise<string> {
  try {
    switch (name) {
      case "search_series":
        return await toolSearch(args);
      case "series_details":
        return await toolDetails(args);
      case "data_freshness":
        return await toolFreshness(args);
      case "prepare_download":
        return await toolPrepareDownload(args, ctx);
      case "hf_download_link":
        return toolHfDownload(args, ctx);
      default:
        return `error: unknown tool "${name}"`;
    }
  } catch (e) {
    return `error: tool "${name}" failed: ${(e as Error).message}`;
  }
}

async function toolSearch(args: Record<string, unknown>): Promise<string> {
  const query = String(args.query ?? "").trim();
  if (query.length < 2) return "error: query must be at least 2 characters.";
  const limit = Math.min(25, Math.max(1, Number(args.limit) || 12));
  const u = new URL(ECON + "/v1/catalog");
  u.searchParams.set("q", query);
  u.searchParams.set("limit", String(limit));
  if (args.source && String(args.source).trim()) u.searchParams.set("source", String(args.source).trim());
  const r = await upstream(u.toString());
  if (!r.ok) return `search failed: HTTP ${r.status}`;
  const d = (await r.json()) as { total?: number; results?: any[] };
  const results = (d.results ?? []).map((x) => ({
    series_id: x.series_id,
    title: x.title,
    source: x.source,
    unit: x.unit,
    geography: x.geography,
    coverage: x.start_date || x.end_date ? `${x.start_date ?? "?"}..${x.end_date ?? "?"}` : null,
  }));
  return clip(JSON.stringify({ total: d.total ?? results.length, showing: results.length, results }, null, 1));
}

async function toolDetails(args: Record<string, unknown>): Promise<string> {
  const id = String(args.series_id ?? "").trim();
  if (!id) return "error: series_id required.";
  const u = `${ECON}/v1/series/${encodeURIComponent(id)}.metadata.json`;
  const r = await upstream(u);
  if (r.status === 404) return `not_found: no series "${id}" in the catalog.`;
  if (!r.ok) return `metadata failed: HTTP ${r.status}`;
  const m = (await r.json()) as Record<string, unknown>;
  // Keep the fields that matter for a cited preview; drop the rest to save tokens.
  const keep = [
    "series_id", "source", "title", "frequency", "unit", "geography",
    "start_date", "end_date", "last_updated", "license", "attribution",
    "citation_short", "citation_long", "homepage", "terms_url",
  ];
  const out: Record<string, unknown> = {};
  for (const k of keep) if (m[k] !== undefined) out[k] = m[k];
  return clip(JSON.stringify(out, null, 1));
}

async function toolFreshness(args: Record<string, unknown>): Promise<string> {
  const r = await upstream(ECON + "/v1/last-updates");
  if (!r.ok) return `freshness failed: HTTP ${r.status}`;
  const d = (await r.json()) as { generated?: string; datasets?: any[] };
  let ds = d.datasets ?? [];
  const src = args.source ? String(args.source).trim().toLowerCase() : "";
  if (src) ds = ds.filter((x) => String(x.source).toLowerCase().includes(src));
  ds = ds.slice(0, 40).map((x) => ({
    source: x.source, unit: x.unit, status: x.status,
    last_updated: x.last_updated, last_obs_date: x.last_obs_date,
    next_update_expected: x.next_update_expected,
  }));
  return clip(JSON.stringify({ generated: d.generated, showing: ds.length, datasets: ds }, null, 1));
}

async function toolPrepareDownload(args: Record<string, unknown>, ctx: ToolCtx): Promise<string> {
  const id = String(args.series_id ?? "").trim();
  if (!id) return "error: series_id required.";
  // Confirm the series exists (honest — never offer a download for a bad id).
  const meta = await upstream(`${ECON}/v1/series/${encodeURIComponent(id)}.metadata.json`);
  if (meta.status === 404) return `not_found: no series "${id}" — cannot prepare a download.`;
  const csvUrl = `${ECON}/v1/series/${encodeURIComponent(id)}.csv`;
  if (!ctx.visitor.registered) {
    ctx.register.needed = true;
    return `gate: "${id}" is available to download, but downloading requires a free ElkassabgiData account (instant, no cost): ${REGISTER_URL}. Tell the user to create one, then they can download it here in one click. Do NOT output the data yourself.`;
  }
  // Registered: hand the widget a client-side, key-authenticated download button.
  ctx.offers.push({ kind: "econ", label: `Download ${id} (CSV)`, url: csvUrl, series_id: id });
  return `ready: prepared a one-click CSV download of "${id}" for the signed-in user (a Download button will appear). Do NOT paste the data rows into the chat.`;
}

function toolHfDownload(args: Record<string, unknown>, ctx: ToolCtx): string {
  const raw = String(args.ticker ?? "").trim();
  if (!/^[A-Za-z0-9.]{1,10}$/.test(raw)) return "error: ticker must be 1-10 letters/digits/dots.";
  const T = raw.toUpperCase();
  const dataset = ["bars", "variables", "quality"].includes(String(args.dataset)) ? String(args.dataset) : "bars";
  const version = String(args.version) === "raw" ? "raw" : "clean";
  const format = String(args.format) === "csv" ? "csv" : "parquet";
  const url = dataset === "bars"
    ? `${HF_API}/v1/download/${T}?version=${version}&format=${format}`
    : `${HF_API}/v1/${dataset}/${T}?version=${version}`;
  if (!ctx.visitor.registered) {
    ctx.register.needed = true;
    return `gate: HF ${dataset} for ${T} is available, but downloading requires a free ElkassabgiData account: ${REGISTER_URL}. These files are full-history (up to millions of rows) — never inline them in chat; the user downloads them with their own key.`;
  }
  ctx.offers.push({ kind: "hf", label: `Download ${T} ${dataset} (${format})`, url, ticker: T });
  return `ready: prepared an HF ${dataset} download for ${T} (${version}/${format}) for the signed-in user. These files are large and full-history — a Download button will appear; never inline the rows in chat.`;
}
