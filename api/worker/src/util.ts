// ---------------------------------------------------------------------------
// src/util.ts  --  honest-status responses, license blocks, cadence math.
//
// HONEST-STATUS IS NON-NEGOTIABLE (CONTRACT.md):
//   200 only with >=1 row; 404 unknown id; 501 not_migrated; 502 resolver_empty;
//   400 unsupported_filter. NEVER an empty 200, NEVER a fabricated date.
// These helpers are the single place those codes are emitted, so the rule is
// enforced uniformly across every handler.
// ---------------------------------------------------------------------------

import type { Env, LicenseRow, LicenseBlock } from "./types";

// The 191 sources with an at-rest resolver (econdl._resolve.supported_sources()).
// A series whose source is NOT in this set returns 501 not_migrated -- loud and
// actionable, exactly as the dev shim does via ResolveError. KEEP IN SYNC with
// clients/python/econdl/_resolve.py::_RESOLVERS (regenerate via supported_sources()).
// Regenerated 2026-07-02 from supported_sources() (was a stale 33-entry list, which
// made ~158 genuinely-migrated sources return 501 instead of the honest 502).
export const SUPPORTED_SOURCES: readonly string[] = [
  "abs", "barro_lee", "bcb", "bcrp", "bea", "bis",
  "bls", "boc", "boe", "bundesbank", "cboe", "census",
  "cnb", "comtrade", "cow", "damodaran", "dbnomics", "defillama",
  "ecb", "edgar_jrc", "ei_statreview", "eia", "ember", "epu",
  "eurostat", "famafrench", "fao_ae", "fao_af", "fao_ec", "fao_ep",
  "fao_es", "fao_et", "fao_ew", "fao_fo", "fao_ga", "fao_gb",
  "fao_ge", "fao_gf", "fao_gl", "fao_gn", "fao_gr", "fao_gt",
  "fao_gy", "fao_ic", "fao_oa", "fao_pp", "fao_qa", "fao_qcl",
  "fao_ql", "fao_qp", "fao_rp", "faostat", "fed_board", "fhfa",
  "frankfurter", "freedomhouse", "fsi_fundforpeace", "gcb", "ggdc", "gppd",
  // IEP (CC BY-NC-SA 4.0, granted 2026-07-06). These four were CATALOGUED and
  // searchable on the site with ZERO CSVs in R2, so every Download button on them
  // failed — 12,282 series advertised with nothing behind them. Derived 2026-07-27
  // (12,282/12,282, verified 100% present in R2 before this flip).
  "gpi", "gti", "ppi", "etr",
  "hf_equities", "idb", "ilostat", "imf", "imf_afrreo", "imf_apdreo",
  "imf_bopagg", "imf_cofer", "imf_commodity", "imf_cpi", "imf_fas", "imf_fdi",
  "imf_fiscaldecentralization", "imf_fm", "imf_fsire", "imf_gender_budgeting", "imf_gender_equality", "imf_gfscofog",
  "imf_gfse", "imf_gfsfalcs", "imf_gfsibs", "imf_gfsmab", "imf_gfsssuc", "imf_hpdd",
  "imf_mcdreo", "imf_namain_idc_n", "imf_pctot", "imf_pgcs", "imf_pgi", "imf_psbsfad",
  "imf_unsdg_imf_inputs", "imf_weo", "imf_whdreo", "imf_world", "insee_bdm", "ipea",
  "irena", "kof_globalization", "ksh", "ksh_stadat", "maddison", "nasa_giss", "nbp",
  "noaa", "nyfed", "oecd", "ofr", "owid", "oxcgrt",
  "penn_world_table", "pip", "polity", "pwt", "rba", "riksbank",
  "sec_edgar", "shiller", "sipri", "snb", "statcan", "stats_nz",
  "swiid", "tcmb", "transparency_ti", "treasury", "ucdp", "unctad_bopcaba",
  "unctad_ciocgeaia", "unctad_cioiuibbicoeair4a", "unctad_cpa", "unctad_cpia", "unctad_cpta", "unctad_fdiiaofasa",
  "unctad_fmcpa", "unctad_fmcpia21", "unctad_gasbeaiogasa", "unctad_gasbtbia", "unctad_gasbtoia", "unctad_gdpgbtoevbkoeatasa",
  "unctad_gdptapccac2pa", "unctad_lscia", "unctad_lsciq", "unctad_mfbcoboa", "unctad_mmcascioeaiopa", "unctad_mpcadioeaia",
  "unctad_mtba", "unctad_mttasa", "unctad_mttgra", "unctad_neera", "unctad_reericba", "unctad_reerigdba",
  "unctad_rfia", "unctad_rgdptapcgra", "unctad_sbeaiotsvsaga", "unctad_sbtisvsaga", "unctad_soigapotta", "unctad_sotwmfvbcoboa",
  "unctad_srbca", "unctad_tabbapotta", "unctad_tabmcioeaiopa", "unctad_tabmscioeaiopa", "unctad_tabpcioeaia", "unctad_taupa",
  "unctad_wstbtocabgoea", "undp_hdr", "unesco_clte", "unesco_cltt", "unesco_dem", "unesco_film",
  "unesco_inno", "unhcr", "usda", "wgi", "who_hwf", "who_rs",
  "who_sdg", "whr", "wikidata", "worldbank", "worldbank_esg", "worldbank_pink",
  "worldbank_wdi", "wto_hs_a_0010", "wto_hs_a_0015", "wto_hs_a_0020", "wto_hs_a_0025", "wto_hs_a_0030",
  "wto_hs_a_0040", "wto_its_mtv_am", "wto_its_mtv_ax", "yale_epi", "zillow",
  // 9 national-statistical PxWeb sources — flow-grain per-table publish (2026-07-22).
  "ssb", "stat_slovenia", "stat_latvia", "dst", "scb", "statfin", "hagstofa", "stat_estonia", "bfs",
];

// Languages with OFFICIAL, source-provided translations loaded into the catalog
// (stored at series.metadata.titles[<lang>]). 'en' is the native `title` column.
// Translations are NEVER machine-generated -- only labels the producer itself
// publishes. KEEP IN SYNC with api/devserver.py::_LANGS. A ?lang= outside this
// set is a 400 (we never silently hand back English for a language we lack).
export const SUPPORTED_LANGS: readonly string[] = ["en", "ar", "es", "fr", "ru", "zh"];

/** The provider segment of a catalog id: the first ':'-delimited token.
 *  Mirrors econdl._catalog.source_of. */
export function sourceOf(seriesId: string): string {
  const i = seriesId.indexOf(":");
  return i === -1 ? seriesId : seriesId.slice(0, i);
}

export function supportedSources(env: Env): Set<string> {
  if (env.SUPPORTED_SOURCES && env.SUPPORTED_SOURCES.trim()) {
    return new Set(env.SUPPORTED_SOURCES.split(",").map((s) => s.trim()).filter(Boolean));
  }
  return new Set(SUPPORTED_SOURCES);
}

const JSON_HEADERS: Record<string, string> = {
  "content-type": "application/json; charset=utf-8",
  "access-control-allow-origin": "*",
  "cache-control": "public, max-age=300",
};

const CSV_HEADERS: Record<string, string> = {
  "content-type": "text/csv; charset=utf-8",
  "access-control-allow-origin": "*",
  "cache-control": "public, max-age=300",
};

export function json(body: unknown, status = 200, extra?: Record<string, string>): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { ...JSON_HEADERS, ...(extra ?? {}) },
  });
}

export function csv(body: string, extra?: Record<string, string>): Response {
  // Exact UTF-8 byte length so the download gate can record real "data served"
  // (econ_download_log.bytes) without re-reading the streamed body.
  const bytes = new TextEncoder().encode(body).length;
  return new Response(body, {
    status: 200,
    headers: { ...CSV_HEADERS, "content-length": String(bytes), ...(extra ?? {}) },
  });
}

// --- honest-status error bodies (machine-readable `error` codes) -----------

/** 404: the id is not in the catalog. */
export function notFound(seriesId: string): Response {
  return json({ error: "not_found", series_id: seriesId, detail: "unknown series id" }, 404);
}

/** 501: the source has no resolver yet (loud, actionable). Mirrors ResolveError. */
export function notMigrated(source: string, seriesId: string): Response {
  return json(
    {
      error: "not_migrated",
      source,
      series_id: seriesId,
      detail:
        `source '${source}' has no at-rest resolver yet; the store is mid-migration. ` +
        `This series cannot be served until its source is wired up. Refusing to ` +
        `silently emit an empty response.`,
    },
    501,
  );
}

/** 502: the source is migrated (has a resolver) but its at-rest object/file is
 *  ABSENT (not published yet). Distinct from resolver_empty (object present, 0
 *  rows) and from not_migrated (no resolver at all). CONTRACT.md v1.1 status pin. */
export function dataUnavailable(source: string, seriesId: string): Response {
  return json(
    {
      error: "data_unavailable",
      source,
      series_id: seriesId,
      detail:
        `source '${source}' is migrated but the at-rest object for this series is ` +
        `not published yet. Refusing to silently emit an empty response.`,
    },
    502,
  );
}

/** 502: the id resolves to a file/object but it has zero rows. */
export function resolverEmpty(seriesId: string): Response {
  return json(
    {
      error: "resolver_empty",
      series_id: seriesId,
      detail:
        "the series id resolves to a stored object but it has zero observations. " +
        "Refusing to emit an empty series silently.",
    },
    502,
  );
}

/** 400: a filter the store cannot honor yet (never a silently unfiltered 200). */
export function unsupportedFilter(detail: string): Response {
  return json({ error: "unsupported_filter", detail }, 400);
}

export function badRequest(detail: string): Response {
  return json({ error: "bad_request", detail }, 400);
}

// --- license block: identical shape used by metadata.json and /v1/sources ---

export function licenseBlock(lic: LicenseRow | null): LicenseBlock | null {
  if (!lic || !lic.license_id) return null;
  return {
    id: lic.license_id,
    name: lic.name,
    url: lic.url,
    reservable: !!lic.reservable,
    commercial_ok: !!lic.commercial_ok,
    attribution_required: !!lic.attribution_required,
    no_modify: !!lic.no_modify,
  };
}

// --- cadence math: last_success + interval, HONEST about unknown cadences ---

// CONTRACT.md spells out {daily:1d, weekly:7d, monthly:30d, quarterly:91d}.
// The live state.db cadence vocabulary is {daily, weekly, monthly, annual,
// static, irregular, null}. We extend the contract's table with annual:365d
// (the only other deterministic cadence) and -- per "never fabricate a date" --
// return null next_update_expected for static/irregular/unknown/null cadences
// instead of inventing one. quarterly is kept for forward-compat though the
// current state.db has no quarterly rows.
const CADENCE_DAYS: Record<string, number> = {
  daily: 1,
  weekly: 7,
  monthly: 30,
  quarterly: 91,
  annual: 365,
};

/** last_success_utc + cadence interval -> ISO date (YYYY-MM-DD), or null when
 *  the cadence is non-deterministic / unknown / there is no last_success. */
export function nextUpdateExpected(
  lastSuccessUtc: string | null,
  cadence: string | null,
): string | null {
  if (!lastSuccessUtc || !cadence) return null;
  const days = CADENCE_DAYS[cadence];
  if (days === undefined) return null; // static / irregular / unknown -> honest null
  const t = Date.parse(lastSuccessUtc);
  if (Number.isNaN(t)) return null;
  const next = new Date(t + days * 86_400_000);
  return next.toISOString().slice(0, 10);
}

/** clamp(parse(limit), 1, max) with a default; for /v1/catalog pagination. */
export function clampInt(raw: string | null, dflt: number, min: number, max: number): number {
  if (raw === null) return dflt;
  const n = Number.parseInt(raw, 10);
  if (Number.isNaN(n)) return dflt;
  return Math.max(min, Math.min(max, n));
}

export function offsetInt(raw: string | null): number {
  if (raw === null) return 0;
  const n = Number.parseInt(raw, 10);
  return Number.isNaN(n) || n < 0 ? 0 : n;
}

// --- i18n: ?lang= resolution + official-title localization -----------------
// Byte-for-byte with api/devserver.py::_req_lang / _localized_title so the
// Worker and the dev shim localize identically.

/** Resolve ?lang=. Returns {lang:'en', error:null} for absent/'en' (the response
 *  is then byte-identical to the pre-i18n contract). For a language we have no
 *  official translations for, returns {lang:'en', error:<400 Response>}; the
 *  caller must return that error -- never a silent English fallback. */
export function reqLang(url: URL): { lang: string; error: Response | null } {
  const raw = (url.searchParams.get("lang") ?? "").trim().toLowerCase();
  if (!raw || raw === "en") return { lang: "en", error: null };
  if (!SUPPORTED_LANGS.includes(raw)) {
    return {
      lang: "en",
      error: json(
        {
          error: "unsupported_language",
          parameter: "lang",
          value: raw,
          detail: `no official translations loaded for '${raw}'`,
          supported: [...SUPPORTED_LANGS],
        },
        400,
      ),
    };
  }
  return { lang: raw, error: null };
}

/** The source-official title for `lang` (meta.titles[lang]) when present, else
 *  the English native title (graceful fallback). `meta` is the PARSED series
 *  metadata object (or null). lang None/'en' returns the English title. */
export function localizedTitle(
  meta: { titles?: Record<string, string> } | null | undefined,
  enTitle: unknown,
  lang: string | null,
): unknown {
  if (!lang || lang === "en") return enTitle;
  const titles = meta && typeof meta === "object" ? meta.titles : null;
  if (titles && typeof titles === "object" && titles[lang]) return titles[lang];
  return enTitle;
}
