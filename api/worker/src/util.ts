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
  // harvard_atlas (CC0 1.0, verified against all three Dataverse DOIs). LIVE and
  // updated daily but had ZERO catalog rows, so 255,217 series were fetched nightly
  // and served to nobody. Catalogued + derived 2026-07-27 (251,217 uploaded this
  // run, 0 failures; 255,217/255,217 verified present in R2 before this flip).
  "harvard_atlas",
  // gapminder (CC BY 4.0, declared in the repo README prose — no LICENSE file and
  // GitHub's licence API returns null, so automated probes find nothing). LIVE and
  // updated daily with ZERO catalog rows: 86,684 series served to nobody.
  // 86,684/86,684 verified in R2 and in D1 before this flip.
  "gapminder",
  // IMF DIRECT (imf-terms). Same IMF datasets we relay via DBnomics, pulled from
  // api.imf.org instead. 21,382 series; derive verified 21,382/21,382 present in R2
  // and in live D1 BEFORE this flip — CSVs first, flag second, because flag-first
  // turns a 501 into a 404 and a 404 says the series does not exist.
  "imf_afrreo_direct", "imf_apdreo_direct", "imf_cofer_direct", "imf_fas_direct",
  "imf_fdi_direct", "imf_whdreo_direct", "imf_world_direct",
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
  // unesco_natmon + unesco_sdg added 2026-07-29: 199,661 series / 2,610,984 obs that
  // were on disk and hosted NOWHERE — no catalog rows, no R2 objects, no registry unit.
  // Re-ingested direct from UIS, MISSING 0 / ORPHANED 0, values checked against parquet,
  // and un-pinned from the denylist floor on Ahmed's decision (the UIS terms are
  // publisher-wide, so their five cleared siblings above already covered them).
  // unesco_sci stays OUT: only 12 of its 1,230 indicator codes exist in the current UIS
  // API, so it cannot be kept current and would be a frozen 2019 snapshot.
  "unesco_inno", "unesco_natmon", "unesco_sdg", "unhcr", "usda", "wgi", "who_hwf", "who_rs",
  // wid added 2026-07-29 — the largest single source in the library: 2,465,197 series /
  // ~124M observations of World Inequality Database data that were held locally and
  // served to NOBODY. Added only after the derive COMPLETED and was verified
  // (catalog 2,465,197 == R2 CSVs 2,465,197, missing 0); listing it earlier would have
  // served 404s. CC BY-NC-SA 4.0 + written grant (Alice, info@wid.world, 2026-07-27).
  "wid",
  // imf_fsi (73,288) + adb (53,458) added 2026-07-29 — two of the four series-shaped,
  // licence-verified sources that were idle and catalogued nowhere. Both derived and
  // verified before listing (MISSING 0, ORPHANED 0). adb's terms are KIDB's own, not
  // the ADB Data Library's, and its attribution carries KIDB's prescribed citation.
  "imf_fsi", "adb",
  // cso (Ireland) — flow-grain per-table publish, 7,896 tables / 49,057,386 rows
  // (2026-07-29). Table grain is what makes it hostable: per SERIES it is 9,993,368 keys
  // at 4.90 obs/series because CSO publishes many short cross-sectional tables, and at
  // table grain a thin table costs exactly one row and one CSV, same as a dense one.
  // 92 of its tables span two subject parquets; those are assembled before upload and all
  // 92 were verified row-for-row against the store (see ledger R133), because presence
  // checks pass on a truncated file.
  "cso",
  // insee_melodi — flow-grain per-dataflow publish, 139 flows / 36,436,053 rows
  // (2026-07-29). Flow grain because no codelist RETRIEVAL PATH was found — /codelist/all and
  // /dsd/{id} both 404 and a flow's catalog entry only names a DSD. That is "not found", NOT
  // "does not exist" (ledger R145); if a path turns up, per-series titles become possible.
  // On what is known today, per-series ids could only be titled with the key
  // itself — 21.3M rows of opaque codes. The flow is the unit INSEE actually titles, and
  // 134/139 carry its own label. It also hosts the source WHOLE: 84 flows are single-period
  // censuses (DS_BPE*, DS_FLORES_*) that are honest cross-sectional micro-data, not the
  // date-in-the-key defect ons_uk has. Licence audited 2026-07-29 (etalab-2.0, same
  // publisher and API host as the already-confirmed insee_bdm).
  "insee_melodi",
  // ons_uk — dataset-grain publish, 42 datasets / 25,408,157 rows / 3,897,884 series
  // (2026-07-29), after the approved re-key. The stored ids used to embed the observation
  // date AND `CV` (a coefficient of variation — a property of one measurement), so every
  // row was its own series: 25,408,157 rows, 25,408,157 keys, and a cursor dict that hit
  // 32.26 GB RSS on a 16 GB runner. Re-keyed to 3,897,884 real series with 0 (key,date)
  // collisions. Dataset grain is right, but the reason first written here — "ONS publishes no
  // per-series title" — was WRONG (ledger R145). ONS publishes dimension `label` fields and a
  // per-dimension `options` endpoint, and the label columns sat beside the code columns in the
  // very CSVs re-keyed above. Dropping labels from the KEY is correct (ONS can re-word a display
  // string, and baking it into an id invites silent re-keying); extending that to the TITLE was
  // not. Codes belong in ids, labels in titles. All 42 datasets carry ONS's own dataset title;
  // the per-series dimension labels remain unused pending that fix.
  // OGL v3.0; the "some content is exempt" carve-out resolves to photographs and video,
  // which cannot reach a statistical series.
  "ons_uk",
  // un_wpp — UN World Population Prospects 2024, 334,236 series (2026-07-29). Derive
  // verified both directions (catalog 334,236 == R2 334,236, MISSING 0, ORPHANED 0) and the
  // download body confirmed. Titles are CURRENTLY the native key — and the comment here used
  // to justify that with "WPP publishes no per-series title", which is WRONG and is corrected
  // rather than quietly deleted (ledger R145). The INDICATOR long name is genuinely absent from
  // WPP's CSVs, but the COUNTRY name is present: ingest_un_wpp.py reads it at line 100 and
  // discards it at line 128 because ISO3 is set. So readable titles ARE derivable from the
  // publisher's own file and this source is under-titled pending that fix. Date ranges
  // ARE real (computed per series from the two published parquets, 334,236/334,236 dated).
  // CC BY 3.0 IGO — and note the licence nearly went the other way: un.org's site-wide notice
  // reads "All rights reserved", but the Population Division publishes its own CC BY 3.0 IGO
  // grant on the WPP download page, which governs this product. D1 held a pre-audit
  // NEEDS-REVIEW default for this source; that divergence was corrected before serving.
  "un_wpp",
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
