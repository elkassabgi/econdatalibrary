// ---------------------------------------------------------------------------
// src/denylist.ts  --  redistribution gate (HTTP 451 + hidden from catalog).
//
// GENERATED FILE -- do not edit by hand. Regenerate with:
//     python -m core.gen_denylist        (then redeploy the worker)
//
// The set below is DERIVED FROM catalog.db so it can never drift from the site,
// which gates on license.reservable. A source is blocked iff its license is not
// verified-redistributable (license.reservable = 0), minus the granted
// exceptions below, plus a legacy safety floor so a regeneration never silently
// un-gates a previously-blocked source.
//
// GRANTED EXCEPTIONS (written permission on file -> kept downloadable):
//   kof_globalization -- Prof. Jan-Egbert Sturm (KOF director, index co-author),
//     2026-07-06: non-commercial academic re-hosting. Honor: NC use only; cite
//     "KOF, ETH Zurich"; link back to the official KOF Globalisation Index page;
//     no commercial resale/sublicensing; KOF may request removal.
//   comtrade -- UN Comtrade, 2026-07-07: our holdings sit in the free branch
//     ("up to 100,000 records"). STANDING GUARD: comtrade holdings must STAY
//     <= 100,000 records; growing past that leaves the free branch and requires
//     re-gating. Cite "UN Comtrade" + link back.
//
// NonCommercial-but-free sources (WHO, Yale EPI, WIID, FSI, GPI, IRENA, SNB,
// Freedom House, WTO stats) are governed by license.reservable in the DB; if any
// appear here it is because their license row is reservable=0 (unverified) --
// fix the license row and regenerate, don't special-case them here.
// ---------------------------------------------------------------------------


export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set([
  "adb",
  "barro_lee",
  "bea_full",
  "bfs",
  "bls_full",
  "bundesbank",
  "cboe",
  "cbs_nl",
  "central_banks",
  "cepii_baci",
  "cepii_gravity",
  "cftc",
  "cow",
  "cso",
  "damodaran",
  "dbnomics",
  "defillama",
  "dst",
  "ecb_sdmx",
  "edgar_13f",
  "edgar_insider",
  "edgar_pointers",
  "ei_statreview",
  "etr",
  "famafrench",
  "fao_fl",
  "fao_fs",
  "fao_gce",
  "fao_gel",
  "fao_gs",
  "fao_tp",
  "fdic",
  "fraser_efw",
  "fred",
  "fred_releases",
  "freedomhouse",
  "fsi",
  "gapminder",
  "gii",
  "gleif",
  "gpi",
  "gti",
  "gus",
  "gus_dbw",
  "hagstofa",
  "harvard_atlas",
  "ibge",
  "idb",
  "ilo",
  "imf_bop",
  "imf_cdis",
  "imf_cpis",
  "imf_dbnomics",
  "imf_dot",
  "imf_fsi",
  "imf_gfsr",
  "imf_ifs",
  "imf_irfcl",
  "imf_mfs",
  "ine_spain",
  "insee_melodi",
  "insee_sdmx",
  "insee_sirene",
  "irena",
  "istat",
  "ksh_hungary",
  "ksh_stadat",
  "nbp",
  "norgesbank",
  "ons_uk",
  "owid",
  "polity",
  "ppi",
  "pxweb",
  "pxweb_bfs",
  "qog",
  "scb",
  "sdmx_nso",
  "shiller",
  "sipri",
  "sipri_polity",
  "social_progress",
  "spi",
  "ssb",
  "stat_austria",
  "stat_estonia",
  "stat_latvia",
  "stat_slovenia",
  "statfin",
  "statsnz",
  "tcmb",
  "un_wpp",
  "unctad_ciocge",
  "unctad_cioiub",
  "unctad_fdi",
  "unctad_gdp",
  "unctad_gdptap",
  "unctad_iopa",
  "unctad_itiddsvsaga",
  "unctad_itiisvsaga",
  "unctad_mfbfora",
  "unctad_mfbforabtosa",
  "unctad_mmcasci",
  "unctad_mpcadi",
  "unctad_mtvv",
  "unctad_mtvvuvtotiappioea",
  "unctad_pcapsnopca",
  "unctad_pcapsnopcsa",
  "unctad_rgdptap",
  "unctad_sbeaiot",
  "unctad_sotwmfv",
  "unctad_svc",
  "unctad_tabmci",
  "unctad_tabmsci",
  "unctad_tabpci",
  "unctad_toi",
  "unctad_trade",
  "unctad_wstbt",
  "undp_hdr2",
  "unesco_natmon",
  "unesco_sci",
  "unesco_sdg",
  "unicef",
  "unsdg",
  "vdem",
  "who_gho",
  "whr",
  "wid",
  "wiid",
  "worldbank_extra",
  "worldbank_pink",
  "wto_bat_bv_m",
  "wto_bat_bv_x",
  "wto_hs_0010",
  "wto_hs_0015",
  "wto_hs_0020",
  "wto_hs_0025",
  "wto_hs_0030",
  "wto_hs_0040",
  "wto_hs_a_0010",
  "wto_hs_a_0015",
  "wto_hs_a_0020",
  "wto_hs_a_0025",
  "wto_hs_a_0030",
  "wto_hs_a_0040",
  "wto_its_mtv_am",
  "wto_its_mtv_ax",
  "zillow",
]);

/** The source id is the part of a series_id before the first ':'. */
export function seriesSource(seriesId: string): string {
  const i = seriesId.indexOf(":");
  return i < 0 ? seriesId : seriesId.slice(0, i);
}

export function isNonRedistributable(seriesId: string): boolean {
  return NON_REDISTRIBUTABLE.has(seriesSource(seriesId));
}

/**
 * Series-level carve-outs. The SOURCE is redistributable, but specific indicators
 * within it embed third-party data the source's licence does not cover, so those
 * series are gated individually. Keyed by source id -> indicator codes (the part
 * of a series_id between the first and second ':'). Hand-maintained; keep in sync
 * with THIRD_PARTY_CARVEOUTS.md.
 *
 * worldbank: GDP (NY.GDP.MKTP.CD) is World-Bank-compiled and served; CPI
 * (FP.CPI.TOTL.ZG) is IMF-sourced and unemployment (SL.UEM.TOTL.ZS) is ILO-sourced
 * -- WB terms bar redistributing third-party data, so those two are gated.
 */
export const SERIES_CARVEOUTS: Readonly<Record<string, readonly string[]>> = {
  worldbank: ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
};

function seriesIndicator(seriesId: string): string {
  const p = seriesId.split(":");
  return p.length > 1 ? p[1] : "";
}

/** True if this specific series is a third-party carve-out of a served source. */
export function isSeriesCarvedOut(seriesId: string): boolean {
  const carved = SERIES_CARVEOUTS[seriesSource(seriesId)];
  return carved ? carved.includes(seriesIndicator(seriesId)) : false;
}

/** Combined data gate: the whole source is non-redistributable, OR this specific
 *  series is a third-party carve-out. Every DATA endpoint must use this. */
export function isGated(seriesId: string): boolean {
  return isNonRedistributable(seriesId) || isSeriesCarvedOut(seriesId);
}

/** LIKE prefixes (`<source>:<indicator>:`) for SQL exclusion of carved series. */
export const SERIES_CARVEOUT_LIKE: readonly string[] = Object.entries(SERIES_CARVEOUTS)
  .flatMap(([src, inds]) => inds.map((ind) => `${src}:${ind}:`));
