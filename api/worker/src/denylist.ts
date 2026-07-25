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
  "central_banks",
  "fraser_efw",
  "fred_releases",
  "fsi",
  "imf_dbnomics",
  "pxweb_bfs",
  "sdmx_nso",
  "sipri_polity",
  "social_progress",
  "spi",
  "stat_austria",
  "wiid",
  "wto_bat_bv_m",
  "wto_bat_bv_x",
  "wto_hs_0010",
  "wto_hs_0015",
  "wto_hs_0020",
  "wto_hs_0025",
  "wto_hs_0030",
  "wto_hs_0040",
  // legacy/phantom ids (not currently in the catalog; kept as a safety floor):
  "cboe",
  "cow",
  "dbnomics",
  "famafrench",
  "fred",
  "freedomhouse",
  "gus",
  "ibge",
  "ine_spain",
  "irena",
  "nbp",
  "norgesbank",
  "polity",
  "qog",
  "shiller",
  "sipri",
  "tcmb",
  "unesco_natmon",
  "unesco_sci",
  "unesco_sdg",
  "unicef",
  "unsdg",
  "vdem",
  "who_gho",
  "whr",
  "wid",
  "wto_hs_a_0010",
  "wto_hs_a_0015",
  "wto_hs_a_0020",
  "wto_hs_a_0025",
  "wto_hs_a_0030",
  "wto_hs_a_0040",
  "wto_its_mtv_am",
  "wto_its_mtv_ax",
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
 * with permission records (held privately).
 *
 * worldbank: GDP (NY.GDP.MKTP.CD) is World-Bank-compiled and served; CPI
 * (FP.CPI.TOTL.ZG) is IMF-sourced and unemployment (SL.UEM.TOTL.ZS) is ILO-sourced
 * -- WB terms bar redistributing third-party data, so those two are gated.
 */
export const SERIES_CARVEOUTS: Readonly<Record<string, readonly string[]>> = {
  worldbank: ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
  // worldbank_wdi carries the SAME third-party indicators as worldbank, but the
  // carve-out was keyed only on `worldbank` — so IMF-sourced CPI and ILO-sourced
  // unemployment were SERVED through worldbank_wdi, bypassing the control.
  // Confirmed LIVE 2026-07-22: worldbank_wdi:SL.UEM.TOTL.ZS returned 401 (served)
  // while the identical indicator was gated under worldbank. Same WB terms apply:
  // third-party data may not be redistributed regardless of which id carries it.
  worldbank_wdi: ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
  // worldbank_pink aggregates third-party benchmark prices. LME (base metals)
  // and LBMA/IBA (precious metals) are not licensed for redistribution as of
  // 2026-07-15 (permission records (held privately)) — these series must never
  // serve, even if the source is un-gated later. Cocoa (ICCO), coffee (ICO)
  // and cotton (Cotlook) origins are still awaiting permission replies.
  worldbank_pink: ["aluminum", "copper", "nickel", "zinc", "gold", "platinum", "silver"],
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
