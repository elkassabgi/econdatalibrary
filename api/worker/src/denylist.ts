// ---------------------------------------------------------------------------
// src/denylist.ts  --  redistribution gate (added 2026-07-06 after the
// redistributability audit). These sources' licences do NOT permit third-party
// redistribution, so their DATA (.csv) is hard-blocked (HTTP 451) and they are
// hidden from catalog search. Metadata (a pointer to the original source) stays
// available. Permission-request emails are out to each provider — re-enable a
// source here the moment written permission is granted.
//
// NOT the NonCommercial sources (WHO, Yale EPI, WIID, FSI, GPI, IRENA, SNB,
// Freedom House, the WTO stats series): those are kept under the free/non-
// commercial policy and are intentionally NOT listed here.
// ---------------------------------------------------------------------------

// GRANTED (re-enabled): kof_globalization — Prof. Jan-Egbert Sturm (KOF director,
//   index co-author) granted non-commercial academic re-hosting on 2026-07-06.
//   Conditions (must be honored in the site's KOF attribution/citation): NC use only;
//   cite "KOF, ETH Zurich"; include a clear link back to the official KOF Globalisation
//   Index page; no commercial resale/sublicensing; KOF may request removal.
export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set([
  // Flatly not redistributable
  "qog", "cboe",
  // Permission required (written approval needed before re-hosting)
  "social_progress", "spi", "whr", "ei_statreview", "comtrade", "cow",
  "famafrench", "fraser_efw", "polity", "sipri", "sipri_polity", "tcmb",
  "wto_bat_bv_x", "wto_hs_0010", "wto_hs_0015", "wto_hs_0020", "wto_hs_0025",
  "wto_hs_0030", "wto_hs_0040", "wto_hs_a_0010", "wto_hs_a_0040",
  // Unclear — gate until the provider confirms redistribution terms
  "nbp", "wid", "barro_lee", "sdmx_nso",
]);

/** The source id is the part of a series_id before the first ':'. */
export function seriesSource(seriesId: string): string {
  const i = seriesId.indexOf(":");
  return i < 0 ? seriesId : seriesId.slice(0, i);
}

export function isNonRedistributable(seriesId: string): boolean {
  return NON_REDISTRIBUTABLE.has(seriesSource(seriesId));
}
