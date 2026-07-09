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
// GRANTED (re-enabled): comtrade — UN Comtrade team replied 2026-07-07 ("In brief,
//   your use case should not require an additional license"), pointing to their
//   use-and-re-dissemination decision tree. Our store (24,086 rows, verified) sits in
//   the tree's explicit no-fee branch: free-of-charge data extraction "offering only
//   a small number of records (up to 100,000)". Conditions honored: cite the source
//   as "UN Comtrade" (their specified wording) + link back. STANDING GUARD: comtrade
//   holdings must STAY <= 100,000 records — growing past that leaves the free branch
//   and requires re-gating (or a premium re-dissemination license).
// GRANTED (re-enabled): whr — World Happiness Report / Gallup (REDACTED) replied
//   2026-07-09: "Since these data are in the public domain, you may proceed with the
//   attribution as you've outlined." Scope = the free Figure 2.1 summary (life-eval
//   3yr averages from 2012, 95% CIs, six-factor contributions). Conditions honored:
//   cite "Helliwell, Layard, Sachs, De Neve, Aknin & Wang, eds., World Happiness
//   Report 2026, University of Oxford: Wellbeing Research Centre" + link back to
//   worldhappiness.report and the Figure 2.1 download. THEIR ASK: send REDACTED the live
//   link once the econ library is public.
export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set([
  // Flatly not redistributable
  "qog", "cboe",
  // Permission required (written approval needed before re-hosting)
  "social_progress", "spi", "ei_statreview", "cow",
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
