"""gen_denylist.py — regenerate api/worker/src/denylist.ts from catalog.db.

Single source of truth for the redistribution gate. Historically the worker's
NON_REDISTRIBUTABLE set was hand-maintained and drifted from the site, which
gates on license.reservable — so a source could read "metadata only" on the
page yet still serve its .csv (observed 2026-07-14: transparency_ti and 141
other reservable=0 sources were downloadable). This script makes the gate
DB-derived so the two can never disagree again:

    NON_REDISTRIBUTABLE = { every source_id with license.reservable = 0 }
                          ∪ LEGACY_KEEP            (never silently un-gate)
                          − GRANTED_EXCEPTIONS     (written permission on file)

Run:  python -m core.gen_denylist        (from the econ repo root)
Then redeploy the worker.  Re-run whenever license flags change.
"""
from __future__ import annotations

import os
import sqlite3

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
DB = os.path.join(ROOT, "data", "catalog.db")
OUT = os.path.join(ROOT, "api", "worker", "src", "denylist.ts")

# Sources that carry WRITTEN redistribution permission — kept downloadable even
# though their license row is still the conservative NEEDS-REVIEW default.
# Provenance lives in the generated header below; keep the two in sync.
GRANTED_EXCEPTIONS = {
    "kof_globalization",  # Prof. Jan-Egbert Sturm (KOF director), 2026-07-06: NC academic re-hosting
    "comtrade",           # UN Comtrade, 2026-07-07: free branch, holdings must stay <= 100k records
}

# PERMANENT series-level carve-outs that must survive EVERY regeneration of the
# FOOTER template below. Guard added 2026-07-20: the 2026-07-16 regen (DeFiLlama
# un-gate, commit be939627f) silently WIPED the worldbank_pink carve-outs that
# commit 5fc56cea1 had added by hand to denylist.ts, because this template only
# carried the worldbank entry. main() asserts each of these appears in the
# generated SERIES_CARVEOUTS block and refuses to write the file otherwise.
REQUIRED_CARVEOUTS = {
    "worldbank": ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
    # Same third-party indicators were served through worldbank_wdi because the
    # carve-out was keyed on `worldbank` alone (live leak confirmed 2026-07-22:
    # worldbank_wdi:SL.UEM.TOTL.ZS served 401 while worldbank's copy was gated).
    "worldbank_wdi": ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
    "worldbank_pink": ["aluminum", "copper", "nickel", "zinc",
                       "gold", "platinum", "silver"],   # LME/LBMA written refusals 2026-07-15
}

# Ids that were explicitly gated by hand and must never be dropped even if they
# are not (or no longer) reservable=0 in the DB (e.g. phantom/renamed ids). This
# is a safety floor: unioning it guarantees the regenerated set never UN-gates
# anything the previous curated denylist blocked.
LEGACY_KEEP = {
    "qog", "cboe", "dbnomics",
    # imf_dbnomics was gated only because its licence row happens to be reservable=0.
    # It was NOT on the floor, so deleting/reclassifying its source row would silently
    # drop it from the gate on the next regeneration (the assertions below would not
    # catch it). It is a live monthly ingest (updater/registry.yaml) feeding
    # imf_ifs/imf_dot/imf_bop, and a DBnomics passthrough, so it must stay pinned.
    "imf_dbnomics",
    "wto_hs_a_0010", "wto_hs_a_0015", "wto_hs_a_0020", "wto_hs_a_0025",
    "wto_hs_a_0030", "wto_hs_a_0040", "wto_its_mtv_am", "wto_its_mtv_ax",
    "whr", "social_progress", "spi", "cow",
    # ei_statreview REMOVED from the floor 2026-07-22: the Energy Institute GRANTED
    # written permission (REDISTRIBUTION_EMAIL_TRAIL.md). Its binding exclusion --
    # "no S&P Global Platts / Commodity Insights price series" -- is satisfied by
    # construction: the 18,464 series we hold span 127 measures that are ALL energy
    # volumes/shares/per-capita/changes plus gdp, population and emissions; a full-
    # population scan found ZERO price-unit or benchmark markers ($/, /bbl, MMBtu,
    # spot, Brent, WTI, Dubai, Henry Hub, TTF, JKM) in any title. The only titles
    # containing "price" are `gdp` ("international-$ in 2011 prices" = a constant-
    # price deflator, not a commodity price). Licence row now encodes the other
    # conditions: reservable=1, commercial_ok=0 (NC only), attribution_required=1.
    # Standing obligation: annual June refresh when EI publishes the new edition.
    "famafrench", "fraser_efw", "polity", "sipri", "sipri_polity", "tcmb",
    "nbp", "wid", "sdmx_nso",
    # Purged from the catalog 2026-07-23 (cannot host -> must not live in the DB). Their rows
    # are gone, so they no longer appear via the reservable=0 scan and would have SILENTLY
    # dropped out of the gate -- verified: they DID leak on the first regeneration after the
    # purge. Pinned so a future re-ingest can never land un-gated.
    #   irena        audit unclear_not_found / NEEDS HUMAN REVIEW
    #   freedomhouse "third-party re-hosting for open public download is not authorized"
    #   shiller      unclear_not_found; gate+email pending
    "irena", "freedomhouse", "shiller",
    # barro_lee REMOVED from the floor 2026-07-22. It was originally gated as
    # "unclear -- gate until confirmed"; DATABASE_LICENSES_VERBATIM.md has since
    # CONFIRMED it `redistributable_attribution` / "CLEARED - re-host OK (attribution)",
    # so the floor's purpose (don't un-gate an unconfirmed source) no longer applies.
    # This is a deliberate, human-authorised un-gate (Ahmed, 2026-07-22) -- NOT a silent
    # regeneration drop. It now carries its own licence row `custom-terms-barro_lee`
    # (reservable=1) so the shared `custom-terms` row stays reservable=0 for SIPRI/Cboe/etc.
}

HEADER = '''// ---------------------------------------------------------------------------
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

'''

FOOTER = '''
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
  // worldbank_wdi carries the SAME third-party indicators as worldbank, but the
  // carve-out was keyed only on `worldbank` — so IMF-sourced CPI and ILO-sourced
  // unemployment were SERVED through worldbank_wdi, bypassing the control.
  // Confirmed LIVE 2026-07-22: worldbank_wdi:SL.UEM.TOTL.ZS returned 401 (served)
  // while the identical indicator was gated under worldbank. Same WB terms apply:
  // third-party data may not be redistributed regardless of which id carries it.
  worldbank_wdi: ["FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"],
  // worldbank_pink aggregates third-party benchmark prices. LME (base metals)
  // and LBMA/IBA (precious metals) REFUSED redistribution in writing on
  // 2026-07-15 (REDISTRIBUTION_EMAIL_TRAIL.md) — these series must never
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
'''


def main() -> None:
    c = sqlite3.connect(DB)
    reservable0 = {
        r[0] for r in c.execute(
            "SELECT s.source_id FROM source s "
            "JOIN license l ON l.license_id = s.license_id "
            "WHERE l.reservable = 0"
        )
    }
    all_sources = {r[0] for r in c.execute("SELECT source_id FROM source")}
    c.close()

    gated = (reservable0 | LEGACY_KEEP) - GRANTED_EXCEPTIONS
    real = sorted(s for s in gated if s in all_sources)
    phantom = sorted(s for s in gated if s not in all_sources)  # kept but flagged

    lines = []
    lines.append(HEADER)
    lines.append("export const NON_REDISTRIBUTABLE: ReadonlySet<string> = new Set([")
    for sid in real:
        lines.append(f'  "{sid}",')
    if phantom:
        lines.append("  // legacy/phantom ids (not currently in the catalog; kept as a safety floor):")
        for sid in phantom:
            lines.append(f'  "{sid}",')
    lines.append("]);")
    lines.append(FOOTER)
    text = "\n".join(lines)

    # REGENERATION GUARD (fail closed BEFORE writing): every permanent carve-out
    # must be present in the generated SERIES_CARVEOUTS block, or we refuse to
    # write at all — a template edit can never silently drop a written refusal.
    block = text.split("export const SERIES_CARVEOUTS", 1)[1].split("};", 1)[0]
    for src, inds in REQUIRED_CARVEOUTS.items():
        assert f"{src}:" in block, (
            f"REFUSING to write: carve-out source '{src}' missing from the "
            f"generated SERIES_CARVEOUTS (template regression — see 5fc56cea1)")
        for ind in inds:
            assert f'"{ind}"' in block, (
                f"REFUSING to write: carve-out {src}:{ind} missing from the "
                f"generated SERIES_CARVEOUTS")

    with open(OUT, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)

    print(f"reservable=0 sources:        {len(reservable0)}")
    print(f"legacy-keep (union):         {len(LEGACY_KEEP)}")
    print(f"granted exceptions (remove): {sorted(GRANTED_EXCEPTIONS)}")
    print(f"-> NON_REDISTRIBUTABLE size: {len(real)} real + {len(phantom)} phantom = {len(real)+len(phantom)}")
    print(f"wrote {OUT}")
    # sanity: the granted ones must NOT be gated; a known leaker MUST be gated
    for must_serve in GRANTED_EXCEPTIONS:
        assert must_serve not in real and must_serve not in phantom, must_serve
    # Known-restricted per the verbatim license audit (2026-07-14) MUST stay gated:
    # WTO refused in writing; cboe/sipri/polity/famafrench = permission-required.
    # (transparency_ti was formerly gated-unverified; the audit CONFIRMED its explicit
    #  redistribution grant "Anyone can extract, download, and make copies... and may
    #  also share that information with third parties", so it is now correctly served.)
    for must_gate in ("wto_hs_a_0010", "cboe", "sipri", "polity", "famafrench"):
        assert must_gate in real or must_gate in phantom, f"{must_gate} must be gated"
    print("sanity OK: grants excluded; known-restricted (WTO/cboe/sipri/...) gated")


if __name__ == "__main__":
    main()
