"""Environment + path configuration (local-now / cloud-later).

A single env var, AQUEDUCT_BACKEND=local|r2, selects the Blob backend (see
updater/blob.py; 'r2' rather than 'cloud' — D1-native state is a v1 non-goal).
Everything else (registry, strategies, orchestrator) is identical.

Every path hangs off ECONDL_ROOT (env) so CI runners can relocate the whole tree;
the local default is the repo root containing this package. No absolute drive
letters may appear in this module — CI has no D: (UPDATER_BUILD_PLAN.md §1.3).
"""
from __future__ import annotations
import os

ROOT = os.path.abspath(os.environ.get("ECONDL_ROOT")
                       or os.path.join(os.path.dirname(__file__), ".."))
BACKEND = os.environ.get("AQUEDUCT_BACKEND", "local")  # local | r2

# MEASURED registry size, pinned by the §1.3 reconciliation run on 2026-07-03 —
# never copied from a doc (honesty rule §5.6). Adding or retiring a source requires
# re-measuring and updating both files in the same commit; orchestrate.run_once()
# refuses to run when the registry disagrees.
# 2026-07-03: 130 unique source_ids (the capability matrix's "133" is script-profile
#   ROWS; full diff in updater/REGISTRY_RECONCILIATION.md).
# 2026-07-06: +3 -> 133. IEP granted CC BY-NC-SA 4.0 non-commercial re-hosting, so
#   gti (Global Terrorism Index), ppi (Positive Peace Index) and etr (Ecological
#   Threat Report) were added alongside gpi (which was already counted; its dead
#   URLs were repaired with the granted IEP source). See [[project_redistributability]].
# 2026-07-22: -10 -> 123. Ten sources we are NOT permitted to re-host were purged from
#   the catalog, but the crawler kept fetching them daily: cow, sipri, cboe, famafrench,
#   nbp, tcmb, irena, freedomhouse, shiller, whr. Crawling data we can never serve wastes
#   the run, and for the providers who declined us in writing it means hitting their API
#   every day after they said no. Removed from registry.yaml; the ingest scripts stay on
#   disk so any future permission is a re-add, not a rewrite. NOTE sipri_polity is a
#   DIFFERENT source (Polity) and deliberately remains.
# 2026-07-23: -10 -> 113. Ahmed's ruling: permission emails went out
#   ~2026-07-08 and two weeks of silence is a NO. Sources we may not host -- refused,
#   silent, or never assessed -- are DELETED, not gated, and must stop being crawled or
#   the daily run just re-uploads them: fred, gus, ibge, ine_spain, norgesbank, qog, unsdg, vdem, who_gho, wid.
#   Also -2 same day: fred_releases and sdmx_nso were still being crawled while GATED, so a
#   run would have re-uploaded to R2 exactly what the purge deleted.
#   Also -6 same day: gated sources with no adapter -- we may not host them, so building a
#   fetcher would be work in service of data we must delete: central_banks, fraser_efw, imf_dbnomics, social_progress, spi, wiid.
# 2026-07-28: +7 -> 112. IMF DIRECT. Seven datasets we were relaying through
#   DBnomics now come from api.imf.org itself: imf_fdi_direct, imf_fas_direct,
#   imf_world_direct, imf_afrreo_direct, imf_apdreo_direct, imf_cofer_direct,
#   imf_whdreo_direct. ADDITIONS, not replacements — IMF retired IFS and re-keyed
#   these datasets, so overwriting the existing imf_<flow> ids would break thousands
#   of live series ids to buy freshness. Verified equal-or-better coverage BEFORE
#   registering (FAS ~2x our series, WORLD +43%, FDI exact, AFRREO ~100%). MCDREO
#   and FM are deliberately excluded: direct serves 57% and 9% of what the relay
#   does, and shipping a thinner source under a "direct" label is a regression.
#   This guard worked exactly as intended — it refused every run the moment the
#   count moved, which is why the number is ASSERTED here and not inferred from the
#   file it is meant to protect.
# 2026-07-28: 112 -> 114. imf_hpdd (191 series) and imf_fiscaldecentralization
#   (8,398) were SERVED and downloadable with NO registry entry at all — never
#   attempted, so they could not even go stale. IMF had renamed their flows
#   (HPDD -> HPD, FISCALDECENTRALIZATION -> FD) and an exact-id lookup read the miss
#   as "discontinued" (ledger R75). Both are repairs IN PLACE, not additions: every
#   live id is preserved (191/191 and 8,398/8,398), proven by value agreement across
#   shared observations rather than by matching code names, and the fetcher
#   re-verifies that >=95% of catalog ids are reproduced before merging anything.
#   Eight more sit behind the same renames (imf_psbsfad, imf_pctot, imf_bopagg,
#   imf_gender_*, imf_pgi, imf_pgcs, imf_unsdg_imf_inputs — 36,390 series); they are
#   deliberately NOT registered yet because each needs that same proof first.
# 2026-07-28 (later): 114 -> 115. fao_qcl, the first of the FAO family to be wired.
#   ALL 25 fao_* sources (136,754 series) are served and downloadable with no
#   registry entry — never attempted, exactly like the IMF ten. The family hid
#   behind the registry's separate `faostat` entry, which is a DIFFERENT source.
#   Repair in place: the DBnomics-era ids ARE FAOSTAT's own codes, so 98.2% of
#   20,238 published ids reproduce exactly, values agree 92.22% across 988,719
#   shared points, and upstream carries 78,944 series to 2024 against our 20,238
#   to 2022. The remaining 24 follow one at a time, each proved the same way.
# 2026-07-28 (later still): 115 -> 119. fao_fo, fao_pp, fao_oa, fao_et join fao_qcl.
#   Of the 12 fao_* sources whose code still matches a live FAOSTAT dataset, exactly
#   five reproduce their published ids cleanly (96-100%) and are repaired in place;
#   the other seven are REFUSED by the prover rather than shipped (gt 27.2%, rp
#   84.0%, gn 83.5%, gf 30.7%, ic 44.7%, ae 0%, af 0%) because a template that
#   reproduces only part of the id space mints a parallel one beside the live series
#   and reports success. The remaining 13 fao_* sources are old FAOSTAT domains that
#   were consolidated upstream (QL+QP+QA merged into QCL; the old emissions domains
#   into the Agrifood-systems family) and need their own mapping work.
# 2026-07-28 (final): 119 -> 121. fao_qa and fao_qp, recovered from a dataset they
#   were CONSOLIDATED INTO. FAOSTAT merged QL, QP and QA into QCL, and a literal key
#   comparison against QCL matched 0% — because our ids are item.area.element where
#   QCL's own are element.area.item. Same three codes read backwards; the apparent
#   absence was a shape mismatch, not missing data (R75 again). The permutation
#   search recovers them at 99.2% and 98.5%. fao_ql (84.3%) and fao_ge (11.1%) are
#   REFUSED — a partial template forks the id space silently.
# 2026-07-28: 121 -> 122. wid — the largest single unlock in the library's history:
#   124,367,162 observations across 2,465,197 series that were held locally and
#   served to NOBODY, because WID publishes no licence text and re-hosting on an
#   assumption was not acceptable. Resolved by reading WID's own rel="license" link
#   (CC BY-NC-SA 4.0) plus written permission dated 2026-07-27. The permission came
#   with a condition — "keep the most updated data sources" — which is why a fetcher
#   was written rather than just publishing the snapshot we already had.
# 2026-07-29: 122 -> 123. unesco_dem — it HAD no registry entry at all, which is why
#   it never updated: the orchestrator only ever iterates registered units, so an
#   unregistered source is not "failing", it is invisible. Its 7,080 series came in
#   through the DBnomics bulk ingest and froze when DBnomics stopped re-indexing
#   UNESCO on 2022-04-04. Now fetched direct from UIS (99.48% of published ids
#   reproduced before wiring). Registered live=false — it earns the tier by proving a
#   delta on a --force dispatch. NOTE: unesco_clte/cltt/film/inno remain unregistered
#   and therefore still frozen; the current UIS API exposes too few of their indicator
#   codes (5.1%, 0%, 1.3%, 0%) to rebuild them from this endpoint, so they need a
#   different route (bulk downloads / SDG database), not a registry line.
# 2026-07-29: 123 -> 125. unesco_natmon + unesco_sdg — 2,610,984 observations across
#   199,661 series that were in the local store and hosted NOWHERE: no catalog rows,
#   no objects on R2, a denylist entry, and no registry unit, so nothing could even
#   report them as missing. Both rebuild exactly from the live UIS API (natmon: 420 of
#   421 live indicators agree; sdg: 68,067 exact id matches). Registered live=false —
#   they are not promoted until the data is actually SERVED, because refreshing a
#   source nobody can download is motion without delivery.
#   unesco_sci (759,045 obs) is deliberately NOT registered: only 12 of its 1,230
#   indicator codes exist in the current UIS API, so it cannot be kept current from
#   this endpoint and hosting it would mean publishing a 2019 snapshot that can never
#   update. Needs a different route or an explicit frozen-archive decision.
# 2026-07-30: 125 -> 134, plus a warning about THIS TRIPWIRE'S OWN FAILURE MODE. Nine IMF
#   direct-from-api.imf.org sources were added across two commits — 7c82c08 (imf_fsic_direct,
#   imf_fsibsis_direct, imf_fsicdm_direct: the FSI family) and b25e9c5 (imf_gfsbs_direct,
#   imf_gfscofog_direct, imf_gfssfcp_direct, imf_gfssoef_direct, imf_gfssoo_direct,
#   imf_gfsssuc_direct: the GFS family) — WITHOUT bumping this constant.
#   A stale count here does not warn and does not degrade: registry.validate() reports it and
#   orchestrate.py raises SystemExit, so from 7c82c08 (01:37 UTC) EVERY run would abort before
#   touching a single source. A counter takes the whole refresh offline. Caught at 03:37 UTC
#   with the 06:00 cron still pending, so no scheduled run actually hit it.
#   The tripwire is worth keeping — it is what stops sources being added silently — but it has
#   to fail at PUSH time rather than at 06:00 UTC. tests/test_registry_count.py now does that.
# 2026-07-30: 134 -> 137. who_hwf + who_rs + who_sdg (34,788 series). These were SERVED and
#   downloadable with no registry entry, so they had never once been attempted. They are
#   registered against DBnomics rather than WHO's own API for a specific, measured reason: our
#   ids ARE DBnomics series codes (WHO_HWF:HWF_0001.AFG.A), and DBnomics' WHO index was re-run
#   2026-07-24 — six days before this entry. That is the exception, not the rule: the same
#   audit found UNCTAD frozen at 2023-06-30, FAO at 2024-05-09 and UNESCO at 2022-04-04, and
#   for those a DBnomics-backed fetcher would run nightly, succeed, and transfer nothing.
#   Id reproduction was checked as a FULL set comparison on WHO/HWF: 4,421 upstream codes vs
#   4,421 published ids, 4,421 exact, 0 missing either direction.
# 2026-07-30: 137 -> 138. boc (Bank of Canada Valet, 12,862 series / 2.73M obs). It had no
#   ingest script AND no registry entry, so it had never had an updater path at all. Registered
#   against Valet directly, not DBnomics: Valet's own series names ARE our series_keys, verified
#   as a full set comparison (12,862 of 12,862 exact, 0 missing upstream). DBnomics' BOC provider
#   was last indexed 2025-02-15 and, more to the point, a matching provider NAME is not
#   provenance (see the bea trap in tools/audit_upstream_liveness.py).
# 2026-07-30: 138 -> 139. snb (Swiss National Bank, 12 cubes / 762 series / 303,358 obs). No
#   ingest script and no registry entry, so it had never had an updater path. Each cube CSV
#   carries its own PublishingDate, which is a publisher-supplied per-cube vintage - better than
#   any HTTP validator. Keys and dates both verified at 100% against the existing store before
#   wiring (762/762 keys, 303,358/303,358 rows).
# 2026-08-04: 141 -> 144. imf_bop_direct, imf_irfcl_direct, imf_cpi_direct — the three IMF
#   datasets pulled first-hand from api.imf.org (SDMX 2.1) instead of a relay. Each has a
#   hand-pinned entry because the _direct family is absent from UPDATE_CAPABILITY_MATRIX.json,
#   and each vintage_signal records a CALLED value (BOP 21.0.0, IRFCL 12.0.0, CPI 5.0.0).
#
#   I ADDED THE ENTRIES AND NOT THIS NUMBER, AND THAT TOOK THE WHOLE UPDATER DOWN. registry
#   validation runs before any source does, so from that commit until this one every run — cloud
#   and local — exited 1 at "expected 141 sources, found 144" having fetched NOTHING. It is not
#   a warning and it does not skip the offending entries; it refuses the run. The guard is right
#   (a silently-appearing source is exactly what it exists to catch); updating it is part of
#   adding a source, not an afterthought. Ledger R347.
# 2026-08-05: +imf_imts_direct (IMTS v1.0.0 called live; DOTS successor) -> 145
# 2026-08-05: +imf_pip_direct (PIP v5.0.0 called live; CPIS successor) -> 146
# 2026-08-05: +imf_dip_direct (DIP v12.0.1 called live; CDIS successor) -> 147
# 2026-08-06: +imf_mcdreo_direct (MCDREO v8.0.0 called live; agency IMF.MCD; Class B1 of the
#   authorized retirement plan — Ahmed's "match the publisher" ruling resolves the 57% scope) -> 172
# 2026-08-06: +imf_fm_direct (FM v5.0.0 called live; agency IMF.FAD, NOT IMF.STA — read from the
#   /dataflow catalogue with MCDREO as the present-control. Last genuinely-missing Class B1 build;
#   the direct scope is ~9% of relay-era imf_fm, resolved by the same "match the publisher"
#   ruling. Ignore the dated FM_2025_OCT_VINTAGE flow) -> 173
# 2026-08-06: +whr RE-ADDED (cycle 35, Ahmed's "trim whr"): the Gallup/WHR written grant covers
#   exactly the Figure 2.1 workbook, and the fetcher is rewritten to the PUBLISHER's own file
#   host (files.worldhappiness.report, real ETag/CL validators) — never OWID (R215). New keys
#   FIG21:* in whr_fig21.parquet; legacy OWID-era rows stay unserved. -> 174
# 2026-08-06: +norgesbank RE-ADDED (cycle 37, Ahmed's answer #5): purged 2026-07-23, then
#   audit-CLEARED (NLOD 2.0) and authorized to serve; entry recovered from git 8d182e5a^;
#   first run backfills the purged store (API probed live). -> 175
# 2026-08-06: +unsdg RE-ADDED (cycle 38, same authorization): UNdata terms grant redistribution
#   verbatim; fetcher reworked with rotation (R190) + chunked merges (R249), self-bounds at
#   200 codes/run; 4 rotated runs backfill all 713 (API live, release 2026.Q2.G.01). -> 176
# 2026-08-07: -imf_hpdd, -imf_fiscaldecentralization RETIRED (Class A quartet, Ahmed's
#   permission granted this hour): both fully absorbed by their EXACT-count direct successors
#   (imf_hpd_direct 191, imf_fd_direct 8,398); data plane retired archive-first via
#   tools/retire_source.py, D1+R2 clean. psbsfad/pctot had no registry entries. -> 174
# 2026-08-07: -imf_fsi, -imf RETIRED (Class A batches 2-3 complete — ALL 33 legacy IMF sources
#   retired archive-first tonight on Ahmed's permission; only these two of the 29 batch-2/3
#   ids had registry entries). fsi's successors = the served FSI trio; 'imf' was the
#   never-promoted 131-id entry. -> 172
# 2026-08-08: +unctad_trademerchtotal — FIRST UNCTAD successor on the publisher's own data
#   API (#70); served 1,220 series same day, keys are CI secrets. -> 173
# 2026-08-08: +unctad_trademerchgr — successor #2 (period-coded axis, |SPAN=<n>Y design);
#   served 1,754 series same day. -> 174
# 2026-08-08: +unctad_trademerchbalance — successor #3; served 610 series same day. -> 175
# 2026-08-08: +unctad_merchvolumequarterly — successor #4 (Quarter axis taught);
#   1,680 series. -> 176
# 2026-08-08: +unctad_termsoftrade — successor #5; 2,328 series. -> 177
# 2026-08-08: +unctad_tradepriceindicesq — successor #6; 258 series. -> 178
# 2026-08-08: +unctad_{concentdiversindices,concentstructindices,rca,tariff,
#   merchtheilindices} — sized-then-served batch, 81,781 series. -> 183
# 2026-08-08: +unctad_totandcomservicesquarterly + the 6 commodity-price datasets
#   (ucpia/ucpim/commoditypriceindicesa/m/commoditypricea/m; monthly 'YYYYM##'
#   format taught) — 11,793 series. -> 190
# 2026-08-08: +12 tiny successors (ifftrademisinvoicing, ictproductionsector,
#   iffcrimesrelated, creativeservgroupe, sdgporfvol, contportthroughput,
#   shipscrapping, vesselvaluebyownership, shipbuilding, vesselvaluebyregistration,
#   biotrademerchgdpshare, sdglulfrg) — 2,318 series. -> 202
# 2026-08-08: +12 wave-3 successors (biotrademerchprodconcent, curraccbalance,
#   cpia, lscim, gni, inclusivegrowth, lsci, portcallsarrivals[+_S semiannual],
#   remittances, poptotal, popdependency) — 8,377 series. -> 214
# 2026-08-08: +12 wave-4 successors (ictuselocation, creativeservindivtot, ftri,
#   seafarers, plsci, portcalls[+_S], gdptotal, genderdomesticvalueadded,
#   tradeservict, wastewatertreatment, fdiflowsstock) — 16,785 series. -> 226
# 2026-08-08: +12 wave-5 successors (pci, seabornetrade, goodsandservicesbpm6,
#   goodsandservbalancebpm6, govexpenditures, gendertradableindustries,
#   environmentalgoodsrca, goodsandservtradeopennessbpm6, oceanservices,
#   biotrademerchmarketindices, merchantfleet, ictuseenterprsize) —
#   61,632 series. -> 238
# 2026-08-08: +11 wave-6 successors (gdpcomponent, ictuseeconactivity[+isic4],
#   tradefoodproccat pair, popagestruct, fleetbeneficialowners,
#   environmentalgoodstrade, digitallydeliverableservices, lsbci,
#   exchangeratecrosstab) — 261,961 series; the mechanical band is fully
#   ingested. -> 249
# 2026-08-08: +3 gate cases at DOT-prefix TABLE grain (intratrade 247 ids /
#   376,909 series; tradeservcattotal 562 / 101,079; biotrademerchshare 544 /
#   144,552 — 1,353 D1 rows for 622,540 series, so no headroom conflict with
#   the eia->wid order). -> 252
# 2026-08-08: +unctad_biotrademerchrca (depth-1 dot-grain, 2,222 ids / 294,674
#   series) + unctad_ecommercetotal (series grain, 33,818). -> 254
# 2026-08-08: +the final 3 gate cases (associatedplastics 809 dot-grain ids /
#   461,427 series; hiddenplastics 1,076 / 756,314; ecommerceinternational
#   series-grain 9,316). ALL SEVEN six-figure gates now served; only the 13
#   true giants remain in the family. -> 257
# 2026-08-08: +unctad_plasticstradebypartner — GIANT #1 at depth-2 dot-grain
#   (1,615 ids / 1,177,515 series). -> 258
# 2026-08-08: +unctad_ictgoods — GIANT #2 at depth-3 dot-grain (4,608 ids /
#   1,374,779 series). -> 259
# 2026-08-08: +unctad_gstptradematrix — GIANT #3 at depth-2 dot-grain (18,714 ids /
#   4,378,337 series). -> 260
# 2026-08-08: +fao_ql — #19 fao campaign source 1: 16,997 of 20,179 ids auto-update
#   via QCL restrict_to_published; 3,182 restructured-tail ids stay frozen (R91). -> 261
# 2026-08-08: +fao_ga — #19 source 2: 7,712 of 15,018 ids auto-update from GCE
#   (Source='FAO TIER 1' pinned); 4 dropped element classes frozen. -> 262
# 2026-08-08: +fao_ge — #19 source 3: 6,209 of 11,813 ids auto-update from GLE
#   (TIER 1 pinned, 9.5x superset restricted); 2 dropped element classes frozen. -> 263
# 2026-08-08: +fao_rp — #19 source 4: 4,504 of 5,440 ids auto-update from RP (own
#   live domain); dropped seed-treatment/mineral-oil items frozen. -> 264
# 2026-08-08: +fao_ic — #19 source 5, FIRST RE-KEY: 403 series moved to Standard-LC
#   codes (value-verified); 1,506 of 2,468 ids auto-update from IC. -> 265
# 2026-08-08: +unctad_creativegoodsvalue — GIANT #4 at depth-2 dot-grain (4,053 ids /
#   5,487,666 series). -> 266
# 2026-08-08: +fao_gb — #19 source 6: 2,899 of 6,980 ids auto-update from GCE
#   (four-part keys carry the methodology); 5 dropped element classes frozen. -> 267
# 2026-08-08: +fao_gn — #19 source 7: 3,976 of 4,761 ids auto-update from GN (own
#   live domain); area-level tail (47 dropped areas) frozen. -> 268
# 2026-08-08: +fao_gf — #19 source 8 (2nd re-key: 23 series): 819 of 2,591 ids
#   auto-update from GF; methodology-break tail frozen. -> 269
# 2026-08-08: +fao_gt — #19 source 9: 2,858 native-gas ids auto-update from GT;
#   AR5 re-key REFUTED by value verification (GWP-basis change), CO2eq frozen. -> 270
# 2026-08-08: +fao_ep (98.7% clean from RP) +fao_gy (59.6% GCE, not RFN) +fao_gr
#   (52.4% GCE) — #19 sources 10-12. -> 273
# 2026-08-08: +unctad_creativegoodsgr — GIANT #5 at depth-2 dot-grain (3,986 ids /
#   3,597,379 series; Period-axis spans). -> 274
# 2026-08-08: +unctad_oceantrade — GIANT #6 at depth-3 dot-grain (32,374 ids /
#   8,984,193 series — the family's largest). -> 275
# 2026-08-08: +unctad_nonplasticsubststradebypartner — GIANT #7 at depth-2 dot-grain
#   (22,079 ids / 9,298,529 series — the family's largest series count). -> 276
EXPECTED_SOURCE_COUNT = 277  # +unctad_criticalmineralstradebypart (giant #8) 2026-08-09

# Production data root (the ~75B-obs library). On cloud this becomes the R2 bucket prefix.
DATA_ROOT = os.path.abspath(os.environ.get("AQUEDUCT_DATA_ROOT", os.path.join(ROOT, "data", "clean_full")))
# Aqueduct's own state lives apart from the data it manages.
STATE_DIR = os.path.abspath(os.environ.get("AQUEDUCT_STATE_DIR", os.path.join(ROOT, "data", "_aqueduct")))
STATE_DB = os.path.join(STATE_DIR, "state.db")
REGISTRY = os.path.abspath(os.environ.get("AQUEDUCT_REGISTRY", os.path.join(ROOT, "updater", "registry.yaml")))
MATRIX_JSON = os.path.join(ROOT, "UPDATE_CAPABILITY_MATRIX.json")
JOBS_DIR = os.path.join(ROOT, "jobs")


def source_dir(source_id: str) -> str:
    return os.path.join(DATA_ROOT, source_id)


def ensure_dirs() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)
