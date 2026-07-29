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
EXPECTED_SOURCE_COUNT = 123

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
