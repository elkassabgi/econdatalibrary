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
EXPECTED_SOURCE_COUNT = 112

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
