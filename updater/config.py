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
EXPECTED_SOURCE_COUNT = 113

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
