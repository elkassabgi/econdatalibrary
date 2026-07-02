"""Environment + path configuration (local-now / cloud-later).

A single env var, AQUEDUCT_BACKEND=local|cloud, selects the StateStore + Blob
implementations. Everything else (registry, strategies, orchestrator) is identical.
"""
from __future__ import annotations
import os

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
BACKEND = os.environ.get("AQUEDUCT_BACKEND", "local")  # local | cloud

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
