"""Aqueduct — econdatalibrary continuous-update system.

Portable core: local (SQLite + filesystem) now, Cloudflare D1 + R2 later.
The orchestrator, registry, and strategy adapters never touch a path or a cloud
SDK directly — everything env-specific is behind StateStore (state.py) and Blob
(blob.py). See CONTINUOUS_UPDATE_DESIGN.md for the full architecture.
"""
