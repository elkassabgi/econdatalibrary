"""Per-source incremental fetchers.

Each source that uses a date-tail strategy (S2 extend_by_date) provides a module
`updater/strategies/fetchers/<source_id>.py` exposing:

    def update(unit, since) -> Result

The fetcher: reads the source's existing parquet(s) to learn each series' last
obs_date (or uses `since`), requests only newer observations via the source's
native date filter, then publishes via merge.merge_and_write (atomic, dedup,
never-shrink) and returns a Result(status, obs=<rows now>, last_obs_date=...).
It must raise TransientError / DefinitiveError per the failure contract.
"""
from __future__ import annotations
import importlib

_CACHE: dict = {}


def get(source_id: str):
    if source_id in _CACHE:
        return _CACHE[source_id]
    mod = importlib.import_module(f"updater.strategies.fetchers.{source_id}")
    if not hasattr(mod, "update"):
        raise KeyError(f"fetcher {source_id!r} has no update(unit, since)")
    _CACHE[source_id] = mod
    return mod


def implemented(source_id: str) -> bool:
    try:
        get(source_id)
        return True
    except ModuleNotFoundError:
        return False
