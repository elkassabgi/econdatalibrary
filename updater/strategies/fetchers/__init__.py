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
    """Is this source's fetcher present AND importable?

    ANSWER THE QUESTION, DO NOT RAISE IT BACK AT THE CALLER. This caught only
    ModuleNotFoundError, so a module that EXISTS but blows up on import propagated out
    of a predicate whose whole job is to return a bool. The live example was
    `fetchers/ksh.py`: ksh was retired 2026-07-29 and its module kept loading
    `jobs/ingest_ksh_hungary.py` by path at import time, raising FileNotFoundError —
    not a ModuleNotFoundError, so not caught. Nothing imports it today, which is exactly
    what made it a landmine: the first sweep to ask "which fetchers are built?" would
    have died on it instead of answering, and `_adapter_ready` in health.py calls this
    for every source in the registry (R248: one malformed item must never end a sweep
    over many).

    A broken module is NOT the same as an absent one, so it is reported rather than
    swallowed — silence here would turn a crashing fetcher into a quiet "not built" and
    the source would simply vanish from the rollout with no one told.
    """
    try:
        get(source_id)
        return True
    except ModuleNotFoundError:
        return False
    except Exception as e:                              # noqa: BLE001 — a predicate must not raise
        print(f"[fetchers] {source_id}: module exists but FAILED TO IMPORT "
              f"({type(e).__name__}: {e}) — treating as not implemented; fix or delete it",
              flush=True)
        return False
