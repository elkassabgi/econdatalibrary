"""Scoped csv.field_size_limit - raise it around ONE reader, then put the old value back.

WHY THIS EXISTS (2026-11-25, PR #332, CI run 45568040117). csv.field_size_limit() is state of the
whole PROCESS, not of one reader. Six modules called it at IMPORT time (jobs/ingest_abs_full.py,
jobs/ingest_bis_full.py, jobs/ingest_ecb.py, jobs/ingest_imf_full.py, jobs/ingest_statcan.py,
tools/apply_series_names.py). So the result of a read depended on what else the process had
imported before it: a reader that needs a large field worked only when one of those modules came
first, and a test of the default limit (131072) failed when one of them was imported earlier in
the same pytest process. Importing a module must not change how a different module reads a CSV.

USE: decorate the function that drives the reader, or use the context manager around it:

    @with_field_size_limit(CSV_FIELD_LIMIT)
    def parse(fh): ...

    with field_size_limit(CSV_FIELD_LIMIT):
        rows = list(csv.reader(fh))

For a GENERATOR function the decorator holds the limit for the whole iteration (from the first
next() to exhaustion or close()), because csv reads its fields lazily inside next(). While such a
generator is suspended the raised limit stays in force for the process; it is restored when the
generator ends or is closed (a dropped generator is closed by the garbage collector).

THREADS. The ingesters parse in thread pools, so two scopes can overlap. A plain save/restore pair
is wrong then: thread A saves 131072 and raises; thread B saves A's raised value; A restores 131072
while B is still mid-read, and B fails on its large field. So the scopes are COUNTED under one lock:
the first scope to open saves the base value, the limit in force is max(base, every open scope's
limit), and only the last scope to close restores the base. A scope never LOWERS the limit.
"""
from __future__ import annotations

import contextlib
import csv
import functools
import inspect
import sys
import threading

# The limit every updater fetcher's probe and fetch runs under (updater/orchestrate.py run_once, and the
# release-watch probe that copies it, updater/release_watch.py _production_probe). One value, one place.
FETCH_FIELD_LIMIT = min(sys.maxsize, 2_147_483_647)

_LOCK = threading.Lock()
_OPEN: list = []          # one _Entry per scope that is open now (two scopes may carry the same value)
_BASE: list = []          # [the value in force when the first open scope began]; empty when none is open


class _Entry:
    """One open scope. Compared by identity, so the close removes exactly the scope that opened."""
    __slots__ = ("limit",)

    def __init__(self, limit: int):
        self.limit = limit


def _in_force_locked() -> int:
    return max([_BASE[0]] + [e.limit for e in _OPEN])


@contextlib.contextmanager
def field_size_limit(limit: int):
    """Raise csv.field_size_limit to at least `limit` for the body; restore the old value after."""
    limit = int(limit)
    entry = _Entry(limit)
    # The open and the close are both inside try/finally, and the close removes THIS scope's entry only if
    # the open got as far as adding it. So an asynchronous exception (the updater's UnitTimeout comes from
    # SIGALRM, armed just before this scope opens) cannot leave an entry behind, which would keep the
    # raised limit for the rest of the process.
    try:
        with _LOCK:
            if not _OPEN:
                _BASE[:] = [csv.field_size_limit()]
            _OPEN.append(entry)
            csv.field_size_limit(_in_force_locked())
        yield
    finally:
        with _LOCK:
            if any(e is entry for e in _OPEN):
                _OPEN[:] = [e for e in _OPEN if e is not entry]
                if _OPEN:
                    csv.field_size_limit(_in_force_locked())
                else:
                    csv.field_size_limit(_BASE[0])
                    _BASE[:] = []


def with_field_size_limit(limit: int):
    """Decorator form of field_size_limit(). Works on plain functions, methods and generators."""
    def deco(fn):
        if inspect.isgeneratorfunction(fn):
            @functools.wraps(fn)
            def gen(*args, **kwargs):
                with field_size_limit(limit):
                    return (yield from fn(*args, **kwargs))
            return gen

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            with field_size_limit(limit):
                return fn(*args, **kwargs)
        return wrapper
    return deco
