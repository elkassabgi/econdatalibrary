# -*- coding: utf-8 -*-
"""The worker's wide-source list must equal the resolver's `_NATIVE_ONLY`.

WHY THIS EXISTS. Four sources are stored WIDE - their at-rest CSV carries the source's own
columns instead of `series_id,obs_date,value` - because `_resolve.py` marks them
`tidy_ok=False`: a long projection of a relational table would be a lie. The worker enforces
the canonical header and, not knowing about them, refused every one as malformed. Measured on
the live worker 2026-09-22: fhfa 89,706 + census 2,993 + wikidata 250 + treasury 14 = 92,963
catalogued series answering 502, from 2026-09-02 until the fix - about 20 days.

The fix gives the worker its own copy of the list, in TypeScript, which the Python side cannot
import. Two copies of a set are two things to drift, and the drift is SILENT in exactly the
direction that hurts: add a wide source in Python, forget the worker, and that source goes
502 for every user while every test still passes. This asserts they are equal.

It reads the TypeScript rather than a restatement of it, so it cannot pass against a copy that
has itself gone stale.
"""
from __future__ import annotations

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

UTIL_TS = os.path.join(ROOT, "api", "worker", "src", "util.ts")
RESOLVE_PY = os.path.join(ROOT, "clients", "python", "econdl", "_resolve.py")


def _worker_list() -> set[str]:
    """Parse NATIVE_ONLY_SOURCES out of util.ts."""
    src = open(UTIL_TS, encoding="utf-8").read()
    m = re.search(r"NATIVE_ONLY_SOURCES:\s*readonly string\[\]\s*=\s*\[(.*?)\]", src, re.S)
    assert m, "NATIVE_ONLY_SOURCES not found in api/worker/src/util.ts"
    body = re.sub(r"//[^\n]*", "", m.group(1))
    return set(re.findall(r'"([a-z0-9_]+)"', body))


def _python_set() -> set[str]:
    """The authority. Imported, not parsed, so a rename is a hard failure here."""
    from econdl import _resolve
    return set(_resolve._NATIVE_ONLY)


def test_the_parse_actually_found_something():
    """Positive control. An empty parse would make the equality test below vacuous -
    two empty sets are equal, and that is precisely the silent pass to avoid."""
    got = _worker_list()
    assert got, "parsed NO ids out of util.ts - the declaration's shape changed"
    assert "fhfa" in got, f"fhfa missing from the parsed worker list: {sorted(got)}"


def test_worker_and_resolver_agree_on_the_wide_sources():
    worker = _worker_list()
    python = _python_set()
    assert worker == python, (
        f"the worker's NATIVE_ONLY_SOURCES and econdl._resolve._NATIVE_ONLY disagree.\n"
        f"  only in the worker : {sorted(worker - python)}\n"
        f"  only in the resolver: {sorted(python - worker)}\n"
        f"A source present in the resolver but MISSING from the worker is served 502 "
        f"'the at-rest object is malformed' for every one of its series, silently - that is "
        f"the 92,963-series outage this test exists to prevent. Update "
        f"api/worker/src/util.ts in the same change."
    )


def test_the_resolver_side_is_not_empty_either():
    """Second control, on the other operand."""
    assert _python_set(), "econdl._resolve._NATIVE_ONLY is empty - the equality above is vacuous"
