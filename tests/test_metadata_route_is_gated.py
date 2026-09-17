"""`/v1/series/{id}.metadata.json` must pass the redistribution gate, like every sibling route.

A gated source is not merely undownloadable anywhere else in this worker — it is unreachable:
`/v1/sources` hides it, `/v1/catalog?source=` answers 451 rather than an empty result, and
`.csv` answers 451 before auth. Until 2026-09-17 the metadata branch answered 200: title,
geography, dates, the licence block and the producer citation, unauthenticated, for any row
still in D1. Measured live on a PUBLISHED carve-out so the probe named nothing protected —
`worldbank:FP.CPI.TOTL.ZG:AGO` returned 200 on `.metadata.json` and 451 on `.csv`.

It mattered because rows for gated sources are still in D1 (the catalogue sync is frozen), so
this was the live path serving them.

Read from the shipped source, because the defect is a property of the route's ORDER and nothing
a unit under test can be handed — the same approach as `test_catalog_source_name_query.py`.
Comments are stripped first: this file's own explanation names the route and the call, and a
bare substring test would match the prose instead of the code (R329/R347, and R673's
grep-versus-code confusion).
"""
from __future__ import annotations

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
INDEX_TS = os.path.join(ROOT, "api", "worker", "src", "index.ts")


def _code() -> str:
    """The shipped source with // comments and block comments removed."""
    src = open(INDEX_TS, encoding="utf-8").read()
    src = re.sub(r"/\*.*?\*/", "", src, flags=re.S)
    return re.sub(r"//.*", "", src)


def _branch(code: str, suffix: str) -> str:
    """The body of the `tail.endsWith("<suffix>")` branch, up to the next endsWith branch."""
    i = code.index(f'tail.endsWith("{suffix}")')
    rest = code[i + 1:]
    j = rest.find("tail.endsWith(")
    return rest[:j] if j > 0 else rest


def test_the_metadata_branch_calls_the_gate():
    code = _code()
    body = _branch(code, ".metadata.json")
    assert "isGated(id)" in body, (
        "the metadata route no longer passes the redistribution gate: title, geography, the "
        "licence block and the citation would be served unauthenticated for a gated id")


def test_the_gate_runs_BEFORE_the_metadata_handler():
    """Order is the property. After the handler it would be dead code."""
    body = _branch(_code(), ".metadata.json")
    assert body.index("isGated(id)") < body.index("handleMetadata("), (
        "isGated must run before handleMetadata, or the metadata is already built and served")


def test_the_csv_branch_still_gates_before_auth():
    """The control, and a real invariant: gating must precede requireDownloadAuth, or a gated
    id would answer 401 to an anonymous caller and 451 only to a signed-in one — which leaks
    the distinction and makes the refusal depend on who asks."""
    body = _branch(_code(), ".csv")
    assert "isGated(id)" in body and "requireDownloadAuth" in body, body[:200]
    assert body.index("isGated(id)") < body.index("requireDownloadAuth"), (
        "the redistribution gate must precede the auth gate on the csv route")


def test_both_branches_are_found_at_all():
    """Guards the two helpers above: if `_branch` stopped matching, every assertion here would
    pass vacuously on an empty string."""
    code = _code()
    for suffix in (".metadata.json", ".csv"):
        body = _branch(code, suffix)
        assert len(body) > 120, f"the {suffix} branch parsed as {len(body)} chars — too short"
        assert "decodeURIComponent" in body, f"the {suffix} branch body does not look like the route"
