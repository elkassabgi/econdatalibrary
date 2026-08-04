"""The registry count guard must be checked at COMMIT time, not only at run time.

updater/orchestrate.py validates the registry with config.EXPECTED_SOURCE_COUNT and, on
mismatch, raises SystemExit BEFORE any source is fetched. That is the right behaviour — a source
appearing in registry.yaml unnoticed is exactly what it exists to catch — but it means the whole
updater stops dead.

On 2026-08-04 I added three IMF entries and not the count. Every run, cloud and local, then
exited 1 at "expected 141 sources, found 144" having fetched NOTHING, for ~14 hours, while I went
on adding sources to a pipeline that was refusing to start (R347).

Nothing in the suite caught it, because nothing asserted the two numbers agree. These tests do.
They fail in CI the moment registry.yaml and EXPECTED_SOURCE_COUNT disagree, which turns a
silent production outage into a red check on the commit that causes it.
"""
from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import config, registry  # noqa: E402


def test_expected_source_count_matches_the_registry():
    """The number the guard enforces IS the number of entries in the file."""
    n = len(registry.load().get("sources", []))
    assert n == config.EXPECTED_SOURCE_COUNT, (
        f"registry.yaml has {n} sources but config.EXPECTED_SOURCE_COUNT is "
        f"{config.EXPECTED_SOURCE_COUNT}. orchestrate.py raises SystemExit on this mismatch "
        f"BEFORE fetching anything, so leaving it stops the entire updater — cloud and local. "
        f"Adding or removing a registry entry means updating that constant in the same change."
    )


def test_registry_validates_the_way_production_validates_it():
    """Call it with the SAME arguments orchestrate.py uses.

    When I added the three entries I ran registry.validate(reg) — no expected_count — got a clean
    result, and treated it as clearance. orchestrate.py calls
    registry.validate(reg, expected_count=config.EXPECTED_SOURCE_COUNT). A pass under weaker
    arguments than production uses is not a pass, so this test pins the production call.
    """
    problems = registry.validate(registry.load(),
                                 expected_count=config.EXPECTED_SOURCE_COUNT)
    assert problems == [], "registry invalid under the PRODUCTION call: " + "; ".join(problems)


def test_the_guard_still_fires_on_a_wrong_count():
    """A guard that cannot fail is not a guard (R346).

    Without this, both tests above would keep passing if validate() ever stopped enforcing
    expected_count at all — the failure mode that would let the next mismatch through silently.
    """
    reg = registry.load()
    wrong = len(reg.get("sources", [])) + 1
    problems = registry.validate(reg, expected_count=wrong)
    assert any("expected" in p and "found" in p for p in problems), (
        "validate() accepted a deliberately wrong expected_count — the count check is not "
        "being enforced, so the two tests above prove nothing")


@pytest.mark.parametrize("sid", ["imf_bop_direct", "imf_irfcl_direct", "imf_cpi_direct"])
def test_the_three_entries_that_caused_the_outage_are_present(sid):
    """Pin the specific entries, so a future count edit cannot 'fix' a mismatch by dropping one.

    The tempting wrong repair for a count mismatch is to change the number until it matches. If
    an entry were deleted instead, the count would agree again and these ids would silently stop
    updating — which is the outcome the whole task was about preventing.
    """
    ids = {e.get("source_id") for e in registry.load().get("sources", [])}
    assert sid in ids, f"{sid} is gone from registry.yaml — it would silently stop auto-updating"
