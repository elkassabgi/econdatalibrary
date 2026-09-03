"""The digest must classify the live tier exactly as the orchestrator does.

WHY THIS MATTERS. `updater-daily.yml` sets AQUEDUCT_LIVE_ONLY=1 and `orchestrate.py` honours it
by executing live-tier sources only, so a non-live source's status is FROZEN — it cannot improve
however many mornings it is reported. Measured 2026-09-03 against the real state file, 7 of the
digest's 36 attention rows were such sources (bls, census, imf_imts_direct, istat, oecd, owid,
sipri_polity): 19% of a list whose entire purpose is to say what needs doing.

THE TRAP THIS PINS. `registry.load()["sources"]` returns the RAW yaml entries, and 15 of the 282
have no `live` key at all. `registry.to_units()` reads it as `bool(entry.get("live", False))`, so
absent means NOT live — but a reader who tested `e.get("live") is False` would classify those 15
as live and undo the fix silently. Two interpretations of one flag is precisely how R676 and R685
happened, so this compares the digest's classification against the UNIT CONFIG the orchestrator
actually consults, rather than against a second copy of the rule.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import registry  # noqa: E402
from updater.send_digest import live_source_ids  # noqa: E402

SOURCES = registry.load()["sources"]


def digest_live_ids() -> set:
    """THE digest's own function, imported — not a copy of it.

    A local re-implementation here would be R684 rule (3) exactly: a test that re-implements the
    rule proves only that it agrees with itself, and send_digest could drift underneath it freely.
    """
    return live_source_ids(SOURCES)


def orchestrator_live_ids() -> set:
    """What the orchestrator will actually run, read from the unit config it consults."""
    out = set()
    for e in SOURCES:
        for u in registry.to_units(e):
            if bool((u.config or {}).get("live")):
                out.add(e["source_id"])
                break
    return out


def test_the_two_agree_exactly():
    d, o = digest_live_ids(), orchestrator_live_ids()
    assert d == o, (
        "the digest and the orchestrator disagree about the live tier — the digest would "
        f"hide or show the wrong sources.\n  only the digest thinks live: {sorted(d - o)}\n"
        f"  only the orchestrator: {sorted(o - d)}")


def test_the_absent_key_case_is_real_and_not_live():
    """If nobody omits `live` any more this test is vacuous, and should say so rather than pass."""
    missing = [e["source_id"] for e in SOURCES if "live" not in e]
    assert missing, ("no registry entry omits `live`, so this guard proves nothing — either "
                     "restore one or delete this test deliberately")
    live = digest_live_ids()
    assert not (set(missing) & live), (
        f"an entry with NO `live` key was classified as live: "
        f"{sorted(set(missing) & live)}. `to_units` reads it as bool(get('live', False)), so "
        f"absent means NOT live and the digest must agree.")


def test_a_source_is_not_live_merely_by_having_a_state_row():
    """The whole point: presence in the state file says nothing about membership of the tier."""
    live = digest_live_ids()
    for sid in ("oecd", "owid", "imf_imts_direct"):
        if any(e["source_id"] == sid for e in SOURCES):
            assert sid not in live, (
                f"{sid} is live:false in registry.yaml but was classified as live")
