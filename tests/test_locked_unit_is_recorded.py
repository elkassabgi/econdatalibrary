"""A unit refused its lease must leave EVIDENCE, naming who holds it.

THE OUTAGE THIS PINS (2026-08-07). `run_once` handled a failed `claim_lease` with

    results.append((unit.key, "locked"))
    continue

— no run row, no state write, nothing printed. So when eia/_all was left leased by
orch-41604, a run that had died on 2026-08-05 with a 64-hour TTL, every subsequent pass was
refused the unit and said nothing. Measured that day: eia's `last_attempt_utc` still read
2026-08-05T15:39:48 on a DAILY cadence, no `updater.run` process was alive, and the workstation
guard was reporting healthy with jobs_alive 3/3. Downstream, a locked unit is INDISTINGUISHABLE
from a source that was never due — the health gate reads state, and state had nothing to say.
A daily source went past two days stale with every dashboard green.

The fix is not "clear leases automatically" — a lease held by a LIVE run is doing its job, and
a sweeper that guessed would eventually kill a real pass. The fix is that being blocked is a
FACT WORTH LOGGING, with the holder named, so `runs` alone answers "why did this stop?".
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def test_lease_holder_names_the_owner_and_expiry(tmp_path):
    from updater.state import StateStore
    st = StateStore(path=str(tmp_path / "s.db"))
    assert st.lease_holder("src/_all") is None, "no lease -> no holder"
    assert st.claim_lease("src/_all", owner="orch-1", ttl_s=3600)
    held = st.lease_holder("src/_all")
    assert held and held["owner"] == "orch-1", held
    assert held["expires_utc"], "a holder must come with an expiry, or nobody can judge it"


def test_held_leases_lists_only_leases_still_in_force(tmp_path):
    from updater.state import StateStore
    st = StateStore(path=str(tmp_path / "s.db"))
    st.claim_lease("live/_all", owner="orch-1", ttl_s=3600)
    st.claim_lease("dead/_all", owner="orch-2", ttl_s=-10)      # already expired
    keys = {h["key"] for h in st.held_leases()}
    assert "live/_all" in keys
    assert "dead/_all" not in keys, (
        "an expired lease blocks nothing — claim_lease overwrites it — so reporting it would "
        "send someone hunting a lock that does not exist")


def test_a_second_owner_is_refused_while_the_lease_holds(tmp_path):
    """The guard rail itself, so the fix above never gets 'simplified' into always-allow."""
    from updater.state import StateStore
    st = StateStore(path=str(tmp_path / "s.db"))
    assert st.claim_lease("src/_all", owner="orch-1", ttl_s=3600)
    assert not st.claim_lease("src/_all", owner="orch-2", ttl_s=3600)
    assert st.lease_holder("src/_all")["owner"] == "orch-1"


def test_run_once_records_the_locked_unit_instead_of_skipping_silently():
    """The behavioural claim: the locked branch must LOG A RUN and NAME THE HOLDER.

    Source-level because driving run_once to a lease conflict needs a full registry, store and
    strategy stack; what actually failed here was that the branch wrote nothing at all, and
    that is visible and unfoolable in the source.
    """
    import inspect
    from updater import orchestrate as O
    src = inspect.getsource(O.run_once)
    lock_branch = src[src.index("if not store.claim_lease("):]
    lock_branch = lock_branch[: lock_branch.index('results.append((unit.key, "locked"))')]
    assert "log_run" in lock_branch, (
        "a refused unit must write a run row — without one it is indistinguishable from a "
        "source that was not due, which hid a two-day eia outage")
    assert "lease_holder" in lock_branch, (
        "the row must NAME THE HOLDER; 'locked' alone does not tell you whether the holder is "
        "alive, which is the only question that matters")
