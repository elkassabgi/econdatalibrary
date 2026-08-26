"""ROTATING: a pure budget-deferral partial is visible but no longer fails the CI gate.

On 2026-08-26 the gate carried 22 budget-bounded sources red on EVERY run — 40+
consecutive failures — because finalize() honestly books `partial` on any deferral
and the gate counted every live ATTENTION source as a failure. A source that can
NEVER run deferral-free (ecb 540 files / 35 min) was red by construction: R244/R359.

Pinned here:
(a) DRIFT TEST (R349): the classifier matches the EXACT note finalize() emits — a
    reworded emitter or matcher fails this test, not production silently;
(b) anything beyond pure deferral (transient, csv_derive failure, coherence unmet)
    keeps ATTENTION and keeps failing the gate;
(c) gate_failures exempts ROTATING and still fails ATTENTION (discriminating pair).
"""
from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.health import _deferral_only, gate_failures  # noqa: E402
from updater.strategies.fetchers._common import Tally, finalize  # noqa: E402


def _unit(status, err):
    return {"unit_id": "_all", "status": status, "last_error": err}


def test_matcher_matches_the_real_emitter_output():
    # build the note through the PRODUCTION emitter, never a hand-typed copy
    t = Tally()
    for _ in range(15):
        t.added_unit(10)
    for _ in range(130):
        t.deferred_unit("DS_TOUR_CAP")
    res = finalize(t, total_rows=1000, last_obs="2026-08-01", source="insee_melodi",
                   series_cursors={"k": "2026-08-01"})
    assert res.status == "partial"
    assert _deferral_only([_unit("partial", res.error)]) is True, \
        "the classifier must accept finalize()'s own deferral note verbatim"


def test_transient_partial_stays_attention():
    t = Tally()
    for _ in range(10):
        t.added_unit(5)
    t.transient_unit("boom")
    res = finalize(t, total_rows=100, last_obs="2026-08-01", source="x",
                   series_cursors={"k": "2026-08-01"})
    assert res.status == "partial"
    assert _deferral_only([_unit("partial", res.error)]) is False


def test_mixed_notes_stay_attention():
    base = "15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken next tick"
    for extra in ("; csv_derive failed 3/10 series [a, b, c]",
                  "; csv coherence unmet: fetcher reported no series_cursors for 5 merged obs",
                  "; 2/9 sub-unit(s) transient-failed; will retry"):
        assert _deferral_only([_unit("partial", base + extra)]) is False, extra
    # and a non-partial status is never rotating, whatever its note says
    assert _deferral_only([_unit("transient_fail", base)]) is False
    assert _deferral_only([]) is False


def test_unsdg_live_note_is_rejected():
    # the FIRST cut's substring blacklist ACCEPTED this verbatim live note (adversarial
    # review 2026-08-26) — 'csv_derive crashed' is a demoting tail the blacklist had no
    # entry for. The anchored whitelist grammar must reject it and any future tail.
    note = ("46 sub-unit(s) attempted, none failed; 667 deferred by budget and taken "
            "next tick; csv_derive crashed (50564 series queued): UnitTimeout('unsdg/_all "
            "(csv phase) exceeded 60 min')")
    assert _deferral_only([_unit("partial", note)]) is False


def test_coverage_note_tails_are_accepted():
    # 'csv coverage note:' tails are NON-failures by design (R372) and must not cost a
    # source its ROTATING class.
    note = ("15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken "
            "next tick [DS_TOUR_CAP, DS_FLORES_A17]; csv coverage note: derive budget "
            "spent — 40 of 90 id(s) deferred to csv_retry_queue, none failed")
    assert _deferral_only([_unit("partial", note)]) is True


def test_zero_attempt_deferral_is_rejected():
    # a rotator that attempts NOTHING every tick is wedged, not rotating (class D:
    # alive, busy, producing nothing) — attempted >= 1 is in the grammar itself.
    note = "0 sub-unit(s) attempted, none failed; 130 deferred by budget and taken next tick"
    assert _deferral_only([_unit("partial", note)]) is False


def test_gate_exempts_rotating_and_still_fails_attention():
    def row(source, health):
        return {"source": source, "health": health, "live": True, "run_location": "cloud",
                "attention": [], "strategy": "s", "cadence": "daily"}
    report = {"sources": [row("rotator", "ROTATING"), row("broken", "ATTENTION"),
                          row("ok_src", "OK")]}
    fails = gate_failures(report)
    assert not any("rotator" in f for f in fails), "ROTATING must not fail the gate"
    assert any("broken" in f for f in fails), "ATTENTION must still fail the gate"
