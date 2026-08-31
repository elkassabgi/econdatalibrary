"""record_killed_unit: the external-kill hole in run_cost_estimate, held shut.

Why this exists: `run_local_heavy.ps1` hard-stops the updater at its wall-clock budget
(exit 124). The in-flight unit dies without a `runs` row, keeps its stale cheap estimate,
re-enters the cheap band, and eats the next night too — measured 2026-08-30/31 when
`unctad_tradefoodcatbyproc` consumed ~154 of 153 budgeted minutes, left no row, and 18 live
local sources (bea, eia, census, statcan, oecd, noaa among them) went a 7th day unattempted.

Both directions per R414: the parser must FIND the killed unit in a real killed-pass log, and
must find NOTHING in a log where every unit completed. Plus the estimator integration: a
`killed_external` row must actually raise `run_cost_estimate`'s answer.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

from record_killed_unit import parse_killed  # noqa: E402

KILLED_LOG = """\
[run] python 3.14.6  pandas 2.3.3  pyarrow 23.0.0
[orchestrator] >>> istat/_all (strategy=sdmx_delta, cadence=monthly)
[orchestrator] <<< istat/_all took 39s
[orchestrator] NOT DUE faostat/_all — cadence=monthly, last_success=...; skipped
[orchestrator] >>> unctad_biotrademerch/_all (strategy=bulk_snapshot_if_changed, cadence=weekly)
[orchestrator] <<< unctad_biotrademerch/_all took 516s
[orchestrator] >>> unctad_tradefoodcatbyproc/_all (strategy=bulk_snapshot_if_changed, cadence=weekly)
"""

CLEAN_LOG = """\
[orchestrator] >>> istat/_all (strategy=sdmx_delta, cadence=monthly)
[orchestrator] <<< istat/_all took 39s
[orchestrator] >>> wid/_all (strategy=extend_by_date, cadence=irregular)
[orchestrator] <<< wid/_all took 88s
"""


def test_finds_the_killed_unit_and_attributes_the_residual():
    src, unit, secs = parse_killed(KILLED_LOG, 9788.0)
    assert (src, unit) == ("unctad_tradefoodcatbyproc", "_all")
    # 9788 total − (39 + 516) completed = 9233, the residual — startup overhead lands on the
    # killed unit deliberately (over-estimating is the safe direction per run_cost_estimate).
    assert secs == 9233.0


def test_a_completed_pass_records_nothing():
    """The accept direction: without it, the tool would stamp phantom kills on clean passes."""
    assert parse_killed(CLEAN_LOG, 200.0) is None


def test_attribution_never_goes_below_the_floor():
    """A kill seconds after the unit starts still records ≥60s, never 0 or negative —
    a 0 would ENTER the MAX window as evidence the source is free, the exact lie being fixed."""
    log = KILLED_LOG
    _, _, secs = parse_killed(log, 500.0)  # total < completed 555s
    assert secs == 60.0


def test_comma_grouped_took_seconds_are_parsed():
    """`took {dur:,.0f}s` — the orchestrator comma-groups. Review DEMONSTRATED v1's `\\d+`
    matching nothing on `took 24,480s`, so the done-set emptied, two units looked open, and
    the tool refused on the giant route's most ordinary night."""
    log = (
        "[orchestrator] >>> oecd/_all (strategy=sdmx_delta, cadence=monthly)\n"
        "[orchestrator] <<< oecd/_all took 24,480s\n"
        "[orchestrator] >>> unctad_tradefoodcatbyproc/_all (strategy=bulk, cadence=weekly)\n"
    )
    src, unit, secs = parse_killed(log, 30000.0)
    assert (src, unit) == ("unctad_tradefoodcatbyproc", "_all")
    assert secs == 30000.0 - 24480.0


def test_orphan_starts_do_not_block_the_last_open_unit():
    """Four orchestrator exit paths never print `<<<` (earned no_change prints NOTHING;
    LOCKED and detect-phase failures continue past the closer) — and the kill itself
    manufactures one: the killed giant's lease persists and the next pass logs
    `>>> ... LOCKED`. Orphan starts are NORMAL; only the LAST `>>>` can be in flight."""
    log = (
        "[orchestrator] >>> boc/_all (strategy=extend_by_date, cadence=daily)\n"
        # boc earned no_change: no <<< line, by design
        "[orchestrator] >>> istat/_all (strategy=sdmx_delta, cadence=monthly)\n"
        "[orchestrator] <<< istat/_all took 39s\n"
        "[orchestrator] >>> unctad_oceantrade/_all (strategy=bulk, cadence=weekly)\n"
    )
    src, unit, secs = parse_killed(log, 5000.0)
    assert (src, unit) == ("unctad_oceantrade", "_all")
    # boc's unaccounted time lands in the residual — the documented-safe over-estimate.
    assert secs == 5000.0 - 39.0


def test_clean_pass_ending_in_a_completed_unit_records_nothing():
    """Even with an earlier orphan (no_change), a pass whose LAST unit completed is clean."""
    log = (
        "[orchestrator] >>> boc/_all (strategy=extend_by_date, cadence=daily)\n"
        "[orchestrator] >>> wid/_all (strategy=extend_by_date, cadence=irregular)\n"
        "[orchestrator] <<< wid/_all took 88s\n"
    )
    assert parse_killed(log, 200.0) is None


def test_a_trailing_locked_pair_means_nothing_was_in_flight():
    """The kill manufactures this shape: the killed giant's lease persists (48h TTL), so the
    NEXT pass logs `>>> giant/...` then `LOCKED giant/...`. LOCKED closes the unit (~0s ran),
    and the unit before it is closed by the serial invariant — so a kill landing here has no
    victim to attribute to, and recording a phantom kill would banish an innocent source."""
    log = (
        "[orchestrator] >>> wid/_all (strategy=extend_by_date, cadence=irregular)\n"
        "[orchestrator] >>> unctad_tradefoodcatbyproc/_all (strategy=bulk, cadence=weekly)\n"
        "[orchestrator] LOCKED unctad_tradefoodcatbyproc/_all — lease held by pass 20260830\n"
    )
    assert parse_killed(log, 3000.0) is None


def test_second_pass_after_a_kill_attributes_past_the_locked_ghost():
    """Pass 2 of the manufactured shape: the killed giant re-appears as >>>+LOCKED, a fresh
    giant starts after it and is itself killed — attribution must reach the FRESH one."""
    log = (
        "[orchestrator] >>> unctad_tradefoodcatbyproc/_all (strategy=bulk, cadence=weekly)\n"
        "[orchestrator] LOCKED unctad_tradefoodcatbyproc/_all — lease held by pass 20260830\n"
        "[orchestrator] >>> unctad_oceantrade/_all (strategy=bulk, cadence=weekly)\n"
    )
    src, unit, secs = parse_killed(log, 4000.0)
    assert (src, unit) == ("unctad_oceantrade", "_all")
    assert secs == 4000.0


def test_killed_external_holds_the_floor(tmp_path):
    """Five fast post-kill rows (locked 0.0s during an outage) must NOT let the floor restore
    the old cheap estimate — the one-relapse-per-outage hole the review named."""
    from updater.state import StateStore
    st = StateStore(path=str(tmp_path / "state.db"))
    st.log_run("giant", "_all", "killed_external", obs=0, dur_s=9233.0)
    for _ in range(5):
        st.log_run("giant", "_all", "locked", obs=0, dur_s=0.0)
    est = st.run_cost_estimate().get("giant")
    assert est == 9233.0, (
        "the kill rolled out of the MAX window and nothing held the floor — estimate %r" % est
    )


def test_estimator_actually_rises_on_a_killed_row(tmp_path):
    """End to end: MAX(dur_s) over the window must see the killed row."""
    from updater.state import StateStore
    st = StateStore(path=str(tmp_path / "state.db"))
    st.log_run("giant", "_all", "no_change", obs=0, dur_s=45.0)      # stale cheap history
    before = st.run_cost_estimate().get("giant")
    st.log_run("giant", "_all", "killed_external", obs=0, dur_s=9233.0)
    after = st.run_cost_estimate().get("giant")
    assert before == 45.0
    assert after == 9233.0, (
        "a killed_external row must raise the estimate — if the estimator filters it out, "
        "the starvation loop this tool closes is still open"
    )
