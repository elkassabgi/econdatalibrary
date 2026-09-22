"""A census figure that cannot be true must never be published, and must be said out loud when measured.

MEASURED 2026-09-16. The census reported statcan at 32,854,575,488 series. Three of its cubes report more
distinct keys than they hold rows:

    98100620.parquet   893,928,960 rows   approx_count_distinct 1,307,042,927
    98100390.parquet   744,085,440 rows                           838,833,294
    98100435.parquet   962,150,400 rows                         1,011,968,085

A series is a set of observations, so distinct keys can never exceed rows. Nothing in the run refused, and
nothing remarked on it - the module docstring of series_census.py has carried the first of those pairs,
unflagged, since before the run.

The existing R420 gate cannot catch this. It compares the new TOTAL against the published one and refuses a
>20% move, so an impossible figure passes whenever it resembles last time's, and `--force-publish` bypasses it
outright. These tests pin the distinction: R420 asks whether the number MOVED; this asks whether it is POSSIBLE,
and no flag lifts it.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))

import series_census  # noqa: E402


def test_a_source_with_more_series_than_observations_is_flagged():
    obs = {"statcan": 893_928_960, "worldbank": 1_000_000}
    ser = {"statcan": 1_307_042_927, "worldbank": 50_000}
    bad = series_census.impossible_sources(obs, ser)
    assert [b[0] for b in bad] == ["statcan"], bad
    assert bad[0][1] == 893_928_960 and bad[0][2] == 1_307_042_927


def test_a_healthy_fleet_is_not_flagged():
    """The negative control. Without this the guard could fire on everything and still look right."""
    obs = {"a": 1_000, "b": 5_000_000, "c": 7}
    ser = {"a": 1_000, "b": 4_999_999, "c": 1}
    assert series_census.impossible_sources(obs, ser) == []


def test_equal_counts_are_allowed():
    """One observation per series is legitimate - a census-profile cell is exactly that. Only MORE is wrong."""
    assert series_census.impossible_sources({"x": 500}, {"x": 500}) == []
    assert series_census.impossible_sources({"x": 500}, {"x": 501})[0][0] == "x"


def test_zero_and_missing_sources_do_not_fire():
    """A source with no rows or no keys is a different condition and must not be reported as impossible."""
    assert series_census.impossible_sources({"x": 0}, {"x": 0}) == []
    assert series_census.impossible_sources({"x": 0}, {"x": 10}) == []      # no obs measured
    assert series_census.impossible_sources({"x": 10}, {"x": 0}) == []      # no keys
    assert series_census.impossible_sources({}, {"x": 10}) == []            # source absent from obs


def test_every_impossible_source_is_returned_not_just_the_first():
    obs = {"a": 100, "b": 200, "c": 300}
    ser = {"a": 101, "b": 199, "c": 3_000}
    assert [b[0] for b in series_census.impossible_sources(obs, ser)] == ["a", "c"]


def test_the_check_is_granularity_agnostic_and_catches_the_three_real_cubes():
    """Fed the three real CUBE figures, the comparison catches all three.

    Note what this does and does not show. These are per-FILE numbers; the census calls this function with
    per-SOURCE numbers. The arithmetic is the same either way, which is the point - but see the test below for
    why that means the 2026-09-16 run was NOT caught.
    """
    obs = {"98100620": 893_928_960, "98100390": 744_085_440, "98100435": 962_150_400}
    ser = {"98100620": 1_307_042_927, "98100390": 838_833_294, "98100435": 1_011_968_085}
    assert len(series_census.impossible_sources(obs, ser)) == 3


def test_the_impossibility_check_would_NOT_have_caught_the_2026_09_16_run():
    """The honest limit, pinned so nobody claims otherwise - including me, who nearly did.

    At SOURCE granularity statcan reported 32,854,575,488 series over 56,846,068,336 observations: series below
    obs, so it passes. The impossibility lived in individual files, and the census computes no per-file
    distincts. This is why one_observation_sources exists.
    """
    obs = {"statcan": 56_846_068_336}
    ser = {"statcan": 32_854_575_488}
    assert series_census.impossible_sources(obs, ser) == [], (
        "if this ever fires, the granularity story in the docstring is wrong and needs rewriting")


def test_the_ratio_check_does_surface_the_2026_09_16_run():
    """What the impossibility check misses, the observations-per-series ratio catches."""
    obs = {"statcan": 56_846_068_336, "healthy": 1_000_000}
    ser = {"statcan": 32_854_575_488, "healthy": 50_000}
    thin = series_census.one_observation_sources(obs, ser)
    assert [t[0] for t in thin] == ["statcan"], thin
    assert abs(thin[0][3] - 1.73) < 0.01, thin


def test_the_ratio_check_has_a_negative_control():
    """A fleet of real time series must produce nothing, or the flag means nothing."""
    obs = {"a": 10_000, "b": 500_000, "c": 36}
    ser = {"a": 1_000, "b": 50_000, "c": 3}
    assert series_census.one_observation_sources(obs, ser) == []


def test_the_ratio_check_orders_by_size_not_by_ratio():
    """The owner needs the biggest contributor first; a tiny source at 1.0 is noise beside a giant at 1.7."""
    obs = {"tiny": 10, "giant": 1_700_000}
    ser = {"tiny": 10, "giant": 1_000_000}
    assert [t[0] for t in series_census.one_observation_sources(obs, ser)] == ["giant", "tiny"]


def test_the_ratio_check_skips_sources_with_no_series_or_no_obs():
    assert series_census.one_observation_sources({"x": 100}, {"x": 0}) == []
    assert series_census.one_observation_sources({"x": 0}, {"x": 100}) == []


def test_the_guard_is_not_the_step_change_gate():
    """R420 asks whether the number moved and can be forced past; this asks whether it can be true.

    A figure identical to the published one moves 0% and sails through R420 - and is still impossible.
    """
    obs = {"s": 1_000}
    ser = {"s": 2_000}
    assert series_census.impossible_sources(obs, ser), (
        "an unchanged but impossible figure must still be caught, or the guard adds nothing over R420")


# --- the wiring ---------------------------------------------------------------------------------------------
# Mutation found that the two behaviours that actually matter - a non-zero exit on a measurement run, and the
# publish refusal - were not covered by the tests above, because they live in main() and main() needs a store,
# a catalogue and R2 to run. These are STRUCTURAL checks over main()'s source: weaker than behavioural, and
# stated as such, but they fail if either behaviour is deleted, which is exactly what the surviving mutants did.

import inspect  # noqa: E402
import re  # noqa: E402


def _main_src():
    return inspect.getsource(series_census.main)


def test_main_computes_the_impossibility_before_deciding_anything():
    assert re.search(r"impossible\s*=\s*impossible_sources\(", _main_src()), (
        "main() no longer computes impossible_sources; the guard is not wired in at all")


def test_a_measurement_only_run_exits_non_zero_when_the_arithmetic_is_impossible():
    """Structural: the early return must depend on `impossible`, not be a bare `return 0`.

    This is the path the 2026-09-16 run took - measurement only - and the path on which the figure was read and
    quoted. A silent exit 0 is what let that happen.
    """
    assert re.search(r"return\s+1\s+if\s+impossible\s+else\s+0", _main_src()), (
        "the measurement-only return no longer signals impossibility in its exit code")


def test_publishing_is_refused_on_impossibility_before_the_force_publish_gate():
    """Structural: the refusal must be guarded by `impossible` AND sit before the R420 gate.

    Order matters: R420 can be overridden with --force-publish, so an impossibility refusal placed after it
    would be reachable by that flag.
    """
    src = _main_src()
    m_imp = re.search(r"if impossible:\s*\n\s*print\(f?\"REFUSING to publish", src)
    assert m_imp, "the publish refusal for impossible arithmetic is gone"

    # The R420 CHECK, not any mention of the flag. A first version of this test used src.find("--force-publish")
    # and matched the impossibility gate's OWN comment - which says the flag does not lift it - so the test was
    # comparing a position against itself. It failed, which is the only reason the flaw was visible.
    m_r420 = re.search(r"\"--force-publish\"\s+not\s+in\s+sys\.argv", src)
    assert m_r420, "the R420 force-publish check vanished; this test's premise is stale"
    assert m_imp.start() < m_r420.start(), (
        "the impossibility refusal must precede the --force-publish check, or the flag would bypass it")
