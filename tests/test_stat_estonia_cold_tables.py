"""stat_estonia's cold-table cadence: it must be a CADENCE, never a skip.

WHY IT EXISTS, measured 2026-08-04. The catalogue holds 4,978 tables; 2,832 of them (56.9%)
are under `Lepetatud_tabelid`, the publisher's own archive tree. At ~60 tables/min under an
18-minute budget the first successful capped run spent 100% of its 1,079 tables inside that
archive and never reached a live subject at all.

WHY TWO SIGNALS, not one -- this is the part measuring corrected:

  * freshness alone over-reaches. A 3-year cutoff calls 94.8% of the archive cold but ALSO
    481 of 1,578 (30.5%) stored tables in LIVE subjects, because finished surveys sit in
    active trees (KO11..KO19 end 2020, SHL0xx end 2015).
  * the archive label alone under-reaches. 963 of the 2,832 archive tables have NO stored
    rows, so a freshness-only rule leaves a third of the archive permanently hot.

THE FAILURE MODE THIS GUARDS. A deferred table that is never revisited is R190: a silent
truncation that reports itself as a healthy `partial` indefinitely. So the tests below are
mostly about COVERAGE -- that repeated passes reach every cold table -- rather than about
any single pass being right.
"""
from __future__ import annotations
import datetime as dt
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater.strategies.fetchers.stat_estonia import (      # noqa: E402
    _DISCONTINUED_SUBJECT, _cold_plan, _is_cold, _table_prefix,
)

TODAY = dt.date(2026, 8, 4)
CUTOFF = 1095.0


def _tables(subject, n, start=0):
    return [{"path": f"{subject}/dir/T{i:04d}.PX"} for i in range(start, start + n)]


# --------------------------------------------------------------------------- #
# _is_cold: the classification rule
# --------------------------------------------------------------------------- #
def test_archive_subject_is_cold_even_with_no_stored_rows():
    """The 963 archive tables with no store. A freshness-only rule leaves them hot for ever."""
    assert _is_cold(_DISCONTINUED_SUBJECT, None, TODAY, CUTOFF) is True


def test_unknown_table_OUTSIDE_the_archive_stays_hot():
    """NEVER DEFER AN UNKNOWN. A live-subject table with no stored rows may be brand new;
    deferring it would delay first data by up to a full cold cycle."""
    assert _is_cold("majandus", None, TODAY, CUTOFF) is False


def test_stale_table_in_a_LIVE_subject_is_cold():
    """The 481. eri-valdkondade-statistika/.../SHL010.PX genuinely ends 2015."""
    assert _is_cold("eri-valdkondade-statistika", dt.date(2015, 12, 31), TODAY, CUTOFF) is True


def test_recent_table_in_a_live_subject_is_hot():
    assert _is_cold("majandus", dt.date(2026, 6, 1), TODAY, CUTOFF) is False


def test_a_future_frontier_is_a_PROJECTION_and_stays_hot():
    """R327: a 2085 frontier is not staleness. Judging it as 'very fresh' would be wrong the
    other way, so the rule declines to judge and leaves the table hot."""
    assert _is_cold("majandus", dt.date(2085, 12, 31), TODAY, CUTOFF) is False


def test_string_dates_and_junk_are_handled_without_deferring():
    assert _is_cold("majandus", "2015-12-31", TODAY, CUTOFF) is True
    assert _is_cold("majandus", "not-a-date", TODAY, CUTOFF) is False


def test_boundary_is_strict():
    exactly = TODAY - dt.timedelta(days=int(CUTOFF))
    assert _is_cold("majandus", exactly, TODAY, CUTOFF) is False
    assert _is_cold("majandus", exactly - dt.timedelta(days=1), TODAY, CUTOFF) is True


# --------------------------------------------------------------------------- #
# _cold_plan: the scheduling rule
# --------------------------------------------------------------------------- #
def test_plan_selects_only_a_slice():
    tabs = _tables(_DISCONTINUED_SUBJECT, 500)
    cold, due = _cold_plan(tabs, _DISCONTINUED_SUBJECT, {}, "", TODAY, CUTOFF, 150)
    assert len(cold) == 500
    assert len(due) == 150


def test_live_subject_with_no_stale_tables_plans_nothing():
    tabs = _tables("majandus", 40)
    stored = {_table_prefix(t["path"]): dt.date(2026, 6, 1) for t in tabs}
    cold, due = _cold_plan(tabs, "majandus", stored, "", TODAY, CUTOFF, 150)
    assert cold == set() and due == set()


def test_every_cold_table_is_reached_within_ceil_n_over_slice_passes():
    """THE COVERAGE PROPERTY. 500 archive tables, 150 per pass -> all seen within 4 passes,
    each pass resuming from the last one ACTUALLY VISITED."""
    tabs = _tables(_DISCONTINUED_SUBJECT, 500)
    seen, bookmark = set(), ""
    for _ in range(4):
        cold, due = _cold_plan(tabs, _DISCONTINUED_SUBJECT, {}, bookmark, TODAY, CUTOFF, 150)
        assert due, "a pass planned nothing — the rotation stalled"
        # simulate visiting all due tables, in list order
        order = [t["path"] for t in tabs if t["path"] in due]
        seen |= set(order)
        bookmark = order[-1]
    assert len(seen) == 500, f"only {len(seen)}/500 archive tables reached in 4 passes"


def test_a_budget_cut_does_not_skip_the_tables_it_never_reached():
    """THE R190 CASE. The pass plans 150 but the deadline stops it after 10. The bookmark
    must advance over the 10 VISITED, so the other 140 are still first in line."""
    tabs = _tables(_DISCONTINUED_SUBJECT, 500)
    _, due = _cold_plan(tabs, _DISCONTINUED_SUBJECT, {}, "", TODAY, CUTOFF, 150)
    order = [t["path"] for t in tabs if t["path"] in due]
    visited, bookmark = order[:10], order[9]          # budget died after 10

    _, due2 = _cold_plan(tabs, _DISCONTINUED_SUBJECT, {}, bookmark, TODAY, CUTOFF, 150)
    missed = set(order[10:])
    assert missed & due2 == missed, (
        "tables planned but never reached were not re-offered next pass — that is the silent "
        "truncation this whole mechanism exists to avoid")
    assert not (set(visited) & due2), "already-visited tables were re-offered ahead of the tail"


def test_nothing_reached_means_the_bookmark_does_not_move():
    """If the budget dies before ANY cold table, the caller leaves the bookmark alone and the
    same slice must come back."""
    tabs = _tables(_DISCONTINUED_SUBJECT, 500)
    _, due_a = _cold_plan(tabs, _DISCONTINUED_SUBJECT, {}, "", TODAY, CUTOFF, 150)
    _, due_b = _cold_plan(tabs, _DISCONTINUED_SUBJECT, {}, "", TODAY, CUTOFF, 150)
    assert due_a == due_b


def test_rotation_wraps_past_the_end():
    tabs = _tables(_DISCONTINUED_SUBJECT, 200)
    last = tabs[-1]["path"]
    _, due = _cold_plan(tabs, _DISCONTINUED_SUBJECT, {}, last, TODAY, CUTOFF, 150)
    assert tabs[0]["path"] in due, "the cycle did not wrap — the head would never be revisited"


def test_a_stale_bookmark_does_not_stall_the_rotation():
    """A renamed/retired table must not freeze the cadence: unknown bookmark -> start at the
    head, which is the same contract rotate_after gives the other two bookmarks."""
    tabs = _tables(_DISCONTINUED_SUBJECT, 300)
    _, due = _cold_plan(tabs, _DISCONTINUED_SUBJECT, {}, "who/knows/GONE.PX",
                        TODAY, CUTOFF, 150)
    assert len(due) == 150 and tabs[0]["path"] in due


def test_slice_larger_than_the_cold_set_takes_everything_once():
    tabs = _tables(_DISCONTINUED_SUBJECT, 20)
    cold, due = _cold_plan(tabs, _DISCONTINUED_SUBJECT, {}, "", TODAY, CUTOFF, 150)
    assert due == cold


def test_mixed_subject_defers_only_the_stale_ones():
    """A live subject holding both: fresh tables stay hot, finished ones join the cadence."""
    tabs = _tables("sotsiaalelu", 6)
    stored = {}
    for i, t in enumerate(tabs):
        stored[_table_prefix(t["path"])] = (dt.date(2026, 6, 1) if i % 2 == 0
                                            else dt.date(2015, 12, 31))
    cold, due = _cold_plan(tabs, "sotsiaalelu", stored, "", TODAY, CUTOFF, 150)
    assert len(cold) == 3 and due == cold
    for i, t in enumerate(tabs):
        assert (t["path"] in cold) is (i % 2 == 1)
