"""A fetcher that PROVES it merged nothing must not be booked a coherence debt.

THE BUG. `updater/orchestrate.py` books a CSV-coherence violation, and a `full_rederive_owed`
row, whenever a fetcher reports no `series_cursors` - guarded by `if res.obs:`. `res.obs` is
the STORE'S TOTAL row count (`finalize`'s own parameter is named `total_rows`, and ~120 of ~123
call sites pass `blob.row_count(path)`), so idb was booked a debt for 15,066,444 observations on
runs that merged ZERO, three times, with the same figure each time - because that is how many
rows its store holds.

TWO PREDICATES WERE REJECTED BEFORE THIS ONE, and the tests below pin why:

  * `res.obs == 0` - false for every source with any data at all.
  * `tally.added == 0` - `added` carries two conventions in production (net new rows in ~40
    fetchers, rows PARSED in _giant/noaa/usda/abs/ecb/bcb/insee_melodi/istat), and under the
    first a SAME-PERIOD REVISION collapses old and new into one row carrying the new value
    (merge.py:186-190, "new wins"), so the count is unchanged while the served value changed.
    Inferring "nothing merged" from it would have silenced eight sources, four of which pair
    that convention with `cursors_from_table`'s fail-open `except: return {}`.

`merged_rows` is therefore an AFFIRMATIVE CLAIM: None means "not reported" and behaves exactly
as before, which is what almost every fetcher truthfully is.
"""
from __future__ import annotations

import os
import sys
import unittest.mock as mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import orchestrate                                   # noqa: E402
from updater.strategies.base import Result                        # noqa: E402
from updater.strategies.fetchers._common import Tally, finalize    # noqa: E402


class _Unit:
    source_id = "testsrc"
    unit_id = "_all"
    key = "testsrc/_all"


def _run(res):
    with mock.patch.object(orchestrate, "_catalog_series_count", return_value=10, create=True):
        return orchestrate._derive_changed_csvs(_Unit(), res, blob=None)


def _res(**kw):
    base = dict(status="partial", obs=15_066_444, series_cursors=None, new_vintage="v1")
    base.update(kw)
    return Result(**base)


def test_a_proven_zero_merge_books_no_debt():
    failed, note, deferred, reasons = _run(_res(merged_rows=0))
    assert note is None, "a run that merged nothing cannot have made a served CSV stale"
    assert failed == [] and deferred == [] and reasons == {}


def test_not_reported_keeps_the_old_behaviour_exactly():
    """None is almost every fetcher. It must still book the note."""
    failed, note, deferred, reasons = _run(_res(merged_rows=None))
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


def test_a_real_merge_with_no_cursors_still_books_the_debt():
    """The case the guard exists for: rows moved and we cannot say which series."""
    failed, note, deferred, reasons = _run(_res(merged_rows=5_000))
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


def test_the_note_no_longer_calls_the_store_total_a_merge_count():
    _, note, _, _ = _run(_res(merged_rows=None))
    assert "merged obs" not in note, note
    assert "store holds" in note, note


def test_a_duck_typed_result_without_the_field_does_not_raise():
    """Driven by stand-ins in other tests, and by anything Result-shaped a fetcher returns.

    A bare `res.merged_rows` raises AttributeError inside the orchestrator's outer except,
    which books transient_fail AFTER a successful publish with every state write skipped -
    the gleif 2-tuple disease.
    """
    class _Duck:
        obs = 42
        series_cursors = None
        new_vintage = "v1"

    _, note, _, _ = _run(_Duck())
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


def test_finalize_defaults_to_not_reported():
    t = Tally()
    t.added_unit(3)
    r = finalize(t, 1_000, "2026-01-01", source="testsrc")
    assert r.merged_rows is None, "silence must be the default, not a claim of zero"
    assert r.obs == 1_000


def test_finalize_carries_an_explicit_zero():
    t = Tally()
    t.transient_unit("boom")
    r = finalize(t, 1_000, None, source="testsrc", merged_rows=0)
    assert r.status == "partial", "the transient path must still be reached"
    assert r.merged_rows == 0, "a transient run can still prove it merged nothing"


def test_finalize_carries_it_on_the_ok_path_too():
    t = Tally()
    t.added_unit(7)
    r = finalize(t, 1_000, "2026-01-01", source="testsrc", merged_rows=7)
    assert r.status == "ok" and r.merged_rows == 7


def test_a_hand_built_Result_defaults_to_not_reported():
    """The dataclass default is the one that reaches the six hand-built Result sites.

    `finalize` always passes `merged_rows=` explicitly, so a test that only goes through
    finalize cannot see this: flipping the dataclass default to 0 would make every
    hand-constructed Result - _giant.py:405 (the capped path, unctad's and oecd's most common
    partial), bfs, edgar_jrc, sec_edgar, unsdg, vdem - silently claim it merged nothing, and
    the guard would go quiet for all of them. Measured: that mutation survives the rest of
    this file.
    """
    r = Result(status="partial", obs=99)
    assert r.merged_rows is None, (
        "a Result built without merged_rows must claim NOTHING, not zero")


def test_idb_reports_the_merge_count_from_BEFORE_its_store_total_substitution():
    """The ordering is the whole fix for idb, and no behavioural test can reach it offline.

    `idb.py` overwrites `published` with the whole store's row count when nothing merged, so
    passing `published` to finalize reports 15,066,444 as a merge count - which is exactly the
    defect. The true value must be captured before that branch. Measured: passing `published`
    survives every other test here.
    """
    import ast
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    path = os.path.join(root, "updater/strategies/fetchers/idb.py")
    with open(path, encoding="utf-8") as fh:
        src = fh.read()
    tree = ast.parse(src, filename="idb.py")

    # the line that substitutes the store total
    subs = [n.lineno for n in ast.walk(tree)
            if isinstance(n, ast.If) and "published == 0" in ast.unparse(n.test)]
    assert len(subs) == 1, f"expected one `published == 0` substitution, found {subs}"

    calls = [n for n in ast.walk(tree)
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id == "finalize"]
    assert calls, "idb.py must call finalize"
    for call in calls:
        kw = {k.arg: k.value for k in call.keywords if k.arg}
        assert "merged_rows" in kw, f"idb.py:{call.lineno} must report merged_rows"
        arg = kw["merged_rows"]
        assert isinstance(arg, ast.Name), f"idb.py:{call.lineno} merged_rows must be a name"
        assert arg.id != "published", (
            f"idb.py:{call.lineno} passes `published`, which by that point holds the WHOLE "
            f"STORE's row count - the exact false number this signal exists to stop")
        # and the name it does pass must be assigned BEFORE the substitution
        assigns = [n.lineno for n in ast.walk(tree)
                   if isinstance(n, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == arg.id for t in n.targets)]
        assert assigns, f"{arg.id} is never assigned"
        assert min(assigns) < subs[0], (
            f"{arg.id} is assigned at line {min(assigns)}, at or after the store-total "
            f"substitution at line {subs[0]} - it would carry the substituted value")


def test_the_three_result_rebuilds_carry_every_optional_field():
    """dst, bls and census rebuild a finalize Result field by field.

    Each previously dropped whatever was not enumerated, so a new field silently vanished for
    exactly those sources - two of which (dst, bls) are in the population this change targets.
    """
    import ast
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    optional = {"merged_rows", "cursor_cap_hit", "changed_keys", "series_cursors",
                "last_obs_date", "new_vintage"}
    for rel in ("updater/strategies/fetchers/dst.py",
                "updater/strategies/fetchers/bls.py",
                "updater/strategies/fetchers/census.py"):
        path = os.path.join(root, rel)
        with open(path, encoding="utf-8") as fh:
            tree = ast.parse(fh.read(), filename=rel)
        rebuilds = [n for n in ast.walk(tree)
                    if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                    and n.func.id == "Result"]
        assert rebuilds, f"{rel}: expected at least one Result(...) rebuild"
        for call in rebuilds:
            named = {k.arg for k in call.keywords if k.arg}
            missing = optional - named
            assert not missing, (
                f"{rel}:{call.lineno} rebuilds a Result and drops {sorted(missing)} - "
                f"those fields silently become defaults for this source")
