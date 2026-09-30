"""A pass whose merges only REVISE stored values must read ok and reach the CSV phase (R1125) - what still applies
after the statcan lane (#65).

statcan booked each cube as added_unit(n - before), i.e. net NEW rows, so a revision-only pass finalized
`no_change` and orchestrate._should_derive_csvs skipped the CSV phase. #88 fixed that in the merging fetcher and
#91 kept a refused cube from stranding the cubes that merged. #65 replaced that fetcher: statcan's update() only
REPORTS now (jobs/statcan_lane.py refreshes and serves the cubes), so the 15 tests that drove the old merging
update() were dropped when #65 merged after #88 and #91 - exactly as #88's merge-order note prescribed. What stays
is source-independent and still true on the lane's tree:
  - finalize(): revised sub-units are ok, never an all-empty structural break, and count as attempted;
  - the status gate admits exactly ok and partial;
  - run_once runs the CSV phase behind that gate (an `and` gate that may carry #65's own conjunct).
"""
from __future__ import annotations

import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate  # noqa: E402


def test_finalize_many_revised_sub_units_are_ok_not_an_all_empty_break():
    """The _common part (from #76): revised sub-units never count toward the all-empty structural raise.
    statcan's floor is 10**9 so its own passes cannot reach it; the default floor (10) can."""
    from updater.strategies.fetchers._common import Tally, finalize
    t = Tally()
    for i in range(12):
        t.revised_unit(f"c{i}")
    res = finalize(t, 0, "2024-01-01", source="x")
    assert res.status == "ok" and "12 sub-unit(s) revised" in res.error, (res.status, res.error)
    t = Tally()
    for i in range(12):
        t.empty_unit()
    with pytest.raises(Exception, match="all 12 attempted sub-units returned empty"):
        finalize(t, 0, "2024-01-01", source="x")


def test_a_revised_sub_unit_counts_as_attempted():
    """A revised cube is one attempted sub-unit: a pass with one revised and one transient cube reads 1/2."""
    from updater.strategies.fetchers._common import Tally, finalize
    t = Tally()
    t.revised_unit("c1")
    t.transient_unit("c2")
    res = finalize(t, 0, "2024-01-01", source="x")
    assert res.status == "partial" and "1/2 sub-unit(s) transient-failed" in res.error, (res.status, res.error)


def test_the_status_gate_admits_exactly_ok_and_partial():
    """R1244 O2: the predicate itself - a revision-only pass reads ok, and ok must be admitted."""
    assert [s for s in ("ok", "partial", "no_change", "transient_fail") if orchestrate._should_derive_csvs(s)] \
        == ["ok", "partial"]


def test_run_once_runs_the_csv_phase_behind_that_gate():
    """R1244 O1: a mutant that never fired run_once's gate survived 73 run_once tests. Pinned through the
    parser: the one call of _derive_changed_csvs in run_once sits under `if _should_derive_csvs(status) and
    not dry:`."""
    import ast
    import inspect
    fn = next(n for n in ast.parse(inspect.getsource(orchestrate)).body
              if isinstance(n, ast.FunctionDef) and n.name == "run_once")
    # an `and` gate that CONTAINS both conditions - not the exact text, so a gate that adds its own conjunct
    # (#65 appends `not _served_by_lane(unit.source_id)`) still passes; an `or`, or a gate without the
    # predicate, does not (R1252: the exact-text pin failed on #65's tree)
    gates = [n for n in ast.walk(fn) if isinstance(n, ast.If) and isinstance(n.test, ast.BoolOp)
             and isinstance(n.test.op, ast.And)
             and {"_should_derive_csvs(status)", "not dry"} <= {ast.unparse(v) for v in n.test.values}]
    assert len(gates) == 1, [ast.unparse(g.test) for g in gates]
    # an extra conjunct must depend on something - `False and ...` (R1244 O1) or `not True` never fires
    constant = [ast.unparse(v) for v in gates[0].test.values if not any(isinstance(t, ast.Name) for t in ast.walk(v))]
    assert not constant, constant
    # and run_once never rebinds the predicate (R1252 V2: a local shadow admitting only 'partial' survived)
    rebinds = [ast.unparse(n)[:80] for n in ast.walk(fn)
               if isinstance(n, (ast.Assign, ast.AnnAssign, ast.AugAssign, ast.FunctionDef, ast.Import, ast.ImportFrom))
               and "_should_derive_csvs" in {getattr(t, "id", None) for t in ast.walk(n) if isinstance(t, ast.Name)
                                             and isinstance(t.ctx, ast.Store)} | {getattr(n, "name", None)}
               | {a.asname or a.name for a in getattr(n, "names", [])
                  if isinstance(n, (ast.Import, ast.ImportFrom))}]
    assert not rebinds, rebinds
    calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "_derive_changed_csvs"]
    inside = [n for s in gates[0].body for n in ast.walk(s)
              if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "_derive_changed_csvs"]
    assert len(calls) == 1 and len(inside) == 1, (len(calls), len(inside))
