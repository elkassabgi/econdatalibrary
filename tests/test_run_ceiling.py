"""Discriminating tests for the ONE-ceiling fix (run 31466202723, R414 rule).

Three consecutive daily runs were step-killed because each phase was bounded
alone but the phases' SUM crossed the step timeout. The fix has two mechanical
parts, and each gets BOTH directions tested (a guard that only proves it blocks
can have been wired to block everything — R414):

  1. the unit start-gate looks ahead a worst case of 2x the per-unit SIGALRM;
  2. every derive call's budget is capped by the ceiling's remainder, with a
     floor above zero because derive.py treats budget_min=0 as UNBOUNDED.
"""
import time

import pytest

from updater import orchestrate


@pytest.fixture(autouse=True)
def _restore_deadline():
    prev = orchestrate._RUN_DEADLINE_TS
    yield
    orchestrate._RUN_DEADLINE_TS = prev


def _gate_would_skip(deadline_ts: float) -> bool:
    """The exact expression the unit loop evaluates before starting a unit."""
    return time.time() + 2 * orchestrate._unit_timeout_min() * 60.0 > deadline_ts


def test_gate_skips_when_worst_case_crosses_ceiling(monkeypatch):
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)  # default 45
    # 80 minutes left < 2x45 worst case -> must refuse to start (worldbank_esg
    # entered minute 207 of a 240 gate and ran 78 min into the step kill).
    assert _gate_would_skip(time.time() + 80 * 60)


def test_gate_allows_when_worst_case_fits(monkeypatch):
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)
    # 100 minutes left > 90 worst case -> must start (a gate that refuses
    # everything is the R414 failure in the other direction).
    assert not _gate_would_skip(time.time() + 100 * 60)


def test_derive_budget_capped_by_remainder(monkeypatch):
    monkeypatch.setenv("AQUEDUCT_DERIVE_BUDGET_MIN", "45")
    orchestrate._RUN_DEADLINE_TS = time.time() + 10 * 60   # 10 min left
    kw = orchestrate._capped_derive_budget()
    assert 9.0 < kw["budget_min"] <= 10.0                   # remainder wins over 45


def test_derive_budget_env_wins_when_remainder_large(monkeypatch):
    monkeypatch.setenv("AQUEDUCT_DERIVE_BUDGET_MIN", "45")
    orchestrate._RUN_DEADLINE_TS = time.time() + 200 * 60
    assert orchestrate._capped_derive_budget()["budget_min"] == pytest.approx(45.0)


def test_derive_budget_floor_never_unbounds(monkeypatch):
    # THE TRAP: derive.py treats budget_min=0 as 'disabled' (unbounded). At or
    # past the ceiling the cap must clamp to a tiny positive value that defers
    # everything — never to the 0 that would unbound the derive.
    monkeypatch.setenv("AQUEDUCT_DERIVE_BUDGET_MIN", "45")
    orchestrate._RUN_DEADLINE_TS = time.time() - 60        # ceiling already passed
    b = orchestrate._capped_derive_budget()["budget_min"]
    assert b > 0 and b <= 0.05 + 1e-9


def test_no_ceiling_means_env_path(monkeypatch):
    orchestrate._RUN_DEADLINE_TS = None
    assert orchestrate._capped_derive_budget() == {}


# ---- the CSV-phase fence (review R1144, "Outside this branch") -------------------------------------
# `(_remaining_run_min() or 60.0)` read a remainder of 0.0 (the ceiling has PASSED) as falsy and armed
# the full 60-minute fence - enough to carry the run into the step kill the fence exists to prevent.

def test_csv_fence_past_the_ceiling_is_the_one_minute_floor():
    orchestrate._RUN_DEADLINE_TS = time.time() - 60        # ceiling passed: _remaining_run_min() == 0.0
    assert orchestrate._remaining_run_min() == 0.0
    assert orchestrate._csv_fence_min() == 1.0


def test_csv_fence_with_no_ceiling_is_sixty():
    orchestrate._RUN_DEADLINE_TS = None                     # AQUEDUCT_RUN_BUDGET_MIN <= 0: unknown
    assert orchestrate._remaining_run_min() is None
    assert orchestrate._csv_fence_min() == 60.0


def test_csv_fence_is_the_remainder_plus_grace_inside_the_cap():
    orchestrate._RUN_DEADLINE_TS = time.time() + 10 * 60
    assert 11.9 < orchestrate._csv_fence_min() <= 12.0      # 10 min left + 2 grace
    orchestrate._RUN_DEADLINE_TS = time.time() + 200 * 60
    assert orchestrate._csv_fence_min() == 60.0             # capped
    orchestrate._RUN_DEADLINE_TS = time.time() + 3          # a few seconds left: still graced, not floored
    assert 2.0 <= orchestrate._csv_fence_min() < 2.1


def test_the_csv_phase_arms_its_fence_from_the_helper():
    """The call site, not only the helper: run_once's `(csv phase)` fence must take its minutes from
    _csv_fence_min(). Read through the parser, so a comment or string cannot satisfy it."""
    import ast
    import inspect
    tree = ast.parse(inspect.getsource(orchestrate))
    fence_calls, assigned = [], {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            assigned.setdefault(node.targets[0].id, []).append(node.value)
        if (isinstance(node, ast.Call) and getattr(node.func, "id", None) == "_unit_deadline"
                and "(csv phase)" in ast.unparse(node.args[0])):
            fence_calls.append(node)
    assert len(fence_calls) == 1, [ast.unparse(c) for c in fence_calls]
    arg = fence_calls[0].args[1]
    assert isinstance(arg, ast.Name), ast.unparse(arg)
    sources = [ast.unparse(v) for v in assigned[arg.id]]
    assert sources == ["_csv_fence_min()"], sources
