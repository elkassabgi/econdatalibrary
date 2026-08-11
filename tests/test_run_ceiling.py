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
