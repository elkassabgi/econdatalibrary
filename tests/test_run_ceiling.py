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


def _bindings(fn, name):
    """Every node inside `fn` that BINDS `name`: plain, augmented, annotated, tuple/list/starred unpacking, for
    and comprehension targets, walrus, with ... as, except ... as, import as. (R1245: the first pin read only
    `name = value` and four ordinary rebindings fooled it.)"""
    import ast

    def binds(target):
        return any(isinstance(n, ast.Name) and n.id == name and isinstance(n.ctx, ast.Store) for n in ast.walk(target))
    out = []
    for node in ast.walk(fn):
        if isinstance(node, ast.Assign) and any(binds(t) for t in node.targets):
            out.append(node)
        elif isinstance(node, (ast.AugAssign, ast.AnnAssign, ast.For, ast.AsyncFor, ast.comprehension)) \
                and binds(node.target):
            out.append(node)
        elif isinstance(node, ast.NamedExpr) and node.target.id == name:
            out.append(node)
        elif isinstance(node, (ast.With, ast.AsyncWith)) and any(
                i.optional_vars is not None and binds(i.optional_vars) for i in node.items):
            out.append(node)
        elif isinstance(node, ast.ExceptHandler) and node.name == name:
            out.append(node)
        elif isinstance(node, (ast.Import, ast.ImportFrom)) and any(
                (a.asname or a.name) == name for a in node.names):
            out.append(node)
    return out


def _run_once_ast():
    import ast
    import inspect
    tree = ast.parse(inspect.getsource(orchestrate))
    fns = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_once"]
    assert len(fns) == 1
    return fns[0]


def test_the_csv_phase_arms_its_fence_from_the_helper():
    """The call site, not only the helper: run_once's `(csv phase)` fence must take its minutes from
    _csv_fence_min(), bound ONCE, and the fenced block must be the one that runs the CSV phase. Read through the
    parser, so a comment or string cannot satisfy it."""
    import ast
    run_once = _run_once_ast()
    fenced = [w for w in ast.walk(run_once) if isinstance(w, ast.With) and any(
        isinstance(i.context_expr, ast.Call) and getattr(i.context_expr.func, "id", None) == "_unit_deadline"
        and "(csv phase)" in ast.unparse(i.context_expr.args[0]) for i in w.items)]
    assert len(fenced) == 1, [ast.unparse(w)[:120] for w in fenced]
    call = next(i.context_expr for i in fenced[0].items)
    arg = call.args[1]
    assert isinstance(arg, ast.Name), ast.unparse(arg)
    binds = _bindings(run_once, arg.id)
    assert len(binds) == 1 and isinstance(binds[0], ast.Assign) and len(binds[0].targets) == 1 \
        and ast.unparse(binds[0].value) == "_csv_fence_min()", [ast.unparse(b)[:120] for b in binds]
    # what the fence COVERS (R1245 RV5: the work moved out of the `with` passed every test)
    inside = {getattr(n.func, "id", None) for s in fenced[0].body for n in ast.walk(s) if isinstance(n, ast.Call)}
    assert "_derive_changed_csvs" in inside, inside


def test_the_retry_drain_has_its_own_fence_and_a_trip_clears_nothing():
    """R1245 finding 4: the drain ran after the csv fence exited, bound only by a soft budget checked between
    ids. Its derive_and_put must sit inside its own _unit_deadline (minutes from _csv_fence_min()), and the
    UnitTimeout handler must book EVERY id as failed - an empty answer reads as "all derived" and clears them."""
    import ast
    run_once = _run_once_ast()
    tries = []
    for t in ast.walk(run_once):
        if not isinstance(t, ast.Try):
            continue
        withs = [w for w in t.body if isinstance(w, ast.With) and any(      # DIRECTLY in this try's body
            isinstance(i.context_expr, ast.Call) and "(csv retry drain)" in ast.unparse(i.context_expr) for i in w.items)]
        if withs:
            tries.append((t, withs))
    assert len(tries) == 1, len(tries)
    t, withs = tries[0]
    call = withs[0].items[0].context_expr
    assert getattr(call.func, "id", None) == "_unit_deadline" and ast.unparse(call.args[1]) == "_csv_fence_min()", \
        ast.unparse(call)
    inside = {ast.unparse(n.func) for s in withs[0].body for n in ast.walk(s) if isinstance(n, ast.Call)}
    assert "_derive_mod.derive_and_put" in inside, inside
    handlers = [h for h in t.handlers if isinstance(h.type, ast.Name) and h.type.id == "UnitTimeout"]
    assert len(handlers) == 1
    assigns = [ast.unparse(s) for s in handlers[0].body if isinstance(s, ast.Assign)]
    assert assigns == ["_out = {'failed': list(_retry_ids)}"], assigns
    # and nothing else in run_once calls the drain's derive outside that fence
    outside = [n for n in ast.walk(run_once) if isinstance(n, ast.Call)
               and ast.unparse(n.func) == "_derive_mod.derive_and_put"]
    assert len(outside) == 1, len(outside)


def test_the_binding_scan_can_fail():
    """The four rebindings that fooled the first pin (R1245 RV1-RV4), plus the others, are each seen."""
    import ast
    for src in ("x = f()\nx += 59.0", "x = f()\nx: float = 60.0", "x = f()\nx, y = 60.0, None",
                "x = f()\nfor x in (60.0,): pass", "x = f()\n(x := 60.0)", "x = f()\nwith g() as x: pass",
                "x = f()\n[0 for x in ()]", "x = f()\nimport math as x"):
        fn = ast.parse("def run_once():\n" + "\n".join("    " + ln for ln in src.splitlines()))
        assert len(_bindings(fn.body[0], "x")) == 2, src


# ---- the unit windows past the ceiling (R1245: the same class one function away) ---------------------------

def test_unit_window_past_the_ceiling_arms_the_floor_not_zero(monkeypatch):
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)
    orchestrate._RUN_DEADLINE_TS = time.time() - 60
    assert orchestrate._unit_window_min() == orchestrate._PAST_CEILING_MIN == 1.0


def test_unit_window_unchanged_inside_the_budget_and_with_no_ceiling(monkeypatch):
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)       # 45
    orchestrate._RUN_DEADLINE_TS = time.time() + 10 * 60
    assert 4.9 < orchestrate._unit_window_min() <= 5.0                   # half the remainder
    orchestrate._RUN_DEADLINE_TS = time.time() + 200 * 60
    assert orchestrate._unit_window_min() == 45.0
    orchestrate._RUN_DEADLINE_TS = None
    assert orchestrate._unit_window_min() == 45.0


@pytest.mark.parametrize("t", ["0", "-5"])
def test_a_disabled_unit_timeout_stays_disabled_past_the_ceiling(monkeypatch, t):
    monkeypatch.setenv("AQUEDUCT_UNIT_TIMEOUT_MIN", t)
    orchestrate._RUN_DEADLINE_TS = time.time() - 60
    assert orchestrate._unit_window_min() == 0.0


def test_past_the_ceiling_the_unit_alarm_really_arms(monkeypatch):
    """The effect, not the number: with a POSIX signal module, the window past the ceiling ARMS a 60 s alarm
    (0.0 armed nothing). Windows has no setitimer, so the module is faked."""
    import sys
    import types
    calls = []
    fake = types.SimpleNamespace(SIGALRM=14, ITIMER_REAL=0, signal=lambda *a: None,
                                 setitimer=lambda which, secs: calls.append(secs))
    monkeypatch.setitem(sys.modules, "signal", fake)
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)
    orchestrate._RUN_DEADLINE_TS = time.time() - 60
    with orchestrate._unit_deadline("zz/_all", orchestrate._unit_window_min()) as d:
        assert d.armed
    with orchestrate._unit_deadline("zz/_all (csv phase)", orchestrate._csv_fence_min()) as d:
        assert d.armed
    assert calls[0] == 60.0 and 60.0 in calls[1:], calls
