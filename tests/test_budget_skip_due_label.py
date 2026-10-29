"""B3: every BUDGET SKIP line says whether the skipped unit was DUE (starvation plan rev 10, PR 1).

The budget gate in run_once runs BEFORE the due check, so on the 2026-10-28 13:45Z desktop pass 21 skip lines were
printed against 15 units due, and the lines were read as 21 lost turns (ledger R1708). The fix is PRINT ONLY:
each line carries `due` / `not due` from the same Strategy.is_due the due check uses. Nothing is reordered, so no
admission decision can change - the production admission harness (eia and unctad_tradeservcatbypartner admitted,
eia with 80 minutes left skipped) is the end-to-end proof, run on a git archive plus a state copy; these tests
pin the line and the order.

The run tests drive the REAL run_once, with the REAL strategy classes (so the real is_due), over two cloud
sources with a fetcher module. A budget of 30 minutes skips both (no cost on record -> a 90-minute reserve), and
`_has_adapter` - the first call after the gate - is replaced by a recorder, so nothing past the gate runs.
"""
from __future__ import annotations

import ast
import inspect
import re

import pytest

from updater import orchestrate
from updater.state import StateStore, now_utc

SOURCES = ["cnb", "frankfurter"]          # both cadence: daily, both with a fetcher module


def _run(tmp_path, monkeypatch, capsys, budget, not_due=(), force=False):
    monkeypatch.delenv("AQUEDUCT_LIVE_ONLY", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", str(budget))
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    store = StateStore(str(tmp_path / "state.db"))
    for sid in not_due:
        # succeeded just now on a daily cadence -> Strategy.is_due says no
        store.upsert_unit(sid, "_all", strategy="extend_by_date", cadence="daily", status="ok",
                          last_success_utc=now_utc(), last_attempt_utc=now_utc())
    admitted = []
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda unit: admitted.append(unit.source_id) or False)
    try:
        orchestrate.run_once(sources=list(SOURCES), dry=True, store=store, force=force)
    except SystemExit:
        pass                                   # "no runnable adapter" when the recorder admitted something
    out = capsys.readouterr().out
    skips = {m.group(1): m.group(2) for m in
             re.finditer(r"BUDGET SKIP (\w+)/_all \((.+?)\): the gate reserves", out)}
    return admitted, skips, out, store


def test_each_skip_line_says_due_or_not_due(tmp_path, monkeypatch, capsys):
    admitted, skips, out, _ = _run(tmp_path, monkeypatch, capsys, budget=30, not_due=["cnb"])
    assert admitted == [], "a 30-minute budget admits neither unit (90-minute reserve)"
    assert skips == {"cnb": "not due", "frankfurter": "due"}, out[-2000:]
    # the rest of the line - the numbers that decided - is unchanged
    assert re.search(r"BUDGET SKIP frankfurter/_all \(due\): the gate reserves 90 min \(worst case, not an "
                     r"estimate\) and \d+ min remain in this pass - deferred to the next one", out)


def test_the_label_is_the_due_check_itself(tmp_path, monkeypatch, capsys):
    """Same state, budget large enough to admit: the unit labelled `not due` above is the one the due check would
    skip. (With _has_adapter recording False the loop stops at PENDING, so this asks is_due directly on the
    stored state the label read.)"""
    _, skips, _, store = _run(tmp_path, monkeypatch, capsys, budget=30, not_due=["cnb"])
    from updater.registry import all_units
    units = {u.source_id: u for u in all_units() if u.source_id in SOURCES and u.unit_id == "_all"}
    assert set(units) == set(SOURCES)
    for sid, label in skips.items():
        strat = orchestrate.get_strategy(units[sid].strategy)
        assert label == ("due" if strat.is_due(units[sid], store.get_unit(sid, "_all")) else "not due"), sid


def test_negative_control_no_skip_no_label_and_the_units_pass_the_gate(tmp_path, monkeypatch, capsys):
    """With no budget (0) nothing is skipped: no BUDGET SKIP line, and both units reach the first call after the
    gate - the label adds no skip of its own."""
    admitted, skips, out, _ = _run(tmp_path, monkeypatch, capsys, budget=0, not_due=["cnb"])
    assert skips == {} and "BUDGET SKIP" not in out
    assert sorted(admitted) == sorted(SOURCES)


def test_forced_run_labels_the_skip_as_forced(tmp_path, monkeypatch, capsys):
    _, skips, _, _ = _run(tmp_path, monkeypatch, capsys, budget=30, not_due=["cnb"], force=True)
    assert skips == {"cnb": "due (--force)", "frankfurter": "due (--force)"}


def test_a_label_that_cannot_be_computed_never_breaks_the_loop(tmp_path, monkeypatch, capsys):
    class Boom:
        def is_due(self, unit, us):
            raise RuntimeError("no")
    monkeypatch.setattr(orchestrate, "get_strategy", lambda name: Boom())
    admitted, skips, out, _ = _run(tmp_path, monkeypatch, capsys, budget=30)
    assert skips == {"cnb": "due unknown (RuntimeError)", "frankfurter": "due unknown (RuntimeError)"}, out[-1500:]
    assert admitted == []


def _run_once_loop():
    tree = ast.parse(inspect.getsource(orchestrate))
    fn = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_once")
    return next(n for n in ast.walk(fn) if isinstance(n, ast.For) and ast.unparse(n.target) == "unit"
                and "BUDGET SKIP" in ast.unparse(n))


def test_the_due_check_still_follows_the_budget_gate_and_the_adapter_check():
    """PRINT ONLY means nothing moved: in the unit loop the budget gate comes first, then _has_adapter, then the
    due check that `continue`s. The label's is_due call lives in its helper, not in the loop."""
    loop = _run_once_loop()

    def first_line(pred):
        lines = [n.lineno for n in ast.walk(loop) if pred(n)]
        assert lines, "pattern not found"
        return min(lines)
    gate = first_line(lambda n: isinstance(n, ast.Constant) and isinstance(n.value, str)
                      and "BUDGET SKIP" in n.value)
    adapter = first_line(lambda n: isinstance(n, ast.Call) and ast.unparse(n.func) == "_has_adapter")
    due = first_line(lambda n: isinstance(n, ast.Call) and ast.unparse(n.func) == "strat.is_due")
    assert gate < adapter < due, (gate, adapter, due)
    calls = [ast.unparse(n.func) for n in ast.walk(loop) if isinstance(n, ast.Call)]
    assert calls.count("strat.is_due") == 1, "the due check that decides appears once"
    assert not any(c.endswith(".is_due") and c != "strat.is_due" for c in calls), calls


def test_the_order_pin_can_fail():
    """Negative control for the pin above: the same predicate over a loop with the due check hoisted first."""
    src = "for unit in units:\n    if not strat.is_due(unit, us):\n        continue\n" \
          "    print('BUDGET SKIP x')\n    runnable = _has_adapter(unit)\n"
    loop = ast.parse(src).body[0]
    due = min(n.lineno for n in ast.walk(loop) if isinstance(n, ast.Call) and ast.unparse(n.func) == "strat.is_due")
    gate = min(n.lineno for n in ast.walk(loop) if isinstance(n, ast.Constant) and "BUDGET SKIP" in str(n.value))
    assert not gate < due


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
