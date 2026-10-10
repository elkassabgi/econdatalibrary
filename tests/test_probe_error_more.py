"""More pins for the probe's catch-all in run_once (review AR-265b of PR #132).

tests/test_probe_error_does_not_end_the_run.py let these forms of the handler through (the review's mutants
b07, b13, b16, b17 and b30 survived it): a branch that catches only some exception families, no guard round
the log line, a guard that also swallows a failed state write, and a result label that the heavy workflow's
list does not know. The last two tests are for an exception whose own __repr__ raises (the review's note N3)
in the CHANGE PROBE; the fetch's branch has the same call and is pinned by a count of the source text only.
The tests use that file's helper: the REAL run_once over two registry sources with a fake strategy.
"""
from __future__ import annotations

import io
import os
import re
import sqlite3
import sys

import pytest

from test_probe_error_does_not_end_the_run import _run
from updater import orchestrate


@pytest.mark.parametrize("cls", [KeyError, TypeError, AttributeError, MemoryError])
def test_an_exception_of_any_class_is_one_units_failure(tmp_path, monkeypatch, cls):
    """The other file raises RuntimeError, ValueError and requests' HTTPError (an OSError) only: a branch that
    caught just those three families passed it. A probe that reads JSON raises these four."""
    strat, store, run = _run(tmp_path, monkeypatch, cls("boom"))
    results = run()
    first = strat.probed[0]
    assert (f"{first}/_all", "error") in results and len(strat.probed) == 2
    assert store.get_unit(first, "_all")["last_error"].startswith(f"detect:UNEXPECTED:{cls.__name__}(")


def test_a_log_that_refuses_the_line_does_not_end_the_run(tmp_path, monkeypatch):
    """The guard round the handler's print. With ascii() the text can always be encoded; what still raises
    inside the print is a stream that fails (a full disk, a closed pipe) - this test - and ascii() of an
    exception whose repr raises (the test further down). Without the guard the run ended here."""

    class _Refuses(io.TextIOBase):
        def write(self, s):
            if "PROBE ERROR" in s:
                raise OSError(28, "No space left on device")
            return len(s)

    strat, store, run = _run(tmp_path, monkeypatch, RuntimeError("plain text"))
    monkeypatch.setattr(sys, "stdout", _Refuses())
    results = run()
    first = strat.probed[0]
    assert (f"{first}/_all", "error") in results and len(strat.probed) == 2
    assert store.get_unit(first, "_all")["status"] == "transient_fail"


def test_a_state_write_that_fails_is_not_swallowed_with_the_log_line(tmp_path, monkeypatch):
    """The guard is round the PRINT only. A failing state write still ends the run, as at every other _record
    call in run_once: swallowed, the unit would be neither recorded nor reported."""
    strat, store, run = _run(tmp_path, monkeypatch, RuntimeError("plain text"))

    def refuse(*a, **k):
        raise sqlite3.OperationalError("database is locked")
    monkeypatch.setattr(orchestrate, "_record", refuse)
    with pytest.raises(sqlite3.OperationalError):
        run()
    assert len(strat.probed) == 1


def test_every_label_in_run_once_is_either_in_the_heavy_list_or_named_here():
    """The workflow test in the other file compares the list with four names written in the test. This one
    reads the labels out of run_once itself, so a NEW label fails here until someone decides which side it is
    on. (It reads the literal labels only; the fetch's own `status` variable - ok, no_change, partial,
    transient_fail - is not a literal.)"""
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    given = set(re.findall(r'results\.append\(\(unit\.key, "(\w+)"\)\)',
                           open(orchestrate.__file__, encoding="utf-8").read()))
    assert len(given) >= 8, given
    text = open(os.path.join(root, ".github", "workflows", "updater-heavy.yml"), encoding="utf-8").read()
    listed = set(text.split('bad=$(grep -E "^\\s+(', 1)[1].split(")", 1)[0].split("|"))
    assert listed <= given, f"the workflow names a label run_once never gives: {sorted(listed - given)}"
    assert given - listed == {"no_change", "due", "locked", "partial"}, sorted(given - listed)


class _ReprRaises(Exception):
    def __repr__(self):
        raise RuntimeError("repr of this exception raises")

    __str__ = __repr__


def test_an_exception_whose_repr_raises_is_still_one_units_failure(tmp_path, monkeypatch):
    """Before: `repr(e)` in the handler's own record raised, the error left run_once and no row was written
    (measured in review AR-265b: "run_once RAISED ... probed=['cnb']; unit row=None"). For such an exception
    no PROBE ERROR line is printed - ascii(e) raises inside the print's guard; the row and the label exist."""
    strat, store, run = _run(tmp_path, monkeypatch, _ReprRaises())
    results = run()
    first = strat.probed[0]
    assert (f"{first}/_all", "error") in results and len(strat.probed) == 2, "the run went on"
    row = store.get_unit(first, "_all")
    assert row["status"] == "transient_fail"
    assert row["last_error"] == "detect:UNEXPECTED:<_ReprRaises: its repr raised>"


def test_repr_of_gives_the_repr_when_there_is_one():
    assert orchestrate._repr_of(ValueError("x")) == "ValueError('x')"
    assert orchestrate._repr_of(_ReprRaises()) == "<_ReprRaises: its repr raised>"
    # both catch-all branches of run_once use it: no bare repr(e) is left beside an UNEXPECTED record
    src = open(orchestrate.__file__, encoding="utf-8").read()
    assert src.count('UNEXPECTED:" + _repr_of(e)') == 2 and 'UNEXPECTED:" + repr(e)' not in src
