"""Every `return` in _derive_changed_csvs must hand back FOUR values.

WIDENED AGAIN (2026-08-16): (failed, note, deferred) -> (failed, note, deferred,
failed_reasons) so the retry queue can store each id's OWN failure reason instead of
one batch summary (cso's 22 census ids sat queued 10 days with the real exception
unrecorded). This file is the enforcement the docstring below asked for: on THIS
widening it flagged every return site, including the historically-missed gleif one.

THE OUTAGE THIS PINS (2026-08-07). The function's contract widened from (failed, note) to
(failed, note, deferred) when the derive budget's deferrals were split out from real failures
(commit 30fa9ed7). Two of its three returns were updated. The third — the early exemption for
a source whose catalogue is provably empty — was missed and kept returning a 2-tuple.

The caller unpacks three:

    csv_failed, csv_err, csv_deferred = _derive_changed_csvs(unit, res, blob)

so reaching that line raised ValueError. It was swallowed by run_once's outer
`except Exception`, which books the unit `transient_fail`. And because EVERY success-path state
write sits downstream of that call, none of them executed: gleif merged 3,395,736 rows, wrote
them to the store, and had the run recorded as a FAILURE with its vintage un-bumped — so it
re-fetched the whole source, every run, and the health gate showed a permanently failing source
that was in fact publishing perfectly.

Measured blast radius at the time: of 168 sources that have ever merged obs, 11 report no
series_cursors, and gleif was the only one whose catalogue is genuinely EMPTY, so it alone
reached the line. R380 later widened the reach by admitting `partial` runs as well.

Why a test and not just the fix: a tuple-arity contract is exactly the kind of thing that
breaks silently on the NEXT widening, and the failure mode is a source that looks broken while
working. This asserts the arity at every return, from the AST, so a new early return cannot
quietly reintroduce it.
"""
from __future__ import annotations

import ast
import inspect
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)


def _return_arities():
    """[(lineno, arity)] for every `return` in _derive_changed_csvs. arity None = bare/other."""
    from updater import orchestrate as O
    src = inspect.getsource(O._derive_changed_csvs)
    tree = ast.parse("".join(src.splitlines(keepends=True)).lstrip())
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Return):
            v = node.value
            if isinstance(v, ast.Tuple):
                out.append((node.lineno, len(v.elts)))
            elif v is None:
                out.append((node.lineno, 0))
            else:
                out.append((node.lineno, None))
    return out


def test_every_return_is_a_four_tuple():
    arities = _return_arities()
    assert arities, "no return statements found — the parse is wrong, not the code"
    bad = [(ln, n) for ln, n in arities if n != 4]
    assert not bad, (
        f"returns with the wrong arity at (line, arity)={bad}. The caller does "
        f"`csv_failed, csv_err, csv_deferred = _derive_changed_csvs(...)`, so a 2-tuple raises "
        f"ValueError, which run_once swallows as `transient_fail` — and every success-path "
        f"state write is downstream, so a publishing source is booked as failing and re-fetches "
        f"in full forever. That is exactly what happened to gleif and its 3,395,736 rows.")


def test_the_caller_still_unpacks_four():
    """If the caller ever goes back to two, the test above would pass while the code breaks."""
    from updater import orchestrate as O
    src = inspect.getsource(O.run_once)
    assert "csv_failed, csv_err, csv_deferred, csv_reasons = _derive_changed_csvs(" in src, (
        "run_once no longer unpacks four values — re-check this contract end to end")


def test_the_empty_catalogue_exemption_still_exists():
    """The 2-tuple lived on a real branch; deleting the branch would 'fix' the test and lose
    the exemption that keeps a legitimately catalogue-less source from demoting every run."""
    from updater import orchestrate as O
    src = inspect.getsource(O._derive_changed_csvs)
    assert "_catalog_series_count" in src, (
        "the measured empty-catalogue exemption is gone; a source with nothing to re-derive "
        "would demote to `partial` on every run and never set last_success_utc")
