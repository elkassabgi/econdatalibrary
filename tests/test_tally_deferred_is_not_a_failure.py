"""A budget deferral is not a transient failure, and must not be counted as one.

WHAT THIS PINS. ecb's recorded state read `252/540 sub-unit(s) transient-failed; will retry` and
abs's `805/1222`. Every named unit said the same thing:

    ECB__CSEC__M__SE__2022.parquet: budget 35 min spent, deferred
    ABS_SEIFA2021_SA2 deferred (budget 35 min)

Nothing had failed. Those units were never ATTEMPTED — the wall-clock budget stopped the sweep and
rotation takes them next tick, which is the design working. They went through
`tally.transient_unit()`, which both inflated `attempted` and called them failures, so the real
failure rate was unreadable: 252 of 540 is alarming, 0 of 288 attempted is fine, and the log showed
the first (R303).

ELEVEN fetchers were doing it — abs, bea, boc, comtrade, ecb, eia, ilostat, snb, ssb, wid — found
by grepping `transient_unit(.*defer` after only three showed up in state.db, because the state only
records the LAST run and most had not deferred on theirs. ilostat's call site even carried the
comment "deferral, not a verdict" while doing exactly the opposite.

The status stays `partial` on purpose. A tick that deferred work did not cover everything and must
not stamp a full-coverage vintage (R231). What changes is that the message no longer calls a
deliberate deferral a failure, and the denominator counts only what was actually attempted.
"""
from __future__ import annotations
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers._common import Tally, finalize   # noqa: E402


def test_deferral_does_not_increment_attempted():
    """`attempted` has to keep meaning attempted, or the denominator lies too."""
    t = Tally()
    t.added_unit(5)
    t.deferred_unit("X: budget spent")
    t.deferred_unit("Y: budget spent")
    assert t.attempted == 1, "only the one unit actually worked on"
    assert t.deferred == 2
    assert t.transient == 0, "a deferral is not a transient failure"


def test_deferral_only_run_is_partial_but_reports_no_failures():
    t = Tally()
    t.added_unit(10)
    t.deferred_unit("flowA: budget 35 min spent, deferred")
    r = finalize(t, 1000, None, source="demo")
    assert r.status == "partial", "incomplete coverage must not claim ok"
    assert "none failed" in r.error
    assert "deferred by budget" in r.error
    assert "transient-failed" not in r.error, "the whole point: stop calling it a failure"


def test_a_real_transient_still_reports_as_a_failure():
    """The fix must not launder genuine failures."""
    t = Tally()
    t.added_unit(3)
    t.transient_unit("flowB: HTTP 503")
    r = finalize(t, 50, None, source="demo")
    assert r.status == "partial"
    assert "transient-failed" in r.error
    assert "flowB: HTTP 503" in r.error


def test_transient_wins_when_both_occur():
    """A run that both failed and deferred must surface the FAILURE, not the deferral."""
    t = Tally()
    t.added_unit(1)
    t.transient_unit("flowB: HTTP 503")
    t.deferred_unit("flowC: budget spent")
    r = finalize(t, 10, None, source="demo")
    assert "transient-failed" in r.error, "a real failure must not be hidden behind a deferral"


def test_clean_run_is_unaffected():
    t = Tally()
    t.added_unit(7)
    r = finalize(t, 70, None, source="demo")
    assert r.status == "ok"
    assert "+7 new rows" in r.error


def test_deferred_units_are_named():
    """Same reasoning as transient/structural ids: an unnamed deferral is unauditable.

    Derived from _named's OWN cap rather than a hardcoded count. This test used 9 ids and
    asserted the "+N more" elision, which silently encoded the cap of the day — raising that
    cap 6 -> 20 on 2026-08-04 turned a still-correct test red. The property being pinned is
    "some are named, and any elision is stated", and that holds at every cap.
    """
    from updater.strategies.fetchers._common import _named
    cap = _named.__defaults__[0]

    t = Tally()
    t.added_unit(1)
    for i in range(cap + 3):                 # deliberately over the bound, whatever it is
        t.deferred_unit(f"flow{i}: budget spent")
    r = finalize(t, 10, None, source="demo")
    assert "flow0: budget spent" in r.error
    assert "+3 more" in r.error, "the list is bounded and says so"

    t2 = Tally()
    t2.added_unit(1)
    for i in range(cap):                     # exactly at the bound: nothing to elide
        t2.deferred_unit(f"flow{i}: budget spent")
    r2 = finalize(t2, 10, None, source="demo")
    assert "more" not in r2.error, "nothing was dropped, so nothing should claim it was"


def test_no_deadline_block_files_a_deferral_as_transient():
    """BEHAVIOUR-derived, not text-derived — the text version found half the class.

    My first sweep grepped `transient_unit(.*defer`, i.e. call sites whose LABEL said
    "deferred". It found 11 fetchers and I shipped that as the fix. It was half: insee_melodi
    writes `tally.transient_unit(code)` with the word "deferred" only in the print above it, and
    so do bis, cso, ember, fed_board, idb, ipea, stats_nz, wikidata and zillow. Ten more, worth
    four of the largest "failure" counts in the live queue (insee_melodi 129/144, ipea 298/1491,
    idb 10/40, ember 4/48 — all deferrals).

    So the guard looks at what follows a DEADLINE CHECK, which is the actual defining property:
    if control reaches `if dl.spent():` the budget is gone and nothing has failed. Wording is
    incidental; the deadline is not.
    """
    import glob
    import re
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    spent = re.compile(r"\b(dl|deadline)\.spent\(\)")
    bad = []
    for p in sorted(glob.glob(os.path.join(root, "updater", "strategies", "fetchers", "*.py"))):
        lines = open(p, encoding="utf-8").read().split("\n")
        for i, line in enumerate(lines):
            if not spent.search(line):
                continue
            # The block a deadline check guards ENDS AT ITS break/continue. Scanning a fixed
            # window instead was wrong and this test caught it: _who_gho breaks with NO tally
            # call, and a genuine `except TransientError -> transient_unit(code)` sits ten lines
            # further down, so a 12-line window blamed the deadline for an unrelated handler.
            # Terminating at the jump is what "this block" actually means.
            block = []
            for line2 in lines[i + 1:i + 20]:
                block.append(line2)
                if re.match(r"\s*(break|continue)\b", line2):
                    break
            block = "\n".join(block)
            if "deferred_unit(" in block:
                continue
            if "transient_unit(" in block:
                bad.append(f"{os.path.basename(p)}:{i + 1}")
    assert not bad, ("a budget deferral is being tallied as a transient FAILURE at: "
                     f"{bad} — use tally.deferred_unit()")
