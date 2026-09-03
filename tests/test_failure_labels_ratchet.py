"""A failure recorded inside a loop must name WHICH sub-unit failed.

WHY. `tally.transient_unit()` with no label makes the stored state row say only

    "201/201 sub-unit(s) transient-failed; will retry"

which is what insee_bdm recorded while failing totally for four days: no flow, no status code, no
exception text. Establishing even that the endpoint was up took a live probe session, and the
information had existed at the raise site all along — the catch discarded it. _named() in
strategies/fetchers/_common.py renders up to 20 labels; sources that pass them (ilostat, ipea,
insee_melodi, stat_slovenia) produce rows a reader can act on immediately.

SCOPE, MEASURED. 279 bare calls exist across the fetchers, but that is the wrong denominator: for
a single-sub-unit fetcher "1/1 failed" already identifies the unit. What matters is a bare call
INSIDE A LOOP, where N distinct failures collapse into one count. Measured by AST on 2026-08-04:
171 such calls across 59 files.

A LABEL ON A SINGLE-UNIT FETCHER STILL CARRIES THE REASON. The denominator above is right
about IDENTITY - "1/1 failed" already says which unit - but it says nothing about WHY, and the
raise site usually knows: an HTTP status, a byte count, an exception type. penn_world_table, pwt,
gleif and _iep were labelled on 2026-09-03 for that reason even though their calls are not
in-loop, which is why this BUDGET fell from 136 to 112 without those files appearing here.

THIS IS A RATCHET, NOT A GATE. Fixing all 171 needs the right in-scope identifier at each site,
which is per-site judgement, not a sweep. So this test pins the number: it may fall, never rise.
New code must label; existing debt gets paid down without blocking anything. When you fix some,
lower BUDGET to the new count — the test tells you what to set it to.
"""
from __future__ import annotations

import ast
import os

FETCHERS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "updater", "strategies", "fetchers")
METHODS = {"transient_unit", "structural_unit"}

# Lower this when you label some. It must never go up.
# 171 -> 165 on 2026-08-04: ecb's six labelled (path-key build, HTTP status, fetch transient,
# body-rows-parsed-to-zero, publish contention, merge refusal). ecb sweeps 540 sub-units, so an
# unlabelled count there was the worst per-call offender in the repo.
# 165 -> 158: vdem's seven, which were SIX different causes sharing one string — network drop,
# transient status, hard 4xx, RData body unparseable, year column renamed, RData rows melting to
# zero, merge refusal. The renamed-column one now prints the columns that DID arrive, which turns
# a bisect into a one-line fix.
# 158 -> 152: bundesbank's six. Its >256-byte size split now travels with the label, because that
# number IS the decision between "schema break" and "truncated response, retry".
# 152 -> 147: _giant's five. Highest leverage of the lot -- _giant drives the biggest sources
# over hundreds of flows, so its unlabelled count was the least actionable row in the system.
# 147 -> 142: ssb's five. ssb sweeps ~1,515 tables, so each label removes 1,515 candidates.
BUDGET = 69  # 2026-08-06: unsdg rework labeled its bare calls (cycle 38)


class _Counter(ast.NodeVisitor):
    """Bare tally.<method>() calls lexically inside a For/While/comprehension."""

    def __init__(self) -> None:
        self.depth = 0
        self.hits: list[tuple[int, str]] = []

    def _loop(self, node):
        self.depth += 1
        self.generic_visit(node)
        self.depth -= 1

    visit_For = visit_AsyncFor = visit_While = _loop
    visit_ListComp = visit_GeneratorExp = _loop

    def visit_Call(self, node):
        f = node.func
        if (isinstance(f, ast.Attribute) and f.attr in METHODS
                and isinstance(f.value, ast.Name) and f.value.id == "tally"
                and not node.args and not node.keywords and self.depth):
            self.hits.append((node.lineno, f.attr))
        self.generic_visit(node)


def _unlabelled_in_loops() -> dict[str, list[tuple[int, str]]]:
    out: dict[str, list[tuple[int, str]]] = {}
    for fn in sorted(os.listdir(FETCHERS)):
        if not fn.endswith(".py"):
            continue
        # PARSE FAILURES ARE REPORTED, NOT SKIPPED. A file this cannot read is a file whose
        # debt goes uncounted, which would let the ratchet drift down for the wrong reason.
        src = open(os.path.join(FETCHERS, fn), encoding="utf-8").read()
        tree = ast.parse(src, filename=fn)
        c = _Counter()
        c.visit(tree)
        if c.hits:
            out[fn] = c.hits
    return out


def test_unlabelled_in_loop_failures_never_increase():
    found = _unlabelled_in_loops()
    n = sum(len(v) for v in found.values())
    worst = sorted(found.items(), key=lambda kv: -len(kv[1]))[:5]
    detail = ", ".join(f"{fn}:{len(h)}" for fn, h in worst)
    assert n <= BUDGET, (
        f"{n} bare tally.transient_unit()/structural_unit() calls inside loops, budget is "
        f"{BUDGET}. A new one was added: pass a label naming the sub-unit and the reason, or the "
        f"state row will only ever say 'N/M sub-unit(s) failed' with no way to tell which. "
        f"Worst files: {detail}")


def test_the_budget_is_not_slack():
    """If the count has FALLEN, tighten the budget in the same commit.

    Otherwise the ratchet accumulates slack and silently stops catching regressions — the number
    would read 171 forever while the real count drifted, and a new unlabelled call could hide
    inside the gap.
    """
    n = sum(len(v) for v in _unlabelled_in_loops().values())
    assert n >= BUDGET, (
        f"only {n} unlabelled in-loop calls remain but BUDGET is still {BUDGET} — good news, now "
        f"set BUDGET = {n} so the ratchet keeps biting.")


def test_the_counter_actually_detects_a_bare_call():
    """A ratchet that counts nothing passes forever (R346).

    Both tests above would hold if _Counter silently matched zero calls, so prove it fires on a
    known-bad snippet and stays quiet on the labelled equivalent.
    """
    bad = ast.parse("for x in y:\n    tally.transient_unit()\n")
    good = ast.parse("for x in y:\n    tally.transient_unit(f'{x}: boom')\n")
    outside = ast.parse("tally.transient_unit()\n")
    for tree, want, what in ((bad, 1, "bare call in a loop"),
                             (good, 0, "labelled call in a loop"),
                             (outside, 0, "bare call outside any loop")):
        c = _Counter()
        c.visit(tree)
        assert len(c.hits) == want, f"counter mis-handled: {what}"
