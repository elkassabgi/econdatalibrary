"""The `catalog_coverage` string has THREE holders, and nothing used to check they agree.

Why this test exists (2026-08-30). The worker answered
`"series-level for 33 sources; source-level for the rest"` for months after 33 stopped being
the number -- accurate when written, silently rotten afterwards, and invisible because no test
mentioned the field. Repairing it then produced a SECOND defect: the replacement,
"series-level for every served source", was FALSE, because plenty of served sources are
catalogued at table or flow grain (measured against data/catalog.db: ons_uk 42 catalogue rows
for 3,897,884 series, insee_melodi 139, istat 14,267, statcan 20, oecd 28, abs 18, bls 9).
That version was caught in adversarial review before it deployed.

So there are two failure modes to hold shut, and they pull in opposite directions:

  * DRIFT   -- the holders disagree, so a caller who develops against the dev shim and then
               points at production sees the field change meaning underneath them.
  * ROT     -- the string embeds a count, which is true on the day it is written and false
               later, with nothing to notice.

The third failure mode, a string that is simply UNTRUE about grain, cannot be settled by a
unit test -- it needs the catalogue. What this test can do is refuse the shape that caused it:
a bare claim of uniform coverage. Hence the caveat requirement.
"""
import ast
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_TS = os.path.join(ROOT, "api", "worker", "src", "catalog.ts")
DEVSERVER = os.path.join(ROOT, "api", "devserver.py")
CONTRACT = os.path.join(ROOT, "api", "CONTRACT.md")


def _norm(s):
    """Collapse whitespace -- CONTRACT.md wraps the string across a line break."""
    return re.sub(r"\s+", " ", s).strip()


# Matches ONLY a chain of string literals joined by `+`, so a `;` *inside* a literal is eaten
# by the literal pattern instead of ending the match. A plain non-greedy `(.*?);` truncated
# "series-level for 33 sources; source-level for the rest" at its internal semicolon, leaving
# an unterminated quote, which yielded an EMPTY value -- and an empty value passes the
# no-count and caveat checks vacuously. Found by mutate_coverage_guard.py, not by review.
_TS_ASSIGN = re.compile(
    r'^const COVERAGE\s*=\s*((?:\s*"(?:[^"\\]|\\.)*"\s*\+?)+)\s*;', re.M)


def _nonempty(value, where):
    assert value, (
        "extracted an EMPTY catalog_coverage from %s -- the extractor is broken, and an empty "
        "string would satisfy the no-count and caveat checks without asserting anything" % where
    )
    return value


def _ts_coverage():
    """Value of `const COVERAGE`, joining a multi-line `"a" + "b"` concatenation."""
    src = open(CATALOG_TS, encoding="utf-8").read()
    m = _TS_ASSIGN.search(src)
    assert m, "no `const COVERAGE` string assignment in catalog.ts"
    parts = re.findall(r'"((?:[^"\\]|\\.)*)"', m.group(1))
    return _nonempty(_norm("".join(parts)), CATALOG_TS)


def _py_coverage():
    """Value of `_CATALOG_COVERAGE`, read with ast so implicit concatenation is handled."""
    tree = ast.parse(open(DEVSERVER, encoding="utf-8").read())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(t, ast.Name) and t.id == "_CATALOG_COVERAGE" for t in node.targets
        ):
            return _nonempty(_norm(ast.literal_eval(node.value)), DEVSERVER)
    pytest.fail("no `_CATALOG_COVERAGE` assignment in devserver.py")


def _contract_coverage():
    text = _norm(open(CONTRACT, encoding="utf-8").read())
    m = re.search(r'"catalog_coverage"\s*:\s*"([^"]*)"', text)
    assert m, "CONTRACT.md does not quote a catalog_coverage value"
    return _nonempty(_norm(m.group(1)), CONTRACT)


def test_worker_and_devserver_agree():
    """The dev shim must answer exactly what the worker answers."""
    assert _ts_coverage() == _py_coverage()


def test_contract_documents_the_deployed_string():
    """The published contract must quote the value the worker actually returns."""
    assert _contract_coverage() == _ts_coverage()


def test_string_embeds_no_count():
    """A number here has no instrument and nothing keeps it true -- that was the 33-source bug."""
    value = _ts_coverage()
    assert not re.search(r"\d", value), (
        "catalog_coverage must not embed a count: %r. A number here is accurate the day it is "
        "written and silently wrong afterwards -- exactly how 'series-level for 33 sources' "
        "survived months of growth past 300." % value
    )


def test_string_keeps_the_absence_caveat():
    """catalog.ts's own header says the field exists so absence is not read as nonexistence.

    A string claiming uniform series-level coverage DELETES that warning, which is worse than
    the stale number it replaces: a caller who searches for an ISTAT indicator, finds nothing,
    and is told coverage is series-level for everything concludes the series does not exist.
    It does -- inside one of 14,267 flow CSVs.
    """
    value = _ts_coverage().lower()
    assert "absence" in value, (
        "catalog_coverage must keep the caveat that absence != unavailable; got %r" % value
    )
    forbidden = ("for every served source", "for all served sources", "for all sources")
    for phrase in forbidden:
        assert phrase not in value, (
            "catalog_coverage claims uniform coverage (%r), which is false: sources are "
            "catalogued at table/flow grain too (ons_uk 42 rows for 3,897,884 series)." % phrase
        )
