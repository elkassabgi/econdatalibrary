"""The `catalog_coverage` string has THREE holders, and nothing used to check they agree.

Why this test exists (2026-08-30). The worker answered
`"series-level for 33 sources; source-level for the rest"` for months after 33 stopped being
the number -- accurate when written, silently rotten afterwards, and invisible because no test
mentioned the field. Repairing it then produced a SECOND defect: the replacement,
"series-level for every served source", was FALSE, because some served sources are catalogued
at table or flow grain: `clients/python/econdl/_resolve.py` registers 11 in `_FLOW_GRAIN` and
13 in `_DOT_TABLE_GRAIN`, and several more are documented individually -- `ons_uk` holds 42
catalogue rows for 3,897,884 series (util.ts:453), `istat` 14,267 flows for 43,564,079 series,
`insee_melodi` 139 flows, `usda` table grain. Caught in adversarial review before it deployed.

CAUTION, learned the hard way in the SECOND review of this same file: do NOT infer grain from
the catalogue row COUNT, and do not infer it from missing frequency/geography either.
  * A LOW count is not coarse grain. `statcan` (20), `oecd` (28), `abs` (18) and `bls` (9) are
    small hand-curated PER-SERIES catalogues -- `bls:CUUR0000SA0` is one series -- and an
    earlier version of this docstring named all four as table-grain examples. They carry a
    scalar frequency AND geography on 100% of rows, which a table row cannot.
  * A HIGH count is not series grain, and ABSENT scalar attributes are not coarse grain:
    `wid` has 2,465,197 catalogue rows carrying neither, yet each names one series
    (`wid:WID:acaincj992:p0p100:992:j:AL`). Sparse metadata is not coarse grain.
The sound direction is only the positive one: scalar frequency and geography on every row
implies per-series. Otherwise consult the registries above or the source's generated page,
which states its own grain ("Served at FLOW grain").

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


def _strip_ts_comments(src):
    """Remove /* */ and // comments, but never inside a string literal.

    Scanned character by character rather than regexed: a `//` inside a URL, or a `"` inside a
    comment, defeats the regex forms. Adversarial review found the regex version read a DECOY
    `const COVERAGE` planted inside a /* */ block in preference to the real one -- so the guard
    could pass while the deployed value was the original bug.
    """
    out, i, n = [], 0, len(src)
    while i < n:
        c = src[i]
        if c in '"\'`':                                  # string literal: copy verbatim
            quote = c
            out.append(c)
            i += 1
            while i < n:
                out.append(src[i])
                if src[i] == "\\":
                    if i + 1 < n:
                        out.append(src[i + 1])
                        i += 2
                        continue
                elif src[i] == quote:
                    i += 1
                    break
                i += 1
            continue
        if src.startswith("/*", i):
            end = src.find("*/", i + 2)
            i = n if end < 0 else end + 2
            out.append(" ")
            continue
        if src.startswith("//", i):
            end = src.find("\n", i)
            i = n if end < 0 else end
            out.append(" ")
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _ts_coverage():
    """Value of `const COVERAGE`, joining a multi-line `"a" + "b"` concatenation."""
    src = _strip_ts_comments(open(CATALOG_TS, encoding="utf-8").read())
    matches = _TS_ASSIGN.findall(src)
    assert matches, "no `const COVERAGE` string assignment in catalog.ts"
    assert len(matches) == 1, (
        "catalog.ts has %d `const COVERAGE` assignments outside comments -- the extractor would "
        "silently read the first, which need not be the one that deploys" % len(matches))
    parts = re.findall(r'"((?:[^"\\]|\\.)*)"', matches[0])
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

    # POLARITY, not keyword. Review found that merely requiring the word "absence" accepts the
    # exact inversion of the warning -- "absence from this catalogue means a series is
    # unavailable" contains it and asserts the opposite. So require the negation itself, in the
    # same clause as the word.
    assert re.search(r"absence[^.;]*\b(?:does not|doesn't|never)\b", value), (
        "catalog_coverage must state that absence does NOT mean unavailable, in so many words; "
        "got %r. Requiring only the word 'absence' would accept its own negation." % value
    )

    forbidden = ("for every served source", "for all served sources", "for all sources",
                 "for each served source", "throughout")
    for phrase in forbidden:
        assert phrase not in value, (
            "catalog_coverage claims uniform series-level coverage (%r), which is false: some "
            "sources are catalogued per table or flow -- ons_uk holds 42 catalogue rows for "
            "3,897,884 series, and _resolve.py registers 11 flow-grain and 13 table-grain "
            "sources." % phrase
        )
