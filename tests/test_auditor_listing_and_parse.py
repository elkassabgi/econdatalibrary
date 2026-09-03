"""The two catalogue auditors each produced a catastrophic FALSE verdict on 2026-09-03.

Both were fixed the same day, and neither had any test — which is how they regressed unnoticed in
the first place. R414: a fix ships in the same commit as a DISCRIMINATING PAIR, one case it must
catch and one it must let through.

WHAT THEY CLAIMED, AND WHAT WAS TRUE

  * `audit_serving_coherence.py` printed `catalogued, NOT servable : 322 (13,952,768 series)` —
    the entire library — because its `SUPPORTED_SOURCES[^=]*=\\s*(?:new Set\\()?\\[(.*?)\\]` regex
    stops at the first `]`, which sits inside the COMMENT BLOCK that opens the array. It captured
    342 characters, parsed ZERO ids, and every `src in sup` test was therefore False. Real answer
    after the fix: 0 unservable, 323 ids parsed — matching `audit_schedule_coverage.py`'s
    independently-derived "resolvable (util.ts) 323". R0.4: never regex a language whose comments
    can contain the delimiter.

  * The same tool labelled `worldbank_pink` as DRIFT because its bare `except` turned an HTTP
    **451 non_redistributable** into a `-1` sentinel. That source is REFUSED IN WRITING (R526);
    the gate firing is the system working. Reporting it as drift both invites someone to "fix" a
    licence refusal and discredits the eight real drifts beside it.

  * `audit_store_vs_catalog.py` listed each store with a ONE-LEVEL `glob`, so nested stores were
    undercounted and four vanished entirely: bea 1 file of 592, gus_dbw 194 of 868, eia 30 of 60,
    and edgar_insider (648), edgar_13f (371), edgar_pointers (256), usda (63) all read as ZERO and
    hit `if not files: continue`. bea's flat count made it report `store 17,699 / cat 913,230 /
    ORPHAN` — an apparent 895,531 catalogue rows with no data, its own worst category. Counted
    over all 592 files: **913,230 distinct keys against 913,230 catalogue rows, exact.** Artifact.
    R261/R389/R390's flat-listing trap, which R390 names usda for by name.

These tests pin the PROPERTIES, not the numbers, so they survive the fleet changing.
"""
from __future__ import annotations

import os
import re
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

UTIL_TS = os.path.join(ROOT, "api", "worker", "src", "util.ts")
STORE = os.path.join(ROOT, "data", "clean_full")
COHERENCE = os.path.join(ROOT, "tools", "audit_serving_coherence.py")
STORE_AUDIT = os.path.join(ROOT, "tools", "audit_store_vs_catalog.py")


def _parse_supported(ts: str) -> set:
    """The parse as the fixed tool performs it — body to the `]` that starts a line, comments stripped."""
    start = re.search(r"SUPPORTED_SOURCES\b[^=]*=\s*(?:new Set\()?\[", ts)
    assert start, "no SUPPORTED_SOURCES array literal"
    body = ts[start.end():]
    end = re.search(r"^\s*\]", body, re.M)
    body = body[: end.start()] if end else body
    body = "\n".join(re.sub(r"//.*$", "", ln) for ln in body.splitlines())
    return set(re.findall(r'"([a-z0-9_]+)"', body))


# ---------------------------------------------------------------- the parser

@pytest.mark.skipif(not os.path.exists(UTIL_TS), reason="util.ts not present (CI has no worker src)")
def test_the_real_util_ts_parses_to_many_ids():
    """The failure was a SILENT ZERO, so the assertion that matters is 'not empty'."""
    ids = _parse_supported(open(UTIL_TS, encoding="utf-8").read())
    assert len(ids) > 100, f"parsed only {len(ids)} ids — the comment trap is back"
    assert "abs" in ids, "a known-served source is missing from the parse"


def test_a_comment_containing_the_delimiter_does_not_truncate_the_parse():
    """THE EXACT SHAPE THAT BROKE IT: a `]` inside the leading comment block."""
    ts = (
        "export const SUPPORTED_SOURCES: readonly string[] = [\n"
        "  // zillow REMOVED 2026-08-01. See note [3] below for the 17 removed above.\n"
        '  "abs",\n  "adb",\n  "bea",\n'
        "]\n"
    )
    ids = _parse_supported(ts)
    assert ids == {"abs", "adb", "bea"}, f"comment truncated the parse: {ids}"


def test_the_old_regex_really_did_fail_so_this_test_is_not_vacuous():
    """Mutation check: the pre-fix regex must return ZERO on the same input."""
    ts = (
        "export const SUPPORTED_SOURCES: readonly string[] = [\n"
        "  // note [3]\n"
        '  "abs",\n'
        "]\n"
    )
    old = re.search(r"SUPPORTED_SOURCES[^=]*=\s*(?:new Set\()?\[(.*?)\]", ts, re.S)
    assert old, "the old regex should still match"
    assert re.findall(r'"([a-z0-9_]+)"', old.group(1)) == [], \
        "the old regex no longer fails, so this file proves nothing"


def test_the_tool_refuses_on_an_empty_parse_rather_than_reporting_a_verdict():
    """R261/R503: an empty parse is 'I could not look', never 'nothing is served'."""
    src = open(COHERENCE, encoding="utf-8").read()
    assert "PARSE FAILED" in src, "the refuse-on-empty guard is gone"
    assert re.search(r"if not sup:\s*\n\s*sys\.exit", src), \
        "an empty parse must exit, not fall through into the report"


# ---------------------------------------------------------------- the 451

def test_a_gated_451_is_not_reported_as_drift():
    src = open(COHERENCE, encoding="utf-8").read()
    assert "451" in src and "GATED" in src, "the 451->GATED branch is gone"
    assert "e.code == 451" in src, "the gate is no longer keyed on the status code"
    assert "lv = -1" not in src, "the -1 sentinel is back, which is what printed DRIFT"


# ---------------------------------------------------------------- the listing

def test_the_store_auditor_lists_recursively():
    src = open(STORE_AUDIT, encoding="utf-8").read()
    assert "os.walk" in src, "the auditor is back to a one-level listing"
    assert not re.search(r'glob\.glob\(os\.path\.join\(STORE, d, "\*\.parquet"\)\)', src), \
        "the flat glob that hid 2,633 files is back"


def test_a_store_with_no_parquet_is_UNCHECKED_not_skipped():
    """R390: 'a guard is the LAST place to tolerate a silent skip.'"""
    src = open(STORE_AUDIT, encoding="utf-8").read()
    assert "UNCHECKED" in src, "a store with no parquet is silently skipped again"


@pytest.mark.skipif(not os.path.isdir(os.path.join(STORE, "bea")),
                    reason="local store not present (CI has no data/)")
def test_a_nested_store_is_visible_to_the_recursive_listing():
    """bea is the case that produced the 895,531-row phantom orphan."""
    d = os.path.join(STORE, "bea")
    flat = len([f for f in os.listdir(d) if f.endswith(".parquet")])
    deep = sum(1 for _r, _dirs, fs in os.walk(d) for f in fs if f.endswith(".parquet"))
    assert deep > flat, "bea is no longer nested, so this guard has nothing to catch"
    assert deep > 100, f"recursive listing sees only {deep} files under bea"


# ---------------------------------------------------------- the HLL fallback
#
# Five giant stores had NO store-vs-catalogue figure: cbs_nl (18.0 GB), eurostat (11.3 GB) and
# gus_dbw (5.5 GB) died with OutOfMemoryException — cbs_nl again at a 32 GB limit, 29.7 of
# 29.8 GiB — while statcan (175.1 GB) and oecd (53.3 GB) were never attempted. So the fleet
# total excluded the library's two largest sources. The fallback is the method
# tools/series_census.py::_distinct_keys already uses; what these tests protect is the LABEL and
# the refusal to assert the expensive verdict from an estimate.


def test_an_exact_scan_that_ooms_falls_back_to_an_estimate():
    src = open(STORE_AUDIT, encoding="utf-8").read()
    assert "approx_count_distinct" in src, "the giants have no figure again"


def test_an_estimate_is_always_labelled_as_one():
    """series_census measured HLL error at +19.3% to -14.0% — an unlabelled estimate is a lie."""
    src = open(STORE_AUDIT, encoding="utf-8").read()
    i = src.index("HLL FALLBACK")
    j = src.index("finally:", i)
    branch = src[i:j]
    # The RECORDED ROW must carry it, not merely the console line — stripping the label from
    # fh.write() while leaving it in print() passed an earlier version of this test.
    written = [ln for ln in branch.splitlines() if "fh.write(" in ln and "{gap}" in ln]
    assert written, "the approximate branch no longer writes a row"
    assert all("[approx" in ln for ln in written),         "an estimate is written to the TSV without its label — it would read as a count"


def test_an_estimate_may_not_assert_ORPHAN():
    """ORPHAN claims users are offered something that 404s. A -14% error manufactures one.

    2026-08-04: 2,050 of 2,408 reported orphans were phantom, which is why the shard-qualified
    retry exists. An estimator with worse error than that must not reach the same verdict.
    """
    src = open(STORE_AUDIT, encoding="utf-8").read()
    # The WHOLE fallback branch: from its marker comment to the enclosing finally.
    i = src.index("HLL FALLBACK")
    j = src.index("finally:", i)
    branch = src[i:j]
    assert "inconclusive" in branch, "the approximate path lost its inconclusive verdict"
    # R120: match the VERDICT TOKEN, not the keyword — the branch's own comment explains why
    # ORPHAN is forbidden, so a bare substring test fails on the explanation.
    assert 'note = "ORPHAN"' not in branch and 'note, orph = "ORPHAN"' not in branch,         "an ESTIMATE can now assert ORPHAN — the expensive verdict"


def test_a_failed_estimate_still_reports_rather_than_vanishing():
    """R390: a branch that cannot evaluate must PRINT, never skip in silence."""
    src = open(STORE_AUDIT, encoding="utf-8").read()
    assert "approx also failed" in src, "a double failure would disappear silently"
