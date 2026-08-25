"""A query that IS a source id must resolve to that source, not depend on the FTS index.

`api/worker/src/catalog.ts` decides whether FTS answered a query with:

    if (results.length > 0) { ...; ftsOk = true; }

which means "FTS returned something", not "FTS answered THIS query". Unscoped, the MATCH runs
against every source at once, so one unrelated hit anywhere suppresses the LIKE fallback for
the whole query.

Measured on live D1 before the fix:

    SELECT COUNT(*) FROM series_fts f JOIN series s ON s.series_id=f.series_id
     WHERE series_fts MATCH 'wid' AND s.source_id<>'wid';      -> 10   (all unctad_rfia)

    GET /v1/catalog?q=wid&limit=5  ->  total=7,395,601

7,395,601 = wid's 7,395,591 code-as-title rows + those 10. The query only looked correct
because wid's search index still held four copies of every series with the raw code stored in
the indexed `title` column. Deduplicating that index — which is wanted, it is 7.4M surplus rows
in a database at 8.35 GB against a hard 10 GB ceiling — would have left `MATCH 'wid'` returning
those 10 unctad_rfia rows, `results.length > 0`, `ftsOk = true`, the fallback never running, and
`q=wid` answering with total=10 and ZERO wid series.

That is not a property of wid. Every source's own name token is searchable precisely because its
code-as-title rows spell it, so the same trap sits under `q=cepii_gravity` and `q=imf_ifs`.

The fix resolves an exact source-id match to that source up front, before either search path.
It is also the cheapest branch — the PK-range browse rather than a leading-wildcard
`series_id LIKE '%...%'` scan over millions of rows.

These tests read the shipped source, because the defect is a property of the code path, not of
anything a unit under test can be handed. Same approach as
`test_sync_catalog_d1_fts_idempotent.py::test_the_shipped_source_deletes_before_inserting`.
"""
from __future__ import annotations

import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_TS = os.path.join(ROOT, "api", "worker", "src", "catalog.ts")


def _src() -> str:
    with open(CATALOG_TS, encoding="utf-8") as fh:
        return fh.read()


def test_an_exact_source_id_query_is_resolved_to_that_source():
    s = _src()
    assert "supportedSources(env).has(cand)" in s, (
        "catalog.ts no longer resolves a q= that is a source id; q=wid will fall back to "
        "whatever the FTS index happens to contain")


def test_the_resolution_happens_before_the_search_paths():
    """Order is the whole point — after the MATCH it would be dead code."""
    s = _src()
    i_resolve = s.index("supportedSources(env).has(cand)")
    i_fts = s.index("SEARCH_FTS")
    # the import line also mentions SEARCH_FTS, so measure against its USE
    i_fts_use = s.index("prepare(SEARCH_FTS")
    assert i_resolve < i_fts_use, (
        "the source-id resolution must run BEFORE the FTS path, or the ftsOk gate wins first")
    assert i_fts > 0


def test_the_resolution_does_not_fire_when_source_was_given_explicitly():
    """?q=wid&source=bea must stay a real search inside bea, not become a wid browse."""
    s = _src()
    m = re.search(r"if \(q && q\.trim\(\) && !src\) \{", s)
    assert m, "the guard must require that no explicit ?source= was supplied (!src)"


def test_the_matched_query_is_cleared_so_it_is_a_browse_not_a_self_match():
    """Leaving q set would run MATCH 'wid' AND source_id='wid' — which the dedup empties."""
    s = _src()
    i = s.index("supportedSources(env).has(cand)")
    window = s[i:i + 400]
    assert re.search(r"\bq\s*=\s*null", window), (
        "q must be cleared once it has been resolved to a source, or the request still "
        "depends on the source's own name being present in the FTS index")


def test_the_denylist_gate_still_runs_after_resolution():
    """A gated source named in q= must get the honest 451, not a silent browse."""
    s = _src()
    i_resolve = s.index("supportedSources(env).has(cand)")
    i_deny = s.index("NON_REDISTRIBUTABLE.has(src)")
    assert i_resolve < i_deny, (
        "resolution must precede the redistribution gate so q=<gated source> is refused, "
        "not served")


def test_negative_control_the_ftsok_gate_is_still_the_permissive_form():
    """R346/R414: this suite must be able to SEE the condition it was written for.

    The fix deliberately does NOT change `results.length > 0`; it routes around it. If a later
    edit tightened that gate, this control fails and the docstring above needs revisiting —
    it is a prompt to re-read, not a defect.
    """
    s = _src()
    assert "results.length > 0" in s, (
        "the permissive ftsOk gate is gone; re-check whether the source-id resolution is "
        "still the right fix or whether the gate itself now answers the question")
