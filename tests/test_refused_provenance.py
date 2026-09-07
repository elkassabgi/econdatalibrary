"""A cataloguer must not draw a conclusion from a refusal list that does not cover the store.

WHAT THIS PINS, and why each half is needed.

`catalog_istat_flows.py` printed, about 100 flows:

    not seen by the derive - new or grown since that run

That is three unchecked propositions in one sentence, and it came from a summary describing an
11-flow `--only` run over a 2,442-flow store. The label is the `else` of `if k in ref`, so an
empty `ref` - whatever produced it - renders as a confident cause. R219 exists for this, and the
comment three lines above the bug says the guard had ALREADY been rewritten once for the same
sin; the rewrite renamed the cause instead of ceasing to assert one.

BOTH DIRECTIONS FAIL, DIFFERENTLY. An adversarial review corrected my first reading here:

  * EMPTY from a scoped run is loud in statcan - `classify_absent` sends everything to
    `unrefused` and the run refuses. Mislabelled, not fail-open.
  * NON-EMPTY from a scoped run IS fail-open: `derive_statcan_tables.py --dry-run --only X
    --max-rows 1000` writes a refusal for a table a full run splits fine, and the cataloguer then
    reports "nothing written, correctly NOT catalogued" and drops it.

So the gate is on PROVENANCE and must reject in both directions. "unreadable" stays distinct from
"partial" because collapsing them is the fail-quiet shape of R503 - the operator needs to know
which one happened.
"""
from __future__ import annotations

import importlib.util
import io
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOLS = os.path.join(os.path.dirname(_HERE), "tools")

CATALOGUERS = {
    "catalog_statcan_tables.py": "table",
    "catalog_istat_flows.py": "flow",
    "catalog_ilostat_indicators.py": "indicator",
}


def _load(fn):
    spec = importlib.util.spec_from_file_location("_c_" + fn.replace(".", "_"),
                                                  os.path.join(_TOOLS, fn))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _src(fn):
    return io.open(os.path.join(_TOOLS, fn), encoding="utf-8").read()


def test_every_cataloguer_has_the_helper():
    for fn in CATALOGUERS:
        m = _load(fn)
        assert hasattr(m, "refused_set"), fn
        assert hasattr(m, "summary_coverage"), fn


def test_a_full_scoped_list_is_used():
    for fn, key in CATALOGUERS.items():
        ids, prov = _load(fn).refused_set(
            {"refused": [{key: "a", "rows": 1}], "refused_scope": "full"}, key)
        assert ids == {"a"} and prov == "full", fn


def test_a_scoped_list_is_PARTIAL_in_both_directions():
    """Empty and non-empty alike. The non-empty case is the fail-open."""
    for fn, key in CATALOGUERS.items():
        m = _load(fn)
        assert m.refused_set({"refused": [], "refused_scope": "partial"}, key)[1] == "partial", fn
        assert m.refused_set({"refused": [{key: "a", "rows": 1}],
                              "refused_scope": "partial"}, key)[1] == "partial", fn


def test_an_absent_scope_is_not_trusted_as_full():
    """Summaries written before `refused_scope` existed carry no provenance. Absent must read as
    unknown - `catalog_statcan_tables.py` trusted `None` as full for the CAP and that is the
    shape of the bug this closes for the LIST."""
    for fn, key in CATALOGUERS.items():
        assert _load(fn).refused_set({"refused": []}, key)[1] == "partial", fn


def test_the_old_scope_key_still_counts_as_full():
    for fn, key in CATALOGUERS.items():
        assert _load(fn).refused_set({"refused": [], "scope": "full"}, key)[1] == "full", fn


def test_unreadable_is_distinct_from_partial():
    """R503: 'I could not read it' and 'it said nothing' must not render identically."""
    for fn, key in CATALOGUERS.items():
        m = _load(fn)
        assert m.refused_set(None, key)[1] == "unreadable", fn
        assert m.refused_set({"refused": 0, "refused_scope": "full"}, key)[1] == "unreadable", fn
        assert m.refused_set({}, key)[1] == "unreadable", fn


def test_a_wrongly_shaped_record_does_not_raise():
    """`r["table"]` raised KeyError, which none of the three except-tuples caught. An
    istat-shaped record in statcan's file gave a traceback where the file's style is to refuse."""
    for fn, key in CATALOGUERS.items():
        ids, prov = _load(fn).refused_set(
            {"refused": [{"WRONG_KEY": "a"}, {key: "b"}, "not-a-dict", 7],
             "refused_scope": "full"}, key)
        assert ids == {"b"}, fn
        assert prov == "full", fn


def test_coverage_line_names_the_denominator():
    for fn in CATALOGUERS:
        line = _load(fn).summary_coverage(
            {"considered": 11, "store_files": 2442, "scope": "only",
             "refused_scope": "partial"}, 2442)
        assert "11" in line and "2,442" in line and "only" in line, fn


def test_coverage_line_survives_an_unreadable_summary():
    for fn in CATALOGUERS:
        assert "UNREADABLE" in _load(fn).summary_coverage(None, 5), fn


def test_coverage_line_says_UNRECORDED_rather_than_guessing():
    """An old summary has no scope at all. Printing nothing there would read as 'fine'."""
    for fn in CATALOGUERS:
        line = _load(fn).summary_coverage({"considered": 11}, 2442)
        assert "UNRECORDED" in line, fn
        assert "2,442" in line, fn


def test_the_cataloguers_actually_gate_on_provenance():
    """R840 - a helper nothing calls is a helper nothing tests. Each must reach for the
    provenance AND stop asserting a cause when it is not `full`."""
    for fn in CATALOGUERS:
        src = _src(fn)
        assert "ref, ref_prov = refused_set(" in src, f"{fn}: helper not called"
        assert 'ref_prov != "full"' in src, f"{fn}: provenance not gated on"
        assert "summary_coverage(" in src, f"{fn}: coverage line not printed"
        assert "cause NOT ESTABLISHED" in src or "NOT used to excuse" in src, (
            f"{fn}: still asserts a cause from an ungated list")


def test_statcan_empties_the_set_rather_than_trusting_it():
    """statcan feeds `ref` to classify_absent, which decides whether a table is silently
    excused. A partial list must excuse nothing."""
    src = _src("catalog_statcan_tables.py")
    i = src.index('ref, ref_prov = refused_set(_sum, "table")')
    j = src.index("ref = set()", i)
    k = src.index("classify_absent(absent, keys, ref", i)
    assert i < j < k, "statcan must clear `ref` before classify_absent sees it"


def test_statcan_reads_the_summary_once():
    """One file, two reads, two policies was the bug. The second read is gone."""
    src = _src("catalog_statcan_tables.py")
    assert src.count('"statcan_tables_summary.json"),') <= 1, (
        "the summary is opened more than once; the two reads drifted apart before")
