"""A scoped re-run must MERGE its refused list into the record, never replace it.

WHY THIS FILE EXISTS.

Each split-map cataloguer prints, as the fix for its own refusal:

    Re-run:  python tools/derive_istat_flows.py --bucket <b> --only <the absent ids>

and that scoped run then rewrites the summary the cataloguer reads next time.
`logs/istat_flows_summary.json` on disk records `"considered": 11` with `"put": 11` against a
2,442-flow store: the clobber is not hypothetical, it already happened, and following the tool's
own instruction is what did it.

Today that costs one mislabelled cause. The moment the cataloguers start treating a scoped
`refused` list as UNKNOWN rather than empty - which they must, because an empty list from an
11-flow run is not evidence about 2,442 - it costs a PERMANENT loud refusal instead, since
nothing restores a full-scoped record except a full derive. statcan's last one recorded
`"seconds": 677658` = 7.84 days. Shipping the three-state read without the merge would turn a
quiet wrong answer into an expensive one.

`derive_istat_flows.py` already merges `_split_map.json` on `--only` for exactly this reason,
fifteen lines above the summary write. This is the same idiom applied to the same file.

WHY `refused_scope` IS A SEPARATE KEY FROM `scope`. An adversarial review established that the
CAP and the REFUSED LIST have opposite risk asymmetries. An unknown cap must never be adopted
(R833: adopting a one-table dry run's 500,000 over a 3,000,000 store reconstitutes R832). But a
refused list merged into a previous FULL record is trustworthy even though this run was scoped.
One field cannot carry both meanings, so `scope` keeps describing the cap's provenance and
`refused_scope` describes the list's.
"""
from __future__ import annotations

import importlib.util
import io
import json
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOLS = os.path.join(os.path.dirname(_HERE), "tools")

MERGERS = {
    "derive_statcan_tables.py": "table",
    "derive_istat_flows.py": "flow",
    "derive_ilostat_indicators.py": "indicator",
}


def _load(fn):
    spec = importlib.util.spec_from_file_location("_m_" + fn.replace(".", "_"),
                                                  os.path.join(_TOOLS, fn))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _write(tmp_path, obj):
    p = os.path.join(str(tmp_path), "summary.json")
    io.open(p, "w", encoding="utf-8").write(json.dumps(obj))
    return p


def test_every_merging_derive_defines_it():
    for fn in MERGERS:
        assert hasattr(_load(fn), "merged_refused"), fn


def test_a_full_run_replaces_outright(tmp_path):
    """A full run IS the store-wide statement; merging a stale record into it would resurrect
    verdicts the new run just overturned."""
    for fn, key in MERGERS.items():
        p = _write(tmp_path, {"refused": [{key: "old", "rows": 1}], "refused_scope": "full"})
        got, scope = _load(fn).merged_refused(p, key, ["a"], [("a", 5)], True)
        assert got == [{key: "a", "rows": 5}], fn
        assert scope == "full", fn


def test_a_scoped_run_keeps_the_other_stems_verdicts(tmp_path):
    """The whole point: `--only a` must not erase what a full run knew about `b`."""
    for fn, key in MERGERS.items():
        p = _write(tmp_path, {"refused": [{key: "a", "rows": 1}, {key: "b", "rows": 2}],
                              "refused_scope": "full"})
        got, scope = _load(fn).merged_refused(p, key, ["a"], [], False)
        assert got == [{key: "b", "rows": 2}], f"{fn}: b's verdict was lost"
        assert scope == "full", f"{fn}: a merge into a full record is still store-wide"


def test_a_scoped_run_overturns_only_what_it_examined(tmp_path):
    for fn, key in MERGERS.items():
        p = _write(tmp_path, {"refused": [{key: "a", "rows": 1}, {key: "b", "rows": 2}],
                              "refused_scope": "full"})
        got, scope = _load(fn).merged_refused(p, key, ["a"], [("a", 9)], False)
        assert {r[key]: r["rows"] for r in got} == {"a": 9, "b": 2}, fn
        assert scope == "full", fn


def test_merging_into_a_PARTIAL_record_stays_partial(tmp_path):
    """Two scoped runs do not add up to a store-wide statement, and claiming they do is the
    fail-open this whole design exists to prevent."""
    for fn, key in MERGERS.items():
        p = _write(tmp_path, {"refused": [{key: "b", "rows": 2}], "refused_scope": "partial"})
        got, scope = _load(fn).merged_refused(p, key, ["a"], [("a", 9)], False)
        assert scope == "partial", fn
        assert got == [{key: "a", "rows": 9}], f"{fn}: a partial record must not be inherited"


def test_an_absent_or_corrupt_record_is_PARTIAL_not_empty(tmp_path):
    """R503: a guard whose except-branch fails open. 'I could not read it' and 'it said nothing
    was refused' must not render identically."""
    for fn, key in MERGERS.items():
        m = _load(fn)
        missing = os.path.join(str(tmp_path), "does_not_exist.json")
        assert m.merged_refused(missing, key, ["a"], [], False)[1] == "partial", fn

        bad = os.path.join(str(tmp_path), "bad.json")
        io.open(bad, "w", encoding="utf-8").write("{not json")
        assert m.merged_refused(bad, key, ["a"], [], False)[1] == "partial", fn

        # a summary whose `refused` is an integer (an older schema) must not crash or pass
        wrong = _write(tmp_path, {"refused": 0, "refused_scope": "full"})
        assert m.merged_refused(wrong, key, ["a"], [], False)[1] == "partial", fn


def test_a_record_with_no_scope_at_all_is_not_trusted(tmp_path):
    """Summaries written before this change carry no `refused_scope`. Absent must read as
    unknown, never as full - `catalog_statcan_tables.py:588` currently trusts `None` as full and
    that is the shape of the bug."""
    for fn, key in MERGERS.items():
        p = _write(tmp_path, {"refused": [{key: "b", "rows": 2}]})
        assert _load(fn).merged_refused(p, key, ["a"], [], False)[1] == "partial", fn


def test_an_old_record_whose_scope_key_says_full_is_accepted(tmp_path):
    """Back-compatibility: before `refused_scope` existed, a full run recorded `scope: full`.
    That IS a store-wide statement and must not be thrown away."""
    for fn, key in MERGERS.items():
        p = _write(tmp_path, {"refused": [{key: "b", "rows": 2}], "scope": "full"})
        got, scope = _load(fn).merged_refused(p, key, ["a"], [], False)
        assert scope == "full", fn
        assert got == [{key: "b", "rows": 2}], fn


def test_the_summary_actually_uses_the_merged_list():
    """R840 - a function nothing calls is a function nothing tests. And `refused_rows` must sum
    the list that `refused` holds, or one summary carries two disagreeing counts of one set."""
    for fn in MERGERS:
        src = io.open(os.path.join(_TOOLS, fn), encoding="utf-8").read()
        assert '"refused": _ref_list,' in src, f"{fn}: summary does not use the merged list"
        assert '"refused_scope": _ref_scope,' in src, f"{fn}: scope of the list not recorded"
        assert '"refused_rows": sum(r.get("rows", 0) for r in _ref_list)' in src, (
            f"{fn}: refused_rows still sums only this run, while refused is merged")


def test_the_examined_set_is_what_was_processed_not_what_was_globbed():
    """The merge drops from the previous record every stem in `_examined`. `--only` filters
    `files`, but `--limit` does NOT - it breaks the loop - so taking `_examined` from `files`
    would erase all 2,442 verdicts on a run that looked at five, which is the exact opposite of
    what the merge exists to do. Caught in review of my own change, before it shipped."""
    for fn in MERGERS:
        src = io.open(os.path.join(_TOOLS, fn), encoding="utf-8").read()
        assert "_examined = _examined_stems" in src, (
            f"{fn}: `_examined` must come from the loop, not from the glob")
        assert "_examined_stems = []" in src, f"{fn}: no accumulator"
        assert "_examined_stems.append(" in src, f"{fn}: accumulator never appended to"
        assert "_examined = [os.path.splitext" not in src, (
            f"{fn}: still deriving the examined set from the globbed file list")


def test_every_loop_exit_records_its_stem():
    """statcan has TWO loop sites (a dry-run branch that `continue`s, and the main path). A stem
    recorded at only one of them means a dry run merges against a set it did not examine."""
    src = io.open(os.path.join(_TOOLS, "derive_statcan_tables.py"), encoding="utf-8").read()
    assert src.count("n_done = i") == src.count("_examined_stems.append("), (
        "every place that advances the processed counter must also record which stem")


def test_the_merge_reads_the_summary_before_it_is_overwritten():
    """Order matters: `json.dump(..., open(summary, "w"))` truncates. If the merge ran after
    that, it would always read an empty file and silently degrade to partial forever."""
    for fn in MERGERS:
        src = io.open(os.path.join(_TOOLS, fn), encoding="utf-8").read()
        assert src.index("_ref_list, _ref_scope = merged_refused(") < src.index('open(summary, "w")'), fn
