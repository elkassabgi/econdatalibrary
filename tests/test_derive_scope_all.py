"""All FOUR derives must record the SCOPE of the run that wrote their summary.

WHY THIS FILE EXISTS.

`derive_statcan_tables.py` learned this in R832/R833: its summary carries the `max_rows` the
cataloguer adopts as fact, and the summary is written unconditionally - by `--dry-run`, `--only`
and `--limit` runs too - so a one-table probe at another cap stamped the whole 8,207-table store's
provenance. `run_scope()` and `tests/test_derive_scope.py` closed that, for statcan alone.

The other three derives were never given it, and on 2026-09-07 the gap bit twice:

  * `logs/istat_flows_summary.json` on disk records `"considered": 11` with `"put": 11` against a
    store of 2,442 flows - a real `--only` run, 0.45% of the source. `catalog_istat_flows.py:151`
    read its empty `refused` list as "nothing was refused" and labelled 100 flows "not seen by the
    derive - new or grown since that run", a confident cause nobody checked (R219, R843 addendum).
  * A two-indicator `--dry-run` probe of the ilostat derive overwrote
    `logs/ilostat_indicators_summary.json`, the only record of that source's full run. `logs/` is
    gitignored, so it is unrecoverable (R843).

The remedy is one design in four tools: every summary carries `scope`, `dry_run`, `store_files`
and (where the flag exists) `max_rows`, so a reader can tell a whole-store statement from a
fragment WITHOUT a tag being interpreted for it - `considered` against `store_files` shows it
arithmetically.

WHAT THIS FILE PINS THAT `test_derive_scope.py` DOES NOT. That file drives statcan only. Four
copies of a function are four chances to drift, and the reason the copies exist rather than an
import is real: these tools are standalone scripts run by path, and their `core` imports are
deliberately LATE (inside `main`) so a script run from any directory still starts. So the copies
stay - and this file is the mechanism that keeps them honest, because prose does not (CLAUDE.md).
"""
from __future__ import annotations

import importlib.util
import io
import os

_HERE = os.path.dirname(os.path.abspath(__file__))
_TOOLS = os.path.join(os.path.dirname(_HERE), "tools")

DERIVES = [
    "derive_statcan_tables.py",
    "derive_istat_flows.py",
    "derive_ilostat_indicators.py",
    "derive_usda_tables.py",
]

# usda has neither --only nor --max-rows; the others have both.
HAS_ONLY = {"derive_statcan_tables.py", "derive_istat_flows.py", "derive_ilostat_indicators.py"}
HAS_MAX_ROWS = HAS_ONLY


def _load(fn):
    spec = importlib.util.spec_from_file_location("_derive_" + fn.replace(".", "_"),
                                                  os.path.join(_TOOLS, fn))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def _src(fn):
    return io.open(os.path.join(_TOOLS, fn), encoding="utf-8").read()


class _Args:
    def __init__(self, **kw):
        self.dry_run = kw.get("dry_run", False)
        self.only = kw.get("only", "")
        self.limit = kw.get("limit", 0)


# The ONE behaviour table. Every derive is held to it.
CASES = [
    ({}, "full"),
    ({"limit": 0}, "full"),
    ({"only": ""}, "full"),
    ({"only": "", "limit": 0}, "full"),
    ({"dry_run": True}, "dry_run"),
    ({"dry_run": True, "limit": 5}, "dry_run"),
    ({"dry_run": True, "only": "x"}, "dry_run"),
    ({"dry_run": True, "only": "x", "limit": 5}, "dry_run"),
    ({"only": "x"}, "only"),
    ({"limit": 1}, "limit"),
    ({"only": "x", "limit": 5}, "only"),
]


def test_every_derive_defines_run_scope():
    for fn in DERIVES:
        assert hasattr(_load(fn), "run_scope"), (
            f"{fn} has no run_scope(); its summary cannot say whether the run was evidence "
            f"about the whole store")


def test_all_four_agree_on_the_whole_behaviour_table():
    """The copies must not drift. One table, four tools, every case."""
    mods = {fn: _load(fn) for fn in DERIVES}
    for kw, want in CASES:
        got = {fn: m.run_scope(_Args(**kw)) for fn, m in mods.items()}
        assert set(got.values()) == {want}, (
            f"run_scope disagreement on {kw!r}: expected {want!r} everywhere, got {got!r}")


def test_dry_run_wins_over_every_other_flag():
    """A dry run is not evidence whatever else was passed - checked FIRST, in all four."""
    for fn in DERIVES:
        m = _load(fn)
        assert m.run_scope(_Args(dry_run=True, only="x", limit=99)) == "dry_run", fn


def test_an_explicit_zero_or_empty_flag_is_still_full():
    """`--limit 0` and `--only ""` restrict nothing, so they must NOT read as scoped."""
    for fn in DERIVES:
        m = _load(fn)
        assert m.run_scope(_Args(limit=0, only="")) == "full", fn


def test_every_summary_records_scope_and_store_files():
    """The function is useless unless the summary dict actually calls it (R840)."""
    for fn in DERIVES:
        src = _src(fn)
        # No trailing comma in the pattern: whichever key happens to sit LAST in the dict
        # literal has none, and a test that pins punctuation fails on a correct edit (R839).
        assert '"scope": run_scope(a)' in src, f"{fn}: summary does not record scope"
        assert '"dry_run": bool(a.dry_run)' in src, f"{fn}: summary does not record dry_run"


def test_the_denominator_is_recorded_in_the_unit_it_actually_measures():
    """usda's `files` are 63 parquet SHARDS while its `tables` are ~72,046 groups, so calling
    that number `store_files` beside `tables` invites a ratio that reads 114,358% on a full run
    and 48% on a 0.04% one. Three orders of magnitude, in a field meant to expose scope."""
    for fn in ("derive_statcan_tables.py", "derive_istat_flows.py",
               "derive_ilostat_indicators.py"):
        assert '"store_files": n_store' in _src(fn), f"{fn}: no file-grain denominator"
    usda = _src("derive_usda_tables.py")
    assert '"store_shards": n_store' in usda, "usda must not call shards `store_files`"
    assert '"store_files"' not in usda, (
        "usda's files are shards, not tables; the key name must not imply a table denominator")


def test_a_limited_run_does_not_claim_whole_store_coverage():
    """`--limit` breaks the LOOP; it never filters `files`. So `considered` (len(files)) reports
    the whole store for a run that stopped after five, and against `store_files` that renders as
    100% coverage of a 0.2% run - a summary contradicting its own `scope` tag."""
    for fn in ("derive_statcan_tables.py", "derive_istat_flows.py",
               "derive_ilostat_indicators.py"):
        src = _src(fn)
        # THE PROPERTY, not the expression: `processed` must be the SIZE OF WHAT WAS
        # EXAMINED. It used to be the loop INDEX (`n_done = i`), which sits after two
        # `continue`s, so a run whose last stems were refused undercounted itself.
        assert '"processed": len(_examined_stems)' in src, (
            f"{fn}: `processed` must be the length of the examined list, not the loop index")
        assert "_examined_stems = []" in src, f"{fn}: no examined accumulator"
    assert '"processed_tables": n_tables' in _src("derive_usda_tables.py"), (
        "usda counts tables, not files, so its processed key must say so")


def test_store_files_is_captured_before_any_narrowing():
    """`store_files` is the DENOMINATOR. Captured after --only it would equal `considered`
    and say nothing - the exact failure this whole file exists to prevent."""
    for fn in DERIVES:
        src = _src(fn)
        assert "n_store = len(files)" in src, f"{fn}: no n_store capture"
        i = src.index("n_store = len(files)")
        # every narrowing step must come AFTER the capture
        for narrowing in ("if a.only:", "files = [f for f in files"):
            j = src.find(narrowing)
            if j != -1:
                assert j > i, (f"{fn}: {narrowing!r} narrows `files` BEFORE n_store is taken, "
                               f"so store_files would equal considered")


def test_the_derives_with_a_cap_record_it():
    """R833: a cap that is only a shared default constant agrees only until one side is
    overridden. The cataloguer must be able to READ what the run actually used."""
    for fn in HAS_MAX_ROWS:
        assert '"max_rows": int(a.max_rows)' in _src(fn), f"{fn}: cap not recorded"


def test_usda_is_not_asked_for_a_cap_it_does_not_have():
    """usda splits nothing, so it has no --max-rows; recording one would be a fiction."""
    src = _src("derive_usda_tables.py")
    assert "--max-rows" not in src
    assert '"max_rows"' not in src


def test_run_scope_is_reachable_without_running_a_derive():
    """R840: a branch only reachable by running a multi-hour job over a store is a branch
    nothing tests. Loading the module must not need a bucket, a store or DuckDB."""
    for fn in DERIVES:
        m = _load(fn)
        assert callable(m.run_scope)
        assert m.run_scope(_Args()) == "full"
