"""A stem is EITHER whole OR split, never both — and the tool must not suggest making it both.

MEASURED 2026-09-07 (R853). `catalog_ilostat_indicators.py` refuses when an over-cap stem has no
split-map entry, and prints the remedy:

    Re-run:  python tools/derive_ilostat_indicators.py --bucket <b> --only <the absent stems>

For BOTH stems it names, a WHOLE object already exists in R2 — 3.03 MB each, gzipped, written
2026-09-01 — with a whole-grain catalogue row beside it. And
`clients/python/econdl/_resolve.py::_resolve_ilostat_indicator` consults `_split_map.json` **only
for ids carrying `#`**: a partless id resolves straight to the whole parquet with no map lookup.

So following the tool's own instruction would not have orphaned the whole id. It would have left
it serving all 504,238 rows beside four part ids serving the same 504,238 rows — a live double
publication with 100% row overlap. Both the row and the object exist and resolve, so
`audit_r2_vs_catalog.py` and the store-vs-catalogue auditors are structurally blind to it, and
neither the derive nor the cataloguer has any `delete_object` or `DELETE`, so it would be
permanent. Across both ilostat prefixes today, stems holding both grains: **0**. It would have
been the first.

TWO GUARDS, AND THE ORDER MATTERS. `dual_grain_stems` catches the state after rows are built;
`published_whole` gates the REMEDY LINE, which is the only one the operator reads *before*
running anything. A guard that fires after the objects are written is a post-mortem.
"""
from __future__ import annotations

import importlib.util
import os
import sqlite3

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_TOOL = os.path.join(_ROOT, "tools", "catalog_ilostat_indicators.py")


def _load():
    spec = importlib.util.spec_from_file_location("_cat_ilo_dg", _TOOL)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


@pytest.fixture
def mod(tmp_path, monkeypatch):
    """The module with ROOT pointed at a tmp tree holding a two-column catalogue."""
    m = _load()
    d = tmp_path / "data"
    d.mkdir()
    con = sqlite3.connect(str(d / "catalog.db"))
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY)")
    con.commit()
    con.close()
    monkeypatch.setattr(m, "ROOT", str(tmp_path))
    return m


def _put(mod, *ids):
    con = sqlite3.connect(os.path.join(mod.ROOT, "data", "catalog.db"))
    con.executemany("INSERT OR REPLACE INTO series (series_id) VALUES (?)", [(i,) for i in ids])
    con.commit()
    con.close()


# ---------------------------------------------------------------- published_whole

def test_a_stem_with_a_whole_row_is_named(mod):
    _put(mod, mod.unit_id("A"), mod.unit_id("B", "X"))
    assert mod.published_whole(["A", "B", "C"]) == {"A"}


def test_a_stem_with_only_part_rows_is_not_named(mod):
    """The direction R845 got wrong: a split stem HAS no whole id, and its absence is not a
    licence to publish one alongside its parts."""
    _put(mod, mod.unit_id("B", "X"), mod.unit_id("B", "Y"))
    assert mod.published_whole(["B"]) == set()


def test_an_empty_catalogue_names_nothing(mod):
    assert mod.published_whole(["A", "B"]) == set()


def test_a_sibling_stem_sharing_a_prefix_is_not_a_match(mod):
    """R853's own near-miss: a PREFIX listing reported `24_447` as having objects, and they were
    `24_447_DF_DCIS_SPOSI_1` and friends — different stems sharing the prefix."""
    _put(mod, mod.unit_id("EMP_NIFL_A_LONGER_NAME"))
    assert mod.published_whole(["EMP_NIFL"]) == set()


def test_an_unreadable_catalogue_warns_and_returns_empty(mod, monkeypatch, capsys):
    """R503: it must not be silent. Returning an empty set quietly reads as 'all safe'."""
    monkeypatch.setattr(mod, "ROOT", os.path.join(mod.ROOT, "does", "not", "exist"))
    out = mod.published_whole(["A"])
    assert out == set()
    assert "UNVERIFIED" in capsys.readouterr().out


# ---------------------------------------------------------------- dual_grain_stems

def _rows(*ids):
    return [(i,) + ("x",) * 10 for i in ids]


def test_parts_emitted_while_a_whole_row_exists_is_refused(mod):
    """The live hazard: the whole id keeps resolving beside the new parts."""
    _put(mod, mod.unit_id("A"))
    out = mod.dual_grain_stems(_rows(mod.unit_id("A", "X")), {"A": {"dim": "sex", "parts": 2}})
    assert "A" in out
    assert "PART ids" in out["A"][0] and "WHOLE row" in out["A"][0]


def test_a_whole_id_emitted_while_part_rows_exist_is_refused(mod):
    """The mirror: the parts keep serving under a whole id that covers them."""
    _put(mod, mod.unit_id("B", "X"))
    out = mod.dual_grain_stems(_rows(mod.unit_id("B")), {})
    assert "B" in out
    assert "WHOLE id" in out["B"][0] and "PART rows" in out["B"][0]


def test_the_ordinary_cases_are_not_refused(mod):
    """A guard that refuses everything is not a guard."""
    _put(mod, mod.unit_id("A"), mod.unit_id("B", "X"))
    assert mod.dual_grain_stems(_rows(mod.unit_id("A")), {}) == {}
    assert mod.dual_grain_stems(_rows(mod.unit_id("B", "X")),
                                {"B": {"dim": "sex", "parts": 2}}) == {}


def test_a_brand_new_stem_is_not_refused(mod):
    assert mod.dual_grain_stems(_rows(mod.unit_id("NEW")), {}) == {}
    assert mod.dual_grain_stems(_rows(mod.unit_id("NEW", "X")),
                                {"NEW": {"dim": "sex", "parts": 2}}) == {}


# ---------------------------------------------------------------- the wiring

def test_both_guards_are_actually_called():
    """R840 — a function nothing calls is a function nothing tests."""
    src = open(_TOOL, encoding="utf-8").read()
    assert "_pub = published_whole(" in src, "the remedy line does not check for a whole grain"
    assert "_dual = dual_grain_stems(" in src, "the row guard is defined but never called"
    # and the remedy must be withheld, not merely annotated
    assert "No safe re-run to offer" in src
    assert "_safe = [k for k in sorted(absent) if k not in _pub]" in src


def test_the_remedy_check_precedes_the_remedy_line():
    """A warning printed after the command has been read is not a warning."""
    src = open(_TOOL, encoding="utf-8").read()
    assert src.index("_pub = published_whole(") < src.index("Re-run (safe for the")
