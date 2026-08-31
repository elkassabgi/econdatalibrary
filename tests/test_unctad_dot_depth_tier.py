"""WU-1 of the cursor-grain sweep: the _UNCTAD_DOT_DEPTH table-grain tier.

Nine composite-trade sources store series-grain dotted keys against dot-prefix
table-grain catalogues; before this tier every one mapped 0% and demoted every run
(CAP-saturated). Depths are MEASURED minted depths (full populations, 8,284,628 keys,
100.0% each — discovery wf_2029ceb4); ictgoods is the depth-3 outlier that makes a
shared constant wrong by construction.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.orchestrate import (_UNCTAD_DOT_DEPTH, _catalog_ids_for,  # noqa: E402
                                 _table_grain_native)


def test_depth_map_members_mirror_the_resolver():
    """Every mapper-tier source must be served by the resolver's depth-agnostic
    _DOT_TABLE_GRAIN predicate — a mapper claiming ids the resolver would not serve
    is the R192 drift this test exists to prevent."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "clients", "python"))
    from econdl._resolve import _DOT_TABLE_GRAIN
    missing = set(_UNCTAD_DOT_DEPTH) - set(_DOT_TABLE_GRAIN)
    assert not missing, f"mapper sources absent from the resolver set: {missing}"


def test_reduction_rule_per_depth():
    # depth 2 (the seven): Economy.Product.Flow.Partner.Measure -> Economy.Product
    assert _table_grain_native("unctad_intratrade", "1601.3.01.897.M5019") == "1601.3"
    # depth 1: Product.Economy.Measure -> Product
    assert _table_grain_native("unctad_biotrademerchrca", "200551.380.M6042") == "200551"
    # depth 3: Economy.Flow.Product.Partner.Measure -> Economy.Flow.Product
    assert _table_grain_native("unctad_ictgoods", "203.02.ICT03.096.M0100") == "203.02.ICT03"
    # the SPAN suffix sits in segment 5 — never reaches the depth-2 prefix
    assert _table_grain_native("unctad_creativegoodsgr",
                               "156.CER070.1412.01.M4017|SPAN=3Y") == "156.CER070"


def test_at_grain_key_is_left_for_the_exact_tier():
    """R331/the eia guard: a key that IS a catalogue id must return None here —
    swallowing it would shadow the exact tier."""
    assert _table_grain_native("unctad_intratrade", "1601.3") is None
    assert _table_grain_native("unctad_biotrademerchrca", "200551") is None
    assert _table_grain_native("unctad_ictgoods", "203.02.ICT03") is None


def test_non_member_sources_are_untouched():
    assert _table_grain_native("unctad_tariff", "1.2.3.4.M0100") is None
    assert _table_grain_native("eurostat", "1.2.3.4") is None


def test_end_to_end_through_the_shipped_mapper(tmp_path, monkeypatch):
    """Drive _catalog_ids_for itself (R511: the shipped function): series-grain keys
    collapse onto their table ids, dedup via `seen`, and an unknown table reports
    unmapped rather than guessing."""
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    for tid in ("1601.3", "1601.4"):
        con.execute("INSERT INTO series VALUES (?,?)",
                    (f"unctad_intratrade:{tid}", "unctad_intratrade"))
    con.commit(); con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    from updater import config
    monkeypatch.setattr(config, "BACKEND", "r2")   # no derive-all rescue (the audit's lesson)

    ids, unmapped = _catalog_ids_for(
        "unctad_intratrade",
        ["1601.3.01.897.M5019", "1601.3.02.897.M5019",   # two series, one table
         "1601.4.01.156.M5019",                            # second table
         "9999.9.01.897.M5019"])                           # unknown table
    assert sorted(ids) == ["unctad_intratrade:1601.3", "unctad_intratrade:1601.4"]
    assert unmapped == ["9999.9.01.897.M5019"]
