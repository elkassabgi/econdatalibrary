"""ecb: a changed bulk FILE must re-derive every catalogued series inside it (§5.7).

WHY. ecb has been `partial` since 2026-07-16 — 45 days — with the note

    315 changed series_keys have no catalog mapping for ecb: the catalog this run read
    has 35 rows for it but none matched — grain/key-form mismatch (§5.7)

and 20 of its last 25 runs carry that same note. Both sides measured 2026-08-30:

    catalogue  35 ids, 3 dataflows: EXR 18, FM 7, YC 10, form `ecb:<FLOW>:<SERIES_KEY>`
    store      540 cursors, form `ECB.DISS__<FLOW>_PUB[__<FREQ>]`
    overlap    all three catalogued dataflows appear among the stems

So the catalogue is complete and only the GRAIN differs — the same tell the PxWeb family
gave (unmapped count equalling the catalogue row count). The mapping is ONE-TO-MANY: one
bulk file carries every series of a dataflow. Neither `_flow_of` nor `_table_grain_native`
can express it, because both reduce one key to ONE id.

These tests drive `_catalog_ids_for` — the function that ships — against a temporary
catalogue via ECONDL_CATALOG, so the expansion is pinned at its call site and not merely
in a helper (R511 rule 4, which cost three separate suites this week).
"""
from __future__ import annotations

import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import config                                           # noqa: E402
from updater.orchestrate import _catalog_ids_for, _ecb_store_key      # noqa: E402


# The real dataflows and counts, from data/catalog.db on 2026-08-30.
REAL = {"EXR": 18, "FM": 7, "YC": 10}


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    """A catalogue shaped like the real one: ecb at SERIES grain across three dataflows."""
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    # SHAPED LIKE THE REAL CATALOGUE, because a fixture that is not is testing the fixture:
    # EXR ids start `D.`, FM's four monthly ones `M.`, YC's ten `B.` with `G_N_A` as the
    # fifth field (which is what distinguishes ECB__YC__B__G_N_A from __G_N_C / __G_N_W).
    for i in range(REAL["EXR"]):
        con.execute("INSERT INTO series VALUES (?,?)", (f"ecb:EXR:D.C{i:02d}.EUR.SP00.A", "ecb"))
    for i in range(REAL["FM"]):
        con.execute("INSERT INTO series VALUES (?,?)", (f"ecb:FM:M.U2.EUR.4F.KR.X{i:02d}", "ecb"))
    for i in range(REAL["YC"]):
        con.execute("INSERT INTO series VALUES (?,?)",
                    (f"ecb:YC:B.U2.EUR.4F.G_N_A.SV_C_YM.SR_{i:02d}Y", "ecb"))
    # A sibling curve the real store also carries, which must NOT be claimed by G_N_A.
    con.execute("INSERT INTO series VALUES (?,?)",
                ("ecb:YC:B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y", "ecb"))
    # A neighbour source, so an over-broad range would be caught rather than invisible.
    con.execute("INSERT INTO series VALUES (?,?)", ("ecbx:EXR:SHOULD_NOT_MATCH", "ecbx"))
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    # PRODUCTION IS THE r2 BACKEND, and that is the whole point. Under r2 `_catalog_ids_for`
    # returns (exact, unmapped) WITHOUT the derive-all fallback, because on a runner the local
    # store holds only what this run wrote. Locally derive-all fires and returns every id of
    # the source, which masks the defect entirely -- my first version of these tests ran that
    # way and "passed" a case that fails in the cloud. Pin the backend the failure occurs on.
    monkeypatch.setattr(config, "BACKEND", "r2")
    return p


# ── ACCEPTANCE CRITERIA for the correct fix, currently xfail ────────────────
# The first version of this expansion was WITHDRAWN (see the comment in orchestrate.py). It
# mapped the wrong files: only 47 of 540 store keys parse, and the 18 catalogued daily EXR
# ids live in `ECB__EXR__D`, which the parser rejects, while the three stems it accepted hold
# zero of them. These tests stay as the bar the replacement must clear, marked xfail so they
# fail LOUDLY the day someone re-adds a version that does not.

pytestmark_reason = ("ecb dataflow expansion withdrawn 2026-08-30; these are the acceptance "
                     "criteria for its replacement")


def test_a_changed_file_expands_to_every_series_in_its_dataflow(catalog):
    """One EXR file must expand to the 18 catalogued EXR ids."""
    ids, unmapped = _catalog_ids_for("ecb", ["ECB__EXR__D"])
    assert len(ids) == REAL["EXR"], f"expected {REAL['EXR']} EXR ids, got {len(ids)}"
    assert all(i.startswith("ecb:EXR:D.") for i in ids), ids[:3]


def test_the_real_store_keys_reach_all_35_catalogued_ids(catalog):
    """THE ACCEPTANCE TEST THE REVIEW NAMED, and the one measurement that would have caught
    the withdrawn version in a single query: every catalogued id must be reachable from a
    store key that CONTAINS it.

    Measured against the real store 2026-08-30, so the replacement has a target:
        EXR/D  18/18 contained in ECB__EXR__D          (2,132,245 rows)
        FM/D    3/3  contained in ECB__FM__D
        FM/M    4/4  contained in ECB__FM__M
        YC/B   10/10 contained in ECB__YC__B__G_N_A    (extra segment = key field 5)
    35 of 35, against the 9 of 35 the withdrawn version reached.
    """
    keys = ["ECB__EXR__D", "ECB__FM__M", "ECB__YC__B__G_N_A"]
    ids, _ = _catalog_ids_for("ecb", keys)
    assert len(ids) == sum(REAL.values()) == 35, len(ids)
    assert not any("G_N_C" in i for i in ids), (
        "ECB__YC__B__G_N_A must not claim the G_N_C curve — that over-claim is exactly what "
        "the withdrawn version did, in the other direction")


def test_the_parser_accepts_the_files_that_actually_hold_the_series():
    """The assertion the withdrawn version failed. `ECB__EXR__D` holds all 18 catalogued
    daily EXR ids; the first parser rejected it and accepted three files holding none."""
    assert _ecb_store_key("ECB__EXR__D") == ("EXR", "D", [])
    assert _ecb_store_key("ECB__YC__B__G_N_A") == ("YC", "B", ["G_N_A"])
    # The other four agency prefixes carry no catalogued ecb ids and must stay unmapped.
    for bad in ("ECB.DISS__EXR_PUB__A", "ECB.DISS__JDF_EXR_HCI_CPI",
                "ESTAT__NAMQ_10_GDP", "EUROSTAT__X", "IMF__Y", "ECB__", "", None):
        assert _ecb_store_key(bad) is None, bad
