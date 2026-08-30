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
from updater.orchestrate import _catalog_ids_for, _ecb_dataflow      # noqa: E402


# The real dataflows and counts, from data/catalog.db on 2026-08-30.
REAL = {"EXR": 18, "FM": 7, "YC": 10}


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    """A catalogue shaped like the real one: ecb at SERIES grain across three dataflows."""
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    for flow, n in REAL.items():
        for i in range(n):
            con.execute("INSERT INTO series VALUES (?,?)",
                        (f"ecb:{flow}:D.C{i:02d}.EUR.SP00.A", "ecb"))
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


def test_the_parser_accepts_only_the_stem_shape():
    """Controls first: anything that is not the bulk-file shape must stay UNMAPPED and
    visible. A plausible-but-wrong id is strictly worse than a reported miss, because the
    miss shows up in the note and the wrong id does not."""
    assert _ecb_dataflow("ECB.DISS__EXR_PUB__A") == "EXR"
    assert _ecb_dataflow("ECB.DISS__EXR_PUB__M") == "EXR"
    assert _ecb_dataflow("ECB.DISS__FM_PUB") == "FM"
    assert _ecb_dataflow("ECB.DISS__YC_PUB__Q") == "YC"
    for bad in ("random_key", "ECB.DISS__", "", None,
                "ECB.DISS__NOPUB__A", "NOTECB__EXR_PUB"):
        assert _ecb_dataflow(bad) is None, bad


def test_a_changed_file_expands_to_every_series_in_its_dataflow(catalog):
    """THE REGRESSION, through the shipped resolver. One file stem, 18 catalogue ids."""
    ids, unmapped = _catalog_ids_for("ecb", ["ECB.DISS__EXR_PUB__A"])
    assert len(ids) == REAL["EXR"], f"expected {REAL['EXR']} EXR ids, got {len(ids)}"
    assert all(i.startswith("ecb:EXR:") for i in ids), ids[:3]
    assert unmapped == [], unmapped


def test_the_real_store_keys_reach_all_35_catalogued_ids(catalog):
    """What the §5.7 note was actually complaining about: 'the catalog has 35 rows for it
    but NONE matched'. All three catalogued dataflows must be reachable together."""
    keys = ["ECB.DISS__EXR_PUB__A", "ECB.DISS__EXR_PUB__M",
            "ECB.DISS__FM_PUB", "ECB.DISS__YC_PUB__Q"]
    ids, _ = _catalog_ids_for("ecb", keys)
    assert len(ids) == sum(REAL.values()) == 35, len(ids)
    assert len({i.split(":")[1] for i in ids}) == 3


def test_a_file_for_an_uncatalogued_dataflow_maps_to_nothing(catalog):
    """The control that keeps this honest. 37 dataflows appear among the 540 stems and we
    catalogue THREE; the other 34 must stay unmapped rather than mint ids.

    Under r2 -- the backend production runs on -- an unmapped key stays unmapped and is
    reported. Locally it would fall through to derive-all and return all 35, which is why
    this test pins the backend."""
    ids, unmapped = _catalog_ids_for("ecb", ["ECB.DISS__BKN_PUB"])
    assert ids == [], ids
    assert unmapped == ["ECB.DISS__BKN_PUB"]


def test_the_range_cannot_bleed_into_a_neighbouring_source(catalog):
    """`ecb:EXR:` .. `ecb:EXR;` must not reach `ecbx:`. A prefix range is only safe when its
    upper bound is the byte after the separator, and getting that wrong would quietly
    re-derive another source's series."""
    ids, _ = _catalog_ids_for("ecb", ["ECB.DISS__EXR_PUB__A"])
    assert not any(i.startswith("ecbx") for i in ids), ids


def test_other_sources_are_untouched(catalog):
    """The expansion is keyed on source_id == 'ecb'; nothing else may change behaviour."""
    ids, unmapped = _catalog_ids_for("some_other_source", ["ECB.DISS__EXR_PUB__A"])
    assert ids == [] and unmapped == ["ECB.DISS__EXR_PUB__A"]
