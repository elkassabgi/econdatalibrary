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
    # FM SPLITS: 3 ids under `D.` and 4 under `M.` -- measured, and stated in this file's own
    # docstring, which my first fixture then contradicted by putting all 7 under `M.`. That
    # error cancelled against dropping ECB__FM__D from the key list below, so the test read 35
    # and passed while exercising neither. Two wrongs summing to the right number is the worst
    # kind of green.
    for i in range(3):
        con.execute("INSERT INTO series VALUES (?,?)", (f"ecb:FM:D.U2.EUR.4F.KR.X{i:02d}.LEV", "ecb"))
    for i in range(REAL["FM"] - 3):
        con.execute("INSERT INTO series VALUES (?,?)", (f"ecb:FM:M.U2.EUR.4F.KR.Y{i:02d}", "ecb"))
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
    # ...AND PIN THE STORE THE PRESENCE GUARD READS. Under r2 the ecb branch also requires
    # `_ecb_file_present` — and this fixture originally left that reading the REAL store, so
    # the acceptance tests passed on this machine (which holds all 540 ecb parquets) and
    # failed 0==18 / 0==35 on EVERY CI run since they shipped, the runner holding no store
    # at all. That is AR-026's own warning ("exercised its true branch by accident")
    # committed a second time in the same file. The fixture now owns a store dir holding
    # exactly the four claiming files; ECB__ZZZ__D stays absent, so the M7 residue test
    # keeps its meaning unchanged.
    store = tmp_path / "store_with_claiming_files"   # distinct name: the presence-guard
    store.mkdir()                                    # tests make their own empty "store*"
    for fn in ("ECB__EXR__D", "ECB__FM__D", "ECB__FM__M", "ECB__YC__B__G_N_A"):
        (store / f"{fn}.parquet").write_bytes(b"")
    monkeypatch.setattr(config, "source_dir", lambda sid: str(store))
    return p


# ── ACCEPTANCE CRITERIA for the correct fix, currently xfail ────────────────
# The first version of this expansion was WITHDRAWN (see the comment in orchestrate.py). It
# mapped the wrong files: only 47 of 540 store keys parse, and the 18 catalogued daily EXR
# ids live in `ECB__EXR__D`, which the parser rejects, while the three stems it accepted hold
# zero of them. These tests stay as the bar the replacement must clear, marked xfail so they
# fail LOUDLY the day someone re-adds a version that does not.

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
    keys = ["ECB__EXR__D", "ECB__FM__D", "ECB__FM__M", "ECB__YC__B__G_N_A"]
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

# ── MUTATION PINS. 8 of 12 mutations survived the full 693-test suite; these close the four
# that matter. M7 is the R511 shape yet again: dropping `if got:` lets the branch `continue`
# on an empty result, which silently shrinks the reported unmapped residue 536 -> 250 (53%
# under-reported) while every test stays green.

def test_a_key_that_claims_nothing_must_NOT_swallow_the_residue(catalog):
    """M7. `ECB__ZZZ__D` parses fine and matches no catalogued id. Without the `if got:`
    guard the branch would `continue` anyway, dropping the key from `unmapped` — so the
    coherence note under-reports how much of the store it cannot see."""
    ids, unmapped = _catalog_ids_for("ecb", ["ECB__ZZZ__D"])
    assert ids == [], ids
    assert unmapped == ["ECB__ZZZ__D"], (
        "a key that maps to nothing must be REPORTED as unmapped, not silently consumed")


def test_the_branch_is_gated_to_ecb(catalog):
    """M8. The rule is ecb-specific, and this test could not tell the difference until the
    fixture gave another source ids the rule WOULD claim.

    My first version asserted `_catalog_ids_for("some_other_source", ["ECB__EXR__D"]) == []`
    — true with the gate and true without it, because that source had no `EXR:D.` ids to
    claim. A control has to be able to come back the other way (R504). `mimic` now carries
    exactly such ids, so removing the gate makes them get claimed and this fails."""
    con = sqlite3.connect(str(catalog))
    for i in range(3):
        con.execute("INSERT INTO series VALUES (?,?)", (f"mimic:EXR:D.C{i:02d}.X", "mimic"))
    con.commit(); con.close()
    ids, unmapped = _catalog_ids_for("mimic", ["ECB__EXR__D"])
    assert ids == [], (
        f"the ecb rule fired for source 'mimic' and claimed {ids} — the branch must be gated "
        f"on source_id, or one source's key form starts claiming another's ids")
    assert unmapped == ["ECB__EXR__D"]


def test_extras_match_a_whole_dot_FIELD_not_a_substring(catalog, monkeypatch):
    """M6. The commit calls the extras 'not decoration', and nothing pinned how they match.
    A substring test would let `ECB__YC__B__G_N` claim the G_N_A curve, which is precisely
    the over-claim that made v1 harmful."""
    ids, _ = _catalog_ids_for("ecb", ["ECB__YC__B__G_N"])
    assert ids == [], (
        f"'G_N' is a SUBSTRING of the field 'G_N_A' but not the field itself; claiming "
        f"those ids would repeat v1's over-claim. Got {ids[:3]}")


def test_the_lower_bound_keeps_its_dot(catalog):
    """M3. Without the '.', `ecb:EXR:D` (a bare seg1, no dot) would be claimed as though it
    were a member of the D. family."""
    catalog_con = sqlite3.connect(str(catalog))
    catalog_con.execute("INSERT INTO series VALUES ('ecb:EXR:D','ecb')")
    catalog_con.commit(); catalog_con.close()
    ids, _ = _catalog_ids_for("ecb", ["ECB__EXR__D"])
    assert "ecb:EXR:D" not in ids, (
        "the bare seg1 is a different id and must not be swept in by the range")

def test_under_r2_an_id_is_not_claimed_when_its_file_is_absent(catalog, monkeypatch, tmp_path):
    """THE GUARD FOR v1's FAILURE CLASS, proved rather than assumed.

    ecb seeds a cursor for every file it VISITS, before fetching, and ten paths in ecb.py
    `continue` without writing. So a "changed" key can name a file this run never wrote, and
    under r2 the deriver would open the local scratch mirror, find nothing, and fail with
    "zero rows matched" — a narrower rerun of what made v1 harmful.

    NOTE WHY THIS TEST NEEDS ITS OWN source_dir. The other tests in this file pass with the
    guard active only because `config.source_dir("ecb")` resolves to the REAL store on this
    machine, where those files exist — so they exercise the guard's TRUE branch by accident
    and could never see it fail. Pointing at an empty directory is what makes the assertion
    mean anything.
    """
    empty = tmp_path / "store"
    empty.mkdir()
    monkeypatch.setattr(config, "source_dir", lambda _s: str(empty))
    ids, unmapped = _catalog_ids_for("ecb", ["ECB__EXR__D"])
    assert ids == [], (
        f"claimed {len(ids)} ids for a file that is not on the machine — under r2 the deriver "
        f"would fail with 'zero rows matched'. Got {ids[:3]}")
    assert unmapped == ["ECB__EXR__D"], "and it must be reported as unmapped, not swallowed"


def test_locally_the_guard_does_not_apply(catalog, monkeypatch, tmp_path):
    """The control. Under the local backend the whole store is present by definition, so the
    presence check must NOT gate — otherwise a local run would silently stop deriving."""
    empty = tmp_path / "store2"
    empty.mkdir()
    monkeypatch.setattr(config, "source_dir", lambda _s: str(empty))
    monkeypatch.setattr(config, "BACKEND", "local")
    ids, _ = _catalog_ids_for("ecb", ["ECB__EXR__D"])
    assert len(ids) == REAL["EXR"], f"local run must still claim all 18, got {len(ids)}"
