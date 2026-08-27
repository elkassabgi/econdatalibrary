"""catalog_scope: subset + the eia dot-prefix mapper (R497).

HISTORY, because the first cut of this change was WRONG and the review caught it.
eia merged +235,050,106 rows and every run demoted to "csv coherence unmet"
`partial` forever (never sets last_success_utc, R231 — red AND unmonitorable,
R244). The first fix claimed "nothing served changed" because 0 of 800 sampled
cursors were catalogued under the EXACT form — the wrong property: eia's 268,495
catalogue ids are TABLE-grain and `_resolve_eia` serves them by dot-PREFIX, so
the "uncatalogued" hourly EBA.* leaves were INSIDE served CSVs, and the proposed
green would have silently frozen 598 served EBA tables (adversarial review FAIL,
ledger R497).

The real fix, pinned here: `_table_grain_native` routes eia through
`_eia_table_prefix` (per-dataset measured depths, `_EIA_DEPTH` == the
cataloguer's own map, drift-guarded below), so changed leaves collapse to their
catalogued table ids and get re-derived. The `catalog_scope: subset` exception
survives only as defense-in-depth with two R497 guards: the sample is
PREFIX-AWARE (a catalogued dot-prefix of a sampled key voids it) and a
cursor-cap-saturated changed-set is refused as truncated evidence. The R359
default — zero-mapped-with-rows demotes — stays for every undeclared source.

Discriminating pairs (R414) on the pure classifier + the mapper, plus the two
integration properties: the coverage note carries the exact prefix the caller's
green-path gate tests (orchestrate.py: `csv_err.startswith("csv coverage note:")`),
and the ROTATING grammar accepts it as a tail.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.orchestrate import _classify_zero_mapped  # noqa: E402
from updater.health import _deferral_only  # noqa: E402


def test_subset_with_zero_hits_is_coverage_not_coherence():
    note, demote = _classify_zero_mapped("eia", "subset", 268502, 0, 500, 4200)
    assert demote is False
    assert note.startswith("csv coverage note:"), note
    assert "catalog_scope" in note and "0 of 500" in note


def test_subset_with_any_hit_still_demotes():
    note, demote = _classify_zero_mapped("eia", "subset", 268502, 3, 500, 4200)
    assert demote is True
    assert note.startswith("csv coherence unmet:"), note
    assert "REFUSED" in note and "3 of 500" in note


def test_cap_saturated_changed_set_is_refused():
    # R497 blocker 2: a cursor-cap-saturated changed-set is truncated evidence —
    # the exception must never be granted from it, even with a clean sample.
    note, demote = _classify_zero_mapped("eia", "subset", 268502, 0, 500, 50000,
                                         cap_saturated=True)
    assert demote is True
    assert "cap-saturated" in note and note.startswith("csv coherence unmet:")


def test_full_scope_zero_mapped_demotes_unchanged():
    note, demote = _classify_zero_mapped("defillama", "full", 24, None, 0, 24)
    assert demote is True
    assert "grain/key-form mismatch" in note and note.startswith("csv coherence unmet:")


def test_no_rows_and_unavailable_still_demote_even_for_subset():
    # n_ids == 0: not catalogued/purged/stale reference — subset cannot rescue it.
    _, demote0 = _classify_zero_mapped("eia", "subset", 0, None, 0, 10)
    assert demote0 is True
    # count unavailable: suppression must never be granted blind.
    _, demote_na = _classify_zero_mapped("eia", "subset", None, None, 0, 10)
    assert demote_na is True


def test_unsampled_subset_demotes():
    # scope declared but the sample never ran (sqlite failure path): no proof, no pass.
    _, demote = _classify_zero_mapped("eia", "subset", 268502, None, 0, 10)
    assert demote is True


def test_rotating_grammar_accepts_the_coverage_tail():
    # A rotator whose run carries this note as its csv tail must keep ROTATING:
    # the classifier splits on '; csv coverage note:' and anchors the first part.
    note, demote = _classify_zero_mapped("eia", "subset", 268502, 0, 500, 4200)
    assert demote is False
    base = "15 sub-unit(s) attempted, none failed; 130 deferred by budget and taken next tick"
    unit = {"unit_id": "_all", "status": "partial", "last_error": base + "; " + note}
    assert _deferral_only([unit]) is True


# ---- the eia dot-prefix mapper rule (R497 blocker 1: the REAL fix) ----------

from updater.orchestrate import _eia_table_prefix, _table_grain_native, _EIA_DEPTH  # noqa: E402


def test_eia_leaf_keys_reduce_to_their_measured_table_prefix():
    # The reviewer's exact failure input: the hourly EBA leaf must map to the
    # catalogued table id (EBA depth 2), not sit unmapped.
    assert _eia_table_prefix("EBA.CISO-ALL.D.H") == "EBA.CISO-ALL"
    assert _table_grain_native("eia", "EBA.AEC-ALL.D.HL") == "EBA.AEC-ALL"
    # AEO vintages group at depth 3 (dataset name itself contains a dot).
    assert _eia_table_prefix(
        "AEO.2014.RLNGLOW20.CNSM_NA_IDAL_BMF_MTC_NA_NA_TRLBTU.A"
    ) == "AEO.2014.RLNGLOW20"
    # depth-2 families.
    assert _eia_table_prefix("STEO.NGINX_ESC.A") == "STEO.NGINX_ESC"
    assert _eia_table_prefix("TOTAL.DMTCEUS.M") == "TOTAL.DMTCEUS"


def test_eia_prefix_returns_none_rather_than_guessing():
    assert _eia_table_prefix("UNKNOWNDS.X.Y") is None          # unmeasured dataset
    assert _eia_table_prefix("EBA.CISO-ALL") is None           # already AT table grain
    assert _eia_table_prefix("PET") is None                    # bare dataset
    assert _table_grain_native("eia", "EBA") is None


def test_eia_depth_map_matches_the_cataloguer_verbatim():
    # DRIFT GUARD (R349 class): the mapper's map must equal the map the cataloguer
    # used to mint the table ids — a divergence silently strands whole datasets.
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "catalog_eia_tables",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "tools", "catalog_eia_tables.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    assert _EIA_DEPTH == mod.DEPTH
