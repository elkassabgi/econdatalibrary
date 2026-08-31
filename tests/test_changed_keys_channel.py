"""The merge-measured changed-key channel end-to-end (cursor-contract steps 3+4).

§5.7 must PREFER `res.changed_keys` when a fetcher provides it, honour an EMPTY dict
as "nothing changed" (coherence MET — never the no-cursors note, never a
full_rederive_owed debt), and fall back to `series_cursors.keys()` byte-identically
for the ~180 un-migrated fetchers.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater import orchestrate  # noqa: E402
from updater.strategies.base import Result  # noqa: E402


class _Unit:
    source_id = "zztest"
    unit_id = "_all"
    key = "zztest/_all"


def _res(**kw):
    return Result(status="ok", obs=kw.pop("obs", 10), **kw)


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    for k in ("EXR:A.AUD.NOK.SP", "EXR:A.USD.NOK.SP", "IR:B.KPRA.SD.R"):
        con.execute("INSERT INTO series VALUES (?,?)", (f"zztest:{k}", "zztest"))
    con.commit(); con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    return p


def test_default_field_is_none():
    assert Result(status="ok").changed_keys is None


def test_series_grain_maps_and_flow_grain_does_not(catalog, monkeypatch):
    """The mapping fact the pilot rests on: series-grain keys (what changed_keys
    carries) hit the exact tier; flow-grain keys (what series_cursors holds today)
    map to nothing under r2 semantics. NOTE the PREFERENCE ORDER itself is pinned by
    test_empty_changed_set_... below — the reviewer's mutants proved both the
    early-return and the preference die on that one assert."""
    ids, unmapped = orchestrate._catalog_ids_for(
        "zztest", ["EXR:A.AUD.NOK.SP", "IR:B.KPRA.SD.R"])
    assert sorted(ids) == ["zztest:EXR:A.AUD.NOK.SP", "zztest:IR:B.KPRA.SD.R"]
    assert unmapped == []
    # the flow-grain keys (what series_cursors holds today) map to NOTHING — under
    # r2 semantics, the backend norgesbank actually runs on. Locally the derive-all
    # rescue (catalogue <= _DERIVE_ALL_CAP) masks the defect, which is exactly why
    # only the CLOUD path starved (the audit's `derive-all-rescue(local)` flag).
    from updater import config
    monkeypatch.setattr(config, "BACKEND", "r2")
    ids2, unmapped2 = orchestrate._catalog_ids_for("zztest", ["EXR", "IR"])
    assert ids2 == [] and sorted(unmapped2) == ["EXR", "IR"]


def test_empty_changed_set_is_coherence_met_not_a_debt(catalog):
    """The idempotent re-fetch: rows merged (obs>0), changed_keys == {} — must return
    CLEAN: no note (no _NO_CURSORS_NOTE, no owed hook trigger), nothing queued."""
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(
        _Unit(), _res(series_cursors={"EXR": "2026-08-28"}, changed_keys={}), blob=None)
    assert failed == [] and deferred == [] and reasons == {}
    assert note is None, f"empty changed set must be coherence MET, got note: {note!r}"


def test_null_series_key_is_dropped_loudly_not_a_crash(catalog):
    """The reviewer's F1: the report's contract admits a None key (null series_key,
    AR-029 note); sorted(None, str) would TypeError → outer except → transient_fail
    AFTER a successful publish (the gleif 2-tuple disease). It must be dropped and
    the rest of the set processed."""
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(
        _Unit(), _res(changed_keys={None: "2024-01-01"}), blob=None)
    assert failed == [] and note is None      # only the None key -> effectively empty


def test_none_falls_back_to_cursors_and_books_no_cursors_note():
    """Un-migrated shape unchanged: no cursors at all + obs -> the §5.7 note."""
    import unittest.mock as mock
    with mock.patch.object(orchestrate, "_catalog_series_count", return_value=10,
                           create=True):
        failed, note, deferred, reasons = orchestrate._derive_changed_csvs(
            _Unit(), _res(series_cursors=None, changed_keys=None), blob=None)
    assert note is not None and note.startswith(orchestrate._NO_CURSORS_NOTE)


def test_norgesbank_wires_the_channel():
    """The pilot fetcher opts in at its merge call and carries the report onto the
    Result — pinned at the call-site level (R511: the shipped function, not a
    re-implementation)."""
    src = open(os.path.join(os.path.dirname(orchestrate.__file__),
                            "strategies", "fetchers", "norgesbank.py"),
               encoding="utf-8").read()
    assert "report_changed_keys=True" in src
    assert src.count("res.changed_keys = ") >= 2   # merge path + quiet path ({})


def test_statcan_wires_the_channel_with_the_all_or_none_rule():
    """WU-5 call-site pin: statcan opts in per-table under the report cap, and ONE
    unreported merge poisons the union (changed_keys stays None) — an incomplete
    dict would claim 'nothing else changed' while a giant table's vectors went
    stale."""
    src = open(os.path.join(os.path.dirname(orchestrate.__file__),
                            "strategies", "fetchers", "statcan.py"),
               encoding="utf-8").read()
    assert "report_changed_keys=True" in src
    assert "changed_complete = False" in src
    assert "if changed_complete:" in src
    assert "res.changed_keys = changed_all" in src


def test_statcan_vector_keys_bridge_via_punctuation(catalog, monkeypatch):
    """The migration's mapping premise: lowercase vector keys ('v65201210') reach the
    uppercase catalogue ids via the punctuation-insensitive fallback — driven through
    the SHIPPED mapper against a fixture catalogue."""
    import sqlite3 as _sq
    p = os.environ["ECONDL_CATALOG"]
    con = _sq.connect(p)
    con.execute("INSERT INTO series VALUES (?,?)", ("statcan:V65201210", "statcan"))
    con.execute("INSERT INTO series VALUES (?,?)", ("statcan:V1234567", "statcan"))
    con.commit(); con.close()
    from updater import config
    monkeypatch.setattr(config, "BACKEND", "r2")
    ids, unmapped = orchestrate._catalog_ids_for("statcan", ["v65201210", "v9999999"])
    assert ids == ["statcan:V65201210"]
    assert unmapped == ["v9999999"]


def test_statcan_registry_declares_subset_scope():
    """The reviewer's note (a): dropping the registry line failed NO test. Pin it —
    the subset scope is what turns uncatalogued-only runs into non-demoting notes."""
    assert orchestrate._catalog_scope("statcan") == "subset"


def test_migrated_complete_set_never_reads_cap_saturated(catalog, monkeypatch):
    """The reviewer's REQUIRED change (CASE D): a MERGE-MEASURED changed set is never
    truncated, so >=50k uncatalogued keys must take the subset note, not the
    'cursor-cap-saturated — truncated evidence' refusal + demote."""
    import sqlite3 as _sq
    from updater import config
    monkeypatch.setattr(config, "BACKEND", "r2")
    monkeypatch.setattr(orchestrate, "_catalog_scope", lambda sid: "subset")
    big = {f"vec{i}": "2026-08-31" for i in range(50_001)}   # >= CURSOR_CAP, complete
    failed, note, deferred, reasons = orchestrate._derive_changed_csvs(
        _Unit(), _res(changed_keys=big), blob=None)
    assert failed == [] and deferred == []
    assert note is not None and "cap-saturated" not in note and "truncated" not in note, note
    assert "coverage note" in note or "coherent" in note, note
