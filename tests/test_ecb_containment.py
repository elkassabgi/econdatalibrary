"""ecb: claim served ids by what a changed file HOLDS, and never publish a CSV that lost served dates
(review R1132, 2026-09-23).

27 of the 35 served ecb ids are also held by ECB.DISS mirror files (MOBILE_EXR, MOBILE_KEY_6, FM_PUB__M,
YC_PUB__B). The name rule claimed only the primaries, so a mirror-only pass (2026-09-23 11:38Z) added a
day to 23 served series and their CSVs stayed a day behind. The resolver serves the union of the ecb/
directory, but a runner holds only the files its pass wrote - so a derive there can miss dates another
file carries. Hermetic: a tmp catalogue and store, the r2 backend pinned (production), a LocalBlob.
"""
from __future__ import annotations

import datetime as dt
import gzip
import os
import sqlite3
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import blob as blob_mod, config, derive, orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402

EXR = [f"ecb:EXR:D.{c}.EUR.SP00.A" for c in ("USD", "GBP", "JPY")]
YC = ["ecb:YC:B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y"]
OTHER = ["ecb:YC:B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y"]


def _native(cid):
    _, flow, key = cid.split(":", 2)
    return f"{flow}.{key}"


def _file(store, stem, ids, extra=("EXR.D.XXX.EUR.SP00.A",), dates=(dt.date(2026, 9, 22),)):
    keys = [_native(i) for i in ids] + list(extra)
    rows = [(k, d) for k in keys for d in dates]
    pq.write_table(pa.table({"series_key": [k for k, _ in rows], "obs_date": pa.array([d for _, d in rows]),
                             "value": [1.0] * len(rows), "freq": ["D"] * len(rows)}),
                   str(store / f"{stem}.parquet"))


@pytest.fixture
def env(tmp_path, monkeypatch):
    p = tmp_path / "catalog.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE series (series_id TEXT PRIMARY KEY, source_id TEXT)")
    con.executemany("INSERT INTO series VALUES (?,?)", [(i, "ecb") for i in EXR + YC + OTHER])
    con.execute("INSERT INTO series VALUES (?,?)", ("ecbx:EXR:D.USD.EUR.SP00.A", "ecbx"))
    con.commit()
    con.close()
    monkeypatch.setenv("ECONDL_CATALOG", str(p))
    monkeypatch.setattr(config, "BACKEND", "r2")
    store = tmp_path / "store"
    store.mkdir()
    monkeypatch.setattr(config, "source_dir", lambda sid: str(store))
    return store


def test_a_mirror_only_pass_claims_the_served_ids_the_mirror_holds(env):
    _file(env, "ECB.DISS__MOBILE_EXR", EXR)
    ids, unmapped = orchestrate._catalog_ids_for("ecb", ["ECB.DISS__MOBILE_EXR"])
    assert sorted(ids) == sorted(EXR) and unmapped == [], (ids, unmapped)


def test_containment_is_exact_a_sibling_curve_is_not_claimed(env):
    _file(env, "ECB__YC__B__G_N_A", YC)
    ids, _ = orchestrate._catalog_ids_for("ecb", ["ECB__YC__B__G_N_A"])
    assert ids == YC, ids


def test_negative_control_a_file_holding_no_served_id_claims_nothing(env):
    _file(env, "ECB.DISS__BSI_PUB__M", [], extra=("BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E",))
    ids, unmapped = orchestrate._catalog_ids_for("ecb", ["ECB.DISS__BSI_PUB__M"])
    assert ids == [] and unmapped == ["ECB.DISS__BSI_PUB__M"], (ids, unmapped)


def test_under_r2_a_file_this_run_did_not_write_claims_nothing(env):
    ids, unmapped = orchestrate._catalog_ids_for("ecb", ["ECB.DISS__MOBILE_EXR"])
    assert ids == [] and unmapped == ["ECB.DISS__MOBILE_EXR"], (ids, unmapped)


def test_an_unreadable_file_falls_back_to_the_name_rule(env):
    (env / "ECB__EXR__D.parquet").write_bytes(b"not a parquet")
    ids, _ = orchestrate._catalog_ids_for("ecb", ["ECB__EXR__D"])
    assert sorted(ids) == sorted(EXR), ids
    (env / "ECB.DISS__MOBILE_EXR.parquet").write_bytes(b"not a parquet")
    ids, unmapped = orchestrate._catalog_ids_for("ecb", ["ECB.DISS__MOBILE_EXR"])
    assert ids == [] and unmapped == ["ECB.DISS__MOBILE_EXR"], "no name rule for a mirror"
    assert orchestrate._catalog_ids_for.ecb_unreadable == ["ECB.DISS__MOBILE_EXR"]


# ---- every ecb upload is merged with the served CSV (in derive_and_put: both callers) -------------
def _csv(rows, sid=EXR[0]):
    return ("series_id,obs_date,value\n" + "".join(f"{sid},{d},{v}\n" for d, v in rows)).encode("utf-8")


def _derive(tmp_path, monkeypatch, served, new, sid=EXR[0], blob=None):
    b = blob or blob_mod.LocalBlob(str(tmp_path / "r2"))
    if served is not None:
        b.put_atomic(derive.r2_key(sid), served)
    monkeypatch.setattr(derive, "_series_csv_bytes", lambda s: new)
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    out = derive.derive_and_put([sid], b, budget_min=0)
    return out, b.get(derive.r2_key(sid))


def test_a_primary_only_pass_after_a_mirror_pass_keeps_the_mirror_date(tmp_path, monkeypatch):
    """R1136 sequence: the mirror added 09-22, the next pass wrote only the primary, which lacks it."""
    served = _csv([("2026-09-21", "1.1"), ("2026-09-22", "1.2")])
    out, now = _derive(tmp_path, monkeypatch, served, _csv([("2026-09-21", "1.1")]))
    assert out["put"] == 1 and out["failed"] == [] and out["served_dates_kept"] == {EXR[0]: 1}, out
    assert now == served, "full data uploaded, byte-identical to the union"


def test_new_rows_win_on_a_shared_date_and_new_dates_are_added(tmp_path, monkeypatch):
    served = _csv([("2026-09-20", "1.0"), ("2026-09-21", "9.9")])
    out, now = _derive(tmp_path, monkeypatch, served, _csv([("2026-09-21", "1.1"), ("2026-09-22", "1.2")]))
    assert now == _csv([("2026-09-20", "1.0"), ("2026-09-21", "1.1"), ("2026-09-22", "1.2")]), now
    assert out["served_dates_kept"] == {EXR[0]: 1}


def test_a_gzipped_served_csv_is_merged_too(tmp_path, monkeypatch):
    served = gzip.compress(_csv([("2026-09-21", "1.1"), ("2026-09-22", "1.2")]))
    out, now = _derive(tmp_path, monkeypatch, served, _csv([("2026-09-21", "1.1")]))
    assert out["served_dates_kept"] == {EXR[0]: 1} and b"2026-09-22" in now, out


def test_nothing_served_yet_uploads_the_new_csv(tmp_path, monkeypatch):
    new = _csv([("2026-09-21", "1.1")])
    out, now = _derive(tmp_path, monkeypatch, None, new)
    assert out["put"] == 1 and now == new and out["served_dates_kept"] == {}


def test_an_unreadable_served_csv_fails_the_id_and_uploads_nothing(tmp_path, monkeypatch):
    b = blob_mod.LocalBlob(str(tmp_path / "r2"))
    monkeypatch.setattr(b, "get", lambda key: (_ for _ in ()).throw(OSError("R2 down")))
    monkeypatch.setattr(derive, "_series_csv_bytes", lambda s: _csv([("2026-09-21", "1.1")]))
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    out = derive.derive_and_put([EXR[0]], b, budget_min=0)
    assert out["put"] == 0 and out["failed"] == [EXR[0]], out
    assert not (tmp_path / "r2").exists() or not any((tmp_path / "r2").rglob("*.csv")), "nothing uploaded"
    assert "unreadable" in out["failed_reasons"][EXR[0]]


def test_a_served_csv_that_cannot_be_merged_fails_the_id(tmp_path, monkeypatch):
    served = b"series_id,obs_date,value,extra\n" + f"{EXR[0]},2026-09-21,1.1,x\n".encode()
    out, now = _derive(tmp_path, monkeypatch, served, _csv([("2026-09-21", "1.1")]))
    assert out["put"] == 0 and out["failed"] == [EXR[0]] and now == served, out
    dup = _csv([("2026-09-21", "1.1"), ("2026-09-21", "1.2")])
    out, _ = _derive(tmp_path / "b", monkeypatch, dup, _csv([("2026-09-21", "1.1")]))
    assert out["failed"] == [EXR[0]], "a date repeated inside one CSV is refused"


def test_negative_control_a_source_without_the_flag_is_uploaded_as_derived(tmp_path, monkeypatch):
    sid = "abs:X:Y"
    served = _csv([("2026-09-21", "1.1"), ("2026-09-22", "1.2")], sid=sid)
    new = _csv([("2026-09-21", "1.1")], sid=sid)
    out, now = _derive(tmp_path, monkeypatch, served, new, sid=sid)
    assert now == new and out["served_dates_kept"] == {}


def test_the_merged_csv_is_in_date_order(tmp_path, monkeypatch):
    out, now = _derive(tmp_path, monkeypatch, _csv([("2026-09-22", "1.2")]), _csv([("2026-09-21", "1.1")]))
    assert now == _csv([("2026-09-21", "1.1"), ("2026-09-22", "1.2")]), now


def test_a_readable_primary_holding_no_served_id_is_not_claimed_by_its_name(env):
    _file(env, "ECB__EXR__D", [], extra=("EXR.D.XXX.EUR.SP00.A",))
    ids, unmapped = orchestrate._catalog_ids_for("ecb", ["ECB__EXR__D"])
    assert ids == [] and unmapped == ["ECB__EXR__D"], "containment answered: the name rule must not run"


def test_only_a_boolean_true_declares_the_merge(monkeypatch):
    monkeypatch.setattr(registry, "load", lambda path=None: {"sources": [
        {"source_id": "zz", "csv_merge_served": "yes"}, {"source_id": "yy", "csv_merge_served": True}]})
    monkeypatch.setattr(derive._merge_served_sources, "_cache", None)
    assert derive._merge_served_sources() == {"yy"}
    monkeypatch.setattr(derive._merge_served_sources, "_cache", None)


def test_the_merge_set_comes_from_the_registry():
    assert "ecb" in derive._merge_served_sources() and "abs" not in derive._merge_served_sources()
    bad = {"sources": [{"source_id": "zz", "strategy": "extend_by_date", "cadence": "daily",
                        "csv_merge_served": "yes"}]}
    assert any("csv_merge_served" in p for p in registry.validate(bad))


# ---- the orchestrator's notes -------------------------------------------------------------------
def _phase(env, tmp_path, monkeypatch, out, changed):
    monkeypatch.setattr(derive, "derive_and_put", lambda ids, blob, **kw: {
        "put": len(ids), "failed": [], "deferred": 0, "deferred_ids": [], "failed_reasons": {},
        "skipped_identical": 0, "deferred_large": {}, **out})
    monkeypatch.setattr(orchestrate, "_record_for_catalog_sync", lambda ids: None)
    st = StateStore(path=str(tmp_path / "state.db"))
    unit = types.SimpleNamespace(key="ecb/_all", source_id="ecb", unit_id="_all")
    res = types.SimpleNamespace(changed_keys={k: "2026-09-22" for k in changed}, series_cursors=None, obs=1)
    return orchestrate._derive_changed_csvs(unit, res, object(), st)


def test_kept_dates_are_disclosed_in_a_non_demoting_note(env, tmp_path, monkeypatch):
    _file(env, "ECB__EXR__D", EXR)
    failed, note, _d, _r = _phase(env, tmp_path, monkeypatch, {"served_dates_kept": {EXR[0]: 1}}, ["ECB__EXR__D"])
    assert failed == [] and note.startswith("csv coverage note:") and "1 served date(s)" in note, note
    assert "; " not in note


def test_a_failed_containment_read_demotes(env, tmp_path, monkeypatch):
    (env / "ECB__EXR__D.parquet").write_bytes(b"not a parquet")
    failed, note, _d, _r = _phase(env, tmp_path, monkeypatch, {}, ["ECB__EXR__D"])
    assert note and not note.startswith("csv coverage note:") and "containment read failed" in note, note


def test_a_zero_mapped_pass_is_a_coverage_note_under_the_declared_subset(env, tmp_path, monkeypatch):
    _file(env, "ECB.DISS__BSI_PUB__M", [], extra=("BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E",))
    failed, note, _d, _r = _phase(env, tmp_path, monkeypatch, {}, ["ECB.DISS__BSI_PUB__M"])
    assert note.startswith("csv coverage note:"), note
    assert orchestrate._catalog_scope("ecb") == "subset"


