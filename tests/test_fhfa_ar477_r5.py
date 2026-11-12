"""AR-477 round 5.

F1 (MEDIUM): when the UNMAPPED-KEYS record (_note_unmapped_debt -> note_full_rederive_owed) fails, its callers
return no ids, so the round-4 flag (csv_failed / csv_deferred) stayed False and run_once recorded the receipt:
the changed keys were in no home and the next pass skipped. _note_unmapped_debt now signals run_once directly
(orchestrate._UNHOMED_SOURCES). P5c (fence trip, mapper and record fail) and P5d (zero-mapped, record fails)
are the reviewer's probes as tests.

F2 (LOW): the fence-trip booking fallback had no PR test (mutant M8a survived). P5a and its control cover it;
P5b covers the crash fallback.

F3 (LOW): the demoted note began "csv coverage note ... none failed", and send_digest.kind_of filed it as
SUBSET. The demotion segment now goes first and carries orchestrate.UNHOMED_NOTE, which kind_of matches first.

The probes drive the real run_once and the real fhfa.update (the r3 rig), with one fault each."""
from __future__ import annotations

import sqlite3

import pytest

pytest.importorskip("pyarrow"); pytest.importorskip("duckdb")
from tests.test_fhfa_diff_changed_keys import D, _master, rig  # noqa: E402,F401
from tests.test_fhfa_ar477_r2 import _deferring, _putting  # noqa: E402
from tests.test_fhfa_ar477_r3 import K1, _derive, _pass, _sidecar_build  # noqa: E402
from tests.test_fhfa_ar477_r3_probe import V1, V2  # noqa: E402
from updater import orchestrate, send_digest  # noqa: E402
from updater.state import StateStore  # noqa: E402


def _boom(self, *a, **k):
    raise RuntimeError("booking write failed")


def _tripping(asked):
    def f(ids, blob, **kw):
        asked.append(list(dict.fromkeys(ids)))
        raise orchestrate.UnitTimeout("probe: csv fence")
    return f


def _crashing(asked):
    def f(ids, blob, **kw):
        asked.append(list(dict.fromkeys(ids)))
        raise RuntimeError("probe: derive crashed")
    return f


def _two_pass(tmp_path, monkeypatch, rig, capsys, fn1, patch_owed=True, extra=None, after1=None, tag="P5"):
    a = tmp_path / "A.db"
    _r0, _u0, rc0, *_ = _pass(tmp_path, monkeypatch, rig, a, {"hpi_master": _master()}, _derive(_putting), V1,
                              capsys)
    assert rc0, "control: pass 0 recorded no receipt"
    changed = {"hpi_master": _master(nsa={("K1", D[0]): 99.0})}
    if patch_owed:
        monkeypatch.setattr(StateStore, "note_csv_desktop_owed", _boom)
    if extra:
        extra()
    r1, us1, rc1, (o1, q1, d1), out1, _ = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(fn1), V2, capsys)
    b1 = _sidecar_build(rig)
    if after1:
        after1()
    r2, us2, rc2, (o2, q2, d2), out2, asked2 = _pass(tmp_path, monkeypatch, rig, a, changed, _derive(_putting), V2,
                                                     capsys)
    line = (f"pass1 status={us1[0]} receipt={'pass1' if rc1 == b1 else 'pass0'} owed={len(o1)} "
            f"queue={[r['series_id'] for r in q1]} desktop={[r['series_id'] for r in d1]} | "
            f"pass2 rebuilt={r2} status={us2[0]} owed={len(o2)} queue={[r['series_id'] for r in q2]} "
            f"desktop={[r['series_id'] for r in d2]}")
    with capsys.disabled():
        print(f"\n{tag} {line}")
    return dict(rc0=rc0, rc1=rc1, b1=b1, us1=us1, q1=q1, d1=d1, o1=o1, out1=out1, r2=r2, o2=o2, d2=d2, q2=q2,
                asked2=asked2)


def _covered(x):
    return (any(K1 in c for c in (x["asked2"] or [])) or x["o2"] or K1 in [r["series_id"] for r in x["d2"]])


# ------------------------------------------------------------------ F2: the queue fallbacks (fence, crash)
def test_p5a_fence_trip_with_failed_booking_records_no_receipt(rig, monkeypatch, tmp_path, capsys):
    x = _two_pass(tmp_path, monkeypatch, rig, capsys, _tripping, tag="P5A")
    assert K1 in [r["series_id"] for r in x["q1"]], "fence-trip fallback did not queue K1"
    assert x["rc1"] != x["b1"], "receipt recorded on a fence trip whose booking failed"
    assert x["r2"] and _covered(x)


def test_p5a_control_fence_trip_with_working_booking_records_receipt(rig, monkeypatch, tmp_path, capsys):
    x = _two_pass(tmp_path, monkeypatch, rig, capsys, _tripping, patch_owed=False, tag="P5A-control")
    assert K1 in [r["series_id"] for r in x["d1"]] and not x["q1"]
    assert x["rc1"] == x["b1"], "control: a fence trip that booked its ids must keep its receipt"


def test_p5b_crash_with_failed_booking_records_no_receipt(rig, monkeypatch, tmp_path, capsys):
    x = _two_pass(tmp_path, monkeypatch, rig, capsys, _crashing, tag="P5B")
    assert K1 in [r["series_id"] for r in x["q1"]], "crash fallback did not queue K1"
    assert x["rc1"] != x["b1"]
    assert x["r2"] and _covered(x)


# ------------------------------------------------------------------ F1: the unmapped-keys record fails
def test_p5c_fence_trip_unmappable_and_unrecordable_keys(rig, monkeypatch, tmp_path, capsys):
    """Fence trip; the mapper fails in _book_fence_trip AND the unmapped-keys record fails: no home at all."""
    real = orchestrate._map_changed
    real_fro = StateStore.note_full_rederive_owed
    calls = {"n": 0}

    def flaky(*a, **k):
        calls["n"] += 1
        if calls["n"] >= 2:          # the first call is the derive's own mapping; the second is _book_fence_trip's
            raise RuntimeError("probe: mapper failed")
        return real(*a, **k)

    def extra():
        calls["n"] = 0
        monkeypatch.setattr(orchestrate, "_map_changed", flaky)
        monkeypatch.setattr(StateStore, "note_full_rederive_owed", _boom)

    def after1():
        monkeypatch.setattr(orchestrate, "_map_changed", real)
        monkeypatch.setattr(StateStore, "note_full_rederive_owed", real_fro)
    x = _two_pass(tmp_path, monkeypatch, rig, capsys, _tripping, patch_owed=False, extra=extra, after1=after1,
                  tag="P5C")
    assert "could NOT be mapped" in x["out1"], x["out1"][-2000:]
    assert x["rc1"] != x["b1"], "receipt recorded although the changed keys were recorded nowhere"
    assert x["r2"] and _covered(x), "pass 2 skipped: K1 in no home"


def _zero_mapped_extra(tmp_path, monkeypatch, fail_record=True):
    def extra():
        con = sqlite3.connect(tmp_path / "catalog.db")
        con.execute("DELETE FROM series WHERE series_id=?", (K1,))
        con.commit(); con.close()
        if fail_record:
            monkeypatch.setattr(StateStore, "note_full_rederive_owed", _boom)
    return extra


def test_p5d_zero_mapped_with_unrecordable_keys(rig, monkeypatch, tmp_path, capsys):
    """Zero-mapped pass (K1's catalogue row is gone) and the unmapped-keys record fails. One state-write fault."""
    real_fro = StateStore.note_full_rederive_owed

    def after1():
        monkeypatch.setattr(StateStore, "note_full_rederive_owed", real_fro)
    x = _two_pass(tmp_path, monkeypatch, rig, capsys, _putting, patch_owed=False,
                  extra=_zero_mapped_extra(tmp_path, monkeypatch), after1=after1, tag="P5D")
    assert x["rc1"] != x["b1"], "receipt recorded although the changed keys were recorded nowhere"
    assert "build receipt NOT recorded" in x["out1"]
    assert x["r2"], "pass 2 skipped although pass 1's changed keys were recorded nowhere"
    assert x["us1"][2].startswith(orchestrate.UNHOMED_NOTE), x["us1"][2]


def test_p5d_control_zero_mapped_with_a_working_record_keeps_its_receipt(rig, monkeypatch, tmp_path, capsys):
    """Control for P5d: the same zero-mapped pass with a working record books full_rederive_owed and keeps
    its receipt (the debt is in a health-read home)."""
    x = _two_pass(tmp_path, monkeypatch, rig, capsys, _putting, patch_owed=False,
                  extra=_zero_mapped_extra(tmp_path, monkeypatch, fail_record=False), tag="P5D-control")
    assert x["o1"], "control: the unmapped keys were not recorded"
    assert x["rc1"] == x["b1"], "control: a recorded debt must keep its receipt"
    assert orchestrate.UNHOMED_NOTE not in (x["us1"][2] or "")


# ------------------------------------------------------------------ F3: the digest files the row as a failure
def test_p5e_a_demoted_deferral_reads_as_unhomed_in_the_digest(rig, monkeypatch, tmp_path, capsys):
    """R4B's pass (budget deferral, booking fails): its unit error starts with UNHOMED_NOTE, inside the 72
    characters the digest shows, and kind_of files it as UNHOMED, not as SUBSET noise."""
    x = _two_pass(tmp_path, monkeypatch, rig, capsys, _deferring, tag="P5E")
    err = x["us1"][2] or ""
    assert x["us1"][0] == "partial"
    assert "csv coverage note" in err, "precondition: the pass also carries the deferral's coverage note"
    assert err[:72].startswith(orchestrate.UNHOMED_NOTE), err
    assert send_digest.kind_of(err) == (4, "UNHOMED"), send_digest.kind_of(err)


def test_p5e_kind_of_controls():
    """A plain coverage note stays SUBSET; the marker wins wherever it sits in the text."""
    assert send_digest.kind_of("csv coverage note: derive budget spent - 3 of 9 id(s) deferred, none failed") \
        == (8, "SUBSET")
    assert send_digest.kind_of(f"csv coverage note: x; {orchestrate.UNHOMED_NOTE}: y") == (4, "UNHOMED")
    assert send_digest.kind_of("something else") == (4, "OTHER")
