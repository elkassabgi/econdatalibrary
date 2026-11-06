"""Who may be a band's HEAD in run_once (review AR-450 round 3).

AR-450's mutation "run_once's band head ignores whether the unit is due" SURVIVED all 47 tests of the three files it
ran. These tests drive the REAL run_once over REAL registry units on the workstation platform (no per-unit hard
timeout) and record, through a wrapper around the real admission_reserve, which unit run_once offers band_head=True.

  1. A NOT-DUE unit never takes the head, even when it sorts first in its band: the next DUE unit does.
  2. A unit with NO ADAPTER (PENDING, or an adapter check that raises) never takes the head: run_once chose the head
     before its own _has_adapter check, and a PENDING unit records no turn, so it would have held the head of its band
     in every pass.
Each test checks its own precondition (who sorts first, who is due) so a wrong setup fails here, not silently.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import orchestrate  # noqa: E402
from updater.state import StateStore  # noqa: E402


@pytest.fixture(autouse=True)
def _restore_run_deadline(monkeypatch):
    """run_once sets the module global orchestrate._RUN_DEADLINE_TS and nothing clears it; left set, it leaks into
    later test files (tests/test_csv_coverage_note.py failed 7 tests on CI, review AR-450 round 5, R1839)."""
    from updater import orchestrate as _orch
    monkeypatch.setattr(_orch, "_RUN_DEADLINE_TS", _orch._RUN_DEADLINE_TS)

OLD_NOT_DUE = "cepii_baci"     # annual cadence: a success 100 days ago is NOT due, and sorts FIRST (oldest)
YOUNG_DUE = "bls"              # weekly cadence: a success 10 days ago IS due, and sorts second
THREE_H = 3 * 3600.0           # both in band 3, and a newest real run of 180 min (a raised reserve)


@pytest.fixture(autouse=True)
def _workstation(monkeypatch):
    monkeypatch.setattr(orchestrate, "hard_timeout_available", lambda: False)
    for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "120")


def _store(tmp_path):
    store = StateStore(str(tmp_path / "state.db"))
    now = datetime.now(timezone.utc)
    from updater import registry
    units = {u.source_id: u for u in registry.all_units() if u.unit_id == "_all"}
    for sid, days in ((OLD_NOT_DUE, 100), (YOUNG_DUE, 10)):
        stamp = (now - timedelta(days=days)).isoformat(timespec="seconds")
        store.upsert_unit(sid, "_all", strategy=units[sid].strategy, status="ok",
                          last_success_utc=stamp, last_attempt_utc=stamp)
        store.db.execute("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) VALUES (?,?,?,?,?,?,?)",
                         (stamp, sid, "_all", "ok", 1, THREE_H, "planted"))
    store.db.commit()
    return store, units


def _heads(tmp_path, monkeypatch, has_adapter):
    """{source: band_head offered} and the order run_once used, from one dry pass."""
    store, units = _store(tmp_path)
    real = orchestrate.admission_reserve
    seen, order = {}, []

    def wrap(unit, est_s, *a, **k):
        seen[unit.source_id] = k.get("band_head", False)
        order.append(unit.source_id)
        return real(unit, est_s, *a, **k)
    monkeypatch.setattr(orchestrate, "admission_reserve", wrap)
    monkeypatch.setattr(orchestrate, "_has_adapter", has_adapter)
    try:
        orchestrate.run_once(sources=[OLD_NOT_DUE, YOUNG_DUE], dry=True, store=store)
    except SystemExit:
        pass
    return seen, order, store, units


def test_a_not_due_unit_never_takes_the_band_head(tmp_path, monkeypatch):
    seen, order, store, units = _heads(tmp_path, monkeypatch, lambda u: True)
    # preconditions: the not-due unit is first in run order, and the due-ness is what the test claims
    assert order == [OLD_NOT_DUE, YOUNG_DUE], order
    assert orchestrate._budget_skip_due_label(units[OLD_NOT_DUE], store, False) == "not due"
    assert orchestrate._budget_skip_due_label(units[YOUNG_DUE], store, False) == "due"
    assert seen == {OLD_NOT_DUE: False, YOUNG_DUE: True}, seen


def test_a_unit_without_an_adapter_never_takes_the_band_head(tmp_path, monkeypatch):
    # make the old unit DUE too (a success two years ago, past its annual cadence), so only its missing adapter can
    # disqualify it; it still sorts first
    store_seen = {}

    def no_adapter_for_old(u):
        return u.source_id != OLD_NOT_DUE
    store, units = _store(tmp_path)
    two_years = (datetime.now(timezone.utc) - timedelta(days=730)).isoformat(timespec="seconds")
    store.upsert_unit(OLD_NOT_DUE, "_all", last_success_utc=two_years, last_attempt_utc=two_years)
    assert orchestrate._budget_skip_due_label(units[OLD_NOT_DUE], store, False) == "due"     # precondition
    real = orchestrate.admission_reserve

    def wrap(unit, est_s, *a, **k):
        store_seen[unit.source_id] = k.get("band_head", False)
        return real(unit, est_s, *a, **k)
    monkeypatch.setattr(orchestrate, "admission_reserve", wrap)
    monkeypatch.setattr(orchestrate, "_has_adapter", no_adapter_for_old)
    try:
        orchestrate.run_once(sources=[OLD_NOT_DUE, YOUNG_DUE], dry=True, store=store)
    except SystemExit:
        pass
    assert store_seen == {OLD_NOT_DUE: False, YOUNG_DUE: True}, store_seen


def test_an_adapter_check_that_raises_never_takes_the_band_head(tmp_path, monkeypatch):
    store, units = _store(tmp_path)

    def boom(u):
        raise ImportError("planted: the fetcher will not import")
    monkeypatch.setattr(orchestrate, "_has_adapter", boom)
    assert orchestrate._can_head_band(units[YOUNG_DUE], store, False) is False
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda u: True)
    assert orchestrate._can_head_band(units[YOUNG_DUE], store, False) is True


def test_with_a_hard_timeout_no_head_is_looked_for(tmp_path, monkeypatch):
    """POSIX (the cloud): the rule does not apply, so run_once offers no unit the head."""
    monkeypatch.setattr(orchestrate, "hard_timeout_available", lambda: True)
    seen, _order, _s, _u = _heads(tmp_path, monkeypatch, lambda u: True)
    assert seen == {OLD_NOT_DUE: False, YOUNG_DUE: False}, seen


LOW_BAND = "bis"               # monthly; a 300-s run puts it in band 1 (120-600 s), so it runs BEFORE band 3
GIANT = "bls"                  # weekly; a 3-h run puts it in band 3 with a raised (180-min) reserve


def test_one_head_per_band_a_lower_band_head_does_not_take_band_3s(tmp_path, monkeypatch):
    """REVIEW AR-450 round 4: the mutant `_band_heads_taken.update(range(9))` (one head per PASS) survived every test,
    because each used one band. Here a band-1 unit takes band 1's head first; the band-3 unit must STILL be band 3's
    head and be offered the normal 90-min reserve - not its raised 180."""
    store = StateStore(str(tmp_path / "state.db"))
    now = datetime.now(timezone.utc)
    from updater import registry
    units = {u.source_id: u for u in registry.all_units() if u.unit_id == "_all"}
    for sid, days, dur in ((LOW_BAND, 40, 300.0), (GIANT, 10, THREE_H)):
        stamp = (now - timedelta(days=days)).isoformat(timespec="seconds")
        store.upsert_unit(sid, "_all", strategy=units[sid].strategy, status="ok",
                          last_success_utc=stamp, last_attempt_utc=stamp)
        store.db.execute("INSERT INTO runs(ts_utc,source_id,unit_id,status,obs,dur_s,note) VALUES (?,?,?,?,?,?,?)",
                         (stamp, sid, "_all", "ok", 1, dur, "planted"))
    store.db.commit()
    # preconditions: two different bands, both due
    est = store.run_cost_estimate()
    assert orchestrate.cost_band(est[LOW_BAND]) == 1 and orchestrate.cost_band(est[GIANT]) == 3, est
    for sid in (LOW_BAND, GIANT):
        assert orchestrate._budget_skip_due_label(units[sid], store, False) == "due", sid
    real = orchestrate.admission_reserve
    seen, order = {}, []

    def wrap(unit, est_s, *a, **k):
        r = real(unit, est_s, *a, **k)
        seen[unit.source_id] = (k.get("band_head", False), r[0])
        order.append(unit.source_id)
        return r
    monkeypatch.setattr(orchestrate, "admission_reserve", wrap)
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda u: True)
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "240")
    try:
        orchestrate.run_once(sources=[LOW_BAND, GIANT], dry=True, store=store)
    except SystemExit:
        pass
    assert order == [LOW_BAND, GIANT], order                       # the lower band's head is chosen first
    assert seen[LOW_BAND][0] is True, seen
    assert seen[GIANT] == (True, 90.0), seen                       # band 3's own head: the normal reserve
