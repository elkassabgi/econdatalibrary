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
