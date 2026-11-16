"""The orchestrator keeps a unit's last_obs_date from going backwards (review AR-491 condition 2).

ilostat now reports the newest observed date of the indicators a pass published (PR #266). In steady
rotation a pass may publish only annual tables (max YYYY-01-01) after one that published a monthly
table: without the guard in run_once ("last_obs_date must never regress") the stored date would drop
and health's RED-DATA would flap. This drives the REAL run_once and the REAL StateStore with a
scripted strategy; nothing else in the write path is replaced.
"""
from __future__ import annotations

import os
import sys
import types

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "clients", "python"))

from updater import orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies import fetchers  # noqa: E402
from updater.strategies.base import Result, Strategy, Unit  # noqa: E402


class _Strat(Strategy):
    name = "sim"

    def __init__(self):
        self.next_last = None
        self.since_seen = []

    def detect_change(self, unit, us):
        return "v-new"

    def run(self, unit, since=None):
        self.since_seen.append(since)
        return Result(status="partial", obs=10, new_vintage="date-tail", merged_rows=1, changed_keys={},
                      last_obs_date=self.next_last,
                      error="1 sub-unit(s) attempted, none failed; 1 deferred by budget, not reached in this pass")


def _wire(tmp_path, monkeypatch):
    store = StateStore(str(tmp_path / "state.db"))
    unit = Unit(source_id="simsrc", unit_id="_all", strategy="sim", cadence="monthly", out_paths=[],
                config={"run_location": "local", "live": True})
    for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
        monkeypatch.delenv(k, raising=False)
    mp = monkeypatch
    mp.setenv("AQUEDUCT_RUN_BUDGET_MIN", "600")
    mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    mp.setattr(registry, "validate", lambda *a, **k: [])
    mp.setattr(registry, "all_units", lambda *a, **k: [unit])
    mp.setattr(fetchers, "get", lambda sid: types.SimpleNamespace())
    mp.setattr(orchestrate, "_has_adapter", lambda u: True)
    mp.setattr(orchestrate, "_wrong_location", lambda u: False)
    mp.setattr(orchestrate, "_protected", lambda u: False)
    mp.setattr(orchestrate, "_is_live", lambda u: True)
    mp.setattr(orchestrate, "_served_by_lane", lambda sid: False)
    mp.setattr(orchestrate, "_derive_changed_csvs", lambda unit, res, blob, st: ([], None, [], {}))
    mp.setattr(orchestrate, "_drain_csv_retry_queue", lambda *a, **k: None)
    mp.setattr(orchestrate, "_drain_leftover_retries", lambda *a, **k: None)
    strat = _Strat()
    mp.setattr(orchestrate, "get_strategy", lambda name: strat)
    return store, strat


def _stored(store):
    return (store.get_unit("simsrc", "_all") or {}).get("last_obs_date")


def _pass(store, strat, last):
    strat.next_last = last
    orchestrate.run_once(store=store, force=True)
    return _stored(store)


def test_a_lower_date_does_not_lower_the_stored_one(tmp_path, monkeypatch):
    store, strat = _wire(tmp_path, monkeypatch)
    assert _pass(store, strat, "2026-10-01") == "2026-10-01"       # positive control: the write happens
    assert _pass(store, strat, "2026-01-01") == "2026-10-01"       # an annual-only pass: kept
    assert _pass(store, strat, None) == "2026-10-01"               # nothing reported: kept
    assert _pass(store, strat, "2026-11-01") == "2026-11-01"       # a newer date still advances
    assert strat.since_seen[1:] == ["2026-10-01", "2026-10-01", "2026-10-01"], strat.since_seen
