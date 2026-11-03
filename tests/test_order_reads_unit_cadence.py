"""run_once orders units by staleness normalised by the unit's OWN cadence.

WHY. The sort key in run_once read the cadence from `unit.config.get("cadence")`. The registry
(updater/registry.py to_units) puts the cadence on the Unit itself - `Unit.cadence` - and never in
its config, so the key read None for every unit and overdue_key fell back to monthly. A daily
source three days stale then ranked the same as a monthly source three days stale, and the
alphabet broke the tie. That defeats overdue_key's own rule: serve the source most in violation of
its own promise first.

The test goes through the real run_once and the real order_units. Only the unit list is replaced
(two synthetic units with the SAME last success), and order_units is wrapped so the run stops
before anything is fetched. The monthly unit's name sorts first alphabetically, so a key that
ignores cadence puts it first and the test fails.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.base import Unit  # noqa: E402


class _Stop(Exception):
    pass


def _ordered(tmp_path, monkeypatch):
    from updater import orchestrate as orch
    from updater.state import StateStore

    units = [
        Unit(source_id="a_monthly_src", unit_id="_all", strategy="script", cadence="monthly",
             config={"live": False, "run_location": "any", "window_stop": False}),
        Unit(source_id="z_daily_src", unit_id="_all", strategy="script", cadence="daily",
             config={"live": False, "run_location": "any", "window_stop": False}),
    ]
    monkeypatch.setattr(orch.registry, "all_units", lambda *a, **k: list(units))
    monkeypatch.setattr(orch.registry, "validate", lambda *a, **k: [])

    store = StateStore(str(tmp_path / "state.db"))
    same = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    for u in units:
        store.upsert_unit(u.source_id, u.unit_id, strategy=u.strategy, last_success_utc=same,
                          status="ok")

    real_order = orch.order_units
    seen = {}

    def probe(us, costs, staleness_key, *a, **k):
        seen["keys"] = {u.source_id: staleness_key(u)[0] for u in us}
        seen["order"] = [u.source_id for u in real_order(us, costs, staleness_key, *a, **k)]
        raise _Stop()

    monkeypatch.setattr(orch, "order_units", probe)
    try:
        orch.run_once(sources=[u.source_id for u in units], dry=True, store=store)
    except _Stop:
        pass
    assert "order" in seen, "run_once never reached order_units"
    return seen


def test_a_stale_daily_unit_sorts_before_an_equally_stale_monthly_unit(tmp_path, monkeypatch):
    seen = _ordered(tmp_path, monkeypatch)
    assert seen["order"] == ["z_daily_src", "a_monthly_src"], seen


def test_the_key_is_normalised_by_each_units_own_cadence(tmp_path, monkeypatch):
    keys = _ordered(tmp_path, monkeypatch)["keys"]
    # 3 days stale: 3x overdue for a daily unit, 0.1x for a monthly unit
    assert -3.1 < keys["z_daily_src"] < -2.9, keys
    assert -0.11 < keys["a_monthly_src"] < -0.09, keys
