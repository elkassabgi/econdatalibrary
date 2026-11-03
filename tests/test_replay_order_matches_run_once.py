"""The starvation replay must order units exactly as run_once does (review AR-421).

run_once's order key read the cadence from unit.config (None for every registry unit, so the order is by age
alone), while tools/measure_ci_lag.py passed u.cadence: the replay modelled an order production never ran, and the
plan's ordering decisions were taken on it. Both now go through orchestrate.order_cadence; these tests fail if the
replay's unit or its key ever diverges from a real registry Unit again.
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "tools"))

import measure_ci_lag as m  # noqa: E402
from updater import orchestrate, registry  # noqa: E402


def _local_entries():
    reg = registry.load()
    return [e for e in reg.get("sources") or [] if e.get("run_location") == "local" and e.get("source_id")]


def test_every_local_unit_gets_the_same_order_cadence_in_the_replay_and_in_production():
    entries = _local_entries()
    assert entries, "no local units found in the registry - the test would pass vacuously"
    sim = m.local_units_from_registry()
    checked = 0
    for e in entries:
        if e["source_id"] not in sim:
            continue
        for u in registry.to_units(e):
            s = m.SimUnit(u.source_id, sim[u.source_id])
            assert orchestrate.order_cadence(s) == orchestrate.order_cadence(u), (
                f"{u.key}: the replay orders by {orchestrate.order_cadence(s)!r}, production by "
                f"{orchestrate.order_cadence(u)!r}")
            checked += 1
    assert checked > 0


def test_the_replay_unit_keeps_its_cadence_for_is_due_but_not_for_the_order():
    s = m.SimUnit("x", "daily")
    assert s.cadence == "daily"                       # is_due and the cadence floor still read it
    assert orchestrate.order_cadence(s) is None       # the order key does not, exactly like a registry Unit


def test_the_replay_pass_orders_two_equally_due_units_as_production_does():
    """Behavioural (AR-421 P1): the replay's own key must go through order_cadence. A daily unit 5 days stale and a
    weekly unit 10 days stale are both due. Production orders by age alone (order_cadence is None for both):
    weekly first. A replay that passed u.cadence would rank the daily one first (5x overdue vs 1.43x)."""
    import datetime as dt
    t0 = dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc)
    big = 60.0
    st = {"d": {"last_success_utc": "2026-09-26T00:00:00+00:00", "last_attempt_utc": "2026-09-26T00:00:00+00:00",
                "status": "ok"},
          "w": {"last_success_utc": "2026-09-21T00:00:00+00:00", "last_attempt_utc": "2026-09-21T00:00:00+00:00",
                "status": "ok"}}
    rows = [("2026-07-01T00:00:00+00:00", s, "_all", "ok", big) for s in ("d", "w")]
    book = m.UnitBook({"d": "daily", "w": "weekly"}, st, rows, {"d": 10.0, "w": 10.0}, {"d": "ok", "w": "ok"}, (), ())
    assert all(book.is_due(s, t0) for s in ("d", "w")), "both units must be due or the order is not tested"
    ran = [x[0] for x in book.run_pass(t0, 1000.0)["ran"]]
    # production's key, computed directly from run_once's function
    want = sorted(("d", "w"), key=lambda s: (orchestrate.overdue_key(st[s]["last_success_utc"],
                                                                     orchestrate.order_cadence(book.units[s]),
                                                                     t0.isoformat()), book.units[s].key))
    assert ran == want == ["w", "d"]


def test_order_cadence_is_what_run_once_passes_to_overdue_key():
    """The key function is shared, not copied: run_once's _staleness calls order_cadence."""
    import inspect
    src = inspect.getsource(orchestrate.run_once)
    assert "overdue_key(last, order_cadence(unit), _now)" in src
    assert '(unit.config or {}).get("cadence")' not in src
