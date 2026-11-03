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


def test_order_cadence_is_what_run_once_passes_to_overdue_key():
    """The key function is shared, not copied: run_once's _staleness calls order_cadence."""
    import inspect
    src = inspect.getsource(orchestrate.run_once)
    assert "overdue_key(last, order_cadence(unit), _now)" in src
    assert '(unit.config or {}).get("cadence")' not in src
