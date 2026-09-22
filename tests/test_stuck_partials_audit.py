"""A partial from this run and a partial from ninety days ago must not read the same.

`updater.health` is NOT blind to stuck partials - `partial` is in ATTENTION_STATUSES and every one
is printed as ATTENTION with its age. What ATTENTION does not do is DISTINGUISH: on 2026-09-22 that
bucket held 25 entries, mixing routine per-run deferrals with one source last attempted 90 days
earlier. The non-escalation is deliberate (health.py::_stuck_transient escalates "only
`transient_fail`, never `partial`" to avoid wolf-crying on permanently-partial giants), so the
answer is a separate count, not a new red.

The behaviour most worth pinning is the one the first version got WRONG: a source the registry does
not list fell back to the 7-day default cadence and printed "12.9x its cadence" for a source that
has no cadence at all. A fabricated ratio stated with the same confidence as a measured one is
worse than no ratio.

Offline: no state store, no registry, no network - the function takes its inputs.
"""
from __future__ import annotations

import datetime as dt
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

spec = importlib.util.spec_from_file_location(
    "audit_stuck_partials", os.path.join(ROOT, "tools", "audit_stuck_partials.py"))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

NOW = dt.datetime(2026, 9, 22, 12, 0, 0, tzinfo=dt.timezone.utc)


def _unit(sid, status="partial", days_ago=None):
    u = {"source_id": sid, "unit_id": "_all", "status": status}
    if days_ago is not None:
        u["last_attempt_utc"] = (NOW - dt.timedelta(days=days_ago)).isoformat()
    return u


def test_a_partial_past_the_threshold_is_reported_with_its_multiple():
    rows = mod.stuck_partials([_unit("s", days_ago=30)], {"s": "daily"}, NOW, min_periods=2)
    assert len(rows) == 1
    src, unit, age, cad, mult = rows[0]
    assert src == "s" and cad == "daily"
    assert round(age) == 30
    assert round(mult) == 30, "a daily source 30 days late is 30x its cadence"


def test_a_partial_within_the_threshold_is_not_reported():
    """Negative control - without it, 'it reported one' proves nothing."""
    assert mod.stuck_partials([_unit("s", days_ago=1)], {"s": "daily"}, NOW, min_periods=2) == []


def test_a_source_absent_from_the_registry_gets_NO_multiple():
    """The bug this file exists for: the 7-day fallback invented '12.9x its cadence' for sources
    that have no cadence at all."""
    rows = mod.stuck_partials([_unit("ghost", days_ago=90)], {"other": "daily"}, NOW,
                              min_periods=2)
    assert len(rows) == 1, "an unregistered source must still be reported - it is stuck"
    _src, _unit_id, age, cad, mult = rows[0]
    assert round(age) == 90, "the age is measured and must be kept"
    assert mult is None, "the ratio is NOT measurable without a cadence and must not be invented"
    assert cad == "NOT-IN-REGISTRY"


def test_a_unit_with_no_attempt_timestamp_is_reported_not_dropped():
    """'I cannot tell how long' is not 'it is fine'. A silently dropped row is how a sweep reports
    a clean half of the truth."""
    rows = mod.stuck_partials([_unit("s")], {"s": "daily"}, NOW, min_periods=2)
    assert len(rows) == 1
    assert rows[0][2] is None


def test_non_partial_statuses_are_ignored():
    units = [_unit("a", status="ok", days_ago=900), _unit("b", status="transient_fail", days_ago=900)]
    assert mod.stuck_partials(units, {"a": "daily", "b": "daily"}, NOW, min_periods=2) == []


def test_the_threshold_scales_with_cadence_not_with_a_fixed_number_of_days():
    """40 days is stuck for a daily source and fine for an annual one - which is the whole reason
    the raw 'wholly frozen' count of 100 was not a defect count."""
    units = [_unit("d", days_ago=40), _unit("y", days_ago=40)]
    cad = {"d": "daily", "y": "annual"}
    got = {r[0] for r in mod.stuck_partials(units, cad, NOW, min_periods=2)}
    assert got == {"d"}, f"expected only the daily source to be stuck, got {got}"
