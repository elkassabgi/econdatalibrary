"""Regression gate: a route that stops reporting must be VISIBLE, even though this gate
correctly refuses to judge the individual sources on it.

THE GAP THIS CLOSES. `gate_failures` declines to judge sources whose `run_location` is not
where the gate runs — right, because a gate must not pronounce on runs it cannot see. But
"not judged here" plus "not judged anywhere else" is NOT JUDGED, and the workstation route is
the ONLY update path for the 17 cloud-infeasible sources, `eia` among them on a DAILY cadence.

SCOPE, STATED HONESTLY. This does NOT catch the 2026-08-02 outage that prompted it. That day
the guard loop died at 15:16 and the local pass went ~7h past due, but bis, bls, cepii_gravity
and faostat still held successes from the previous day, so no three-day threshold could fire.
Short outages on a route whose pass runs every ~20h are invisible here by construction; the
instrument for those is the guard's heartbeat on the machine itself. What this catches is the
sustained case — the machine off or wedged for days, the whole route delivering nothing —
which is the version that actually rots data, and which was equally invisible before.

WHY THE ROUTE CLAIM IS LEGITIMATE WHEN THE SOURCE CLAIM IS NOT. "Is noaa healthy?" cannot be
answered from the cloud. "Has the shared state received any successful local run in three
days?" can — a statement about the ROUTE, not about any source on it.

The false-positive cases matter as much as the true one, since a gate that fires wrongly is
one nobody reads: one fresh source must clear the route, and a route that has NEVER succeeded
is unconfigured rather than silent.
"""
from __future__ import annotations
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from updater import health   # noqa: E402


def _src(sid, loc, age, live=True):
    return {"source": sid, "run_location": loc, "live": live,
            "last_success_age_d": age, "health": "OK"}


def _report(rows):
    return {"sources": rows}


def _run_as(monkeypatch, where="cloud"):
    monkeypatch.setattr(health, "execution_location", lambda: where)


def test_silent_route_is_reported(monkeypatch):
    _run_as(monkeypatch)
    rep = _report([
        _src("ecb", "cloud", 0.2),
        _src("noaa", "local", 11.0),
        _src("eia", "local", None),        # never succeeded
        _src("wid", "local", 9.5),
    ])
    out = health.route_silence(rep)
    assert len(out) == 1, out
    assert "ROUTE 'local' SILENT" in out[0]
    assert "3 live source(s)" in out[0]
    assert "9.5d ago" in out[0], "must report the NEWEST success, not the oldest"


def test_route_that_never_succeeded_is_unconfigured_not_silent(monkeypatch):
    # Every source None = the route was never wired up. Reporting it would make the gate
    # permanently red from day one, which is how a gate stops being read.
    _run_as(monkeypatch)
    rep = _report([_src("noaa", "local", None), _src("eia", "local", None)])
    assert health.route_silence(rep) == []


def test_one_fresh_source_clears_the_route(monkeypatch):
    # The route is delivering; a single stale source on it is a SOURCE problem, judged where
    # it runs. Tripping here would make this a second cry-wolf gate.
    _run_as(monkeypatch)
    rep = _report([
        _src("noaa", "local", 11.0),
        _src("eia", "local", 0.5),         # this machine is clearly alive
        _src("wid", "local", None),
    ])
    assert health.route_silence(rep) == []


def test_gate_never_reports_on_its_own_route(monkeypatch):
    # Cloud sources are judged source-by-source by gate_failures; counting them here too
    # would double-report and could fail a run for something already covered.
    _run_as(monkeypatch)
    rep = _report([_src("ecb", "cloud", 40.0), _src("bis", "cloud", 40.0)])
    assert health.route_silence(rep) == []


def test_non_live_sources_do_not_create_or_clear_a_route(monkeypatch):
    _run_as(monkeypatch)
    # A fresh NON-live source must not vouch for the route...
    rep = _report([_src("noaa", "local", 11.0), _src("scratch", "local", 0.1, live=False)])
    assert len(health.route_silence(rep)) == 1
    # ...and a route made only of non-live sources is not a route worth reporting.
    assert health.route_silence(_report([_src("scratch", "local", 99.0, live=False)])) == []


def test_local_invocation_judges_the_cloud_route(monkeypatch):
    # Symmetric by construction: run from the workstation, the CLOUD becomes the other route.
    _run_as(monkeypatch, "local")
    rep = _report([_src("ecb", "cloud", 12.0), _src("noaa", "local", 0.1)])
    out = health.route_silence(rep)
    assert len(out) == 1 and "ROUTE 'cloud' SILENT" in out[0]
