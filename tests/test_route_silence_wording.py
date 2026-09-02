"""A route that is RUNNING and a route that has STOPPED must not read the same (R629).

"NOT ONE has succeeded within 3d" is true of both, and on its own it is misleading: a
multi-unit source is permanently `partial` by design and `partial` never sets
last_success_utc, so a machine attempting every night reads exactly like one that is switched
off. That sentence is what sent a whole investigation down the wrong path - it concluded the
local machine had stopped, when the machine was running and the sources were not reaching a
state that records success.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import updater.health as H  # noqa: E402

ROWS = [
    {"source": "eia", "live": True, "run_location": "local",
     "last_success_age_d": None, "last_attempt_age_d": 0.4},
    {"source": "noaa", "live": True, "run_location": "local",
     "last_success_age_d": 31.8, "last_attempt_age_d": 0.4},
]


def _lines(rows, monkeypatch):
    monkeypatch.setattr(H, "execution_location", lambda: "cloud")
    return H.route_silence({"sources": rows})


def test_a_running_route_is_not_reported_as_a_stopped_machine(monkeypatch):
    out = _lines(ROWS, monkeypatch)
    assert len(out) == 1, out
    line = out[0]
    assert "WERE ATTEMPTED" in line and "the route is running" in line, line
    assert "guard loop" not in line, line          # do not send a reader to the wrong place


def test_a_stopped_route_still_says_check_the_machine(monkeypatch):
    rows = [dict(r, last_attempt_age_d=12.0) for r in ROWS]
    out = _lines(rows, monkeypatch)
    assert len(out) == 1, out
    line = out[0]
    assert "NONE of them was even attempted" in line and "guard loop" in line, line


def test_one_fresh_success_still_clears_the_route(monkeypatch):
    rows = [dict(ROWS[0], last_success_age_d=0.5), ROWS[1]]
    assert _lines(rows, monkeypatch) == []


def test_a_route_that_has_never_succeeded_is_unconfigured_not_silent(monkeypatch):
    rows = [dict(r, last_success_age_d=None) for r in ROWS]
    assert _lines(rows, monkeypatch) == []
