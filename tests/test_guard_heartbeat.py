"""Regression gate: a dead workstation watchdog must redden a run, and an ABSENT beat must
never read as healthy.

WHAT THIS PINS. The 17 run_location=local sources update only from the workstation. The cloud
health gate deliberately declines to judge them, so its silence says nothing — and
health.route_silence, which does judge the route, states in its own docstring that it cannot
catch a short outage (the sources still carry yesterday's successes, so no three-day threshold
fires). On 2026-08-02 the guard loop died at 15:16 and the local heavy pass went ~7h past due
with nothing anywhere reporting it.

THE FAILURE MODE MOST WORTH A TEST is not "stale" — it is ABSENT. A checker that treats a
missing object as a pass converts "the instrument was never installed" into "everything is
fine", which is precisely the silence this whole mechanism exists to end. So the absent case
asserts a non-zero exit, not merely a message.

AGE IS READ FROM THE CONTENT, never from R2's LastModified: LastModified describes when a PUT
was accepted, so re-uploading a stale body would look perfectly fresh. test_age_comes_from_the
_body_not_the_object pins that by handing back an object whose body is old.
"""
from __future__ import annotations
import datetime as dt
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import tools.guard_heartbeat as gh                            # noqa: E402


class _Body:
    def __init__(self, raw): self._raw = raw
    def read(self): return self._raw


class _FakeClient:
    """Stands in for the R2 client. `raw` None means the object does not exist."""
    def __init__(self, raw, boom=False):
        self.raw, self.boom = raw, boom

    def get_object(self, Bucket=None, Key=None):             # noqa: N803
        if self.boom or self.raw is None:
            raise RuntimeError("NoSuchKey")
        return {"Body": _Body(self.raw)}


def _beat(minutes_ago: float, jobs=None, tracked=None) -> bytes:
    ts = dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=minutes_ago)
    return json.dumps({
        "utc": ts.isoformat(),
        "host": "TESTHOST",
        "jobs_alive": ["ingest_cbs_nl.py"] if jobs is None else jobs,
        "tracked": ["ingest_cbs_nl.py"] if tracked is None else tracked,
    }).encode("utf-8")


def _patch(monkeypatch, raw, boom=False):
    monkeypatch.setattr(gh.r2_util, "client", lambda write=False: _FakeClient(raw, boom))


def test_fresh_beat_passes(monkeypatch):
    _patch(monkeypatch, _beat(2.0))
    assert gh.check(gh.DEFAULT_MAX_AGE_MIN) == 0


def test_stale_beat_fails(monkeypatch, capsys):
    _patch(monkeypatch, _beat(120.0))
    assert gh.check(gh.DEFAULT_MAX_AGE_MIN) == 1
    out = capsys.readouterr().out
    assert "STALE" in out
    # The message must say what it COSTS, or a reader triages it as noise.
    assert "run_location=local" in out or "no other update path" in out.lower()


def test_absent_beat_FAILS_and_says_uninstrumented(monkeypatch, capsys):
    """The one that matters most: missing != healthy."""
    _patch(monkeypatch, None)
    assert gh.check(gh.DEFAULT_MAX_AGE_MIN) == 1, (
        "an absent heartbeat read as a pass would recreate the exact silence this exists "
        "to end")
    out = capsys.readouterr().out
    assert "ABSENT" in out
    assert "not proven healthy" in out


def test_unreadable_beat_fails(monkeypatch):
    _patch(monkeypatch, b"<html>404</html>")
    assert gh.check(gh.DEFAULT_MAX_AGE_MIN) == 1


def test_naive_timestamp_is_treated_as_utc_not_local(monkeypatch):
    """A beat written without an offset must not be read as local time — on this machine that
    is a 5-6 hour skew, enough to turn a fresh beat stale or a dead loop live."""
    ts = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=3)).replace(tzinfo=None)
    raw = json.dumps({"utc": ts.isoformat(), "host": "H",
                      "jobs_alive": [], "tracked": []}).encode()
    _patch(monkeypatch, raw)
    assert gh.check(gh.DEFAULT_MAX_AGE_MIN) == 0


def test_age_comes_from_the_body_not_the_object(monkeypatch, capsys):
    """The fake client carries no LastModified at all. If check() ever starts trusting object
    metadata instead of the stamped instant, this stops even being answerable — which is the
    point: a re-uploaded stale body must not look fresh."""
    _patch(monkeypatch, _beat(90.0))
    assert gh.check(gh.DEFAULT_MAX_AGE_MIN) == 1
    assert "90." in capsys.readouterr().out


def test_alive_watchdog_with_dead_crawlers_is_reported_but_not_failed(monkeypatch, capsys):
    """A crawler that FINISHED is legitimately absent, so this cannot be a failure — but it
    must not be silent either, or 'the watchdog is up' hides 'and it is doing nothing'."""
    _patch(monkeypatch, _beat(1.0, jobs=[],
                              tracked=["ingest_cbs_nl.py", "ingest_gus_dbw.py"]))
    assert gh.check(gh.DEFAULT_MAX_AGE_MIN) == 0
    out = capsys.readouterr().out
    assert "NOTE" in out and "not running" in out
    assert "cannot distinguish" in out


@pytest.mark.parametrize("minutes,expected", [(0.0, 0), (44.0, 0), (46.0, 1), (600.0, 1)])
def test_threshold_boundary(monkeypatch, minutes, expected):
    """45 min tolerates several missed 5-minute ticks (a guard call that overruns is killed at
    120s and the loop continues) while still catching a dead watchdog long before the local
    heavy pass is due again."""
    _patch(monkeypatch, _beat(minutes))
    assert gh.check(gh.DEFAULT_MAX_AGE_MIN) == expected
