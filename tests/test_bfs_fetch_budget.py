"""bfs stops STARTING tables when its wall-clock fetch budget is spent, defers the rest, still
merges what it fetched, and saves the rotation bookmark.

WHY. Nothing bounded a bfs pass except the orchestrator's 45-minute unit timeout, and bfs merges
once, after the loop, so a killed pass merged nothing. The daily runs of 2026-11-19, 11-20 and
11-21 were each killed at 2,700 s (pass durations had grown from ~1,100-1,400 s to 1,789 s on
11-12) and nothing stored after 11-12. The fix is the Deadline every other PxWeb fetcher has,
sized from the unit's own (possibly clamped) window minus a margin (review R1989).

No request leaves the machine: the session is the fake from test_bfs_retry_after, and the
Deadline is a fake whose clock is the number of times spent() was asked. The loop asks it twice
per table that gets as far as its data POST: once before the metadata GET, once before the POST.
"""
from __future__ import annotations

import pytest

from updater import health
from updater.strategies.fetchers import _melodi_state
from updater.strategies.fetchers import bfs as B
from tests.test_bfs_retry_after import TABLES, _PassSess, pass_env, slept  # noqa: F401  (fixtures)

NEVER = 10 ** 6


class _FakeDeadline:
    """spent() turns True on its (after+1)-th call."""

    def __init__(self, after):
        self.after, self.calls, self.minutes = after, 0, None

    def __call__(self, minutes):          # used as the Deadline class
        self.minutes = minutes
        return self

    def spent(self):
        self.calls += 1
        return self.calls > self.after

    def elapsed_min(self):
        return float(self.minutes or 0)


@pytest.fixture
def window45(monkeypatch):
    monkeypatch.delenv("BFS_BUDGET_MIN", raising=False)
    monkeypatch.setattr(_melodi_state, "unit_window_s", lambda: (45 * 60.0, "test"))


def test_a_spent_budget_defers_the_rest_merges_and_rotates(monkeypatch, pass_env, window45):
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=6))          # 3 tables, 2 checks each
    s = _PassSess(TABLES, [], {})
    res = pass_env(s)
    assert s.meta_order == ["T0", "T1", "T2"], s.meta_order              # nothing started after
    assert pass_env.merged == [["BFS:T0:x=1", "BFS:T1:x=1", "BFS:T2:x=1"]]
    assert res.status == "partial"
    assert res.error.startswith("3 sub-unit(s) attempted, none failed; 7 deferred by budget"), res.error
    assert b'"after": "T2"' in pass_env.store["_rotation.json"]


def test_a_budget_spent_during_the_metadata_get_skips_the_post(monkeypatch, pass_env, window45):
    """The deadline passes while T2's metadata GET runs: T2's POST is not sent, T2 is deferred
    like the rest, and the bookmark stays on T1 so the next pass starts at T2."""
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=5))
    s = _PassSess(TABLES, [], {})
    res = pass_env(s)
    assert s.meta_order == ["T0", "T1", "T2"] and s.post_order == ["T0", "T1"], (s.meta_order, s.post_order)
    assert pass_env.merged == [["BFS:T0:x=1", "BFS:T1:x=1"]]
    assert res.error.startswith("2 sub-unit(s) attempted, none failed; 8 deferred by budget"), res.error
    assert b'"after": "T1"' in pass_env.store["_rotation.json"]


def test_the_note_reads_rotating_not_attention_in_the_health_gate(monkeypatch, pass_env, window45):
    """The note must match the gate's own matcher, so a slow pass is ROTATING, not a gate failure."""
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=4))
    res = pass_env(_PassSess(TABLES, [], {}))
    assert health._DEFERRAL_BASE.match(res.error), res.error


def test_the_next_pass_starts_after_the_last_table_started(monkeypatch, pass_env, window45):
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=6))
    pass_env(_PassSess(TABLES, [], {}))
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=NEVER))
    s = _PassSess(TABLES, [], {})
    res = pass_env(s)
    assert s.meta_order == TABLES[3:] + TABLES[:3], s.meta_order
    assert res.status == "ok"


def test_negative_control_an_unspent_budget_changes_nothing(monkeypatch, pass_env, window45):
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=NEVER))
    s = _PassSess(TABLES, [], {})
    res = pass_env(s)
    assert s.meta_order == TABLES and s.post_order == TABLES
    assert res.status == "ok" and "deferred" not in (res.error or "")
    assert "_rotation.json" not in pass_env.store                         # no stop, no bookmark


def test_a_retry_after_stop_still_wins_over_the_budget(monkeypatch, pass_env, window45):
    """A Retry-After stop keeps its own transient label; the budget does not relabel it."""
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=NEVER))
    s = _PassSess(TABLES, TABLES[2:], {"Retry-After": "3600"})
    res = pass_env(s)
    assert "Retry-After 3600 s" in res.error and "7 later table(s) deferred" in res.error


def test_the_deadline_is_built_from_the_budget(monkeypatch, pass_env, window45):
    fake = _FakeDeadline(after=NEVER)
    monkeypatch.setattr(B, "Deadline", fake)
    monkeypatch.setenv("BFS_BUDGET_MIN", "12")
    pass_env(_PassSess(TABLES[:2], [], {}))
    assert fake.minutes == 12.0


@pytest.mark.parametrize("window_min,env,want", [
    (45, None, 30.0),          # the usual window: 45 - 15
    (45, "10", 10.0),          # the env var may lower it
    (45, "60", 30.0),          # ... never raise it past the window
    (45, "", 30.0), (45, "garbage", 30.0), (45, "0", 30.0), (45, "-3", 30.0),
    (30, None, 15.0),          # a clamped late window
    (12, None, 5.0),           # floor
])
def test_budget_min_follows_the_unit_window(monkeypatch, window_min, env, want):
    monkeypatch.setattr(_melodi_state, "unit_window_s", lambda: (window_min * 60.0, "test"))
    if env is None:
        monkeypatch.delenv("BFS_BUDGET_MIN", raising=False)
    else:
        monkeypatch.setenv("BFS_BUDGET_MIN", env)
    assert B._budget_min() == want


def test_a_broken_window_reader_falls_back_to_45(monkeypatch):
    def boom():
        raise RuntimeError("no orchestrator")
    monkeypatch.setattr(_melodi_state, "unit_window_s", boom)
    monkeypatch.delenv("BFS_BUDGET_MIN", raising=False)
    assert B._budget_min() == 30.0


def test_the_margin_covers_the_worst_get_in_flight_and_the_merge():
    """After the last deadline check a metadata GET can still run MAX_ATTEMPTS x TIMEOUT plus its
    back-off (~6.1 min), Retry-After sleeps inside it (~3 min) and then the merge (<2 min on 11-12)."""
    worst_get_min = (B.MAX_ATTEMPTS * B.TIMEOUT + sum(min(2 ** a, 20) for a in range(B.MAX_ATTEMPTS))) / 60
    assert B.BUDGET_MARGIN_MIN >= worst_get_min + 3 + 2
