"""bfs stops STARTING tables when its wall-clock fetch budget is spent, defers the rest, still
merges what it fetched, and saves the rotation bookmark.

WHY. Nothing bounded a bfs pass except the orchestrator's 45-minute unit timeout, and bfs merges
once, after the loop, so a killed pass merged nothing. The daily runs of 2026-11-19, 11-20 and
11-21 were each killed at 2,700 s (pass durations had grown from ~1,100-1,400 s to 1,789 s on
11-12) and nothing stored after 11-12. The fix is the Deadline every other PxWeb fetcher has.

No request leaves the machine: the session is the fake from test_bfs_retry_after, and the
Deadline is a fake whose clock is the number of times spent() was asked.
"""
from __future__ import annotations

import pytest

from updater import health
from updater.strategies.fetchers import bfs as B
from tests.test_bfs_retry_after import TABLES, _PassSess, pass_env, slept  # noqa: F401  (fixtures)


class _FakeDeadline:
    """spent() turns True on its (after+1)-th call: `after` tables get started."""

    def __init__(self, after):
        self.after, self.calls = after, 0

    def __call__(self, minutes):          # used as the Deadline class
        self.minutes = minutes
        return self

    def spent(self):
        self.calls += 1
        return self.calls > self.after

    def elapsed_min(self):
        return float(self.minutes)


def test_a_spent_budget_defers_the_rest_merges_and_rotates(monkeypatch, pass_env):
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=3))
    s = _PassSess(TABLES, [], {})
    res = pass_env(s)
    assert s.meta_order == ["T0", "T1", "T2"], s.meta_order              # nothing started after
    assert pass_env.merged == [["BFS:T0:x=1", "BFS:T1:x=1", "BFS:T2:x=1"]]
    assert res.status == "partial"
    assert res.error.startswith("3 sub-unit(s) attempted, none failed; 7 deferred by budget"), res.error
    assert b'"after": "T2"' in pass_env.store["_rotation.json"]


def test_the_note_reads_rotating_not_attention_in_the_health_gate(monkeypatch, pass_env):
    """The note must match the gate's own matcher, so a slow pass is ROTATING, not a gate failure."""
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=4))
    res = pass_env(_PassSess(TABLES, [], {}))
    assert health._DEFERRAL_BASE.match(res.error), res.error


def test_the_next_pass_starts_after_the_last_table_started(monkeypatch, pass_env):
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=3))
    pass_env(_PassSess(TABLES, [], {}))
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=10 ** 6))
    s = _PassSess(TABLES, [], {})
    res = pass_env(s)
    assert s.meta_order == TABLES[3:] + TABLES[:3], s.meta_order
    assert res.status == "ok"


def test_negative_control_an_unspent_budget_changes_nothing(monkeypatch, pass_env):
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=10 ** 6))
    s = _PassSess(TABLES, [], {})
    res = pass_env(s)
    assert s.meta_order == TABLES
    assert res.status == "ok" and "deferred" not in (res.error or "")
    assert "_rotation.json" not in pass_env.store                         # no stop, no bookmark


def test_a_retry_after_stop_still_wins_over_the_budget(monkeypatch, pass_env):
    """A Retry-After stop keeps its own transient label; the budget does not relabel it."""
    monkeypatch.setattr(B, "Deadline", _FakeDeadline(after=10 ** 6))
    s = _PassSess(TABLES, TABLES[2:], {"Retry-After": "3600"})
    res = pass_env(s)
    assert "Retry-After 3600 s" in res.error and "7 later table(s) deferred" in res.error


def test_the_real_deadline_is_built_from_the_budget(monkeypatch, pass_env):
    fake = _FakeDeadline(after=10 ** 6)
    monkeypatch.setattr(B, "Deadline", fake)
    monkeypatch.setenv("BFS_BUDGET_MIN", "12")
    pass_env(_PassSess(TABLES[:2], [], {}))
    assert fake.minutes == 12.0


@pytest.mark.parametrize("env,want", [(None, 25.0), ("10", 10.0), ("7.5", 7.5),
                                      ("", 25.0), ("garbage", 25.0), ("0", 25.0), ("-3", 25.0)])
def test_budget_min_parsing(monkeypatch, env, want):
    if env is None:
        monkeypatch.delenv("BFS_BUDGET_MIN", raising=False)
    else:
        monkeypatch.setenv("BFS_BUDGET_MIN", env)
    assert B._budget_min() == want


def test_the_default_budget_leaves_room_under_the_unit_timeout():
    """25 of 45 minutes: the merge of the one ~5.4M-row store file and the orchestrator's own work
    need the rest (the last complete pass, 2026-11-12, took 1,789 s in total)."""
    assert B.BUDGET_MIN_DEFAULT <= 45 - 15
