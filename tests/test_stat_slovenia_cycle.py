"""stat_slovenia: `ok` once every group was worked this cycle, never on a budget-stopped pass (2026-09-23).

Its 40-minute budget stop booked nothing, so a pass that reached ~70% of the 146 groups (2026-09-12:
75 of 146 left; 09-19: 102) would read ok and wait its cadence. Two mis-flagged tables kept the source
partial and hid it (review AR-125). The real update() runs over a real store; SURS is faked.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import types

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import stat_slovenia as S  # noqa: E402

GROUPS = ("AAA", "BBB", "CCC")
ROWS = {"AAA": 1, "BBB": 2, "CCC": 4}          # distinct sizes, so a miscount shows


class _Deadline:
    def __init__(self, allow):
        self.allow, self.asked = allow, 0

    def spent(self):
        self.asked += 1
        return self.asked > self.allow

    def elapsed_min(self):
        return 40.0


def _wire(monkeypatch, tmp_path, allow, fail=()):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(S, "_out_dir", lambda: str(tmp_path))
    monkeypatch.setattr(S, "RATE", 0)
    (tmp_path / "_catalog.json").write_text(json.dumps([{"id": f"{g}01S.px"} for g in GROUPS]))
    for g in GROUPS:
        p = tmp_path / f"{g}.parquet"
        if not p.exists():
            n = ROWS[g]
            pq.write_table(pa.table({"series_key": [f"SI:{g}01S:X={i}" for i in range(n)],
                                     "obs_date": pa.array([dt.date(2026, 12, 31)] * n),
                                     "value": [1.0] * n}), p)
    monkeypatch.setattr(S, "Deadline", lambda minutes=None: _Deadline(allow))
    asked = []

    def _meta(sess, tid):
        asked.append(tid[:3])
        if tid[:3] in fail:
            raise S.TransientError("pretend SURS timed out")
        # current through the stored frontier: alive, nothing newer, no POST
        return {"title": "t", "variables": [{"code": "X", "values": ["0"]},
                                            {"code": "LETO", "values": ["2026"], "time": True}]}
    monkeypatch.setattr(S, "_get_meta", _meta)
    monkeypatch.setattr(S, "_post_query", lambda *a, **k: pytest.fail("no POST expected"))
    return asked, types.SimpleNamespace(config={}, key="stat_slovenia/_all")


def test_ok_only_on_the_pass_that_completes_the_cycle(monkeypatch, tmp_path):
    runs = []
    for _ in range(4):
        asked, unit = _wire(monkeypatch, tmp_path, allow=1)          # one group per pass
        res = S.update(unit, None)
        runs.append((res.status != "partial", asked))
    assert [r[0] for r in runs] == [False, False, True, False], runs
    assert [r[1] for r in runs] == [["AAA"], ["BBB"], ["CCC"], ["AAA"]], runs


def test_a_budget_stop_books_the_unworked_groups_deferred(monkeypatch, tmp_path):
    asked, unit = _wire(monkeypatch, tmp_path, allow=1)
    res = S.update(unit, None)
    assert res.status == "partial" and "2 deferred" in (res.error or ""), res.error
    assert "BBB: budget" in res.error and "CCC: budget" in res.error


def test_a_group_whose_table_failed_stays_owed(monkeypatch, tmp_path):
    asked, unit = _wire(monkeypatch, tmp_path, allow=99, fail=("BBB",))
    assert S.update(unit, None).status == "partial"
    assert S.RotationCycle(str(tmp_path), list(GROUPS)).unvisited() == ["BBB"]


def test_a_worked_group_is_not_fetched_again_this_cycle(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=1)
    S.update(_wire(monkeypatch, tmp_path, allow=1)[1], None)          # AAA
    asked, unit = _wire(monkeypatch, tmp_path, allow=99)
    res = S.update(unit, None)
    assert "AAA" not in asked and res.status != "partial", (asked, res.status, res.error)


def test_obs_is_the_store_total_on_every_pass(monkeypatch, tmp_path):
    stored = sum(ROWS.values())
    for allow in (0, 1, 1, 99):
        res = S.update(_wire(monkeypatch, tmp_path, allow=allow)[1], None)
        assert res.obs == stored, (allow, res.obs, stored)


def test_negative_control_one_pass_that_reaches_every_group_is_not_partial(monkeypatch, tmp_path):
    asked, unit = _wire(monkeypatch, tmp_path, allow=99)
    res = S.update(unit, None)
    assert res.status != "partial" and asked == list(GROUPS), (res.status, asked)
