"""abs: `ok` once every flow was attempted this cycle, not `partial` on every budget-stopped pass.

Daily run 35783253243: "118 sub-unit(s) attempted, none failed; 1104 deferred by budget" - every
pass read partial, so abs sat in the daily gate's failure list with nothing failing. The real
update() runs; the ABS calls and the clock are faked, the flow parquets and the cycle file are real.
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import abs as A  # noqa: E402

FLOWS = ("AAA", "BBB", "CCC")


class _Deadline:
    def __init__(self, allow):
        self.allow, self.asked, self.budget_min = allow, 0, 35

    def spent(self):
        self.asked += 1
        return self.asked > self.allow

    def elapsed_min(self):
        return 35.0


def _wire(monkeypatch, tmp_path, allow):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(A.config, "source_dir", lambda source: str(tmp_path))
    for f in FLOWS:
        p = tmp_path / f"{f}.parquet"
        if not p.exists():
            pq.write_table(pa.table({"series_key": [f"{f}.k"], "obs_date": pa.array([dt.date(2026, 1, 1)]),
                                     "value": [1.0]}), p)
    monkeypatch.setattr(A, "Deadline", lambda minutes=None: _Deadline(allow))
    monkeypatch.setattr(A.ing, "session", lambda: None)
    asked = []

    def _collect(sess, flow, key, params=None):
        asked.append(flow)
        return [f"{flow}.k"], [dt.date(2026, 8, 1)], [2.0]
    monkeypatch.setattr(A.ing, "collect", _collect)
    return asked


def test_abs_reads_ok_when_the_cycle_completes_and_starts_a_new_one_after(monkeypatch, tmp_path):
    runs = []
    for _ in range(4):
        asked = _wire(monkeypatch, tmp_path, allow=1)
        res = A.update(None, None)
        runs.append((res.status, asked))
    assert [r[0] != "partial" for r in runs] == [False, False, True, False], runs
    assert [r[1] for r in runs] == [["AAA"], ["BBB"], ["CCC"], ["AAA"]], runs


def test_only_flows_not_yet_attempted_this_cycle_are_booked(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=1)
    A.update(None, None)                                   # AAA
    _wire(monkeypatch, tmp_path, allow=1)
    res = A.update(None, None)                             # BBB; CCC owed, AAA is not
    assert "CCC deferred" in res.error and "AAA deferred" not in res.error, res.error


def test_negative_control_one_pass_that_reaches_every_flow_is_not_partial(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=99)
    assert A.update(None, None).status != "partial"
