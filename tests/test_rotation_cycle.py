"""RotationCycle: `ok` = every sub-unit visited since the last ok (R303; statfin R1096, 2026-09-23).

stat_latvia's 2026-09-18 run stopped after 11 of 17 groups ("6 of 17 group(s) deferred") and
reported ok, so the 6 waited the monthly cadence. The helper, then stat_latvia end to end - the real
update(), only the network and the clock faked.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import _common as C  # noqa: E402
from updater.strategies.fetchers import stat_latvia as sl  # noqa: E402


@pytest.fixture(autouse=True)
def _local(monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)


# --------------------------------------------------------------------------- #
# the helper
# --------------------------------------------------------------------------- #
def test_the_cycle_completes_only_when_every_unit_was_visited(tmp_path):
    units = ["a", "b", "c"]
    t = C.Tally()
    cyc = C.RotationCycle(str(tmp_path), units)
    cyc.visit("a")
    assert cyc.defer_unvisited(t) == 2 and t.deferred == 2
    assert cyc.close_if_complete(C.Tally()) is False
    cyc2 = C.RotationCycle(str(tmp_path), units)                  # a later pass reads it back
    assert cyc2.unvisited() == ["b", "c"]
    cyc2.visit("b")
    cyc2.visit("c")
    assert cyc2.close_if_complete(C.Tally()) is True
    assert C.RotationCycle(str(tmp_path), units).unvisited() == units, "a closed cycle starts over"


def test_a_failed_sub_unit_keeps_the_cycle_open(tmp_path):
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    cyc.visit("a")
    t = C.Tally()
    t.transient_unit("a/x: timed out")
    assert cyc.close_if_complete(t) is False
    assert C.RotationCycle(str(tmp_path), ["a"]).unvisited() == [], "still complete, not reset"


def test_an_unreadable_or_foreign_file_owes_everything(tmp_path):
    for body in ("{not json", '"a string"', "[1, 2]", json.dumps({"visited": ["gone", "a"]})):
        (tmp_path / C.RotationCycle.FILE).write_text(body)
        cyc = C.RotationCycle(str(tmp_path), ["a", "b"])
        assert set(cyc.unvisited()) >= {"b"} and "gone" not in cyc.visited


# --------------------------------------------------------------------------- #
# stat_latvia end to end
# --------------------------------------------------------------------------- #
TABLES = [{"db": "OSP_PUB", "path": f"{g}/T{i}.px"} for g in ("EMP", "POP", "WAG") for i in (1, 2)]
GROUPS = sorted({sl._group_filename(t["db"], t["path"]) for t in TABLES})


class _Deadline:
    def __init__(self, allow):
        self.allow, self.asked = allow, 0

    def spent(self):
        self.asked += 1
        return self.asked > self.allow

    def elapsed_min(self):
        return 30.0


def _wire(monkeypatch, tmp_path, allow):
    monkeypatch.setattr(sl.config, "source_dir", lambda source: str(tmp_path))
    (tmp_path / "_catalog.json").write_text(json.dumps(TABLES))
    for g in GROUPS:                                  # stat_latvia maintains groups that exist
        if not (tmp_path / g).exists():
            pq.write_table(pa.table({"series_key": ["x"], "obs_date": pa.array([dt.date(2026, 1, 1)]),
                                     "value": [1.0]}), tmp_path / g)
    monkeypatch.setattr(sl, "Deadline", lambda minutes=None: _Deadline(allow))
    seen = []

    def _q(sess, t, boundary):
        seen.append(t["path"].split("/")[0])
        return [(t["path"] + ":k", dt.date(2026, 8, 1), 2.0)], "data"
    monkeypatch.setattr(sl, "_query_table_delta", _q)
    return seen


def test_stat_latvia_reads_partial_until_the_cycle_completes(monkeypatch, tmp_path):
    runs = []
    for _ in range(4):
        seen = _wire(monkeypatch, tmp_path, allow=1)
        res = sl.update(None, None)
        runs.append((res.status, sorted(set(seen))))
    assert [r[0] for r in runs] == ["partial", "partial", "ok", "partial"], \
        f"the 4th pass starts a NEW cycle - a completed one must reset, not stay complete: {runs}"
    assert [r[1] for r in runs] == [["EMP"], ["POP"], ["WAG"], ["EMP"]], "the rotation still advances"


def test_stat_latvia_negative_control_a_full_pass_is_ok(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=99)
    assert sl.update(None, None).status == "ok"
