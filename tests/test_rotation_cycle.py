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


def test_a_structural_failure_also_keeps_the_cycle_open(tmp_path):
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    cyc.visit("a")
    t = C.Tally()
    t.structural_unit("a/x: 200 but parsed 0")
    assert cyc.close_if_complete(t) is False


def test_a_failed_unit_is_not_visited_and_a_later_failure_unvisits(tmp_path):
    cyc = C.RotationCycle(str(tmp_path), ["a", "b"])
    cyc.visit("a", failed=True)
    assert cyc.unvisited() == ["a", "b"]
    cyc.visit("a")
    cyc.visit("a", failed=True)                                   # re-attempted and failed again
    assert "a" in C.RotationCycle(str(tmp_path), ["a", "b"]).unvisited()


def test_a_save_that_fails_is_printed_not_swallowed(tmp_path, monkeypatch, capsys):
    cyc = C.RotationCycle(str(tmp_path), ["a"])

    def _boom(path, data):
        raise OSError("disk full")
    monkeypatch.setattr(cyc._blob, "write_bytes_atomic", _boom)
    cyc.visit("a")
    assert "could not save" in capsys.readouterr().out


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


def _wire(monkeypatch, tmp_path, allow, failing=()):
    monkeypatch.setattr(sl.config, "source_dir", lambda source: str(tmp_path))
    (tmp_path / "_catalog.json").write_text(json.dumps(TABLES))
    for g in GROUPS:                                  # stat_latvia maintains groups that exist
        if not (tmp_path / g).exists():
            pq.write_table(pa.table({"series_key": ["x"], "obs_date": pa.array([dt.date(2026, 1, 1)]),
                                     "value": [1.0]}), tmp_path / g)
    monkeypatch.setattr(sl, "Deadline", lambda minutes=None: _Deadline(allow))
    seen = []

    def _q(sess, t, boundary):
        g = t["path"].split("/")[0]
        seen.append(g)
        if g in failing:
            raise sl.TransientError("pretend CSP timed out")
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


def test_a_group_whose_tables_failed_is_refetched_before_the_cycle_can_close(monkeypatch, tmp_path):
    """Review R1103, P2: EMP failed on pass 1, pass 2 fetched POP+WAG and closed the cycle as ok -
    EMP was never fetched again."""
    _wire(monkeypatch, tmp_path, allow=1, failing={"EMP"})
    assert sl.update(None, None).status == "partial"                        # EMP failed
    seen = _wire(monkeypatch, tmp_path, allow=2)
    res = sl.update(None, None)                     # POP, WAG, then the budget stops before EMP
    assert sorted(set(seen)) == ["POP", "WAG"]
    assert res.status == "partial" and "OSP_PUB_EMP" in (res.error or ""), \
        f"EMP failed and was never refetched, so the cycle must not close: {res.status} {res.error}"
    seen = _wire(monkeypatch, tmp_path, allow=99)
    assert sl.update(None, None).status == "ok" and "EMP" in seen


def test_a_subset_run_does_not_close_the_cycle(monkeypatch, tmp_path):
    """Review R1103, P1: groups `only_set` skipped were marked visited, so a 1-of-3 subset closed."""
    _wire(monkeypatch, tmp_path, allow=99)
    monkeypatch.setenv("STAT_LATVIA_ONLY_GROUPS", GROUPS[0])
    sl.update(None, None)
    cyc = C.RotationCycle(str(tmp_path), GROUPS)
    assert set(cyc.unvisited()) == set(GROUPS[1:]), cyc.visited


def test_the_bookmark_is_saved_per_group_not_only_at_the_end(monkeypatch, tmp_path):
    """Review R1103, P4 (R273): a kill mid-pass kept the cycle file but not the bookmark."""
    _wire(monkeypatch, tmp_path, allow=99)

    class _Kill(BaseException):
        pass
    real = sl._query_table_delta
    calls = {"n": 0}

    def _q(sess, t, boundary):
        calls["n"] += 1
        if calls["n"] > 2:                     # killed inside the second group
            raise _Kill()
        return real(sess, t, boundary)
    monkeypatch.setattr(sl, "_query_table_delta", _q)
    with pytest.raises(_Kill):
        sl.update(None, None)
    bm = json.loads((tmp_path / C.ROTATION_FILE).read_text())["after"]
    assert bm == GROUPS[1], f"the bookmark must name the group in flight, got {bm!r}"


def test_a_group_absent_on_disk_does_not_hold_the_cycle_open(monkeypatch, tmp_path):
    """Review AR-122 P7: OSP_OD_tautassk is catalogued with no store file; skipping its visit would
    keep every budget-cut pass partial for ever."""
    _wire(monkeypatch, tmp_path, allow=99)
    os.remove(tmp_path / GROUPS[1])                                   # POP: catalogued, not held
    runs = []
    for _ in range(3):
        _wire(monkeypatch, tmp_path, allow=1)
        os.remove(tmp_path / GROUPS[1]) if (tmp_path / GROUPS[1]).exists() else None
        runs.append(sl.update(None, None).status)
    assert "ok" in runs, f"the absent group must count as done: {runs}"


def test_a_structural_table_keeps_stat_latvia_from_ok(monkeypatch, tmp_path):
    seen = _wire(monkeypatch, tmp_path, allow=99)
    real = sl._query_table_delta

    def _q(sess, t, boundary):
        if t["path"] == "EMP/T1.px":
            seen.append("EMP")
            return [], "structural"
        return real(sess, t, boundary)
    monkeypatch.setattr(sl, "_query_table_delta", _q)
    with pytest.raises(sl.DefinitiveError):
        sl.update(None, None)
    assert "OSP_PUB_EMP.parquet" in C.RotationCycle(str(tmp_path), GROUPS).unvisited()


def test_a_pass_skips_groups_already_visited_this_cycle(monkeypatch, tmp_path):
    """Review R1105, P1: re-walking visited groups spent the budget on work already done."""
    _wire(monkeypatch, tmp_path, allow=1)
    sl.update(None, None)                                          # EMP
    seen = _wire(monkeypatch, tmp_path, allow=99)
    res = sl.update(None, None)
    assert sorted(set(seen)) == ["POP", "WAG"] and res.status == "ok", (seen, res.status)
    seen = _wire(monkeypatch, tmp_path, allow=99)
    sl.update(None, None)                                          # a new cycle: everything again
    assert sorted(set(seen)) == ["EMP", "POP", "WAG"]


def test_stat_latvia_negative_control_a_full_pass_is_ok(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=99)
    assert sl.update(None, None).status == "ok"
