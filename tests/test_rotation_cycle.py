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


def test_skipped_groups_still_count_toward_the_reported_total(monkeypatch, tmp_path):
    """Review AR-123 / R1105 P5: `obs` (served as obs_count) dropped the skipped groups' rows."""
    _wire(monkeypatch, tmp_path, allow=99)
    full = sl.update(None, None).obs
    _wire(monkeypatch, tmp_path, allow=1)
    sl.update(None, None)                                          # visits one group
    _wire(monkeypatch, tmp_path, allow=99)
    completing = sl.update(None, None).obs                         # skips it, does the others
    assert completing == full, (completing, full)


def test_stat_latvia_negative_control_a_full_pass_is_ok(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=99)
    assert sl.update(None, None).status == "ok"


# --------------------------------------------------------------------------- #
# quarantine: one unit that fails for good must not freeze the rest (review R1111)
# --------------------------------------------------------------------------- #
def test_a_unit_that_fails_twice_in_a_row_is_quarantined_and_the_cycle_closes_over_it(tmp_path, capsys):
    units = ["a", "b"]
    cyc = C.RotationCycle(str(tmp_path), units)
    cyc.visit("a")
    cyc.visit("b", failed=True)
    t = C.Tally()
    t.structural_unit("b/x: 200 but parsed 0")
    assert cyc.close_if_complete(t) is False, "ONE failure must not reset a cycle a retry could close"
    cyc = C.RotationCycle(str(tmp_path), units)                   # the next pass: only b is owed
    cyc.visit("b", failed=True)
    t = C.Tally()
    t.structural_unit("b/x: 200 but parsed 0")
    assert cyc.quarantined() == {"b"}
    assert cyc.close_if_complete(t) is True
    assert "closed over 1 unit(s)" in capsys.readouterr().out
    saved = json.loads((tmp_path / C.RotationCycle.FILE).read_text())
    assert saved["visited"] == [] and saved["failing"] == {"b": 2} and saved["closed_over_quarantined"] == ["b"]
    assert C.RotationCycle(str(tmp_path), units).unvisited() == units, "a is refreshed again"


def test_a_failure_outside_quarantine_still_blocks_the_close(tmp_path):
    cyc = C.RotationCycle(str(tmp_path), ["a", "b", "c"])
    cyc.failing = {"b": 5}
    cyc.visit("a", failed=True)                                   # a: first failure, not quarantined
    cyc.visit("b", failed=True)
    cyc.visit("c")
    t = C.Tally()
    t.transient_unit("a")
    t.transient_unit("b")
    assert cyc.close_if_complete(t) is False


def test_an_unowned_failure_blocks_the_close(tmp_path):
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    cyc.visit("a")
    t = C.Tally()
    t.transient_unit("source-level: catalogue fetch failed")
    assert cyc.close_if_complete(t) is False


def test_a_quarantined_unit_that_recovers_leaves_quarantine(tmp_path):
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    cyc.failing = {"a": 3}
    cyc.visit("a")
    assert cyc.quarantined() == set()
    assert json.loads((tmp_path / C.RotationCycle.FILE).read_text())["failing"] == {}


def test_a_unit_counts_one_failure_per_pass(tmp_path):
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    cyc.visit("a", failed=True)
    cyc.visit("a", failed=True)                                   # the same pass, twice
    assert cyc.failing == {"a": 1} and cyc.quarantined() == set()


def test_stat_latvia_one_group_failing_for_good_does_not_freeze_the_others(monkeypatch, tmp_path):
    """R1111 P6, on stat_latvia: EMP fails every pass. Before, passes 2 and 3 asked only EMP for ever."""
    asked = []
    for _ in range(4):
        seen = _wire(monkeypatch, tmp_path, allow=99, failing={"EMP"})
        res = sl.update(None, None)
        asked.append(sorted(set(seen)))
        assert res.status == "partial" and "EMP" in (res.error or ""), "the failure stays loud"
    # pass 2 retries only the owed EMP (one failure is not quarantine); once quarantined, every
    # full pass closes the cycle, so every other group is refreshed on every pass again
    assert asked == [["EMP", "POP", "WAG"], ["EMP"], ["EMP", "POP", "WAG"], ["EMP", "POP", "WAG"]], asked


# --------------------------------------------------------------------------- #
# in flight: a unit whose work raises or is killed still counts (review AR-127 P5)
# --------------------------------------------------------------------------- #
def test_a_unit_left_in_flight_counts_as_one_failed_attempt(tmp_path, capsys):
    cyc = C.RotationCycle(str(tmp_path), ["a", "b"])
    cyc.visit("a")
    cyc.begin("b")                                               # ... and the pass dies here
    cyc = C.RotationCycle(str(tmp_path), ["a", "b"])
    assert cyc.failing == {"b": 1} and "in flight" in capsys.readouterr().out
    saved = json.loads((tmp_path / C.RotationCycle.FILE).read_text())
    assert saved["in_flight"] is None and saved["failing"] == {"b": 1}
    C.RotationCycle(str(tmp_path), ["a", "b"])                    # read again: counted ONCE
    assert json.loads((tmp_path / C.RotationCycle.FILE).read_text())["failing"] == {"b": 1}


def test_a_unit_that_finished_is_not_in_flight(tmp_path):
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    cyc.begin("a")
    cyc.visit("a")
    assert C.RotationCycle(str(tmp_path), ["a"]).failing == {}


def test_a_unit_released_without_a_verdict_is_neither_failed_nor_visited(tmp_path):
    """A budget cut inside a unit (ssb) is not a failure: release() clears the in-flight mark."""
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    cyc.begin("a")
    cyc.release("a")
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    assert cyc.failing == {} and cyc.unvisited() == ["a"]


def test_a_pass_that_died_is_closed_at_the_next_load_once_the_rest_was_visited(tmp_path):
    cyc = C.RotationCycle(str(tmp_path), ["a", "b"])
    cyc.failing = {"b": 1}
    cyc.visit("a")
    cyc.begin("b")                                               # dies again: 2 in a row
    cyc = C.RotationCycle(str(tmp_path), ["a", "b"])
    assert cyc.quarantined() == {"b"} and cyc.unvisited() == ["a", "b"], "closed at load"
    saved = json.loads((tmp_path / C.RotationCycle.FILE).read_text())
    assert saved["completed_utc"] and saved["closed_over_quarantined"] == ["b"]


def test_a_clean_close_clears_a_stale_closed_over_list(tmp_path):
    """Review AR-127 P4."""
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    cyc.failing = {"a": 2}
    cyc.visit("a", failed=True)
    t = C.Tally()
    t.structural_unit("a")
    assert cyc.close_if_complete(t)
    cyc = C.RotationCycle(str(tmp_path), ["a"])
    cyc.visit("a")
    assert cyc.close_if_complete(C.Tally())
    assert "closed_over_quarantined" not in json.loads((tmp_path / C.RotationCycle.FILE).read_text())


def test_stat_latvia_a_group_whose_merge_raises_every_pass_does_not_freeze_the_others(monkeypatch, tmp_path):
    """AR-127 P5: a merge DefinitiveError escapes update(); the group never reached visit()."""
    real = sl.merge.merge_and_write

    def _refuse(path, tbl, **k):
        if "EMP" in os.path.basename(path):
            raise sl.DefinitiveError("pretend never-shrink refused EMP")
        return real(path, tbl, **k)
    asked = []
    for _ in range(6):
        seen = _wire(monkeypatch, tmp_path, allow=99)
        monkeypatch.setattr(sl.merge, "merge_and_write", _refuse)
        try:
            sl.update(None, None)
        except sl.DefinitiveError:
            pass
        asked.append(sorted(set(seen)))
    later = [a for a in asked[2:] if "POP" in a and "WAG" in a]
    assert later, f"POP and WAG must be refetched once EMP is quarantined: {asked}"
