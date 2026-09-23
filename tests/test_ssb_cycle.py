"""ssb: `ok` once a whole rotation cycle is worked, not `partial` on every budget-stopped pass.

Before (2026-09-19 run note): "979 sub-unit(s) attempted, none failed; 168 deferred by budget" -
every pass the budget stopped read partial, so ssb had NEVER succeeded and sat in the daily health
gate's failure list permanently. The real update() runs; the catalogue, the PxWeb calls and the clock
are faked, the group parquets and the cycle file are real (local backend, tmp dir).
"""
from __future__ import annotations

import datetime as dt
import os
import sys

import pytest

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from updater.strategies.fetchers import ssb  # noqa: E402


class _Deadline:
    """spent() is asked once per group and once per table; this lets `allow` asks through."""
    def __init__(self, allow):
        self.allow, self.asked = allow, 0

    def spent(self):
        self.asked += 1
        return self.asked > self.allow

    def elapsed_min(self):
        return 40.0


def _wire(monkeypatch, tmp_path, allow, tables_per_group=1):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(ssb.config, "source_dir", lambda source: str(tmp_path))
    groups = ("Aa", "Bb", "Cc")
    cat = [{"id": f"{g}{i}"} for g in groups for i in range(tables_per_group)]
    monkeypatch.setattr(ssb, "_load_catalog", lambda out_dir: cat)
    monkeypatch.setattr(ssb, "_group_of", lambda tid: tid[:2])
    for g in groups:
        p = tmp_path / f"grp_{g}.parquet"
        if not p.exists():
            pq.write_table(pa.table({"series_key": ["x"], "obs_date": pa.array([dt.date(2026, 1, 1)]),
                                     "value": [1.0]}), p)
    monkeypatch.setattr(ssb, "_per_table_max", lambda path: {})
    monkeypatch.setattr(ssb, "Deadline", lambda minutes=None: _Deadline(allow))
    monkeypatch.setattr(ssb, "RATE", 0)
    asked = []

    def _meta(sess, tid):
        asked.append(tid[:2])
        return None                                   # a quiet table: empty_unit
    monkeypatch.setattr(ssb, "_get_meta", _meta)
    return asked


def test_ssb_reads_ok_when_the_cycle_completes_and_starts_a_new_one_after(monkeypatch, tmp_path):
    runs = []
    for _ in range(4):
        asked = _wire(monkeypatch, tmp_path, allow=2)             # one group (1 + 1 asks) per pass
        res = ssb.update(None, None)
        runs.append((res.status, sorted(set(asked))))
    assert [r[0] != "partial" for r in runs] == [False, False, True, False], runs
    assert [r[1] for r in runs] == [["Aa"], ["Bb"], ["Cc"], ["Aa"]], runs


def test_a_group_the_budget_cut_part_way_stays_owed_and_is_booked_once(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=2, tables_per_group=2)   # Aa: group + 1 table, then cut
    res = ssb.update(None, None)
    assert res.status == "partial"
    note = res.error or ""
    assert "Aa1: budget spent, table deferred" in note
    assert "grp_Aa.parquet: budget" not in note, "the cut group is booked by its tables, not twice"
    assert "grp_Bb.parquet" in note and "grp_Cc.parquet" in note
    cyc = ssb.RotationCycle(str(tmp_path), ["grp_Aa.parquet", "grp_Bb.parquet", "grp_Cc.parquet"])
    assert "grp_Aa.parquet" in cyc.unvisited(), "a cut group is not visited"


def test_a_group_whose_table_failed_stays_owed(monkeypatch, tmp_path):
    """stat_latvia review R1103 P2: a group whose table failed must not count as done."""
    _wire(monkeypatch, tmp_path, allow=99)

    def _meta(sess, tid):
        if tid.startswith("Bb"):
            raise ssb.TransientError("pretend SSB timed out")
        return None
    monkeypatch.setattr(ssb, "_get_meta", _meta)
    assert ssb.update(None, None).status == "partial"
    cyc = ssb.RotationCycle(str(tmp_path), ["grp_Aa.parquet", "grp_Bb.parquet", "grp_Cc.parquet"])
    assert cyc.unvisited() == ["grp_Bb.parquet"], cyc.visited


def test_the_completing_pass_does_not_walk_into_visited_groups(monkeypatch, tmp_path):
    """Review R1105 P1: with several tables per group, the pass that finished the last owed group
    walked on into a visited one, the budget stopped INSIDE it, and table deferrals made the
    completing pass partial - ssb almost never read ok."""
    _wire(monkeypatch, tmp_path, allow=6, tables_per_group=3)      # Aa (1+3), Bb top + 1 table: cut
    assert ssb.update(None, None).status == "partial"
    asked = _wire(monkeypatch, tmp_path, allow=8, tables_per_group=3)   # exactly Bb + Cc (4 + 4)
    res = ssb.update(None, None)
    assert "Aa" not in set(asked), "Aa was visited on pass 1: it must be skipped"
    assert res.status != "partial", (res.status, res.error)


def test_a_budget_stop_books_each_unvisited_group_once(monkeypatch, tmp_path):
    """Review AR-123: `break` -> `continue` would book every unvisited group again per remaining
    group; the note's deferral count pins it."""
    _wire(monkeypatch, tmp_path, allow=2)                          # Aa done, stop at Bb's top
    res = ssb.update(None, None)
    assert "2 deferred" in (res.error or ""), res.error


def test_skipped_groups_still_count_toward_the_reported_total(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=99)
    full = ssb.update(None, None).obs
    _wire(monkeypatch, tmp_path, allow=2)
    ssb.update(None, None)                                         # Aa, then a new cycle begins
    _wire(monkeypatch, tmp_path, allow=99)
    assert ssb.update(None, None).obs == full


def test_negative_control_one_pass_that_reaches_everything_is_not_partial(monkeypatch, tmp_path):
    _wire(monkeypatch, tmp_path, allow=99)
    assert ssb.update(None, None).status != "partial"
