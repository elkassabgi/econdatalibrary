"""unsdg: `ok` means a complete rotation cycle over every listed series code (the shared RotationCycle, #66).

Before this every pass deferred ~550-670 of the 713 codes to its budget and read `partial`, so unsdg never
succeeded and, as a never-succeeded unit, was due only once per ~6.5 days. Hermetic: the SDG API is faked;
the store is a tmp dir under the LOCAL backend and the merge is the real merge.merge_and_write.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater.strategies.fetchers import unsdg as U  # noqa: E402

CODES = ["A1", "B2", "C3", "D4", "E5"]


def _unit(budget):
    return types.SimpleNamespace(config={"max_series": budget})


@pytest.fixture
def world(tmp_path, monkeypatch):
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)
    monkeypatch.setattr(U.config, "source_dir", lambda s: str(tmp_path / s))
    monkeypatch.setattr(U, "RATE", 0)
    monkeypatch.setattr(U, "_series_list", lambda: ([{"code": c, "release": "2026.Q2"} for c in CODES], "ok"))
    fetched = []
    state = {"transient": set()}

    def fetch(code):
        fetched.append(code)
        if code in state["transient"]:
            return [], [], [], "transient"
        return [f"{code}:4"], [dt.date(2025, 12, 31)], [1.0], "ok"
    monkeypatch.setattr(U, "_fetch_series", fetch)
    return types.SimpleNamespace(dir=tmp_path / "unsdg", fetched=fetched, state=state)


def _visited(world):
    return set(json.loads((world.dir / "_cycle.json").read_text())["visited"])


def test_ok_only_when_the_cycle_is_complete_and_visited_codes_are_skipped(world):
    r1 = U.update(_unit(2), None)
    assert r1.status == "partial" and world.fetched == ["A1", "B2"], (r1.status, world.fetched)
    r2 = U.update(_unit(2), None)
    assert r2.status == "partial" and world.fetched[2:] == ["C3", "D4"], world.fetched
    r3 = U.update(_unit(2), None)
    assert r3.status == "ok" and world.fetched[4:] == ["E5"], (r3.status, r3.error, world.fetched)
    assert _visited(world) == set(), "the completing pass resets the cycle"
    r4 = U.update(_unit(2), None)                         # a fresh cycle is owed again
    assert r4.status == "partial" and len(world.fetched) == 7


def test_a_budget_cut_pass_books_the_unvisited_codes_as_deferred(world):
    r = U.update(_unit(2), None)
    assert "3 deferred" in (r.error or ""), r.error
    assert _visited(world) == {"A1", "B2"}


def test_a_transient_code_stays_owed_and_is_not_also_counted_deferred(world, monkeypatch):
    world.state["transient"] = {"B2"}
    tallies = []

    class CountingTally(U.Tally):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            tallies.append(self)
    monkeypatch.setattr(U, "Tally", CountingTally)
    r = U.update(_unit(5), None)
    assert r.status == "partial" and "B2" not in _visited(world)
    assert _visited(world) == {"A1", "C3", "D4", "E5"}
    # the only unvisited code is the one that failed: tallied transient, NOT also deferred (finalize's
    # message names only the transient, so the count is read from the tally itself)
    assert tallies[-1].transient == 1 and tallies[-1].deferred == 0, (tallies[-1].transient,
                                                                        tallies[-1].deferred)
    world.state["transient"] = set()
    r2 = U.update(_unit(5), None)
    assert r2.status == "ok" and world.fetched[-1] == "B2" and world.fetched.count("A1") == 1


def test_a_code_is_visited_only_after_its_chunk_is_merged(world, monkeypatch):
    """A kill between the fetch and the chunk's merge must leave the code owed, not done."""
    class Kill(BaseException):
        pass

    def die(*a, **k):
        raise Kill()
    monkeypatch.setattr(U.merge, "merge_and_write", die)
    with pytest.raises(Kill):
        U.update(_unit(5), None)
    raw = (world.dir / "_cycle.json")
    assert not raw.exists() or _visited(world) == set(), "fetched but never merged: nothing is visited"


def test_the_data_clock_is_annual_and_measured():
    """data_cadence was 'quarterly' (UNSD's release rhythm) on data dated only 31 December."""
    import yaml
    reg = yaml.safe_load(open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8"))
    e = next(s for s in reg["sources"] if s["source_id"] == "unsdg")
    assert e["data_cadence"] == "annual"
    src = open(os.path.join(ROOT, "updater", "registry.yaml"), encoding="utf-8").read()
    block = src[src.index("- source_id: unsdg"):]
    assert "MEASURED 2026-09-29" in block[:block.index("data_cadence:")], "health.py requires the measurement"
