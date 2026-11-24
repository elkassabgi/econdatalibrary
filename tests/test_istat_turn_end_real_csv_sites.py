"""istat's turn end at the REAL call sites of the CSV phase, and on the exits that are not window stops.

WHY (phase C review of the istat window_stop PRs, condition C1; ledger R2142, 2026-11-24). The per-turn cap bounds the
whole istat turn through Result.turn_end_ts, and the code is right. But every test of it stubbed
_derive_changed_csvs and _drain_csv_retry_queue with recorders that call _capped_derive_budget() themselves, and no
test ran the measured 10-25 outage exit. Three mutants passed every test:
  RC1  istat keeps turn_end_ts only on a window stop or an `ok`: dropped on the every-host-dead break (the 10-25
       shape: sdmx redirect-loops, esploradati TCP-dead), on the budget stop, and on ROUTE B without a stop;
  RC4  the REAL in-loop retry drain (_drain_csv_retry_queue, called with no deadline) sizes its budget from the
       run ceiling only;
  RC5  the REAL fresh derive (_derive_changed_csvs -> derive.derive_and_put) sizes its budget from the run
       ceiling only.

WHAT THESE TESTS RUN. The REAL istat.update() (fake session, fake clock; no network, no R2), the REAL run_once,
the REAL _derive_changed_csvs and the REAL _drain_csv_retry_queue against a StateStore. Only two things are
replaced: _map_changed (the catalogue lookup, which needs a catalogue) and derive.derive_and_put, by a recorder of
the budget_min it is GIVEN that spends fake time at the measured derive rate inside that budget (24 ids a minute,
logs 2026-11-11..16, the phase C probe's figure) and returns what derive.py returns (unreached ids in both `failed`
and `deferred_ids`).

The expected numbers come from the stated rule (the cap's end = turn start + 60 min; the derive budget =
min(AQUEDUCT_DERIVE_BUDGET_MIN 45, minutes left to the earlier end)), not from the functions under test.
"""
from __future__ import annotations

import os
import sys
import time

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))

import test_istat_window_stop as W  # noqa: E402

from updater import derive, orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies import fetchers  # noqa: E402
from updater.strategies.base import Result  # noqa: E402
from updater.strategies.fetchers import istat  # noqa: E402

rig = W.rig
RATE = 24.0                      # ids a minute: the measured derive rate
CAP_S = 3600.0                   # the shipped per-turn cap, 60 min
QUEUED = 7341                    # ids queued from 10-24 when the 10-25 turn ran
FRESH_PER_KEY = 2412             # 10-25 handed over 4,824 ids on its 2 cursors (896 handled + 3,928 deferred)


class _DeriveRecorder:
    """derive.derive_and_put in the tests' place: records (kind, ids given, budget_min given, fake minute of the
    call), spends len(ids) / RATE fake minutes or the whole budget if that is less, and returns derive.py's shape.
    kind: 'drain' for the planted queue ids (istat:q#...), 'fresh' for this pass's mapped ids."""

    def __init__(self, clock, t0):
        self.clock, self.t0 = clock, t0
        self.calls: list = []

    def __call__(self, ids, blob, **kw):
        ids = [str(s) for s in ids]
        b = kw.get("budget_min")
        kind = "drain" if ids and ids[0].startswith("istat:q#") else "fresh"
        self.calls.append((kind, len(ids), b, (self.clock.time() - self.t0) / 60.0))
        need = len(ids) / RATE
        if b is None or need <= b:
            self.clock.t += need * 60.0
            done = len(ids)
        else:
            self.clock.t += b * 60.0
            done = min(len(ids), int(b * RATE + 1e-9))
        rest = ids[done:]
        return {"put": done, "put_ids": ids[:done], "failed": list(rest), "deferred_ids": list(rest)}

    def of(self, kind):
        got = [c for c in self.calls if c[0] == kind]
        assert len(got) == 1, (kind, self.calls)
        return got[0]


class _Recording(W._Strat):
    """W._Strat (the REAL istat.update) that also keeps the Result it returned."""

    def __init__(self, clock, sink):
        super().__init__(clock, 0.0)
        self.sink = sink

    def run(self, unit, since=None):
        res = super().run(unit, since)
        self.sink.append(res)
        return res


def _patch_real_csv_phase(mp, tmp_path, rec, ids_per_key):
    """Leave _derive_changed_csvs and _drain_csv_retry_queue REAL; replace only the catalogue lookup and the
    derive itself."""
    mp.setattr(orchestrate.config, "STATE_DIR", str(tmp_path / "_aqueduct"))
    mp.setattr(orchestrate, "_csv_desktop_excluded", lambda sid: set())
    mp.setattr(orchestrate, "_map_changed", lambda sid, changed, merge_measured: (
        [f"{sid}:fresh#{k}#{j}" for k in sorted(str(c) for c in changed) for j in range(ids_per_key)], []))
    blob = object()
    mp.setattr(orchestrate, "_resolve_blob", lambda: blob)
    mp.setattr(derive, "derive_and_put", rec)


def _run_once_real_csv(rig, monkeypatch, budget_min, ids_per_key, queued):
    """One REAL run_once over the REAL istat registry unit on the rig's fake clock, with the REAL CSV phase.
    Returns (istat's Result, the recorder, the istat runs rows [(status, dur_s)], ids still queued)."""
    unit = W._istat_unit()
    store = StateStore(str(rig.out.parent.parent / "state.db"))
    if queued:
        store.enqueue_csv_retry("istat", [f"istat:q#{i}" for i in range(queued)], "planted: queued earlier")
    mp = monkeypatch
    for k in ("AQUEDUCT_DERIVE_BUDGET_MIN", "AQUEDUCT_ISTAT_TURN_MAX_MIN"):
        mp.delenv(k, raising=False)                  # the shipped defaults: derive budget 45, cap 60
    mp.setattr(orchestrate, "time", rig.clock)
    mp.setattr(orchestrate, "hard_timeout_available", lambda: False)
    mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    mp.setattr(registry, "validate", lambda *a, **k: [])
    mp.setattr(registry, "all_units", lambda *a, **k: [unit])
    for name, fn in (("_has_adapter", lambda u: True), ("_wrong_location", lambda u: False),
                     ("_protected", lambda u: False), ("_is_live", lambda u: True),
                     ("_served_by_lane", lambda sid: False)):
        mp.setattr(orchestrate, name, fn)
    mp.setattr(orchestrate, "_drain_leftover_retries", lambda *a, **k: None)
    sink: list = []
    mp.setattr(orchestrate, "get_strategy", lambda name: _Recording(rig.clock, sink))
    mp.setattr(fetchers, "get", lambda sid: istat)
    mp.setenv("AQUEDUCT_RUN_BUDGET_MIN", str(budget_min))
    rec = _DeriveRecorder(rig.clock, rig.clock.time())
    _patch_real_csv_phase(mp, rig.out.parent.parent, rec, ids_per_key)
    orchestrate.run_once(store=store)
    rows = [tuple(r) for r in store.db.execute(
        "SELECT status, dur_s FROM runs WHERE source_id='istat' ORDER BY id").fetchall()]
    left = sorted(r["series_id"] for r in store.csv_retries("istat") if r["series_id"].startswith("istat:q#"))
    assert len(sink) == 1, sink
    return sink[0], rec, rows, left


# ------------------------------------------------------------------ 1. the measured 10-25 outage, through run_once

def test_the_10_25_outage_turn_through_run_once_is_the_cap_not_45_plus_45(rig, monkeypatch):
    """THE MEASURED SHAPE (logs/local_heavy_updater_20261025-220113.log): sdmx redirect-loops, esploradati is
    TCP-dead, so istat walks one flow and stops on the every-host-dead break - NOT a window stop - in a 240-min
    pass. The turn's CSV phase is the REAL fresh derive (4,824 ids handed over) and the REAL retry drain (7,341
    queued). The cap's end is on the Result, so the fresh derive gets min(45, ~60) = 45 and the drain what is left
    of the cap, ~15 - not another 45. The runs row is the whole turn: 60.0 min. (The phase C probe measured 90.0
    min on 150263427, the head before the turn end; mutant RC1 makes this test fail.)"""
    rig.flows(5)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: base == W.SDMX)
    host = W._Istat(rig.clock, 1.0, False, "redirect")
    rig.mp.setattr(istat.requests, "Session", lambda: host)
    t0 = rig.clock.time()
    res, rec, rows, left = _run_once_real_csv(rig, monkeypatch, 240, FRESH_PER_KEY, QUEUED)
    # the exit is the outage break, not a window stop (the exit RC1 dropped the turn end on)
    assert res.status == "partial" and "every ISTAT host unusable" in (res.error or ""), res.error
    assert res.window_stop is None, res.window_stop
    assert res.turn_end_ts == pytest.approx(t0 + CAP_S, abs=1.0), (res.turn_end_ts, t0)
    kind, n, fresh_b, fresh_at = rec.of("fresh")
    assert n >= FRESH_PER_KEY and fresh_at < 1.0, rec.calls             # the fetch took seconds
    assert fresh_b == pytest.approx(45.0), rec.calls                    # min(45, ~60 left of the cap)
    kind, n, drain_b, drain_at = rec.of("drain")
    assert n == QUEUED, rec.calls
    assert drain_at == pytest.approx(fresh_at + 45.0, abs=0.01), rec.calls
    assert drain_b == pytest.approx(60.0 - drain_at, abs=0.02) and drain_b < 16.0, rec.calls
    assert len(left) == QUEUED - int(drain_b * RATE + 1e-9), len(left)
    assert [s for s, _d in rows] == ["partial"], rows
    assert 3590.0 <= rows[0][1] <= 3610.0, rows                       # 60.0 min, not 90
    assert orchestrate._UNIT_TURN_END_TS is None


# ------------------------------------------------------------------ 2. a capped turn: both REAL sites get the cap's rest

def test_a_capped_turn_through_run_once_gives_the_real_fresh_derive_and_drain_what_is_left_of_the_cap(rig,
                                                                                                   monkeypatch):
    """Cap 60, 60 flows of 61 s in a 240-min pass: the cap ends the fetch ~40.7 min in. The REAL fresh derive (1 id
    a flow) is given what is left of the cap, ~19.3 min - not min(45, ~199) = 45 (mutant RC5) - and spends ~1.7.
    The REAL retry drain (7,341 queued) is given what is left after that - not 45 (mutant RC4) - and spends it, so
    the runs row is 60.0 min."""
    rig.flows(60)
    host = W._Istat(rig.clock, 60.0, False, "serve")
    rig.mp.setattr(istat.requests, "Session", lambda: host)
    t0 = rig.clock.time()
    res, rec, rows, left = _run_once_real_csv(rig, monkeypatch, 240, 1, QUEUED)
    assert res.turn_end_ts == pytest.approx(t0 + CAP_S, abs=1.0), (res.turn_end_ts, t0)
    kind, n, fresh_b, fresh_at = rec.of("fresh")
    assert 30.0 < fresh_at < 45.0, rec.calls                            # the cap ended the fetch
    assert fresh_b == pytest.approx(60.0 - fresh_at, abs=0.02) and fresh_b < 30.0, rec.calls
    kind, n, drain_b, drain_at = rec.of("drain")
    assert drain_at > fresh_at and n == QUEUED, rec.calls
    assert drain_b == pytest.approx(60.0 - drain_at, abs=0.02) and drain_b < 30.0, rec.calls
    assert [s for s, _d in rows] == ["partial"], rows
    assert 3590.0 <= rows[0][1] <= 3610.0, rows
    assert orchestrate._UNIT_TURN_END_TS is None


# ------------------------------------------------------------------ 3. the two REAL call sites, called directly

@pytest.fixture
def sites(tmp_path, monkeypatch):
    """The REAL _derive_changed_csvs and the REAL _drain_csv_retry_queue on the REAL istat unit, against a
    StateStore, with derive_and_put recording what it is given (real clock; nothing is spent)."""
    monkeypatch.delenv("AQUEDUCT_DERIVE_BUDGET_MIN", raising=False)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    monkeypatch.setattr(orchestrate, "_UNIT_TURN_END_TS", None)
    seen: list = []

    def rec(ids, blob, **kw):
        ids = [str(s) for s in ids]
        seen.append(("drain" if ids[0].startswith("istat:q#") else "fresh", kw.get("budget_min")))
        return {"put": len(ids), "put_ids": ids, "failed": []}

    _patch_real_csv_phase(monkeypatch, tmp_path, rec, 3)
    st = StateStore(str(tmp_path / "state.db"))
    unit = W._istat_unit()

    def run(ceiling_min, turn_end_min):
        now = time.time()
        orchestrate._RUN_DEADLINE_TS = None if ceiling_min is None else now + ceiling_min * 60.0
        res = Result(status="partial", obs=1, series_cursors={"F01": "2026-01-01", "F02": "2026-01-01"},
                     turn_end_ts=None if turn_end_min is None else now + turn_end_min * 60.0)
        orchestrate._begin_unit_turn_end(res)              # what run_once does just before the CSV phase
        try:
            st.enqueue_csv_retry("istat", ["istat:q#1", "istat:q#2"], "planted")
            seen.clear()
            orchestrate._derive_changed_csvs(unit, res, None, st)
            orchestrate._drain_csv_retry_queue(unit, None, st)
        finally:
            orchestrate._end_unit_turn_end()
        return dict(seen)
    yield run
    orchestrate._end_unit_turn_end()


@pytest.mark.parametrize("ceiling_min", [200.0, None], ids=["far_run_ceiling", "no_run_ceiling"])
def test_the_real_fresh_derive_and_drain_are_given_what_is_left_of_the_turn(sites, ceiling_min):
    """A turn end 7 min ahead: the REAL fresh derive and the REAL in-loop drain are each given budget_min ~7, with a
    far run ceiling (RC4/RC5 give 45) and with none (RC4/RC5 give no budget at all)."""
    got = sites(ceiling_min, 7.0)
    assert got["fresh"] == pytest.approx(7.0, abs=0.1), got
    assert got["drain"] == pytest.approx(7.0, abs=0.1), got


@pytest.mark.parametrize("ceiling_min, turn_end_min, want", [
    (200.0, None, 45.0),                 # no turn end: min(45, the run ceiling), as before
    (200.0, 300.0, 45.0),                # a turn end after the ceiling changes nothing
    (10.0, 30.0, 10.0),                  # the ceiling first: it bounds both, as before
    (None, None, None),                  # neither: no budget passed (derive's own default)
], ids=["no_turn_end", "turn_end_after_ceiling", "ceiling_first", "neither"])
def test_negative_controls_the_recorder_sees_the_run_ceiling_when_the_turn_end_is_not_first(
        sites, ceiling_min, turn_end_min, want):
    """The recorder can tell the bounds apart: without a turn end that comes first, both REAL sites get exactly
    what they got before the turn end existed."""
    got = sites(ceiling_min, turn_end_min)
    if want is None:
        assert got == {"fresh": None, "drain": None}, got
    else:
        assert got["fresh"] == pytest.approx(want, abs=0.1), got
        assert got["drain"] == pytest.approx(want, abs=0.1), got


# ------------------------------------------------------------------ 4. istat: the turn end on every exit that is not a stop

def test_the_turn_end_is_on_the_result_on_the_every_host_dead_break(rig, capsys):
    """The 10-25 exit at the fetcher: cap 60, a far run ceiling, the outage break ends the run after one flow. No
    window stop, yet the cap's end is on the Result (RC1 dropped it here)."""
    rig.mp.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", "60")
    rig.flows(6)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: base == W.SDMX)
    t0 = rig.clock.time()
    res, _h = rig.run(delay=1.0, ceiling_s=4 * 3600.0, sdmx="redirect")
    assert "stopping after 1 flow(s) attempted" in capsys.readouterr().out
    assert res.status == "partial" and "every ISTAT host unusable" in res.error and res.window_stop is None
    assert res.turn_end_ts == pytest.approx(t0 + CAP_S), (res.turn_end_ts, t0)


def test_the_turn_end_is_on_the_result_on_the_budget_stop(rig):
    """istat's own budget (BUDGET_MIN 3, the override cleared) ends the run inside the cap: not a window stop, and
    the cap's end is still on the Result."""
    rig.mp.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", "60")
    rig.mp.delenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", raising=False)
    rig.mp.setattr(istat, "BUDGET_MIN", 3.0)
    rig.flows(10)
    t0 = rig.clock.time()
    res, host = rig.run(delay=60.0, ceiling_s=10 * 3600.0)
    assert 0 < len(host.starts) < 10 and "deferred by budget" in res.error and res.window_stop is None, res.error
    assert res.turn_end_ts == pytest.approx(t0 + CAP_S), (res.turn_end_ts, t0)


def test_the_turn_end_is_on_the_result_on_a_route_b_hand_over_without_a_stop(rig):
    """F03 structural, no run ceiling, every flow walked: ROUTE B hands over with window_stop None, and the cap's end
    is on the Result."""
    rig.mp.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", "60")
    names = rig.flows(5)
    t0 = rig.clock.time()
    res, host = W._run_plan(rig, {"F03": "structural"}, None, delay=1.0)
    assert list(host.starts) == names and "handed over" in res.error and res.window_stop is None, res.error
    assert res.turn_end_ts == pytest.approx(t0 + CAP_S), (res.turn_end_ts, t0)
