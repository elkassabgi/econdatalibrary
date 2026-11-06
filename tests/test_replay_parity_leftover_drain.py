"""The starvation replay's LEFTOVER CSV DRAIN against the REAL run_once (review of the replay: item 5; PRs #212/#213).

After the unit loop run_once drains the csv_retry_queue of the sources whose every unit was NOT DUE, BUDGET SKIPPED,
an earned no_change or a declared non-turn, oldest-enqueued first, in min(time to the run deadline - 15, 45) minutes,
and the pass does not end (and push) before it is done. The replay had no drain: its passes ended at the unit loop.

THE DIFFERENTIAL. One planted state file (units, runs rows, csv_retry_queue rows); run_once runs on it with the REAL
_drain_leftover_retries, and the replay's book is loaded from the same file. Compared: the candidates and their order
(which sources drain, for how long; which are named as not reached), the pass's end, and (with a drain rate) the
queue left per source. The derive's speed is an input on both sides (production: _drain_csv_retry_queue scripted on
the virtual clock at `rate` ids/min, or using all the time it is given; replay: drain_rate), so what is compared is
the drain's SCHEDULING - candidates, order, budget, floor, accounting.

REAL: run_once, drain_candidates, _drain_leftover_retries (drain_order, leftover_drain_budget_min, the floor, the
leases, the r2 mirror gate's backend check), the earned no_change branch, the StateStore. FAKE: the registry, the
fetchers (scripted), the CSV phase of a unit that ran, the derive inside the drain (scripted, above).

PENCIL (budget 100; bands by cost: n 0 (a probe-only no_change, 0 s), a / b 1, d 2, c 3):
  n no_change (probe) -> candidate; a ok 0->5; b NOT DUE -> candidate; d ok 5->25 (ran: blocked); c 25 + 90 > 100
  BUDGET SKIP -> candidate. Drain budget min(75 - 15, 45) = 45, order c (enqueued 08-01), b (09-01), n (09-15);
  d (07-01, the oldest) is blocked.
  - no rate: c takes all 45 -> b, n not reached; the pass ends at 70.
  - 2 ids/min: c 50 ids -> 25 min, queue 0; b 100 ids, 20 min left -> 40 cleared, 60 left; n not reached; ends at 70.
  - with w (declared, 300 min, first in band 3; its fetcher's own tail and progress piece): admitted at 25, it stops
    at ITS stop point having kept nothing -> a non-turn and a candidate. Tail 8 / piece 70: stops at 92, 8 min left,
    no drain, c, b, n, w named. Tail 50 / piece 30: stops at 50, the drain gets 35 min.
"""
from __future__ import annotations

import datetime as dt
import os
import re
import sys
import types

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "tools"))

import measure_ci_lag as m  # noqa: E402
from updater import orchestrate, registry  # noqa: E402
from updater import state as state_mod  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies import fetchers  # noqa: E402
from updater.strategies.base import Result, Strategy, Unit  # noqa: E402


@pytest.fixture(autouse=True)
def _workstation_platform(monkeypatch):
    """The replay models the WORKSTATION (no per-unit hard timeout: admission_reserve(..., hard_timeout=False)), so
    run_once must see the same platform on any machine, Linux CI included (2026-11-06)."""
    monkeypatch.setattr(orchestrate, "hard_timeout_available", lambda: False)


# 2026-11-06: c's newest real run is 6,000 s, so on the workstation it reserves 100 min (admission_reserve's
# no-hard-timeout rule), not the 90-min cap. The drained amounts below do not depend on it.
C_RESERVE = 100.0

BASE = {"n": "monthly", "a": "monthly", "b": "monthly", "d": "monthly", "c": "monthly"}
QUEUE = {"n": (30, "2026-09-15"), "b": (100, "2026-09-01"), "d": (10, "2026-07-01"), "c": (50, "2026-08-01")}


class _Clock:
    def __init__(self, t):
        self.t = float(t)

    def time(self):
        return self.t

    def __getattr__(self, name):
        import time as _t
        return getattr(_t, name)


def _iso_ts(ts):
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat(timespec="seconds")


class Harness:
    def __init__(self, tmp_path, monkeypatch, rate=None, declared=None, tail=90.0):
        self.mp, self.rate, self.tail = monkeypatch, rate, tail
        self.declared = dict(declared or {})
        self.cad = dict(BASE, **{s: "monthly" for s in self.declared})
        t0 = int((dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=20)).timestamp())
        self.clock = _Clock(t0)
        self.db_path = str(tmp_path / "state.db")
        self.store = StateStore(self.db_path)
        self._plant(t0)
        self.book = m.load_unit_book(self.db_path, dt.datetime.fromtimestamp(t0, dt.timezone.utc), dt.timedelta(0),
                                     cadences=dict(self.cad), window_stop=set(self.declared))
        self.left = dict(self.book.remaining)
        units = [Unit(source_id=s, unit_id="_all", strategy="sim", cadence=c, out_paths=[],
                      config={"run_location": "local", "live": True, "window_stop": s in self.declared})
                 for s, c in self.cad.items()]
        # declared {sid: (reserve, tail, piece)}: the figures both sides read (window_stop_tail_min / _piece_min)
        mods = {s: types.SimpleNamespace(window_admission_reserve_min=lambda r=r: r,
                                         window_stop_tail_min=lambda t=t: t, window_stop_piece_min=lambda p=p: p)
                for s, (r, t, p) in self.declared.items()}
        self.mods = mods
        for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN", "AQUEDUCT_RUN_LOCATION",
                  "AQUEDUCT_BACKEND", "AQUEDUCT_RETRY_DRAIN_MARGIN_MIN", "AQUEDUCT_RETRY_DRAIN_MAX_MIN",
                  "AQUEDUCT_DERIVE_BUDGET_MIN"):
            monkeypatch.delenv(k, raising=False)
        mp = monkeypatch
        now = lambda: _iso_ts(self.clock.t)                         # noqa: E731
        mp.setattr(orchestrate, "time", self.clock)
        mp.setattr(orchestrate, "now_utc", now)
        mp.setattr(state_mod, "now_utc", now)
        mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
        mp.setattr(registry, "validate", lambda *a, **k: [])
        mp.setattr(registry, "all_units", lambda *a, **k: list(units))
        mp.setattr(fetchers, "get", lambda sid: mods.get(sid))
        mp.setattr(orchestrate, "_has_adapter", lambda u: True)
        mp.setattr(orchestrate, "_wrong_location", lambda u: False)
        mp.setattr(orchestrate, "_protected", lambda u: False)
        mp.setattr(orchestrate, "_is_live", lambda u: True)
        mp.setattr(orchestrate, "_served_by_lane", lambda sid: False)
        mp.setattr(orchestrate, "_derive_changed_csvs", lambda unit, res, blob, st: ([], None, [], {}))
        mp.setattr(orchestrate, "_drain_csv_retry_queue", self.drain)
        h = self

        class _Strat(Strategy):
            name = "sim"

            def is_due(self, unit, us, now=None):
                return Strategy.is_due(self, unit, us, now=dt.datetime.fromtimestamp(h.clock.t, dt.timezone.utc))

            def detect_change(self, unit, us):
                if unit.source_id == "n":                               # the earned no_change: probe == stored
                    self._cur = "v1"
                    return None
                self._cur = None
                return "v-new"

            def run(self, unit, since=None):
                return h.work(unit)

        strat = _Strat()
        mp.setattr(orchestrate, "get_strategy", lambda name: strat)

    def _plant(self, t0):
        def ago(days):
            return _iso_ts(t0 - days * 86400)
        db = self.store.db
        for sid, ls, v in (("n", ago(40), "v1"), ("a", ago(40), None), ("b", ago(1), None), ("d", ago(40), None),
                           ("c", ago(40), None)):
            db.execute("INSERT INTO unit_state(source_id, unit_id, strategy, upstream_vintage, last_success_utc, "
                       "last_attempt_utc, status) VALUES (?, '_all', 'sim', ?, ?, ?, 'ok')", (sid, v, ls, ls))
        rows = [(ago(40), "n", "no_change", 0.0), (ago(40), "a", "ok", 300.0), (ago(1), "b", "ok", 300.0),
                (ago(40), "d", "ok", 1200.0), (ago(40), "c", "ok", 6000.0)]
        rows += [(ago(70), s, "partial", 18000.0) for s in self.declared]
        db.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES (?, ?, '_all', ?, ?)", rows)
        queue = dict(QUEUE, **{s: (5, "2026-10-01") for s in self.declared})
        for sid, (n, day) in queue.items():
            db.executemany("INSERT INTO csv_retry_queue(series_id, source_id, enqueued_utc, attempts) VALUES (?,?,?,0)",
                           [(f"{sid}:s{i:04d}", sid, f"{day}T00:00:00+00:00") for i in range(n)])
        db.commit()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")       # load_unit_book reads ?immutable=1, which skips the WAL

    def work(self, unit):
        sid = unit.source_id
        r = self.left[sid]
        limit = (orchestrate._RUN_DEADLINE_TS - self.clock.t) / 60.0
        if sid not in self.declared or r <= limit:
            self.clock.t += r * 60.0
            return Result(status=self.book.outcome[sid], obs=1, new_vintage="v-new", merged_rows=1)
        # the declared FETCHER's rule, step by step (as tests/test_replay_parity_declared_units.py): pieces that end
        # before ceiling - tail; none done -> one runs into the stop point; returns at once when the next cannot
        stop_at = orchestrate._RUN_DEADLINE_TS - self.mods[sid].window_stop_tail_min() * 60.0
        piece, done = self.mods[sid].window_stop_piece_min() * 60.0, 0.0
        while True:
            if piece > 0:
                if self.clock.t + piece <= stop_at:
                    self.clock.t += piece
                    done += piece
                    continue
                if done == 0:
                    self.clock.t = max(self.clock.t, stop_at)
                break
            step = min(60.0, stop_at - self.clock.t)
            if step <= 0:
                break
            self.clock.t += step
            done += step
        self.left[sid] = r - done / 60.0
        return Result(status="partial", obs=0, new_vintage=None, merged_rows=0, changed_keys={},
                      error="stopped before the run ceiling", window_stop="progress" if done > 0 else "none")

    def drain(self, unit, blob, store, deadline_ts=None, id_gate=None):
        """The derive inside the leftover drain, on the virtual clock: all the time it is given, or `rate` ids/min."""
        assert deadline_ts is not None, "the in-loop drain is stubbed out of this differential"
        left = (deadline_ts - self.clock.t) / 60.0
        ids = [r["series_id"] for r in store.csv_retries(unit.source_id)]
        use = left if self.rate is None else min(left, min(len(ids), orchestrate._CSV_RETRY_CAP) / self.rate)
        self.clock.t += use * 60.0
        if self.rate is not None:
            gone = ids[:min(len(ids), orchestrate._CSV_RETRY_CAP, int(use * self.rate + 1e-9))]
            store.clear_csv_retries(gone)
        return {"queued": len(ids), "attempted": len(ids), "cleared": 0, "still_queued": 0, "held_back": 0}

    def production(self, budget, capsys):
        self.mp.setenv("AQUEDUCT_RUN_BUDGET_MIN", str(budget))
        start = dt.datetime.fromtimestamp(self.clock.t, dt.timezone.utc)
        orchestrate.run_once(store=self.store)
        out = capsys.readouterr().out
        drained = [(s, int(n.replace(",", "")) / 60.0) for s, n in
                   re.findall(r"<<< (\w+)/_all took ([0-9,]+)s \(csv retry drain", out)]
        nd = re.search(r"(?:leftover time spent - \d+ source\(s\) not drained this pass|no leftover time \([^)]*\) - "
                       r"\d+ source\(s\) with parked ids not drained): ([\w, ]+)", out)
        return start, {"drained": drained, "not_drained": nd.group(1).split(", ") if nd else [],
                       "end_min": round((self.clock.t - start.timestamp()) / 60.0, 6),
                       "queue": {s: n for s, (n, _lo) in self.store.csv_retry_sources().items()}}, out

    def replay(self, start, budget):
        r = self.book.run_pass(start, float(budget), tail_min=self.tail, drain_rate=self.rate)
        return {"drained": [(s, round(x, 6)) for s, x in r["retry_drained"]], "not_drained": r["retry_not_drained"],
                "end_min": round(r["used_min"], 6), "queue": {s: v[0] for s, v in self.book.retry_queue.items()}}, r


def _both(tmp_path, monkeypatch, capsys, budget=100, **kw):
    h = Harness(tmp_path, monkeypatch, **kw)
    start, p, out = h.production(budget, capsys)
    r, raw = h.replay(start, budget)
    return p, r, raw, out


@pytest.mark.parametrize("budget", [100, 50])
@pytest.mark.parametrize("rate", [None, 2.0])
def test_the_replay_drains_what_run_once_drains(tmp_path, monkeypatch, capsys, rate, budget):
    p, r, _raw, out = _both(tmp_path, monkeypatch, capsys, budget=budget, rate=rate)
    for k in ("drained", "not_drained", "end_min", "queue"):
        assert p[k] == r[k], f"{k}: run_once {p[k]!r} replay {r[k]!r}\n{out[-2500:]}"


def test_pencil_no_rate(tmp_path, monkeypatch, capsys):
    p, _r, raw, _out = _both(tmp_path, monkeypatch, capsys)
    assert [x[0] for x in raw["ran"]] == ["n", "a", "d"] and raw["not_due"] == ["b"]
    assert raw["budget_skipped"] == [("c", C_RESERVE)]
    assert p["drained"] == [("c", 45.0)] and p["not_drained"] == ["b", "n"] and p["end_min"] == 70.0


def test_pencil_rate(tmp_path, monkeypatch, capsys):
    p, _r, _raw, _out = _both(tmp_path, monkeypatch, capsys, rate=2.0)
    assert p["drained"] == [("c", 25.0), ("b", 20.0)] and p["not_drained"] == ["n"] and p["end_min"] == 70.0
    assert p["queue"] == {"n": 30, "b": 60, "d": 10}


def test_pencil_the_margin_binds(tmp_path, monkeypatch, capsys):
    # budget 50: n 0, a 0->5; b NOT DUE; d 5 + 60 > 50 and c 5 + 90 > 50 BUDGET SKIP -> both candidates. Drain
    # min(45 - 15, 45) = 30 (the margin binds, not the cap), order d (07-01), c, b, n: d takes all 30; ends at 35
    p, r, raw, _out = _both(tmp_path, monkeypatch, capsys, budget=50)
    assert [x[0] for x in raw["ran"]] == ["n", "a"] and raw["budget_skipped"] == [("d", 60.0), ("c", C_RESERVE)]
    assert p["drained"] == r["drained"] == [("d", 30.0)] and p["not_drained"] == ["c", "b", "n"]
    assert p["end_min"] == r["end_min"] == 35.0


def test_a_non_turn_near_the_ceiling_leaves_no_drain(tmp_path, monkeypatch, capsys):
    # w (reserve 17, tail 8, piece 70) admitted at 25: window 75 - 8 = 67 < one piece -> runs into its stop point at
    # 92, keeps nothing -> a non-turn and a candidate; 8 min left -> min(8 - 15, 45) = 0: no drain, 4 named
    p, r, raw, out = _both(tmp_path, monkeypatch, capsys, declared={"w": (17.0, 8.0, 70.0)})
    assert ("w", 67.0, "window_stopped") in raw["ran"]
    assert p["drained"] == r["drained"] == [] and p["end_min"] == r["end_min"] == 92.0
    assert p["not_drained"] == r["not_drained"] == ["c", "b", "n", "w"], out[-1500:]


def test_a_non_turn_that_stops_early_leaves_the_drain_its_time(tmp_path, monkeypatch, capsys):
    # w (reserve 17, tail 50, piece 30) admitted at 25: window 75 - 50 = 25 < one piece -> stops at 50, keeps
    # nothing; 50 min left -> drain min(35, 45) = 35: c (oldest) takes it all, ends at 85; b, n, w not reached
    p, r, raw, out = _both(tmp_path, monkeypatch, capsys, declared={"w": (17.0, 50.0, 30.0)})
    assert ("w", 25.0, "window_stopped") in raw["ran"] and raw["budget_skipped"] == [("c", C_RESERVE)]
    assert p["drained"] == r["drained"] == [("c", 35.0)] and p["end_min"] == r["end_min"] == 85.0
    assert p["not_drained"] == r["not_drained"] == ["b", "n", "w"], out[-1500:]


def test_negative_control_no_drain_in_the_replay_diverges(tmp_path, monkeypatch, capsys):
    """The differential can fail: a replay pass that ignores the drain (what the replay was) ends at 25, not 70."""
    h = Harness(tmp_path, monkeypatch)
    start, p, _out = h.production(100, capsys)
    h.book.retry_queue = {}
    r, _raw = h.replay(start, 100)
    assert p["end_min"] == 70.0 and r["end_min"] == 25.0 and p["drained"] != r["drained"]


# ------------------------------------------------------------------ replay-only cases and the shared functions

def test_a_hard_stop_has_no_drain_and_a_capped_drain_holds_the_pass():
    t0 = m._parse("2026-10-01T00:00:00Z")
    rows = [("2026-09-01T00:00:00+00:00", "g", "_all", "ok", 6000.0),
            ("2026-09-01T00:00:00+00:00", "q", "_all", "ok", 300.0)]
    st = {"q": {"last_success_utc": "2026-09-30T00:00:00+00:00", "status": "ok"}}      # q not due: a candidate
    queue = {"q": (10, "2026-09-01T00:00:00+00:00")}
    # g (300 min of work, not clocked) is killed at the hard stop: run_once never reaches its drain
    b = m.UnitBook({"g": "monthly", "q": "monthly"}, st, rows, {"g": 300.0, "q": 5.0}, {}, retry_queue=queue)
    r = b.run_pass(t0, 200.0)
    assert r["hard_stop"] == "g" and r["retry_drained"] == [] and r["retry_not_drained"] == []
    # nothing ran: 2880-min budget, so the drain is capped at 45 - the pass holds 45 min for q
    b = m.UnitBook({"q": "monthly"}, st, rows, {"q": 5.0}, {}, retry_queue=queue)
    r = b.run_pass(t0, 2880.0)
    assert r["ran"] == [] and r["retry_drained"] == [("q", 45.0)] and r["used_min"] == 45.0


def test_the_replay_drain_uses_the_production_functions(monkeypatch):
    t0 = m._parse("2026-10-01T00:00:00Z")
    st = {"q": {"last_success_utc": "2026-09-30T00:00:00+00:00", "status": "ok"}}
    rows = [("2026-09-01T00:00:00+00:00", "q", "_all", "ok", 300.0)]

    def used(**patch):
        for k, v in patch.items():
            monkeypatch.setattr(orchestrate, k, v)
        b = m.UnitBook({"q": "monthly"}, st, rows, {"q": 5.0}, {}, retry_queue={"q": (10, "x")})
        return b.run_pass(t0, 100.0)["retry_drained"]
    assert used() == [("q", 45.0)]                                   # min(100 - 15, 45)
    assert used(leftover_drain_budget_min=lambda r, mg, cap: 7.0) == [("q", 7.0)]
    monkeypatch.undo()
    assert used(drain_candidates=lambda ok, blk: {}) == []
    monkeypatch.undo()
    assert used(drain_order=lambda c, q, lane=(): ({}, [])) == []


def test_the_extracted_functions_are_what_run_once_and_the_drain_call():
    import inspect
    assert "drain_candidates(_drain_ok, _drain_block)" in inspect.getsource(orchestrate.run_once)
    src = inspect.getsource(orchestrate._drain_leftover_retries)
    assert "drain_order(candidates, queued)[0]" in src and "drain_order(candidates, queued, lane)[1]" in src
    assert "leftover_drain_budget_min(" in inspect.getsource(orchestrate._leftover_drain_min)
    assert orchestrate.leftover_drain_budget_min(None, 15.0, 45.0) == 45.0
    assert orchestrate.leftover_drain_budget_min(30.0, 15.0, 45.0) == 15.0
    assert orchestrate.leftover_drain_budget_min(10.0, 15.0, 45.0) == 0.0


def test_the_loader_reads_the_queue(tmp_path):
    import sqlite3
    db = tmp_path / "s.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE unit_state(source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT, "
                "last_success_utc TEXT, last_attempt_utc TEXT, status TEXT, last_obs_date TEXT, obs_count INTEGER, "
                "attempt_count INTEGER, last_error TEXT)")
    con.execute("CREATE TABLE runs(id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, source_id TEXT, unit_id TEXT, "
                "status TEXT, obs INTEGER, dur_s REAL, note TEXT)")
    con.execute("CREATE TABLE csv_retry_queue(series_id TEXT PRIMARY KEY, source_id TEXT, enqueued_utc TEXT, "
                "attempts INTEGER DEFAULT 0, last_error TEXT)")
    con.executemany("INSERT INTO csv_retry_queue VALUES (?, ?, ?, 0, NULL)",
                    [("u:1", "u", "2026-10-02T00:00:00+00:00"), ("u:2", "u", "2026-10-01T00:00:00+00:00"),
                     ("x:1", "x", "2026-10-01T00:00:00+00:00")])            # x is not a replayed unit
    con.commit()
    con.close()
    b = m.load_unit_book(str(db), m._parse("2026-10-28T00:00:00Z"), dt.timedelta(days=-1), cadences={"u": "monthly"},
                         window_stop=set())
    assert b.retry_queue == {"u": [2, "2026-09-30T00:00:00+00:00"]}         # moved by the shift
