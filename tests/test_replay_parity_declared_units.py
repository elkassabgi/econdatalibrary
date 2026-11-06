"""The starvation replay (tools/measure_ci_lag.py UnitBook.run_pass) against the REAL run_once, for the window-stoppable
units of PR #215 (review of the replay: items 1-3; review AR-422 of this PR).

PR #215 changed three things for a unit whose registry entry says `window_stop: true`, and the replay modelled none:
  1. its ORDER key is declared_last_turn_utc (max of last turn, attempt, kill and its newest `window_stopped` run);
     the replay sorted it by last_turn_utc, and its loader dropped the `window_stopped` rows that key reads;
  2. its START-GATE reserve is its fetcher's declared figure (min(90, declared)); the replay reserved the old rule;
  3. a window stop with NO durable progress is NOT A TURN: no unit state, a `window_stopped` runs row; one with
     progress is the fetcher's `partial`. The replay wrote transient_fail for both.
And how a declared fetcher STOPS (AR-422): at ceiling - its own tail, keeping what it did, and it RETURNS there -
the minutes between its stop point and the ceiling stay with the pass, and the gate may still admit a unit in them.

THE DIFFERENTIAL. One planted state file is read by both sides: run_once runs on it (a virtual clock, scripted
fetchers), and the replay's book is loaded FROM THE SAME FILE by load_unit_book. Every scheduling decision and every
record must be identical: which units ran and in what order, which were budget-skipped and on what reserve, which
were not due, every unit_state stamp and every runs row.

THE PRODUCTION SIDE'S FETCHER IS NOT THE REPLAY'S RULE. A declared fake works STEP BY STEP the way the declared
fetchers do (census.update / wid.update / imts run_sliced / _unctad._windowed): before each step it checks the time
left to orchestrate._RUN_DEADLINE_TS against its tail; a piece-unit fetcher (imts: a slice) starts a piece only when
it ends before its stop point and returns at once when the next one cannot, and one that did none runs it into the
stop point; window_stop is "progress" iff a step was done. Its tail and piece come from its module's
window_stop_tail_min() / window_stop_piece_min(), the functions the replay reads (the real fetchers expose the same
pair, built on their own stop checks' env helpers; tests at the end pin them). An undeclared fake runs its work.

What is REAL on the production side: run_once, order_units, overdue_key, order_last_turn (last_turn_utc /
declared_last_turn_utc), admission_reserve and _window_stop_reserve, _window_stop_no_progress, Strategy.is_due, the
StateStore and its run_cost_estimate / last_kill_utc / last_window_stop_utc. FAKE: the registry (a unit list), the
fetchers (scripted as above), the CSV phase and both drains (stubs).

PENCIL, scenario A (reserves: cen 17 / tail 5 / piece 0, imt 55 / tail 25 / piece 30 (a slice), big 90 = min(90,
3 x 100), kil 90, small 10; bands by cost: small 1, kil 2, cen / big / imt 3):
  pass 1 (100): small 0->5 ok; kil 5->55 ok; cen (never ran, first in band 3) 55 + 17 <= 100, 150 min does not fit 45
                -> works 45 - 5 = 40 min to its stop point -> `partial` (40 min), returns at 95; big 95 + 90 SKIP;
                imt (its window_stopped row of a day ago puts it after big) 95 + 55 SKIP.
  pass 2 (150, +6 h): small, kil not due; cen partial with no success -> partial clock, NOT DUE; order big (40 d),
                imt (1 d), cen; big 0->100 ok; imt 100 + 55 > 150 SKIP; cen passes the gate, NOT DUE.
  pass 3 (130, +6 h): order imt (1.5 d), big, cen; imt 55 <= 130, 200 does not fit -> window 105, 3 slices = 90 min
                -> `partial`, returns at 90; big 90 + 90 SKIP; cen 90 + 17 <= 130, NOT DUE.
Scenario B, the non-turn (nt: reserve 10, tail 8, piece 5): z ok 0->2; nt 2 + 10 <= 14, window 12 - 8 = 4 < one
piece -> runs into its stop point at 6 and keeps nothing -> NOT A TURN (window_stopped, 4 min); the pass ends at 6.
Pass 2 (+6 h, 14): nt, still due, gets a 6-min window: one piece -> `partial` (5 min), returns at 5.
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

TAIL = 60.0      # the replay's tail_min: it must NOT decide a declared unit's stop (a value no declared tail has)


@pytest.fixture(autouse=True)
def _workstation_platform(monkeypatch):
    """The replay models the WORKSTATION (admission_reserve(..., hard_timeout=False)); run_once must see the same
    platform on any machine, Linux CI included (2026-11-06). Under it an undeclared unit reserves at least its newest
    real run (big 6,000 s -> 100 min) unless it is its band's head (the undeclared cen -> the normal 90)."""
    monkeypatch.setattr(orchestrate, "hard_timeout_available", lambda: False)

# scenario: cadences, declared {sid: (reserve, tail, piece)}, unit_state rows, runs rows (days ago, sid, status, dur_s)
A = {"cadence": {"small": "daily", "kil": "monthly", "cen": "monthly", "big": "monthly", "imt": "monthly"},
     "declared": {"cen": (17.0, 5.0, 0.0), "imt": (55.0, 25.0, 30.0)},
     "state": [("small", 2, "ok"), ("kil", 40, "ok"), ("big", 40, "ok"), ("imt", 60, "ok")],
     "rows": [(70, "cen", "partial", 9000.0), (40, "small", "ok", 300.0), (2, "small", "ok", 300.0),
              (40, "kil", "ok", 3000.0), (10, "kil", "killed_external", 3000.0), (40, "big", "ok", 6000.0),
              (60, "imt", "ok", 12000.0), (1, "imt", "window_stopped", 30.0)]}
B = {"cadence": {"z": "monthly", "nt": "monthly"},
     "declared": {"nt": (10.0, 8.0, 5.0)},
     "state": [("z", 40, "ok")],
     "rows": [(40, "z", "ok", 120.0), (40, "nt", "partial", 6000.0)]}


class _Clock:
    def __init__(self, t):
        self.t = float(t)

    def time(self):
        return self.t

    def __getattr__(self, name):
        import time as _t
        return getattr(_t, name)


def _iso_ts(ts: float) -> str:
    return dt.datetime.fromtimestamp(ts, dt.timezone.utc).isoformat(timespec="seconds")


class Harness:
    """run_once on a virtual clock over one planted state file; the replay's book loaded from that file."""

    def __init__(self, tmp_path, monkeypatch, sc=A):
        self.mp, self.sc = monkeypatch, sc
        self.cad, self.decl = sc["cadence"], sc["declared"]
        # virtual time ends well before the wall clock: last_turn_utc ignores kills dated after datetime.now()
        t0 = int((dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=20)).timestamp())
        self.clock = _Clock(t0)
        self.db_path = str(tmp_path / "state.db")
        self.store = StateStore(self.db_path)
        self._plant(t0)
        self.mods = {s: types.SimpleNamespace(window_admission_reserve_min=lambda r=r: r,
                                              window_stop_tail_min=lambda t=t: t,
                                              window_stop_piece_min=lambda p=p: p)
                     for s, (r, t, p) in self.decl.items()}
        mp = monkeypatch
        mp.setattr(fetchers, "get", lambda sid: self.mods.get(sid))   # both sides read the same fetcher figures
        self.book = m.load_unit_book(self.db_path, dt.datetime.fromtimestamp(t0, dt.timezone.utc), dt.timedelta(0),
                                     cadences=dict(self.cad), window_stop=set(self.decl))
        self.left = dict(self.book.remaining)         # the work on record; the stop RULE below is production's
        self.ran = []
        self.units = [Unit(source_id=s, unit_id="_all", strategy="sim", cadence=c, out_paths=[],
                           config={"run_location": "local", "live": True, "window_stop": s in self.decl})
                      for s, c in self.cad.items()]
        for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
            monkeypatch.delenv(k, raising=False)
        now = lambda: _iso_ts(self.clock.t)                       # noqa: E731
        mp.setattr(orchestrate, "time", self.clock)
        mp.setattr(orchestrate, "now_utc", now)
        mp.setattr(state_mod, "now_utc", now)
        mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
        mp.setattr(registry, "validate", lambda *a, **k: [])
        mp.setattr(registry, "all_units", lambda *a, **k: list(self.units))
        mp.setattr(orchestrate, "_has_adapter", lambda u: True)
        mp.setattr(orchestrate, "_wrong_location", lambda u: False)
        mp.setattr(orchestrate, "_protected", lambda u: False)
        mp.setattr(orchestrate, "_is_live", lambda u: True)
        mp.setattr(orchestrate, "_served_by_lane", lambda sid: False)
        mp.setattr(orchestrate, "_derive_changed_csvs", lambda unit, res, blob, st: ([], None, [], {}))
        mp.setattr(orchestrate, "_drain_csv_retry_queue", lambda *a, **k: None)
        mp.setattr(orchestrate, "_drain_leftover_retries", lambda *a, **k: None)
        h = self

        class _Strat(Strategy):
            name = "sim"

            def is_due(self, unit, us, now=None):
                return Strategy.is_due(self, unit, us, now=dt.datetime.fromtimestamp(h.clock.t, dt.timezone.utc))

            def detect_change(self, unit, us):
                return "v-new"

            def run(self, unit, since=None):
                return h.work(unit)

        strat = _Strat()
        mp.setattr(orchestrate, "get_strategy", lambda name: strat)

    def _plant(self, t0):
        def ago(days):
            return _iso_ts(t0 - days * 86400)
        db = self.store.db
        for sid, d, st in self.sc["state"]:
            db.execute("INSERT INTO unit_state(source_id, unit_id, strategy, last_success_utc, last_attempt_utc, status)"
                       " VALUES (?, '_all', 'sim', ?, ?, ?)", (sid, ago(d), ago(d), st))
        db.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES (?, ?, '_all', ?, ?)",
                       [(ago(d), s, st, dur) for d, s, st, dur in self.sc["rows"]])
        db.commit()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")   # load_unit_book reads ?immutable=1, which skips the WAL

    def work(self, unit):
        sid = unit.source_id
        self.ran.append(sid)
        r = self.left[sid]
        dl = orchestrate._RUN_DEADLINE_TS
        if sid not in self.decl or r * 60.0 <= dl - self.clock.t:
            assert r * 60.0 <= dl - self.clock.t + m.PS1_GRACE_MIN * 60.0, f"{sid} would be hard-stopped"
            self.clock.t += r * 60.0
            self.left[sid] = self.book.work[sid]
            return Result(status=self.book.outcome[sid], obs=1, new_vintage="v-new", merged_rows=1)
        # THE DECLARED FETCHER'S RULE, step by step (see the module docstring)
        mod = self.mods[sid]
        stop_at = dl - mod.window_stop_tail_min() * 60.0
        piece = mod.window_stop_piece_min() * 60.0
        done = 0.0
        while True:
            if piece > 0:
                if self.clock.t + piece <= stop_at:
                    self.clock.t += piece                  # a whole piece (a slice) pulled and kept
                    done += piece
                    continue
                if done == 0:
                    self.clock.t = max(self.clock.t, stop_at)   # a piece started and cut at the stop point
                break                                      # the next-piece check failed: return at once
            step = min(60.0, stop_at - self.clock.t)       # the stop check precedes every request
            if step <= 0:
                break
            self.clock.t += step
            done += step
        self.left[sid] = r - done / 60.0
        return Result(status="partial", obs=0, new_vintage=None, merged_rows=0, changed_keys={},
                      error="stopped before the run ceiling", window_stop="progress" if done > 0 else "none")

    def production_pass(self, budget, capsys):
        self.mp.setenv("AQUEDUCT_RUN_BUDGET_MIN", str(budget))
        n0 = self.store.db.execute("SELECT COALESCE(MAX(id), 0) FROM runs").fetchone()[0]
        self.ran = []
        start = dt.datetime.fromtimestamp(self.clock.t, dt.timezone.utc)
        orchestrate.run_once(store=self.store)
        out = capsys.readouterr().out
        skipped = [(s, float(n)) for s, n in re.findall(r"BUDGET SKIP (\w+)/_all \([^)]*\): the gate reserves (\d+) min",
                                                        out)]
        not_due = re.findall(r"NOT DUE (\w+)/_all", out)
        rows = [tuple(r) for r in self.store.db.execute(
            "SELECT ts_utc, source_id, status, dur_s FROM runs WHERE id > ? ORDER BY id", (n0,))]
        return start, {"ran": list(self.ran), "budget_skipped": skipped, "not_due": not_due, "rows": rows,
                       "state": self.prod_state(), "end_min": round((self.clock.t - start.timestamp()) / 60.0, 6)}

    def prod_state(self):
        out = {}
        for s in self.cad:
            r = self.store.get_unit(s, "_all") or {}
            out[s] = (r.get("status"), r.get("last_attempt_utc"), r.get("last_success_utc"))
        return out

    def replay_pass(self, start, budget):
        n0 = self.book.db.execute("SELECT COALESCE(MAX(id), 0) FROM runs").fetchone()[0]
        res = self.book.run_pass(start, float(budget), tail_min=TAIL)
        rows = [tuple(r) for r in self.book.db.execute(
            "SELECT ts_utc, source_id, status, dur_s FROM runs WHERE id > ? ORDER BY id", (n0,))]
        st = {s: (self.book.state[s].get("status"), self.book.state[s].get("last_attempt_utc"),
                  self.book.state[s].get("last_success_utc")) for s in self.cad}
        return {"ran": [x[0] for x in res["ran"]], "budget_skipped": [(s, round(n)) for s, n in res["budget_skipped"]],
                "not_due": res["not_due"], "rows": rows, "state": st, "end_min": round(res["used_min"], 6),
                "res": res}


PASSES_A = ((100, None), (150, 6 * 3600), (130, 6 * 3600))
PASSES_B = ((14, None), (14, 6 * 3600))


def _passes(tmp_path, monkeypatch, capsys, sc=A, passes=PASSES_A):
    h = Harness(tmp_path, monkeypatch, sc)
    out = []
    for budget, gap in passes:
        if gap:
            h.clock.t += gap
        start, p = h.production_pass(budget, capsys)
        out.append((p, h.replay_pass(start, budget)))
    assert not h.book.stop_fallback, "the replay must read the planted fetchers' figures, not fall back"
    return h, out


@pytest.mark.parametrize("sc, passes", [(A, PASSES_A), (B, PASSES_B)], ids=["A", "B"])
def test_the_replay_and_run_once_take_the_same_decisions_and_write_the_same_records(tmp_path, monkeypatch, capsys,
                                                                                     sc, passes):
    _h, out = _passes(tmp_path, monkeypatch, capsys, sc, passes)
    for i, (p, r) in enumerate(out, 1):
        for k in ("ran", "budget_skipped", "not_due", "rows", "state", "end_min"):
            assert p[k] == r[k], f"pass {i} {k}: run_once {p[k]!r}\n replay {r[k]!r}"


def test_the_planted_passes_are_the_pencil_figures(tmp_path, monkeypatch, capsys):
    """The pencil numbers in the module docstring, asserted on run_once's side (the test above ties the replay to it),
    so a harness that drifted into some other scenario cannot keep the differential vacuous."""
    _h, out = _passes(tmp_path, monkeypatch, capsys)
    (p1, _), (p2, _), (p3, _) = out
    assert p1["ran"] == ["small", "kil", "cen"] and p1["budget_skipped"] == [("big", 100.0), ("imt", 55.0)]
    assert [x[1:] for x in p1["rows"]] == [("small", "ok", 300.0), ("kil", "ok", 3000.0), ("cen", "partial", 2400.0)]
    assert p1["state"]["cen"][0] == "partial" and p1["end_min"] == 95.0     # returned at its stop point, not 100
    assert p2["ran"] == ["big"] and p2["budget_skipped"] == [("imt", 55.0)]
    assert sorted(p2["not_due"]) == ["cen", "kil", "small"]                  # cen: the partial clock
    assert p3["ran"] == ["imt"] and p3["budget_skipped"] == [("big", 100.0)] and "cen" in p3["not_due"]
    assert [x[1:] for x in p3["rows"]] == [("imt", "partial", 90 * 60.0)] and p3["end_min"] == 90.0
    assert p3["state"]["imt"][0] == "partial" and p3["state"]["imt"][2] == p2["state"]["imt"][2]   # success kept


def test_the_non_turn_pencil(tmp_path, monkeypatch, capsys):
    _h, out = _passes(tmp_path, monkeypatch, capsys, B, PASSES_B)
    (p1, _), (p2, _) = out
    assert p1["ran"] == ["z", "nt"] and [x[1:] for x in p1["rows"]] == [("z", "ok", 120.0),
                                                                       ("nt", "window_stopped", 240.0)]
    assert p1["state"]["nt"] == (None, None, None) and p1["end_min"] == 6.0           # not a turn; still due
    # due again (no state): 0 + 10 <= 14, window 14 - 8 = 6 fits one 5-min piece -> `partial`, returns at 5
    assert p2["ran"] == ["nt"] and [x[1:] for x in p2["rows"]] == [("nt", "partial", 300.0)] and p2["end_min"] == 5.0


def test_negative_control_the_same_harness_detects_the_replays_old_stop_rule(tmp_path, monkeypatch, capsys):
    """The differential can fail on the stop rule itself: a book that stops declared units by the replay's tail_min
    at the ceiling (the rule AR-422 found) records cen as a non-turn charged 45 min, where run_once's fetcher kept
    40 min and returned at 95."""
    h = Harness(tmp_path, monkeypatch)
    h.book._declared_stop = lambda sid, limit: (limit, max(0.0, limit - TAIL))
    start, p = h.production_pass(100, capsys)
    r = h.replay_pass(start, 100)
    assert p["rows"][-1][1:3] == ("cen", "partial") and r["rows"][-1][1:3] == ("cen", "window_stopped")
    assert (p["end_min"], r["end_min"]) == (95.0, 100.0)


def test_negative_control_the_same_harness_detects_an_undeclared_book(tmp_path, monkeypatch, capsys):
    """A book loaded WITHOUT the declaration (what the replay was before this change) diverges in the first pass -
    cen is reserved 90 and skipped: its newest real run is 9,000 s = 150 min, but as the first due unit of its band it
    is the band head, which admission_reserve offers the normal reserve (2026-11-06)."""
    h = Harness(tmp_path, monkeypatch)
    h.book = m.load_unit_book(h.db_path, dt.datetime.fromtimestamp(h.clock.t, dt.timezone.utc), dt.timedelta(0),
                              cadences=dict(A["cadence"]), window_stop=set())
    start, p = h.production_pass(100, capsys)
    r = h.replay_pass(start, 100)
    assert p["ran"] == ["small", "kil", "cen"] and r["ran"] == ["small", "kil"]
    assert ("cen", 90) in r["budget_skipped"]        # undeclared, cen heads its band: offered the normal reserve


# ------------------------------------------------------------------ the REAL declared fetchers' figures

T0 = dt.datetime(2026, 10, 1, tzinfo=dt.timezone.utc)
CEN, NP = "census", "unctad_nonplasticsubststradebypartner"


@pytest.fixture
def clean_env(monkeypatch):
    for k in ("AQUEDUCT_CENSUS_MARGIN_MIN", "AQUEDUCT_WID_MARGIN_MIN", "AQUEDUCT_IMTS_MARGIN_MIN",
              "AQUEDUCT_IMTS_MERGE_RESERVE_MIN", "AQUEDUCT_UNCTAD_MARGIN_MIN", "AQUEDUCT_UNCTAD_ASSEMBLY_RESERVE_MIN",
              "AQUEDUCT_UNIT_TIMEOUT_MIN"):
        monkeypatch.delenv(k, raising=False)


def test_every_declared_fetcher_exposes_its_stop_figures(clean_env):
    """Each fetcher that declares window_stop gives the replay its stop point and its progress unit; the default
    figures are the ones its stop check uses (census and wid: the 5-min floor; imts: floor 5 + merge reserve 20,
    one slice; nonplastics: floor 5 + assembly reserve 60)."""
    ws = m.local_window_stop_from_registry()
    got = {s: (fetchers.get(s).window_stop_tail_min(), fetchers.get(s).window_stop_piece_min()) for s in sorted(ws)}
    # 2026-11-06: four more unctad modules on the windowed update, the same figures as nonplastics
    assert got == {"census": (5.0, 0.0), "imf_imts_direct": (25.0, 30.0), NP: (65.0, 0.0), "wid": (5.0, 0.0),
                   "unctad_creativegoodsvalue": (65.0, 0.0), "unctad_criticalmineralstradebypart": (65.0, 0.0),
                   "unctad_gstptradematrix": (65.0, 0.0), "unctad_tradeservcatbypartner": (65.0, 0.0)}, got
    b = m.UnitBook({s: "monthly" for s in ws}, {}, [], {s: 1.0 for s in ws}, {}, window_stop=ws)
    assert all(b.stop_figures(s)[2] == "fetcher" for s in ws) and not b.stop_fallback


def test_the_stop_figures_follow_the_fetchers_env(clean_env, monkeypatch):
    """One source of truth: the env the fetcher's own stop check reads moves the replay's figure too."""
    monkeypatch.setenv("AQUEDUCT_CENSUS_MARGIN_MIN", "12")
    monkeypatch.setenv("AQUEDUCT_UNCTAD_ASSEMBLY_RESERVE_MIN", "40")
    b = m.UnitBook({CEN: "monthly", NP: "monthly"}, {}, [], {CEN: 1.0, NP: 1.0}, {}, window_stop={CEN, NP})
    assert b.stop_figures(CEN)[:2] == (12.0, 0.0) and b.stop_figures(NP)[:2] == (45.0, 0.0)


def test_ar422_census_in_a_60_min_pass_records_partial_and_waits_the_partial_clock(clean_env):
    """AR-422's planted D3 (F:/work/ar422 test_ar422_planted_v2.py, in substance): census declared, no success, 500
    min of work, a 60-min pass. Its fetcher stops at 60 - 5 having attempted flows -> `partial`, not a non-turn; with
    no success on record it then waits the partial clock, so a day later it is not attempted. With W on or off."""
    rows = [((T0 - dt.timedelta(days=3)).isoformat(), CEN, "_all", "partial", 500 * 60.0)]
    for kw in ({}, {"w_on": True}):
        b = m.UnitBook({CEN: "monthly"}, {CEN: {}}, list(rows), {CEN: 500.0}, {CEN: "partial"}, window_stop={CEN})
        r1 = b.run_pass(T0, 60.0, **kw)
        assert r1["hard_stop"] is None and b.state[CEN]["status"] == "partial", kw
        assert r1["ran"] == [(CEN, 55.0, "stopped")] and b.remaining[CEN] == 445.0, r1["ran"]
        b.run_pass(T0 + dt.timedelta(days=1), 600.0, **kw)
        assert b.attempts[CEN] == 1, kw


def test_a_nonplastics_stop_leaves_census_its_window(clean_env):
    """nonplastics (reserve 75, tail 65) first in a 100-min pass: it stops at 100 - 65 = 35 having spilled chunks
    (`partial`) and RETURNS there, so run_once still admits census (35 + 17 <= 100), which works to 100 - 5 = 95.
    The replay before this change charged nonplastics the whole window and skipped census; on main it reserved 90
    for both."""
    rows = [((T0 - dt.timedelta(days=40)).isoformat(), NP, "_all", "ok", 300.0),        # band 1: first
            ((T0 - dt.timedelta(days=40)).isoformat(), CEN, "_all", "ok", 9000.0)]      # band 3
    b = m.UnitBook({NP: "monthly", CEN: "monthly"}, {}, rows, {NP: 500.0, CEN: 500.0}, {}, window_stop={NP, CEN})
    r = b.run_pass(T0, 100.0)
    assert r["ran"] == [(NP, 35.0, "stopped"), (CEN, 60.0, "stopped")] and r["budget_skipped"] == []
    assert b.state[NP]["status"] == b.state[CEN]["status"] == "partial" and r["used_min"] == 95.0


# ------------------------------------------------------------------ the shared functions and the declared set

def test_the_replay_declares_exactly_the_units_the_registry_declares():
    """Every local registry unit: the replay's SimUnit carries the same window_stop flag in its config as the real
    Unit registry.to_units builds, and the declared set is not empty (the test would pass vacuously)."""
    sim, ws = m.local_units_from_registry(), m.local_window_stop_from_registry()
    assert ws, "no declared local unit: nothing to compare"
    checked = 0
    for e in registry.load().get("sources") or []:
        if e.get("run_location") != "local" or e.get("source_id") not in sim:
            continue
        for u in registry.to_units(e):
            s = m.SimUnit(u.source_id, sim[u.source_id], window_stop=u.source_id in ws)
            assert s.config.get("window_stop") == (u.config or {}).get("window_stop"), u.key
            assert orchestrate.order_cadence(s) == orchestrate.order_cadence(u), u.key
            checked += 1
    assert checked and {"census", "wid", "imf_imts_direct"} <= ws


def test_the_replay_orders_and_admits_through_the_production_functions(monkeypatch):
    """Shared, not copied: replacing orchestrate.order_last_turn / admission_reserve changes the replay's pass."""
    t0 = m._parse("2026-10-01T00:00:00Z")
    rows = [("2026-09-01T00:00:00+00:00", s, "_all", "ok", 600.0) for s in ("a", "b")]
    st = {s: {"last_success_utc": "2026-09-0%dT00:00:00+00:00" % i, "status": "ok"} for i, s in ((1, "a"), (2, "b"))}

    def ran(**patch):
        for k, v in patch.items():
            monkeypatch.setattr(orchestrate, k, v)
        b = m.UnitBook({"a": "monthly", "b": "monthly"}, st, rows, {"a": 10.0, "b": 10.0}, {}, window_stop={"a"})
        return [x[0] for x in b.run_pass(t0, 100.0)["ran"]]

    assert ran() == ["a", "b"]                                                # a is older
    flip = {"a": "2026-09-30T00:00:00+00:00", "b": "2026-09-02T00:00:00+00:00"}
    assert ran(order_last_turn=lambda u, row, k, w: flip[u.source_id]) == ["b", "a"]
    monkeypatch.undo()
    assert ran(admission_reserve=lambda u, est, t=None, **kw: (101.0 if u.source_id == "a" else 10.0, "", None, None)) == \
        ["b"]


def test_the_declared_reserve_is_capped_at_90(monkeypatch):
    """AR-422 M8: a declared reserve above the default worst case is capped at 90 (orchestrate._DECLARED_RESERVE_CAP_MIN),
    in production's admission_reserve and so in the replay."""
    monkeypatch.delenv("AQUEDUCT_UNIT_TIMEOUT_MIN", raising=False)
    monkeypatch.setattr(fetchers, "get", lambda sid: types.SimpleNamespace(window_admission_reserve_min=lambda: 500.0))
    u = Unit(source_id="x", unit_id="_all", strategy="s", cadence="monthly", out_paths=[], config={"window_stop": True})
    assert orchestrate.admission_reserve(u, None)[0] == 90.0
    b = m.UnitBook({"x": "monthly"}, {}, [], {"x": 1.0}, {}, window_stop={"x"})
    assert b.run_pass(T0, 89.0)["budget_skipped"] == [("x", 90.0)]


def test_a_window_stopped_row_reaches_the_book_but_not_its_work_or_outcome(tmp_path):
    """load_unit_book keeps `window_stopped` rows in the book (the declared order key reads them) and leaves them out
    of the work term and the outcome (a 30-s row is not the unit's remaining work)."""
    import sqlite3
    db = tmp_path / "s.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE unit_state(source_id TEXT, unit_id TEXT, strategy TEXT, upstream_vintage TEXT, "
                "last_success_utc TEXT, last_attempt_utc TEXT, status TEXT, last_obs_date TEXT, obs_count INTEGER, "
                "attempt_count INTEGER, last_error TEXT)")
    con.execute("CREATE TABLE runs(id INTEGER PRIMARY KEY AUTOINCREMENT, ts_utc TEXT, source_id TEXT, unit_id TEXT, "
                "status TEXT, obs INTEGER, dur_s REAL, note TEXT)")
    con.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES (?, 'w', '_all', ?, ?)",
                    [("2026-10-01T00:00:00+00:00", "partial", 6000.0),
                     ("2026-10-05T00:00:00+00:00", "window_stopped", 30.0)])
    con.commit()
    con.close()
    b = m.load_unit_book(str(db), m._parse("2026-10-28T00:00:00Z"), dt.timedelta(0), cadences={"w": "monthly"},
                         window_stop={"w"})
    assert b.work["w"] == 100.0 and b.outcome["w"] == "partial" and b.units["w"].window_stop
    assert b.db.execute("SELECT status, dur_s FROM runs ORDER BY id").fetchall() == [("partial", 6000.0),
                                                                                    ("window_stopped", 30.0)]
    wst = StateStore.last_window_stop_utc(m._StoreShim(b.db))
    assert orchestrate.order_last_turn(b.units["w"], {}, None, wst[("w", "_all")]) == "2026-10-05T00:00:00+00:00"
