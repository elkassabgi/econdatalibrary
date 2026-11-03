"""The starvation replay (tools/measure_ci_lag.py UnitBook.run_pass) against the REAL run_once, for the window-stoppable
units of PR #215 (review of the replay: items 1-3).

PR #215 changed three things for a unit whose registry entry says `window_stop: true`, and the replay modelled none:
  1. its ORDER key is declared_last_turn_utc (max of last turn, attempt, kill and its newest `window_stopped` run);
     the replay sorted it by last_turn_utc, and its loader dropped the `window_stopped` rows that key reads;
  2. its START-GATE reserve is its fetcher's declared figure (min(90, declared)); the replay reserved the old rule;
  3. a window stop with NO durable progress is NOT A TURN: no unit state, a `window_stopped` runs row; one with
     progress is the fetcher's `partial`. The replay wrote transient_fail for both.

THE DIFFERENTIAL. One planted state file is read by both sides: run_once runs on it (a virtual clock, a scripted
fetcher), and the replay's book is loaded FROM THE SAME FILE by load_unit_book. Over three passes every scheduling
decision and every record must be identical: which units ran and in what order, which were budget-skipped and on
what reserve, which were not due, every unit_state stamp and every runs row. The scripted fetcher does what the
replay's work model says a unit does (the work on record; a declared unit that does not fit stops at the ceiling and
keeps (time left - tail) minutes), so what is compared is the SCHEDULER, which is the replay's claim.

What is REAL on the production side: run_once, order_units, overdue_key, order_last_turn (last_turn_utc /
declared_last_turn_utc), admission_reserve and _window_stop_reserve, _window_stop_no_progress, Strategy.is_due, the
StateStore and its run_cost_estimate / last_kill_utc / last_window_stop_utc. FAKE: the registry (a unit list), the
fetchers (scripted on the virtual clock; their declared reserves planted), the CSV phase and both drains (stubs).

PENCIL (budget per pass; reserves: cen 17 and imt 55 declared, big 90 = min(90, 3 x 100), kil 90 = min(90, 3 x 50),
small 10 = max(3 x 5, 10); tail 60; bands by cost: small 1, kil 2, cen / big / imt 3):
  pass 1 (100): small 0->5 ok; kil 5->55 ok (5 + 90 <= 100); cen (never ran, first in band 3) 55 + 17 <= 100, 150 min
                does not fit 45 -> stops at 100 keeping max(0, 45 - 60) = 0 -> NOT A TURN (window_stopped, 45 min);
                big (40 d) 100 + 90 > 100 SKIP; imt (its window_stopped row of a day ago puts it after big) SKIP 55.
  pass 2 (150, +6 h): small, kil not due; order big (40 d), imt (1 d), cen (window stop 4 h ago);
                big 0->100 ok; imt 100 + 55 > 150 SKIP; cen 117 <= 150, still due (no state), 50 left -> NOT A TURN.
  pass 3 (130, +6 h): small, kil not due; order imt (1.5 d), big, cen; imt 55 <= 130, 200 does not fit 130 ->
                stops keeping 130 - 60 = 70 -> `partial`; big 130 + 90 > 130 SKIP; cen 130 + 17 > 130 SKIP.
With the replay as it was the differential fails in pass 1 already (cen reserved 90 and skipped: the negative
control below). Each fix was also reverted one at a time (the mutants are listed in the PR): every one fails here.
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

TAIL = 60.0
DECLARED = {"cen": 17.0, "imt": 55.0}
CADENCE = {"small": "daily", "kil": "monthly", "cen": "monthly", "big": "monthly", "imt": "monthly"}


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

    def __init__(self, tmp_path, monkeypatch):
        self.mp = monkeypatch
        # virtual time ends well before the wall clock: last_turn_utc ignores kills dated after datetime.now()
        t0 = int((dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=20)).timestamp())
        self.clock = _Clock(t0)
        self.db_path = str(tmp_path / "state.db")
        self.store = StateStore(self.db_path)
        self._plant(t0)
        self.book = m.load_unit_book(self.db_path, dt.datetime.fromtimestamp(t0, dt.timezone.utc), dt.timedelta(0),
                                     cadences=dict(CADENCE), window_stop=set(DECLARED))
        # the scripted fetcher works to the replay's own model, read from the book
        self.left = dict(self.book.remaining)
        self.ran = []
        self.units = [Unit(source_id=s, unit_id="_all", strategy="sim", cadence=c, out_paths=[],
                           config={"run_location": "local", "live": True, "window_stop": s in DECLARED})
                      for s, c in CADENCE.items()]
        mods = {s: types.SimpleNamespace(window_admission_reserve_min=lambda r=r: r) for s, r in DECLARED.items()}
        for k in ("AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
            monkeypatch.delenv(k, raising=False)
        mp = monkeypatch
        now = lambda: _iso_ts(self.clock.t)                       # noqa: E731
        mp.setattr(orchestrate, "time", self.clock)
        mp.setattr(orchestrate, "now_utc", now)
        mp.setattr(state_mod, "now_utc", now)
        mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
        mp.setattr(registry, "validate", lambda *a, **k: [])
        mp.setattr(registry, "all_units", lambda *a, **k: list(self.units))
        mp.setattr(fetchers, "get", lambda sid: mods.get(sid))
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
        for sid, ls, la, st in (("small", ago(2), ago(2), "ok"), ("kil", ago(40), ago(40), "ok"),
                                ("big", ago(40), ago(40), "ok"), ("imt", ago(60), ago(60), "ok")):
            db.execute("INSERT INTO unit_state(source_id, unit_id, strategy, last_success_utc, last_attempt_utc, status)"
                       " VALUES (?, '_all', 'sim', ?, ?, ?)", (sid, ls, la, st))
        rows = [(ago(70), "cen", "partial", 9000.0),                 # cen: 150 min, never a success, no state
                (ago(40), "small", "ok", 300.0), (ago(2), "small", "ok", 300.0),
                (ago(40), "kil", "ok", 3000.0), (ago(10), "kil", "killed_external", 3000.0),   # order by the kill
                (ago(40), "big", "ok", 6000.0),
                (ago(60), "imt", "ok", 12000.0), (ago(1), "imt", "window_stopped", 30.0)]      # order by this row
        db.executemany("INSERT INTO runs(ts_utc, source_id, unit_id, status, dur_s) VALUES (?, ?, '_all', ?, ?)",
                       rows)
        db.commit()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")   # load_unit_book reads ?immutable=1, which skips the WAL

    def work(self, unit):
        sid = unit.source_id
        self.ran.append(sid)
        r = self.left[sid]
        limit = (orchestrate._RUN_DEADLINE_TS - self.clock.t) / 60.0
        if sid not in DECLARED or r <= limit:
            assert r <= limit + m.PS1_GRACE_MIN, f"{sid} would be hard-stopped: not a run_once outcome"
            self.clock.t += r * 60.0
            self.left[sid] = self.book.work[sid]
            st = self.book.outcome[sid]
            return Result(status=st, obs=1, new_vintage="v-new", merged_rows=1)
        kept = max(0.0, limit - TAIL)
        self.clock.t = orchestrate._RUN_DEADLINE_TS
        self.left[sid] = r - kept
        return Result(status="partial", obs=0, new_vintage=None, merged_rows=0, changed_keys={},
                      error="stopped at the run ceiling", window_stop="progress" if kept > 0 else "none")

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
                       "state": self.prod_state()}

    def prod_state(self):
        out = {}
        for s in CADENCE:
            r = self.store.get_unit(s, "_all") or {}
            out[s] = (r.get("status"), r.get("last_attempt_utc"), r.get("last_success_utc"))
        return out

    def replay_pass(self, start, budget):
        n0 = self.book.db.execute("SELECT COALESCE(MAX(id), 0) FROM runs").fetchone()[0]
        res = self.book.run_pass(start, float(budget), tail_min=TAIL)
        rows = [tuple(r) for r in self.book.db.execute(
            "SELECT ts_utc, source_id, status, dur_s FROM runs WHERE id > ? ORDER BY id", (n0,))]
        st = {s: (self.book.state[s].get("status"), self.book.state[s].get("last_attempt_utc"),
                  self.book.state[s].get("last_success_utc")) for s in CADENCE}
        return {"ran": [x[0] for x in res["ran"]], "budget_skipped": [(s, round(n)) for s, n in res["budget_skipped"]],
                "not_due": res["not_due"], "rows": rows, "state": st, "res": res}


PASSES = ((100, None), (150, 6 * 3600), (130, 6 * 3600))


def _passes(tmp_path, monkeypatch, capsys):
    h = Harness(tmp_path, monkeypatch)
    out = []
    for budget, gap in PASSES:
        if gap:
            h.clock.t += gap
        start, p = h.production_pass(budget, capsys)
        r = h.replay_pass(start, budget)
        out.append((p, r))
        h.clock.t = max(h.clock.t, start.timestamp())
    return h, out


def test_the_replay_and_run_once_take_the_same_decisions_and_write_the_same_records(tmp_path, monkeypatch, capsys):
    _h, out = _passes(tmp_path, monkeypatch, capsys)
    for i, (p, r) in enumerate(out, 1):
        for k in ("ran", "budget_skipped", "not_due", "rows", "state"):
            assert p[k] == r[k], f"pass {i} {k}: run_once {p[k]!r}\n replay {r[k]!r}"


def test_the_planted_passes_are_the_pencil_figures(tmp_path, monkeypatch, capsys):
    """The pencil numbers in the module docstring, asserted on run_once's side (the test above ties the replay to it),
    so a harness that drifted into some other scenario cannot keep the differential vacuous."""
    _h, out = _passes(tmp_path, monkeypatch, capsys)
    (p1, _), (p2, _), (p3, _) = out
    assert p1["ran"] == ["small", "kil", "cen"] and p1["budget_skipped"] == [("big", 90.0), ("imt", 55.0)]
    assert [x[1:3] for x in p1["rows"]] == [("small", "ok"), ("kil", "ok"), ("cen", "window_stopped")]
    assert p1["rows"][-1][3] == 45 * 60.0 and p1["state"]["cen"] == (None, None, None)      # not a turn
    assert p2["ran"] == ["big", "cen"] and p2["budget_skipped"] == [("imt", 55.0)]
    assert sorted(p2["not_due"]) == ["kil", "small"]
    assert [x[1:] for x in p2["rows"]] == [("big", "ok", 6000.0), ("cen", "window_stopped", 3000.0)]
    assert p3["ran"] == ["imt"] and p3["budget_skipped"] == [("big", 90.0), ("cen", 17.0)]
    assert [x[1:] for x in p3["rows"]] == [("imt", "partial", 130 * 60.0)]
    assert p3["state"]["imt"][0] == "partial" and p3["state"]["imt"][2] == p2["state"]["imt"][2]   # success kept


def test_negative_control_the_same_harness_detects_an_undeclared_book(tmp_path, monkeypatch, capsys):
    """The differential can fail: a book loaded WITHOUT the declaration (what the replay was before this change)
    diverges from run_once in the first pass - cen is reserved min(90, 3 x 150) = 90 and skipped."""
    h = Harness(tmp_path, monkeypatch)
    h.book = m.load_unit_book(h.db_path, dt.datetime.fromtimestamp(h.clock.t, dt.timezone.utc), dt.timedelta(0),
                              cadences=dict(CADENCE), window_stop=set())
    start, p = h.production_pass(100, capsys)
    r = h.replay_pass(start, 100)
    assert p["ran"] == ["small", "kil", "cen"] and r["ran"] == ["small", "kil"]
    assert ("cen", 90) in r["budget_skipped"]


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
    assert ran(admission_reserve=lambda u, est, t=None: (101.0 if u.source_id == "a" else 10.0, "", None, None)) == \
        ["b"]


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
