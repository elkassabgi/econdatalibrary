"""The starvation replay's two new unit kinds against the REAL run_once (the istat window_stop plan review, C5).

tools/measure_ci_lag.py UnitBook models, since 2026-11-24:
  ceiling_stop  an UNDECLARED unit whose fetcher stops at the run ceiling anyway - istat after PR #325 and before its
                declaration. It is ordered and admitted as an undeclared unit (its latest real run, the 60-day decay),
                and its stop is a `partial` turn with dur = the minutes used. This is the post-#325 baseline the
                review asked for: a ceiling stop given R minutes records about R, so the reserve tracks the pass.
  turn_cap      a declared unit whose fetcher ends its turn as at a run ceiling `cap` minutes after it started
                (istat's AQUEDUCT_ISTAT_TURN_MAX_MIN).

THE DIFFERENTIAL (test_replay_parity_declared_units.py's Harness, unchanged): one planted state file; run_once runs on
it with scripted fetchers that work step by step and check the time left before each step, exactly as istat.update()
does (no step starts within its tail of the ceiling - or of the cap's end); the replay's book is loaded from the same
file. Every decision and every record must match: which units ran, budget skips and their reserves, not-due units,
every runs row, every unit_state stamp, the minutes used.

PENCIL (cs: undeclared ceiling-stopper, tail 20; dc: declared, reserve 21, tail 20, capped at 60; small: daily 5 min;
cs and dc never finish - a rotation longer than any pass):
  pass 1 (200): small 0->5 ok; dc (band 3, older turn) 5 + 21 <= 200, capped: works 60 - 20 = 40 -> partial, returns
                at 45; cs: its last real run 150 min, waited 10 d -> 90 + 60 x (1 - 10/60) = 140; 45 + 140 <= 200 ->
                works to 200 - 20 = 180 -> partial (135 min), returns at 180. Its reserve for the next pass is that
                135-min run: the ratchet the review measured. Later passes are whatever production decides; the
                differential ties the replay to them.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import test_replay_parity_declared_units as P  # noqa: E402
from test_replay_parity_declared_units import Harness, m  # noqa: E402
from updater import orchestrate  # noqa: E402
from updater.strategies.base import Result  # noqa: E402

CS, DC = "cs", "dc"
CAP_MIN = 60.0
NEVER = 10_000.0                       # minutes: never finishes in a pass

C = {"cadence": {"small": "daily", CS: "monthly", DC: "monthly"},
     "declared": {DC: (21.0, 20.0, 0.0)},
     "state": [("small", 2, "ok")],
     "rows": [(40, "small", "ok", 300.0), (2, "small", "ok", 300.0),
              (10, CS, "partial", 9000.0), (12, DC, "partial", 7000.0)]}
CS_TAIL = 20.0


@pytest.fixture(autouse=True)
def _workstation_platform(monkeypatch):
    monkeypatch.setattr(orchestrate, "hard_timeout_available", lambda: False)


class CapHarness(Harness):
    """Harness with one undeclared ceiling-stopper (CS) and one declared unit capped per turn (DC)."""

    def __init__(self, tmp_path, monkeypatch, sc=C, ceiling_stop=True, cap=True):
        super().__init__(tmp_path, monkeypatch, sc)
        self.cs_on, self.cap_on = ceiling_stop, cap
        # the ceiling-stopper's fetcher exposes its stop figures (not a reserve: it is undeclared)
        self.mods[CS] = types.SimpleNamespace(window_stop_tail_min=lambda: CS_TAIL, window_stop_piece_min=lambda: 0.0)
        t0 = self.book_t0
        self.book = m.load_unit_book(self.db_path, t0, dt.timedelta(0), cadences=dict(self.cad),
                                     window_stop=set(self.decl), ceiling_stop={CS} if ceiling_stop else (),
                                     turn_cap={DC: CAP_MIN} if cap else None,
                                     work_override={CS: NEVER, DC: NEVER})
        self.left = dict(self.book.remaining)

    def _plant(self, t0):
        self.book_t0 = dt.datetime.fromtimestamp(t0, dt.timezone.utc)
        db = self.store.db
        # cs and dc: an OLD success (100 d) and a partial since, so both are due at every pass
        for sid in (CS, DC):
            db.execute("INSERT INTO unit_state(source_id, unit_id, strategy, last_success_utc, last_attempt_utc, "
                       "status) VALUES (?, '_all', 'sim', ?, ?, 'partial')",
                       (sid, P._iso_ts(t0 - 100 * 86400), P._iso_ts(t0 - (10 if sid == CS else 12) * 86400)))
        super()._plant(t0)

    def work(self, unit):
        sid = unit.source_id
        if sid not in (CS, DC):
            return super().work(unit)
        self.ran.append(sid)
        dl = orchestrate._RUN_DEADLINE_TS
        end = dl
        if sid == DC and self.cap_on:
            end = min(dl, self.clock.t + CAP_MIN * 60.0)          # the turn's own ceiling
        tail = (CS_TAIL if sid == CS else self.mods[DC].window_stop_tail_min()) * 60.0
        stop_at = end - tail
        done = 0.0
        while True:                                               # the stop check precedes every step
            step = min(60.0, stop_at - self.clock.t)
            if step <= 0:
                break
            self.clock.t += step
            done += step
        self.left[sid] -= done / 60.0
        signal = None if sid == CS else ("progress" if done > 0 else "none")
        return Result(status="partial", obs=0, new_vintage=None, merged_rows=0, changed_keys={},
                      error="stopped before the run ceiling", window_stop=signal)


PASSES = ((200, None), (150, 24 * 3600), (240, 24 * 3600), (100, 24 * 3600), (260, 24 * 3600))


def _passes(tmp_path, monkeypatch, capsys, **kw):
    h = CapHarness(tmp_path, monkeypatch, **kw)
    out = []
    for budget, gap in PASSES:
        if gap:
            h.clock.t += gap
        start, p = h.production_pass(budget, capsys)
        out.append((p, h.replay_pass(start, budget)))
    assert not h.book.stop_fallback, "the replay must read the planted fetchers' figures"
    return h, out


@pytest.mark.parametrize("cap", [True, False], ids=["capped", "uncapped"])
def test_the_replay_and_run_once_agree_on_a_ceiling_stopper_and_a_capped_unit(tmp_path, monkeypatch, capsys, cap):
    _h, out = _passes(tmp_path, monkeypatch, capsys, cap=cap)
    for i, (p, r) in enumerate(out, 1):
        for k in ("ran", "budget_skipped", "not_due", "rows", "state", "end_min"):
            assert p[k] == r[k], f"pass {i} {k}: run_once {p[k]!r}\n replay {r[k]!r}"


def test_the_pencil_first_pass(tmp_path, monkeypatch, capsys):
    """The module docstring's pass 1, on run_once's side (the differential ties the replay to it), so the harness
    cannot drift into a scenario where neither kind is exercised."""
    _h, out = _passes(tmp_path, monkeypatch, capsys)
    p1 = out[0][0]
    assert p1["ran"][:3] == ["small", DC, CS], p1["ran"]
    rows = [x[1:] for x in p1["rows"]]
    assert ("dc", "partial", 40 * 60.0) in rows, rows                      # capped: 60 - 20
    assert ("cs", "partial", 135 * 60.0) in rows, rows                     # to the ceiling minus its tail
    assert p1["end_min"] == 180.0
    st = p1["state"][CS]                                                   # (status, last_attempt, last_success)
    assert st[0] == "partial" and st[2] and st[2] < st[1], st              # the old success is kept
    # pass 2 (150, a day later): dc again (45 min used), then cs reserves its 135-min run, decayed by ~1 day of
    # waiting to ~134 > 105 left -> BUDGET SKIP: the post-#325 ratchet in the baseline
    p2 = out[1][0]
    assert CS not in p2["ran"] and [s for s, n in p2["budget_skipped"] if s == CS and 133 <= n <= 135], p2


def test_negative_control_a_book_without_the_ceiling_stop_diverges(tmp_path, monkeypatch, capsys):
    """Without ceiling_stop the replay runs CS unclocked to the hard stop (the pre-#325 model): it is killed, where
    run_once's fetcher stopped at its tail and booked `partial`. So the differential can fail."""
    h = CapHarness(tmp_path, monkeypatch, ceiling_stop=False)
    start, p = h.production_pass(200, capsys)
    r = h.replay_pass(start, 200)
    assert ("cs", "partial") in [x[1:3] for x in p["rows"]]
    assert ("cs", "killed_external") in [x[1:3] for x in r["rows"]], r["rows"]
