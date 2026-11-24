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
import math
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


# ------------------------------------------------------------------ THE CSV PHASE (phase B review B1)
#
# The fetchers above return no cursors, so run_once's CSV phase never ran in this differential, and the replay
# booked a stopped turn as its fetch alone - the gap the review found (a cap-60 istat turn read 39.8 min while in
# production the CSV step then ran two 45-min budgets). Here both stopping units WALK FLOWS (60 s each) and hand
# each walked flow over as a cursor; the CSV phase is stubbed one level down, at the derive, so run_once's own
# sequence runs - _begin_unit_turn_end, the fresh derive (budget from the REAL _capped_derive_budget), the in-loop
# retry drain (same), the deferral enqueue into the REAL csv_retry_queue - and the replay's CsvStep must match it:
# every decision, every runs row (dur to 0.1 s, run_once's rounding), every stamp, the minutes used, and the
# queue each pass leaves. dc is capped at 60 min, hands over its turn end (Result.turn_end_ts), keeps a CSV
# allowance of 120 s per handed-over flow (istat's rule) and starts with 500 queued ids; cs is the undeclared
# baseline (no allowance, run ceiling only).
FLOW_S, IDS_PER_FLOW, IDS_PER_MIN, ALLOC_S = 60.0, 10, 6.0, 120.0
DC_QUEUED = 500                          # dc's csv_retry_queue at the start, planted on both sides


class CsvHarness(CapHarness):
    def __init__(self, tmp_path, monkeypatch, turn_bound=True, alloc_s=ALLOC_S, book_csv=True,
                 book_turn_bound=None):
        super().__init__(tmp_path, monkeypatch)
        self.turn_bound, self.alloc_s, self.n_turn = turn_bound, alloc_s, 0
        bt = turn_bound if book_turn_bound is None else book_turn_bound
        steps = {DC: m.CsvStep(FLOW_S, IDS_PER_FLOW, IDS_PER_MIN, alloc_s=alloc_s, turn_bound=bt),
                 CS: m.CsvStep(FLOW_S, IDS_PER_FLOW, IDS_PER_MIN)}
        self.book = m.load_unit_book(self.db_path, self.book_t0, dt.timedelta(0), cadences=dict(self.cad),
                                     window_stop=set(self.decl), ceiling_stop={CS}, turn_cap={DC: CAP_MIN},
                                     work_override={CS: NEVER, DC: NEVER},
                                     csv_step=steps if book_csv else None,
                                     csv_queue={DC: DC_QUEUED} if book_csv else None)
        self.store.enqueue_csv_retry(DC, [f"dc0#{i}" for i in range(DC_QUEUED)], "planted")
        self.left = dict(self.book.remaining)
        monkeypatch.setattr(orchestrate, "_derive_changed_csvs", self._derive)
        monkeypatch.setattr(orchestrate, "_drain_csv_retry_queue", self._drain)

    def _spend(self, n_ids):
        """One derive_and_put over n_ids at IDS_PER_MIN, with the budget the REAL _capped_derive_budget gives now
        (it reads the run ceiling and the unit's turn end): (ids done, minutes)."""
        b = orchestrate._capped_derive_budget().get("budget_min")
        need = n_ids / IDS_PER_MIN
        if need <= b:
            self.clock.t += need * 60.0
            return n_ids
        self.clock.t += b * 60.0
        return min(n_ids, int(math.floor(b * IDS_PER_MIN + 1e-9)))

    def _derive(self, unit, res, blob, st):
        ids = [f"{k}#{j}" for k in sorted(res.series_cursors or {}) for j in range(IDS_PER_FLOW)]
        if not ids:
            return [], None, [], {}
        done = self._spend(len(ids))
        return [], None, ids[done:], {}

    def _drain(self, unit, blob, store, *a, **k):
        ids = [r["series_id"] for r in store.csv_retries(unit.source_id)][:orchestrate._CSV_RETRY_CAP]
        if ids:
            store.clear_csv_retries(ids[:self._spend(len(ids))])

    def work(self, unit):
        sid = unit.source_id
        if sid not in (CS, DC):
            return P.Harness.work(self, unit)
        self.ran.append(sid)
        dl = orchestrate._RUN_DEADLINE_TS
        cap_end = self.clock.t + CAP_MIN * 60.0 if sid == DC else None
        end = dl if cap_end is None else min(dl, cap_end)
        tail = (CS_TAIL if sid == CS else self.mods[DC].window_stop_tail_min()) * 60.0
        alloc = self.alloc_s if sid == DC else 0.0
        n = 0
        while end - self.clock.t >= tail + n * alloc:         # istat's stop check, before every flow
            self.clock.t += FLOW_S
            n += 1
        self.left[sid] -= n * FLOW_S / 60.0
        self.n_turn += 1
        cursors = {f"{sid}{self.n_turn}f{i}": "2026-01-01" for i in range(n)}
        turn_end = cap_end if (self.turn_bound and cap_end is not None and cap_end < dl) else None
        signal = None if sid == CS else ("progress" if n else "none")
        return Result(status="partial", obs=0, new_vintage=None, series_cursors=cursors or None,
                      error="stopped before the run ceiling", window_stop=signal, turn_end_ts=turn_end)

    def queues(self):
        return ({s: len(self.store.csv_retries(s)) for s in (CS, DC)},
                {s: self.book.csv_queue.get(s, 0) for s in (CS, DC)})


def _norm(rows):
    return [(ts, s, st, round(d, 1)) for ts, s, st, d in rows]


def _csv_passes(tmp_path, monkeypatch, capsys, **kw):
    h = CsvHarness(tmp_path, monkeypatch, **kw)
    out = []
    for budget, gap in PASSES:
        if gap:
            h.clock.t += gap
        start, p = h.production_pass(budget, capsys)
        r = h.replay_pass(start, budget)
        out.append((p, r, h.queues()))
    return h, out


def test_the_replay_and_run_once_agree_on_the_csv_phase_of_a_capped_and_a_ceiling_stopping_unit(tmp_path, monkeypatch,
                                                                                                capsys):
    h, out = _csv_passes(tmp_path, monkeypatch, capsys)
    for i, (p, r, (qp, qr)) in enumerate(out, 1):
        for k in ("ran", "budget_skipped", "not_due", "state", "end_min"):
            assert p[k] == r[k], f"pass {i} {k}: run_once {p[k]!r}\n replay {r[k]!r}"
        assert _norm(p["rows"]) == _norm(r["rows"]), f"pass {i} rows: run_once {p['rows']!r}\n replay {r['rows']!r}"
        assert qp == qr, f"pass {i} csv_retry_queue: run_once {qp} replay {qr}"
    log = h.book.csv_log
    # the differential exercised every branch: a fresh derive cut by its budget, a drain, a turn bound by its cap
    assert any(x["deferred"] > 0 for x in log) and any(x["drained"] > 0 for x in log), log
    # a capped turn whose retry drain was cut by its TURN END: the whole turn is exactly the cap
    assert any(x["sid"] == DC and x.get("turn_end") is not None and x["drained"] > 0
               and abs(x["fetch_min"] + x["fresh_min"] + x["drain_min"] - CAP_MIN) < 1e-6 for x in log), log


def test_the_csv_phase_is_in_the_turn_the_pencil(tmp_path, monkeypatch, capsys):
    """Pass 1 (200 min), pencil, on run_once's side: small 0 -> 5. dc, capped at 60 min (its turn ends at 65):
    flows of 60 s while (65 - now) >= 20 min + n x 120 s -> 14 flows (5 -> 19), 140 ids = 23.3 min of derive
    (-> 42.3), then the drain of the 500 planted ids with min(45, 65 - 42.3 = 22.7) min -> 136 ids, to 65.0. The
    turn is the cap, 60 min (its row 3,600 s); before the turn end it would have been 14 + 23.3 + 45. cs reserves
    its 150-min run decayed by 10 days of waiting (140 min): 65 + 140 > 200 -> BUDGET SKIP. Queue dc 500 - 136."""
    h, out = _csv_passes(tmp_path, monkeypatch, capsys)
    p1, _r1, (q1, _qr) = out[0]
    rows = {x[1]: x for x in p1["rows"]}
    assert p1["ran"] == ["small", DC], p1["ran"]
    assert rows[DC][2:] == ("partial", 3600.0), rows[DC]
    assert [s for s, _n in p1["budget_skipped"]] == [CS], p1["budget_skipped"]
    assert p1["end_min"] == 65.0
    assert q1 == {CS: 0, DC: 364}, q1


def test_negative_control_a_replay_without_the_csv_phase_diverges(tmp_path, monkeypatch, capsys):
    """The replay of PR #327 as reviewed (no CsvStep: a stopped turn is its fetch alone) against run_once with the
    CSV phase: the rows differ, so this differential can fail."""
    _h, out = _csv_passes(tmp_path, monkeypatch, capsys, book_csv=False)
    p1, r1, _q = out[0]
    assert _norm(p1["rows"]) != _norm(r1["rows"]), (p1["rows"], r1["rows"])


def test_negative_control_a_replay_that_ignores_the_turn_end_diverges(tmp_path, monkeypatch, capsys):
    """run_once bounds dc's CSV phase by its turn end; a replay that bounds it by the run ceiling only books a
    longer dc turn and a different pass."""
    _h, out = _csv_passes(tmp_path, monkeypatch, capsys, book_turn_bound=False)
    assert any(_norm(p["rows"]) != _norm(r["rows"]) for p, r, _q in out), "the turn end was never exercised"
