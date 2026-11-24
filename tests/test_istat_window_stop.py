"""istat DECLARES window_stop (registry `window_stop: true`, 2026-11-23).

WHY. Since PR #325 istat stops at the run ceiling on its own (tests/test_istat_run_ceiling.py), but it was
UNDECLARED, so on the workstation (no hard per-unit timeout) the start gate reserved its LATEST REAL RUN
(orchestrate.admission_reserve). A run that ends at the ceiling is as long as the time it was given, so the
reserve tracks the longest pass and refuses every shorter one (R1838's lock-out). The local pass of 2026-11-23
22:45 printed: "BUDGET SKIP istat/_all (due): the gate reserves 187 min (its latest real run: ...) and 81 min
remain in this pass". A declared unit reserves only what it needs to stop cleanly and hand over.

WHAT THESE TESTS PIN, through the REAL istat.update() (fake session, fake clock; no network, no R2) and, where
it is about recording, the REAL run_once over the REAL registry entry:
  1. The three figures (window_admission_reserve_min, window_stop_tail_min, window_stop_piece_min) are the run
     ceiling check's own numbers: margin + FLOW_FLOOR_S, the same helper and constant, so the gate and the
     fetcher cannot disagree. Pinned by BEHAVIOUR: with the reserve's minutes left a flow starts; half a
     second less and none does.
  2. Result.window_stop is set ONLY by a ceiling stop: "none" when no flow was attempted, else "progress"
     (also when every flow failed). An every-host-dead break (an ISTAT outage) and a budget stop leave it
     None, so an outage is recorded as the `partial` turn it is and never as a `window_stopped` non-turn.
  3. Through run_once: tonight's shape (latest real run 187 min, 81 min left, no hard timeout) is admitted on
     the declared reserve; a ceiling stop before any flow writes a `window_stopped` row and no unit state; a
     host-dead run writes a `partial` unit state and no `window_stopped` row.

The expected numbers are written from the stated rule and istat's existing constants (RETRIES, HOST_TIMEOUT,
the backoff), not read from the new functions.
"""
from __future__ import annotations

import datetime as dt
import os
import sys
import time as _real_time

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests  # noqa: E402

from updater import orchestrate, registry  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies import fetchers  # noqa: E402
from updater.strategies.base import Strategy  # noqa: E402
from updater.strategies.fetchers import _common, istat  # noqa: E402

SDMX = "https://sdmx.istat.it/SDMXWS/rest/"
ESPLORA = "https://esploradati.istat.it/SDMXWS/rest/"
EPOCH = 1_800_000_000.0
# One flow's allowance, from the stated rule: every CSV GET on esploradati runs to its 300-s timeout, RETRIES
# times, plus the backoff sleeps min(4 x attempt, 30) between the retry passes: 3 x 300 + 4 + 8 = 912 s.
FLOOR_S = (istat.RETRIES * istat.HOST_TIMEOUT[ESPLORA]
           + sum(min(4 * a, 30) for a in range(1, istat.RETRIES)))


def _istat_unit():
    """The REAL registry unit (its config carries the registry's window_stop flag)."""
    (u,) = [u for u in registry.all_units() if u.source_id == "istat"]
    return u


class _Clock:
    def __init__(self):
        self.t = 0.0

    def time(self):
        return EPOCH + self.t

    def monotonic(self):
        return self.t

    def sleep(self, s):
        self.t += max(0.0, float(s))

    def __getattr__(self, name):
        return getattr(_real_time, name)


class _Resp:
    def __init__(self, status, content=b""):
        self.status_code = status
        self.content = content


class _Istat:
    """Fake ISTAT: every GET costs `delay` fake seconds and answers 200 + one row, or 503 when `fail`.
    sdmx='redirect' makes every sdmx.istat.it GET raise TooManyRedirects (that host's shape since 2026-08-23)."""

    def __init__(self, clock, delay=60.0, fail=False, sdmx="serve"):
        self.clock, self.delay, self.fail, self.sdmx = clock, float(delay), fail, sdmx
        self.starts: dict = {}
        self.gets = 0

    def get(self, url, headers=None, timeout=None):
        self.gets += 1
        flow = url.split(f"{istat.AGENCY},", 1)[1].split("/", 1)[0]
        self.starts.setdefault(flow, self.clock.t)
        if self.sdmx == "redirect" and url.startswith(SDMX):
            raise requests.TooManyRedirects("Exceeded 30 redirects.")
        self.clock.t += self.delay
        if self.fail:
            return _Resp(503, b"")
        return _Resp(200, (b"DATAFLOW,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE\n"
                           + f"IT1:{flow}(1.0),A,IT,2026,1.5\n".encode()))


@pytest.fixture
def rig(tmp_path, monkeypatch):
    out = tmp_path / "data" / "clean_full" / "istat"
    out.mkdir(parents=True)
    clock = _Clock()
    for k in ("AQUEDUCT_BACKEND", "ISTAT_MAX_FLOWS", "ISTAT_BUDGET_MIN", "AQUEDUCT_ISTAT_MARGIN_MIN",
              "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", "360")      # what run_local_heavy.ps1 sets
    monkeypatch.setattr(istat.config, "source_dir", lambda sid: str(out))
    monkeypatch.setattr(istat, "time", clock)
    monkeypatch.setattr(_common, "time", clock)
    monkeypatch.setattr(istat, "_tcp_reachable", lambda base: True)

    def _no_socket(*_a, **_k):
        raise AssertionError("a test opened a real socket")

    import socket
    monkeypatch.setattr(socket, "create_connection", _no_socket)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    istat._DEAD_HOSTS.clear()
    istat._TRANSPORT_FAILS.clear()
    istat._TCP_PROBED.clear()

    class Rig:
        pass

    r = Rig()
    r.out, r.clock, r.mp = out, clock, monkeypatch

    def flows(n, rows=True):
        """n flow files. rows=False writes EMPTY files: no stored max date, so the run seeds no cursor."""
        for i in range(1, n + 1):
            pq.write_table(pa.table({
                "series_key": pa.array(["FREQ=A:REF_AREA=IT"] if rows else [], pa.string()),
                "obs_date": pa.array([dt.date(2020, 12, 31)] if rows else [], pa.date32()),
                "value": pa.array([1.0] if rows else [], pa.float64()),
            }), str(out / f"F{i:02d}.parquet"))
        return [f"F{i:02d}" for i in range(1, n + 1)]

    def run(delay=60.0, ceiling_s=None, fail=False, sdmx="serve"):
        host = _Istat(clock, delay, fail, sdmx)
        monkeypatch.setattr(istat.requests, "Session", lambda: host)
        monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS",
                            None if ceiling_s is None else clock.time() + ceiling_s)
        return istat.update(None, None), host

    r.flows, r.run = flows, run
    yield r
    istat._DEAD_HOSTS.clear()
    istat._TRANSPORT_FAILS.clear()
    istat._TCP_PROBED.clear()


# ------------------------------------------------------------------ 1. the figures are the check's own numbers

@pytest.mark.parametrize("margin, tail", [(None, (300 + FLOOR_S) / 60.0), ("7", (420 + FLOOR_S) / 60.0),
                                          ("0", FLOOR_S / 60.0)])
def test_the_tail_is_margin_plus_one_flows_allowance_and_the_reserve_adds_the_setup(rig, margin, tail):
    """C3: tail = margin + FLOW_FLOOR_S (NOT census's margin alone), piece 0, reserve = tail + the setup allowance,
    all read at call time from the helper the stop check reads."""
    if margin is not None:
        rig.mp.setenv("AQUEDUCT_ISTAT_MARGIN_MIN", margin)
    assert istat.window_stop_tail_min() == pytest.approx(tail, abs=1e-9)
    assert istat.window_stop_piece_min() == 0.0
    assert istat.window_admission_reserve_min() == pytest.approx(tail + istat.ADMISSION_SETUP_MIN, abs=1e-9)
    assert istat.window_admission_reserve_min() > istat.window_stop_tail_min()
    if margin is None:
        assert istat.window_admission_reserve_min() == 21.2          # the default the registry pin carries


@pytest.mark.parametrize("bad", ["abc", "-1", "nan"])
def test_a_bad_margin_value_keeps_the_default_reserve(rig, bad):
    """The same fall-back as the stop check: a bad value must not remove the margin from the gate either."""
    rig.mp.setenv("AQUEDUCT_ISTAT_MARGIN_MIN", bad)
    assert istat.window_stop_tail_min() == pytest.approx((300 + FLOOR_S) / 60.0)
    assert istat.window_admission_reserve_min() == pytest.approx((300 + FLOOR_S) / 60.0 + istat.ADMISSION_SETUP_MIN)


@pytest.mark.parametrize("margin", [None, "7"])
def test_admitted_at_its_reserve_istat_still_starts_a_flow_after_the_setup_minute(rig, margin):
    """THE GATE AND THE FETCHER AGREE, by behaviour. A unit admitted with exactly the reserve's minutes left, whose
    setup (lease, listing, bookmark) then takes up to ADMISSION_SETUP_MIN, reaches update() with the tail + 0.5 s
    left: one 61-s flow starts, then the next check refuses. Moving AQUEDUCT_ISTAT_MARGIN_MIN moves both."""
    if margin is not None:
        rig.mp.setenv("AQUEDUCT_ISTAT_MARGIN_MIN", margin)
    names = rig.flows(5)
    left_at_update = (istat.window_admission_reserve_min() - istat.ADMISSION_SETUP_MIN) * 60.0 + 0.5
    res, host = rig.run(delay=60.0, ceiling_s=left_at_update)
    assert list(host.starts) == names[:1], list(host.starts)
    assert res.status == "partial" and "deferred by budget" in res.error, res.error
    assert res.window_stop == "progress", res.window_stop
    assert orchestrate._window_stop_no_progress(_istat_unit(), res) is False


@pytest.mark.parametrize("margin", [None, "7"])
def test_a_window_just_under_the_tail_starts_no_flow_and_says_none(rig, margin):
    """Half a second under the tail: no flow starts, and the Result says window_stop "none" with no cursor - the
    shape run_once records as a non-turn (C3)."""
    if margin is not None:
        rig.mp.setenv("AQUEDUCT_ISTAT_MARGIN_MIN", margin)
    rig.flows(5)
    res, host = rig.run(delay=60.0, ceiling_s=istat.window_stop_tail_min() * 60.0 - 0.5)
    assert not host.starts, list(host.starts)
    assert res.status == "partial" and "deferred by budget" in res.error, res.error
    assert res.window_stop == "none" and not res.series_cursors, (res.window_stop, res.series_cursors)
    assert orchestrate._window_stop_no_progress(_istat_unit(), res) is True

# ------------------------------------------------------------------ 2. the signal: a ceiling stop only

def test_a_ceiling_stop_after_flows_that_all_failed_is_progress_not_a_non_turn(rig):
    """Every flow answers 503 on every attempt (a host that is up but failing). The ceiling stops the run with
    flows attempted: window_stop "progress", so run_once records the `partial` with its transient note."""
    rig.flows(10, rows=False)                       # no stored rows: no cursor can rescue the verdict
    res, host = rig.run(delay=1.0, ceiling_s=(300 + FLOOR_S) + 120.0, fail=True)
    assert host.starts, "premise: no flow was attempted"
    assert res.status == "partial" and "transient-failed" in res.error, res.error
    assert not res.series_cursors, "premise: empty flows seed no cursor"
    assert res.window_stop == "progress", res.window_stop
    assert orchestrate._window_stop_no_progress(_istat_unit(), res) is False


@pytest.mark.parametrize("rows", [True, False], ids=["stored_rows", "empty_flows"])
@pytest.mark.parametrize("shape", ["sdmx_redirects_esploradati_tcp_dead", "both_tcp_dead"])
@pytest.mark.parametrize("ceiling_s", [None, 4 * 3600.0], ids=["no_ceiling", "far_ceiling"])
def test_an_every_host_dead_run_is_not_a_window_stop(rig, shape, rows, ceiling_s, capsys):
    """THE OUTAGE MUST STAY VISIBLE. The 2026-11-19/20 shape (sdmx redirect-loops, esploradati fails its TCP
    probe) and the both-TCP-dead shape stop the run after one flow with every host dead. That is not the run
    ceiling, so window_stop stays None and _window_stop_no_progress is False: run_once records the `partial`
    with "every ISTAT host unusable", and health and the outage signal still see it. With empty flow files no
    cursor is seeded, so the verdict cannot lean on cursors."""
    rig.flows(6, rows=rows)
    reachable = {"sdmx_redirects_esploradati_tcp_dead": {SDMX}, "both_tcp_dead": set()}[shape]
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: base in reachable)
    res, host = rig.run(delay=1.0, ceiling_s=ceiling_s, sdmx="redirect")
    out = capsys.readouterr().out
    assert "stopping after 1 flow(s) attempted" in out, out
    assert res.status == "partial" and "every ISTAT host unusable" in res.error, res.error
    assert bool(res.series_cursors) == rows, res.series_cursors
    assert res.window_stop is None, res.window_stop
    assert orchestrate._window_stop_no_progress(_istat_unit(), res) is False


def test_a_host_dying_in_the_last_flow_before_the_ceiling_is_still_progress(rig, capsys):
    """Flow 1 is the one in which the last host dies (not named, no longest update), and the ceiling check of
    flow 2 then fires BEFORE the all-hosts-dead check. That stop is a ceiling stop with a flow attempted:
    "progress", never "none" - a rule that tests only the cursors (empty here) would hide the outage."""
    rig.flows(6, rows=False)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: base == SDMX)
    # flow 1 starts with margin + floor + 0.5 s left; its RATE pause (1 s) leaves flow 2 below the need
    res, host = rig.run(delay=1.0, ceiling_s=(300 + FLOOR_S) + 0.5, sdmx="redirect")
    out = capsys.readouterr().out
    assert host.gets == 1 and "[istat] run ceiling:" in out, out
    assert not res.series_cursors
    assert res.window_stop == "progress", res.window_stop
    assert orchestrate._window_stop_no_progress(_istat_unit(), res) is False


def test_a_budget_stop_is_not_a_window_stop(rig):
    """istat's own Deadline (BUDGET_MIN, the override cleared) ends the run, with the ceiling far away:
    window_stop stays None (census's rule: only the window end is a window stop)."""
    rig.mp.delenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", raising=False)
    rig.mp.setattr(istat, "BUDGET_MIN", 3.0)
    rig.flows(10)
    res, host = rig.run(delay=60.0, ceiling_s=10 * 3600.0)
    assert 0 < len(host.starts) < 10, list(host.starts)
    assert res.status == "partial" and "deferred by budget" in res.error, res.error
    assert res.window_stop is None, res.window_stop


def test_control_a_run_that_finishes_sets_no_signal(rig):
    names = rig.flows(4)
    res, host = rig.run(delay=1.0, ceiling_s=10 * 3600.0)
    assert list(host.starts) == names and res.status == "ok", res.error
    assert res.window_stop is None


# ------------------------------------------------------------------ 3. through the REAL run_once

def test_tonights_pass_admits_istat_on_its_declared_reserve(tmp_path, monkeypatch, capsys):
    """The 2026-11-23 22:45 shape: istat's latest real run 187 min (a `partial`), the workstation (no hard
    timeout), a pass with 81 min left. Undeclared, the gate printed BUDGET SKIP with "reserves 187 min (its
    latest real run ...)"; declared, istat passes on its 21.2-min reserve. Nothing past the gate runs (the
    _has_adapter recorder, the test_admit_window_stoppable pattern)."""
    for k in ("AQUEDUCT_ISTAT_MARGIN_MIN", "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS", "AQUEDUCT_UNIT_TIMEOUT_MIN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setattr(orchestrate, "hard_timeout_available", lambda: False)
    monkeypatch.setenv("AQUEDUCT_RUN_BUDGET_MIN", "81")
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", orchestrate._RUN_DEADLINE_TS)
    store = StateStore(str(tmp_path / "state.db"))
    now = dt.datetime.now(dt.timezone.utc)
    store.upsert_unit("istat", "_all", strategy="sdmx_delta", status="partial",
                      last_success_utc=(now - dt.timedelta(days=132)).isoformat(timespec="seconds"),
                      last_attempt_utc=(now - dt.timedelta(days=1)).isoformat(timespec="seconds"))
    store.log_run("istat", "_all", "partial", dur_s=187 * 60.0)
    admitted = []
    monkeypatch.setattr(orchestrate, "_has_adapter", lambda unit: admitted.append(unit.source_id) or False)
    try:
        orchestrate.run_once(sources=["istat", "census"], dry=True, store=store)
    except SystemExit:
        pass
    out = capsys.readouterr().out
    assert "BUDGET SKIP istat/_all" not in out, out[-3000:]
    assert "WINDOW STOP istat/_all: passes the start gate on its declared 21.2-min reserve" in out, out[-3000:]
    assert "istat" in admitted


class _Strat(Strategy):
    """The real is_due; detect_change spends `probe_s` fake seconds; run() is the REAL istat.update."""
    name = "sim"

    def __init__(self, clock, probe_s):
        self.clock, self.probe_s = clock, probe_s

    def detect_change(self, unit, us):
        self.clock.t += self.probe_s
        return "v-new"                 # a changed vintage (None means "unchanged" and skips the fetch)

    def run(self, unit, since=None):
        return istat.update(unit, since)


def _run_once(rig, monkeypatch, budget_min, probe_s=0.0):
    """One REAL run_once over the REAL istat registry unit, on the rig's fake clock (orchestrate and istat read
    the same one), with the CSV phase and the drains stubbed. Not a single-source dispatch, so the gate applies."""
    unit = _istat_unit()
    store = StateStore(str(rig.out.parent.parent / "state.db"))
    mp = monkeypatch
    mp.setattr(orchestrate, "time", rig.clock)
    mp.setattr(orchestrate, "hard_timeout_available", lambda: False)
    mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    mp.setattr(registry, "validate", lambda *a, **k: [])
    mp.setattr(registry, "all_units", lambda *a, **k: [unit])
    mp.setattr(orchestrate, "_has_adapter", lambda u: True)
    mp.setattr(orchestrate, "_wrong_location", lambda u: False)
    mp.setattr(orchestrate, "_protected", lambda u: False)
    mp.setattr(orchestrate, "_is_live", lambda u: True)
    mp.setattr(orchestrate, "_served_by_lane", lambda sid: False)
    mp.setattr(orchestrate, "_derive_changed_csvs", lambda unit, res, blob, st: ([], None, [], {}))
    mp.setattr(orchestrate, "_drain_csv_retry_queue", lambda *a, **k: None)
    mp.setattr(orchestrate, "_drain_leftover_retries", lambda *a, **k: None)
    mp.setattr(orchestrate, "get_strategy", lambda name: _Strat(rig.clock, probe_s))
    mp.setattr(fetchers, "get", lambda sid: istat)
    mp.setenv("AQUEDUCT_RUN_BUDGET_MIN", str(budget_min))
    orchestrate.run_once(store=store)
    runs = store.db.execute("SELECT status FROM runs WHERE source_id='istat' ORDER BY id").fetchall()
    return [r[0] for r in runs], store.get_unit("istat", "_all")


def test_run_once_a_ceiling_stop_before_any_flow_is_a_window_stopped_row_and_no_state(rig, monkeypatch):
    """Admitted with 21.3 min left (above the 21.2-min reserve); its probe planted at 70 s - MORE than the 1-min
    setup allowance - so update() starts with 20.13 min left, under the 20.2-min tail: it stops before any flow.
    run_once writes a `window_stopped` runs row and NO unit state (istat stays due, its order key moves back).
    Undeclared (origin/main), the gate refuses it."""
    rig.flows(5)
    statuses, st = _run_once(rig, monkeypatch, budget_min=21.3, probe_s=70.0)
    assert statuses == ["window_stopped"], statuses
    assert st is None, st


@pytest.mark.parametrize("rows", [True, False], ids=["stored_rows", "empty_flows"])
def test_run_once_a_host_dead_run_is_a_partial_turn_never_window_stopped(rig, monkeypatch, rows):
    """NEGATIVE CONTROL for the non-turn: the outage shape (sdmx redirect-loops, esploradati TCP-dead) inside a
    long pass. run_once records the `partial` turn - unit state with last_attempt and the outage note - and no
    `window_stopped` row. Holds on origin/main too; a rule that set "none" on this stop (or on empty cursors)
    fails it."""
    rig.flows(5, rows=rows)
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: base == SDMX)
    host = _Istat(rig.clock, 1.0, False, "redirect")
    rig.mp.setattr(istat.requests, "Session", lambda: host)
    statuses, st = _run_once(rig, monkeypatch, budget_min=240)
    assert "window_stopped" not in statuses and statuses == ["partial"], statuses
    assert st is not None and st["status"] == "partial" and st["last_attempt_utc"], st
    assert "every ISTAT host unusable" in (st.get("last_error") or ""), st


# ------------------------------------------------------------------ C2: the other rows of the signal table

STRUCT_BODY = b"<?xml version='1.0'?><nothing>" + b"x" * 400 + b"</nothing>"


class _PlanIstat(_Istat):
    """_Istat with a per-flow plan: 'structural' (a 200 whose real body parses 0 rows as CSV and as XML) or
    'timeout' (the GET runs to the timeout it was GIVEN, then ReadTimeout). Other flows: _Istat's behaviour."""

    def __init__(self, clock, plan, delay=60.0):
        super().__init__(clock, delay)
        self.plan = dict(plan)

    def get(self, url, headers=None, timeout=None):
        flow = url.split(f"{istat.AGENCY},", 1)[1].split("/", 1)[0]
        what = self.plan.get(flow)
        if what is None:
            return super().get(url, headers, timeout)
        self.gets += 1
        self.starts.setdefault(flow, self.clock.t)
        if what == "timeout":
            self.clock.t += float(timeout)
            raise requests.ReadTimeout("planted")
        self.clock.t += self.delay
        return _Resp(200, STRUCT_BODY)


def _run_plan(rig, plan, ceiling_s, delay=60.0):
    host = _PlanIstat(rig.clock, plan, delay)
    rig.mp.setattr(istat.requests, "Session", lambda: host)
    rig.mp.setattr(orchestrate, "_RUN_DEADLINE_TS", None if ceiling_s is None else rig.clock.time() + ceiling_s)
    return istat.update(None, None), host


def test_a_route_b_hand_over_after_a_ceiling_stop_is_progress(rig):
    """F03 structural, then the ceiling stops the run: ROUTE B returns `partial` with the walked cursors, and the
    signal is "progress" (a ceiling stop with flows started)."""
    rig.flows(8)
    res, host = _run_plan(rig, {"F03": "structural"}, 30 * 60.0)
    assert res.status == "partial" and "handed over" in res.error and "F03" in res.error, res.error
    assert res.window_stop == "progress", res.window_stop
    assert orchestrate._window_stop_no_progress(_istat_unit(), res) is False


def test_a_route_b_hand_over_without_a_window_stop_sets_no_signal(rig):
    """F03 structural, no run ceiling, every flow walked: the hand-over is a Result with window_stop None."""
    names = rig.flows(5)
    res, host = _run_plan(rig, {"F03": "structural"}, None, delay=1.0)
    assert list(host.starts) == names and res.status == "partial" and "handed over" in res.error, res.error
    assert res.window_stop is None, res.window_stop


def test_a_flow_cut_by_the_request_clamp_is_progress(rig):
    """Both hosts time out on F01, which starts with margin + floor + 1 s left: the clamp cuts it at ceiling -
    margin (PR A). It was started, so the signal is "progress" even with no cursor (empty flows). Four GETs had
    timed out at the hosts' own timeouts first, so the flow is TRANSIENT, not deferred (PR A, phase B review A1)."""
    rig.flows(3, rows=False)
    res, host = _run_plan(rig, {"F01": "timeout"}, 300 + FLOOR_S + 1.0)
    assert res.status == "partial", res.error
    assert "F01: 4 host attempt(s) failed, then the window ended at the run ceiling" in res.error, res.error
    assert not res.series_cursors
    assert res.window_stop == "progress", res.window_stop
    assert orchestrate._window_stop_no_progress(_istat_unit(), res) is False


# ------------------------------------------------------------------ C7: the per-turn cap

@pytest.mark.parametrize("override", ["360", "100000"])
def test_the_cap_ends_the_turn_as_a_ceiling_would_and_the_override_does_not_lift_it(rig, override, capsys):
    """AQUEDUCT_ISTAT_TURN_MAX_MIN=60, the run ceiling 10 h away: flows of 61 s start only while at least margin +
    max(2 x longest, floor) = 1,212 s are left to the cap, so 40 flows start and the turn returns ~41 min in. The
    stop is the ceiling stop's: deferred, the last flow named in the bookmark, window_stop "progress"."""
    rig.mp.setenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", override)
    rig.mp.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", "60")
    names = rig.flows(60)
    res, host = rig.run(delay=60.0, ceiling_s=10 * 3600.0)
    starts = list(host.starts)
    assert starts == names[:40], (len(starts), starts[-2:])
    assert max(host.starts.values()) <= 3600.0 - (300 + FLOOR_S), "a flow started inside the cap's tail"
    assert res.status == "partial" and "per-turn cap" in res.error and "deferred by budget" in res.error, res.error
    assert res.window_stop == "progress", res.window_stop
    out = capsys.readouterr().out
    assert "[istat] per-turn cap 60 min" in out and "[istat] per-turn cap:" in out, out[-1500:]
    assert _common.load_rotation(str(rig.out)) == "F40.parquet"
    res2, host2 = rig.run(delay=60.0, ceiling_s=10 * 3600.0)
    assert list(host2.starts)[:1] == ["F41"], list(host2.starts)[:2]


@pytest.mark.parametrize("raw", ["0", "off", None])
def test_control_no_cap_when_off_or_unset(rig, raw):
    """NEGATIVE CONTROL: "0" / "off" switch the cap off, and unset takes TURN_MAX_MIN_DEFAULT (planted None here;
    the shipped 60 is pinned below): every flow runs under a far ceiling."""
    if raw is not None:
        rig.mp.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", raw)
    rig.mp.setattr(istat, "TURN_MAX_MIN_DEFAULT", None)
    names = rig.flows(50)
    res, host = rig.run(delay=60.0, ceiling_s=10 * 3600.0)
    assert list(host.starts) == names and res.status == "ok", res.error


def test_a_bad_cap_value_takes_the_default_and_says_so(rig, capsys):
    rig.mp.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", "abc")
    rig.mp.setattr(istat, "TURN_MAX_MIN_DEFAULT", 60.0)
    rig.flows(50)
    res, host = rig.run(delay=60.0, ceiling_s=10 * 3600.0)
    assert "AQUEDUCT_ISTAT_TURN_MAX_MIN='abc' is not a finite number > 0 - using the default (60 min)" in \
        capsys.readouterr().out
    assert len(host.starts) == 40, len(host.starts)


def test_a_cap_under_the_reserve_is_raised_to_it_so_a_flow_can_start(rig, capsys):
    """A 5-min cap could never start a flow ("none" for ever): it is raised to the 21.2-min reserve, under which
    exactly one 61-s flow starts."""
    rig.mp.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", "5")
    names = rig.flows(5)
    res, host = rig.run(delay=60.0, ceiling_s=10 * 3600.0)
    assert "per-turn cap 5 min is under the 21.2-min reserve" in capsys.readouterr().out
    assert list(host.starts) == names[:1] and res.window_stop == "progress", (list(host.starts), res.window_stop)


def test_the_cap_bounds_a_turn_with_no_run_ceiling_and_clamps_its_requests(rig):
    """No run ceiling at all (outside run_once): the cap still ends the turn, and its requests are clamped to the
    cap's end minus the margin. Cap 25 min; F01 times out on both hosts: 120 + 300, 4, 120 + 300, 8, then 120 s
    and a 300-s GET clamped to the 228 s left - the flow is cut at 1,200 s, "progress". Five GETs had timed out at
    the hosts' own timeouts first, so it is TRANSIENT at the per-turn cap (phase B review A1), not deferred."""
    rig.mp.setenv("AQUEDUCT_ISTAT_TURN_MAX_MIN", "25")
    rig.flows(3)
    t0 = rig.clock.t
    res, host = _run_plan(rig, {"F01": "timeout"}, None)
    assert rig.clock.t - t0 <= 25 * 60.0 - 300.0 + 1.0, rig.clock.t - t0
    assert res.status == "partial", res.error
    assert "F01: 5 host attempt(s) failed, then the window ended at the per-turn cap" in res.error, res.error
    assert res.window_stop == "progress", res.window_stop


def test_the_shipped_default_caps_a_turn_at_60_min(rig, capsys):
    """TURN_MAX_MIN_DEFAULT = 60 (chosen from the C5 simulation, see istat.py): with the variable unset and the run
    ceiling 10 h away, a turn starts flows only while 1,212 s are left to its 60-min cap - 40 flows of 61 s."""
    assert istat.TURN_MAX_MIN_DEFAULT == 60.0
    rig.flows(60)
    res, host = rig.run(delay=60.0, ceiling_s=10 * 3600.0)
    assert len(host.starts) == 40 and res.window_stop == "progress", (len(host.starts), res.window_stop)
    assert "[istat] per-turn cap 60 min" in capsys.readouterr().out
