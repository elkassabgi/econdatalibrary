"""istat hands over its walked flows when a flow is structural, and clamps every request to the run window.

WHY (review of the istat window_stop plan, 2026-11-24, VERDICT_planA F3, conditions C1 and C8).
  C1. finalize() raises DefinitiveError on ANY structural flow, and the orchestrator then books `partial` with NO
      Result - no CSV step, no series cursors. On the state copy 9 of 40 istat runs since 2026-09-01 ended that
      way, and every run that attempted 69 or more flows did, except one in which every flow failed. So what a
      long run merged almost never reached the served CSVs. ROUTE B (census.py, hagstofa AR-407 O7): update()
      catches that raise and returns `partial` WITH the cursors of every walked flow, naming the structural ones.
  C8. Requests were not clamped to the run ceiling: one flow measured ~1,044 s on 2026-11-22, above the 912-s
      allowance, and two live hosts timing out cost 1,272 s. Every GET's timeout is now at most the time left to
      ceiling - margin; no GET or retry backoff starts after it; the clamp-cut GET is not counted against the host.
      A flow cut there is DEFERRED (not failed, not named in the bookmark) when it met no real failure, and
      TRANSIENT (named, health reads it as a failure) after at least one - the phase B review's A1: two hosts that
      hang gave four genuine timeouts before the cut, and the deferral read as a clean run in the stuck alarm.

Through the REAL istat.update() with a fake session and a fake clock (no network, no R2); one test goes through
the REAL run_once to show the cursors reach the CSV step. Every test that pins a new behaviour fails on origin/main
abfd77594; the negative controls hold on both.
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

from updater import health, orchestrate, registry  # noqa: E402
from updater.errors import DefinitiveError  # noqa: E402
from updater.state import StateStore  # noqa: E402
from updater.strategies.base import Strategy  # noqa: E402
from updater.strategies.fetchers import _common, istat  # noqa: E402

SDMX = "https://sdmx.istat.it/SDMXWS/rest/"
ESPLORA = "https://esploradati.istat.it/SDMXWS/rest/"
EPOCH = 1_800_000_000.0
MARGIN_S = 300.0                         # AQUEDUCT_ISTAT_MARGIN_MIN 5, set below
FLOOR_S = (istat.RETRIES * istat.HOST_TIMEOUT[ESPLORA]
           + sum(min(4 * a, 30) for a in range(1, istat.RETRIES)))          # 912 s, from the stated rule
STRUCT_BODY = b"<?xml version='1.0'?><nothing>" + b"x" * 400 + b"</nothing>"


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


class _Host:
    """Fake ISTAT. plan[flow]: 'ok' (200 + one row), 'empty' (404 NoRecordsFound), 'transient' (503),
    'structural' (a 200 whose real body parses 0 rows as CSV and as XML) or 'timeout' (the GET runs to the
    timeout it was GIVEN, then raises ReadTimeout). `slow[flow]` = fake seconds a 200 takes. Every GET is
    recorded as (flow, base, accept, timeout given, fake time at the start)."""

    def __init__(self, clock, plan=None, delay=1.0, slow=None):
        self.clock, self.plan, self.delay, self.slow = clock, dict(plan or {}), float(delay), dict(slow or {})
        self.gets: list = []

    def get(self, url, headers=None, timeout=None):
        flow = url.split(f"{istat.AGENCY},", 1)[1].split("/", 1)[0]
        base = SDMX if url.startswith(SDMX) else ESPLORA
        self.gets.append((flow, base, (headers or {}).get("Accept"), timeout, self.clock.t))
        what = self.plan.get(flow, "ok")
        if what == "timeout":
            self.clock.t += float(timeout)
            raise requests.ReadTimeout("planted: read timed out")
        self.clock.t += self.slow.get(flow, self.delay)
        if what == "ok":
            return _Resp(200, b"DATAFLOW,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE\n"
                              + f"IT1:{flow}(1.0),A,IT,2026,1.5\n".encode())
        if what == "empty":
            return _Resp(404, b"NoRecordsFound")
        if what == "structural":
            return _Resp(200, STRUCT_BODY)
        return _Resp(503, b"")


@pytest.fixture
def rig(tmp_path, monkeypatch):
    out = tmp_path / "data" / "clean_full" / "istat"
    out.mkdir(parents=True)
    clock = _Clock()
    for k in ("AQUEDUCT_BACKEND", "ISTAT_MAX_FLOWS", "ISTAT_BUDGET_MIN", "AQUEDUCT_LIVE_ONLY", "GITHUB_ACTIONS",
              "AQUEDUCT_UNIT_TIMEOUT_MIN"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", "360")
    monkeypatch.setenv("AQUEDUCT_ISTAT_MARGIN_MIN", "5")
    # PR #326's clamp arithmetic, without PR #327's CSV allowance (tests/test_istat_window_stop.py pins that)
    monkeypatch.setenv("AQUEDUCT_ISTAT_CSV_S_PER_FLOW", "0")
    monkeypatch.setattr(istat.config, "source_dir", lambda sid: str(out))
    monkeypatch.setattr(istat, "time", clock)
    monkeypatch.setattr(_common, "time", clock)
    monkeypatch.setattr(istat, "_tcp_reachable", lambda base: True)

    def _no_socket(*_a, **_k):
        raise AssertionError("a test opened a real socket")

    import socket
    monkeypatch.setattr(socket, "create_connection", _no_socket)
    monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None)
    for c in (istat._DEAD_HOSTS, istat._TRANSPORT_FAILS, istat._TCP_PROBED):
        c.clear()

    class Rig:
        pass

    r = Rig()
    r.out, r.clock, r.mp = out, clock, monkeypatch

    def flows(n, rows=True):
        for i in range(1, n + 1):
            pq.write_table(pa.table({
                "series_key": pa.array(["FREQ=A:REF_AREA=IT"] if rows else [], pa.string()),
                "obs_date": pa.array([dt.date(2020, 12, 31)] if rows else [], pa.date32()),
                "value": pa.array([1.0] if rows else [], pa.float64()),
            }), str(out / f"F{i:02d}.parquet"))
        return [f"F{i:02d}" for i in range(1, n + 1)]

    def run(host, ceiling_s=None):
        r.host = host
        monkeypatch.setattr(istat.requests, "Session", lambda: host)
        monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS", None if ceiling_s is None else clock.time() + ceiling_s)
        return istat.update(None, None)

    r.flows, r.run = flows, run
    r.bookmark = lambda: _common.load_rotation(str(out))
    yield r
    for c in (istat._DEAD_HOSTS, istat._TRANSPORT_FAILS, istat._TCP_PROBED):
        c.clear()


# ------------------------------------------------------------------ C1: the structural hand-over (ROUTE B)

@pytest.mark.parametrize("ceiling_s", [None, 30 * 60.0], ids=["no_ceiling", "ceiling_stop"])
def test_a_structural_flow_no_longer_loses_the_walked_flows(rig, ceiling_s):
    """F03 is structural (a 200 whose real body parses 0 rows), F05 transient. The run returns `partial` WITH the
    cursor of every flow it walked, and the note names F03 and says what else it left. On origin/main finalize()
    raised DefinitiveError here and nothing reached the CSV step."""
    names = rig.flows(8)
    host = _Host(rig.clock, {"F03": "structural", "F05": "transient"}, delay=60.0)
    res = rig.run(host, ceiling_s)
    walked = sorted({g[0] for g in host.gets})
    assert res.status == "partial", res.error
    assert sorted(res.series_cursors) == walked and "F03" in walked and "F05" in walked, res.series_cursors
    assert "F03: real body parsed 0 rows" in res.error and "handed over to the CSV step" in res.error, res.error
    assert "1 transient-failed" in res.error, res.error
    if ceiling_s is None:
        assert walked == names
    else:
        assert "deferred, not reached in this pass" in res.error, res.error


def test_negative_control_a_structural_flow_with_no_cursor_still_raises(rig):
    """Empty flow files seed no cursor, so there is nothing to hand over: the whole-source refusal stands, as
    before (and as census's ROUTE B)."""
    rig.flows(4, rows=False)
    plan = {"F01": "empty", "F02": "structural", "F03": "empty", "F04": "empty"}     # nothing merges either
    with pytest.raises(DefinitiveError, match="F02"):
        rig.run(_Host(rig.clock, plan))


def test_negative_control_the_all_empty_floor_still_raises(rig):
    """Every flow answers 404 NoRecordsFound over a large window: finalize()'s other raise. Nothing merged, so
    ROUTE B does not catch it (it hands over only when a STRUCTURAL flow raised)."""
    rig.flows(5)
    with pytest.raises(DefinitiveError, match="returned empty/404 over a large window"):
        rig.run(_Host(rig.clock, {f"F0{i}": "empty" for i in range(1, 6)}))


class _Strat(Strategy):
    name = "sim"

    def detect_change(self, unit, us):
        return "v-new"

    def run(self, unit, since=None):
        return istat.update(unit, since)


def test_run_once_derives_the_walked_flows_of_a_run_with_a_structural_flow(rig, monkeypatch):
    """Through the REAL run_once: the orchestrator's CSV step receives the walked flows. On origin/main the
    DefinitiveError reached run_once, which booked `partial` with no Result, and the CSV step never ran."""
    names = rig.flows(5)
    host = _Host(rig.clock, {"F02": "structural"})
    monkeypatch.setattr(istat.requests, "Session", lambda: host)
    (unit,) = [u for u in registry.all_units() if u.source_id == "istat"]
    store = StateStore(str(rig.out.parent.parent / "state.db"))
    derived = []
    mp = monkeypatch
    mp.setattr(orchestrate, "time", rig.clock)
    mp.setattr(orchestrate, "hard_timeout_available", lambda: False)
    mp.setattr(registry, "validate", lambda *a, **k: [])
    mp.setattr(registry, "all_units", lambda *a, **k: [unit])
    for name, fn in (("_has_adapter", lambda u: True), ("_wrong_location", lambda u: False),
                     ("_protected", lambda u: False), ("_is_live", lambda u: True),
                     ("_served_by_lane", lambda sid: False)):
        mp.setattr(orchestrate, name, fn)
    mp.setattr(orchestrate, "_derive_changed_csvs",
               lambda u, res, blob, st: (derived.append(sorted(res.series_cursors or {})), ([], None, [], {}))[1])
    mp.setattr(orchestrate, "_drain_csv_retry_queue", lambda *a, **k: None)
    mp.setattr(orchestrate, "_drain_leftover_retries", lambda *a, **k: None)
    mp.setattr(orchestrate, "get_strategy", lambda name: _Strat())
    mp.setenv("AQUEDUCT_RUN_BUDGET_MIN", "240")
    orchestrate.run_once(store=store)
    assert derived == [names], derived
    st = store.get_unit("istat", "_all")
    assert st["status"] == "partial" and "F02" in (st.get("last_error") or ""), st


# ------------------------------------------------------------------ C8: every request clamped to the window

def test_two_live_hosts_timing_out_no_longer_carry_a_flow_past_the_margin(rig):
    """The 1,272-s flow: both hosts live and every GET timing out (sdmx 120 s, esploradati 300 s, 3 passes, 4 + 8 s
    backoff). It starts with margin + floor + 1 s left - the first-flow check admits it - so unclamped it ends 359 s
    after ceiling - margin. Clamped, the run returns by ceiling - margin and the clamp-cut GET is not counted against
    the host. Four GETs had already timed out at the hosts' own timeouts, so the flow is TRANSIENT, not deferred
    (phase B review A1; this test pinned `deferred` before) and named in the bookmark like any flow whose hosts all
    failed (R784 (a)): the next run starts after it."""
    rig.flows(3)
    _common.save_rotation(str(rig.out), "F01.parquet")
    ceiling_s = MARGIN_S + FLOOR_S + 1.0
    res = rig.run(_Host(rig.clock, {"F02": "timeout"}), ceiling_s)
    assert rig.clock.t <= ceiling_s - MARGIN_S + 1.0, f"returned {rig.clock.t - (ceiling_s - MARGIN_S):.0f} s late"
    assert res.status == "partial" and "1/1 sub-unit(s) transient-failed" in res.error, res.error
    assert "F02: 4 host attempt(s) failed, then the window ended" in res.error, res.error
    assert health._stuck_class(res.status, res.error) == "fail", res.error
    assert rig.bookmark() == "F02.parquet", rig.bookmark()
    # 913 s to ceiling - margin: 120 + 300, backoff 4, 120 + 300, backoff 8 leave 61 s, so the 5th GET is given 61 s
    given = [g[3] for g in rig.host.gets]
    assert len(given) == 5 and given[:4] == [120, 300, 120, 300], given
    assert given[4] == pytest.approx(61.0, abs=1e-3), given
    assert istat._TRANSPORT_FAILS.get(SDMX, 0) == 2 and not istat._DEAD_HOSTS, (istat._TRANSPORT_FAILS,
                                                                                  istat._DEAD_HOSTS)
    res2 = rig.run(_Host(rig.clock), None)
    assert [g[0] for g in rig.host.gets][:1] == ["F03"], rig.host.gets[:2]
    assert res2.status == "ok", res2.error


class _Hang(_Host):
    """Every GET on the hosts in `hang` runs to the timeout it was GIVEN, then raises ReadTimeout; on sdmx.istat.it,
    when it is not in `hang`, every GET redirect-loops (its shape since 2026-08-23)."""

    def __init__(self, clock, hang=(SDMX, ESPLORA)):
        super().__init__(clock)
        self.hang = tuple(hang)

    def get(self, url, headers=None, timeout=None):
        base = SDMX if url.startswith(SDMX) else ESPLORA
        flow = url.split(f"{istat.AGENCY},", 1)[1].split("/", 1)[0]
        self.gets.append((flow, base, (headers or {}).get("Accept"), timeout, self.clock.t))
        if base not in self.hang:
            raise requests.TooManyRedirects("Exceeded 30 redirects.")
        self.clock.t += float(timeout)
        raise requests.ReadTimeout("planted: read timed out")


def test_two_hanging_hosts_with_the_first_flow_at_the_tail_read_as_a_failure_not_a_clean_deferral(rig):
    """THE REVIEWER'S PROBE (phase B review A1, review/probe_clamp.out), through the REAL update(): both hosts live
    and hanging, the first flow started with margin + floor + 0.5 s left. GETs 120, 300, 120, 300 (four genuine
    host timeouts), then the clamp cuts the fifth. Before: booked DEFERRED, the note "0 sub-unit(s) attempted, none
    failed; 1 deferred by budget", health's stuck class `success`, and the bookmark pinned at F01 turn after turn.
    Now: TRANSIENT, health reads `fail`, and the bookmark names F01, so the next turn does not repeat it."""
    rig.flows(5)
    ceiling_s = MARGIN_S + FLOOR_S + 0.5
    res = rig.run(_Hang(rig.clock), ceiling_s)
    timeouts = [g[3] for g in rig.host.gets]
    assert timeouts[:4] == [120, 300, 120, 300] and len(timeouts) == 5 and timeouts[4] < 120, timeouts
    assert res.status == "partial" and "1/1 sub-unit(s) transient-failed" in res.error, res.error
    assert "none failed" not in res.error and not health._is_clean_deferral_note(res.error), res.error
    assert health._stuck_class(res.status, res.error) == "fail", res.error
    assert rig.bookmark() == "F01.parquet", rig.bookmark()
    for c in (istat._DEAD_HOSTS, istat._TRANSPORT_FAILS, istat._TCP_PROBED):
        c.clear()
    res2 = rig.run(_Hang(rig.clock), ceiling_s)                   # the next turn starts after F01
    assert rig.host.gets[0][0] == "F02", rig.host.gets[:1]
    assert health._stuck_class(res2.status, res2.error) == "fail", res2.error


def test_control_one_hanging_host_fits_the_floor_and_is_transient_as_before(rig):
    """CONTROL (the reviewer's): sdmx redirect-loops (dead after one GET), esploradati hangs. The flow's three
    300-s GETs and 4 + 8 s of backoff fit the 912-s floor, so the clamp never cuts it: `every host failed`,
    transient, health `fail`, named in the bookmark. The same on 4083d833a."""
    rig.flows(5)
    ceiling_s = MARGIN_S + FLOOR_S + 0.5
    res = rig.run(_Hang(rig.clock, hang=(ESPLORA,)), ceiling_s)
    assert [g[3] for g in rig.host.gets if g[1] == ESPLORA] == [300, 300, 300], rig.host.gets
    assert res.status == "partial" and "F01: every host failed" in res.error, res.error
    assert "window ended" not in res.error, res.error
    assert health._stuck_class(res.status, res.error) == "fail", res.error
    assert rig.bookmark() == "F01.parquet", rig.bookmark()


def test_no_retry_backoff_starts_that_would_cross_the_margin(rig, monkeypatch):
    """The second retry's backoff is planted at 100 s while only 69 s are left to ceiling - margin: it is not
    started, and the run returns 69 s BEFORE ceiling - margin. A backoff slept anyway would return after it."""
    rig.flows(2)
    monkeypatch.setattr(istat, "_backoff_s", lambda attempt: 4.0 if attempt == 1 else 100.0)
    ceiling_s = MARGIN_S + FLOOR_S + 1.0
    res = rig.run(_Host(rig.clock, {"F01": "timeout"}), ceiling_s)
    assert rig.clock.t == pytest.approx(ceiling_s - MARGIN_S - 69.0, abs=1e-3), rig.clock.t
    assert len(rig.host.gets) == 4 and "backoff would reach the margin" in res.error, (rig.host.gets, res.error)


def test_every_get_ends_by_the_margin_before_the_ceiling(rig):
    """Property over a run: no GET is given more time than is left to ceiling - margin (floor 1 s), and none
    STARTS after it. F01 answers in 1 s, so the 2 x longest term stays small and F02 - both hosts timing out, 1,272 s
    unclamped - starts with 1,310 s left (>= margin + floor). On origin/main its GETs kept the host timeouts (120 /
    300 s) and ran 359 s past ceiling - margin."""
    rig.flows(6)
    ceiling_s = MARGIN_S + FLOOR_S + 100.0
    rig.run(_Host(rig.clock, {f"F0{i}": "timeout" for i in range(2, 7)}), ceiling_s)
    req_end = ceiling_s - MARGIN_S
    assert len({g[0] for g in rig.host.gets}) == 2, "premise: F01 and F02 were started"
    for flow, base, _acc, timeout, t in rig.host.gets:
        assert t < req_end, (flow, base, t)
        assert timeout <= max(1.0, req_end - t) + 1e-6, (flow, base, timeout, req_end - t)


def test_the_xml_fallback_is_not_started_past_the_margin_and_the_flow_is_deferred_not_structural(rig):
    """THE XML PATH. sdmx redirect-loops (dead after one GET); esploradati answers F01's CSV with a 200 whose body
    parses 0 rows, slowly (950 s), so no time is left for the XML GET. The flow is DEFERRED: never booked
    structural on the CSV body alone, finalize() does not raise, and the next run starts at F01.
    ALSO A1's NEGATIVE CONTROL: the cut flow met no real failure - the redirect loop is not one (_failed_for_real) -
    so it stays a clean deferral (health `success`), not transient."""
    rig.flows(3)
    ceiling_s = MARGIN_S + FLOOR_S + 1.0

    class _H(_Host):
        def get(self, url, headers=None, timeout=None):
            if url.startswith(SDMX):
                self.gets.append(("F01", SDMX, None, timeout, self.clock.t))
                raise requests.TooManyRedirects("Exceeded 30 redirects.")
            return super().get(url, headers, timeout)

    host = _H(rig.clock, {"F01": "structural"}, slow={"F01": 950.0})
    res = rig.run(host, ceiling_s)
    accepts = [g[2] for g in host.gets if g[1] == ESPLORA]
    assert accepts == [istat.CSV_ACCEPT], accepts              # the XML GET was not started
    assert res.status == "partial" and "cut at the window end" in res.error, res.error
    assert "structural" not in res.error and "parsed 0 rows" not in res.error, res.error
    assert "transient-failed" not in res.error and "none failed" in res.error, res.error
    assert health._stuck_class(res.status, res.error) == "success", res.error
    assert rig.bookmark() in ("", None), rig.bookmark()


def test_control_with_no_ceiling_every_get_keeps_its_host_timeout(rig):
    """NEGATIVE CONTROL for the clamp: with no run ceiling nothing is clamped (sdmx 120 s, esploradati 300 s) and
    the timing-out flow runs its full retry allowance as before. Holds on origin/main too."""
    rig.flows(2)
    res = rig.run(_Host(rig.clock, {"F01": "timeout"}), None)
    assert sorted({(g[1], g[3]) for g in rig.host.gets}) == [(ESPLORA, 300), (SDMX, 120)], rig.host.gets
    assert res.status == "partial" and "F01: every host failed" in res.error, res.error
