"""istat stops at the run ceiling, keeps its place on a kill, and says what it is doing.

WHAT HAPPENED (local pass of 2026-11-22, diagnosis and review in ledger R2093). istat started at
~23:28Z with 178 min left before the run ceiling. tools/run_local_heavy.ps1 sets
AQUEDUCT_BUDGET_MIN_OVERRIDE=360, which REPLACES istat's 30-min Deadline, and the Deadline is the
only bound istat had. esploradati answered slowly, so the host-death counter never fired. istat
merged 451 flows in 183 min, printed no line, and was killed by the pass's hard stop at 02:36:35Z.
The kill returned no Result, and the rotation bookmark, saved only AFTER the loop, did not move,
so the next run starts at the same flow. eia, queued behind istat, got no turn.

WHAT THESE TESTS PIN, through the REAL istat.update() with a fake session and a fake clock (no
network, no server, no R2):
  1. With the override at 360 and a ceiling 30 min away, no flow STARTS once the time left is
     below margin + max(2 x the longest flow so far, one flow's retry allowance), and the run ends
     `partial` with a DEFERRAL (not a failure) before the ceiling. A second run resumes where the
     first stopped.
  2. The 2 x longest term is live: a flow that the floor alone would admit is refused.
  3. A kill (an exception) in flow 5 leaves the bookmark at flow 4, the last flow whose attempt
     FINISHED, whatever its outcome. The next run starts at flow 5, so the killed flow re-runs
     and nothing is skipped (R273, R784).
  4. CONTROL: with no ceiling (or a far one) the run walks every flow exactly as before.
  5. A progress line every 50 flows or 5 minutes.
  6. (review of PR #325, F1) The flow in which the last usable host dies is NOT named: on the
     2026-11-19/20 shape (sdmx redirect-loops, esploradati fails its TCP probe) the bookmark stays,
     and the next run starts at that flow.
  7. (F2, R784 (b)) The LAST flow before a ceiling stop is named whether it was transient,
     structural or tripped the merge guard, and the next run starts after it.

The expected bound is written here from the stated rule and istat's existing constants (RETRIES,
HOST_TIMEOUT), not read from the new code, so this file runs unchanged against the old fetcher.
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

from updater import orchestrate  # noqa: E402  (puts updater.orchestrate in sys.modules)
from updater.errors import DefinitiveError  # noqa: E402
from updater.strategies.fetchers import _common, istat  # noqa: E402

SDMX = "https://sdmx.istat.it/SDMXWS/rest/"
ESPLORA = "https://esploradati.istat.it/SDMXWS/rest/"
EPOCH = 1_800_000_000.0          # the fake clock's epoch base
MARGIN_MIN = 5.0                 # AQUEDUCT_ISTAT_MARGIN_MIN, set explicitly below
MARGIN_S = MARGIN_MIN * 60
# One flow's retry allowance: every CSV GET on esploradati runs to its timeout, plus the backoff
# sleeps between the retry passes (min(4 x attempt, 30) s). With RETRIES 3 and 300 s: 912 s.
FLOOR_S = (istat.RETRIES * istat.HOST_TIMEOUT[ESPLORA]
           + sum(min(4 * a, 30) for a in range(1, istat.RETRIES)))


class _Kill(BaseException):
    """Stands in for the pass's hard stop arriving mid-flow."""


class _Clock:
    """One fake clock for time(), monotonic() and sleep(); anything else is the real module."""

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


def _csv(flow):
    return (b"DATAFLOW,FREQ,REF_AREA,TIME_PERIOD,OBS_VALUE\n"
            + f"IT1:{flow}(1.0),A,IT,2026,1.5\n".encode())


class _Istat:
    """Fake ISTAT. plan[flow] is 'ok' (200 + one row), 'empty' (404 NoRecordsFound), 'transient'
    (503), 'structural' (a 200 with a real body that parses 0 rows as CSV and as XML) or 'kill'.
    Every GET costs `delay` fake seconds. sdmx='redirect' makes every sdmx.istat.it GET raise
    TooManyRedirects, as that host does today. `starts` records each flow's FIRST GET time, in the
    order the flows were started; `gets` counts every GET."""

    def __init__(self, clock, plan=None, delay=60.0, sdmx="serve"):
        self.clock, self.plan, self.delay, self.sdmx = clock, dict(plan or {}), float(delay), sdmx
        self.starts: dict = {}
        self.gets = 0

    def get(self, url, headers=None, timeout=None):
        assert url.startswith(tuple(b for _l, b in istat.HOSTS)), url
        self.gets += 1
        flow = url.split(f"{istat.AGENCY},", 1)[1].split("/", 1)[0]
        self.starts.setdefault(flow, self.clock.t)
        if self.sdmx == "redirect" and url.startswith(SDMX):
            raise requests.TooManyRedirects("Exceeded 30 redirects.")
        what = self.plan.get(flow, "ok")
        if what == "kill":
            raise _Kill(flow)
        self.clock.t += self.delay
        if what == "ok":
            return _Resp(200, _csv(flow))
        if what == "empty":
            return _Resp(404, b"NoRecordsFound")
        if what == "structural":
            return _Resp(200, b"<?xml version='1.0'?><nothing>" + b"x" * 400 + b"</nothing>")
        return _Resp(503, b"")


@pytest.fixture
def rig(tmp_path, monkeypatch):
    out = tmp_path / "data" / "clean_full" / "istat"
    out.mkdir(parents=True)
    clock = _Clock()
    monkeypatch.delenv("AQUEDUCT_BACKEND", raising=False)          # local store under tmp_path
    monkeypatch.delenv("ISTAT_MAX_FLOWS", raising=False)
    monkeypatch.delenv("ISTAT_BUDGET_MIN", raising=False)
    monkeypatch.setenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", "360")      # what run_local_heavy.ps1 sets
    monkeypatch.setenv("AQUEDUCT_ISTAT_MARGIN_MIN", str(MARGIN_MIN))
    monkeypatch.setattr(istat.config, "source_dir", lambda sid: str(out))
    monkeypatch.setattr(istat, "time", clock)
    monkeypatch.setattr(_common, "time", clock)                    # Deadline reads the same clock
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

    def flows(n):
        for i in range(1, n + 1):
            pq.write_table(pa.table({
                "series_key": pa.array(["FREQ=A:REF_AREA=IT"], pa.string()),
                "obs_date": pa.array([dt.date(2020, 12, 31)], pa.date32()),
                "value": pa.array([1.0], pa.float64()),
            }), str(out / f"F{i:02d}.parquet"))
        return [f"F{i:02d}" for i in range(1, n + 1)]

    def run(plan=None, delay=60.0, ceiling_s=None, sdmx="serve"):
        """One real update(). ceiling_s: seconds from NOW on the fake clock, or None."""
        host = _Istat(clock, plan, delay, sdmx)
        r.last_host = host                       # readable even when update() raises
        monkeypatch.setattr(istat.requests, "Session", lambda: host)
        monkeypatch.setattr(orchestrate, "_RUN_DEADLINE_TS",
                            None if ceiling_s is None else clock.time() + ceiling_s)
        t0 = clock.t
        res = istat.update(None, None)
        return res, host, t0

    r.flows, r.run = flows, run
    r.bookmark = lambda: _common.load_rotation(str(out))
    yield r
    istat._DEAD_HOSTS.clear()
    istat._TRANSPORT_FAILS.clear()
    istat._TCP_PROBED.clear()


def _late(starts: dict, t0: float, ceiling_s: float, per_flow: float, before_get: float = 0.0) -> list:
    """Flows that started with less time left than the rule allows. Before flow i the longest flow
    so far is `per_flow` (every flow here costs the same) or 0 for the first flow of the run.
    `before_get`: fake seconds a flow spends before its first GET (the store read), so a flow's
    start is its first GET time minus that."""
    bad = []
    for i, (flow, t) in enumerate(starts.items()):
        need = MARGIN_S + max(2 * (per_flow if i else 0.0), FLOOR_S)
        left = ceiling_s - (t - before_get - t0)
        if left < need:
            bad.append((flow, round(left), round(need)))
    return bad


@pytest.mark.parametrize("override", ["360", "100000"])
def test_no_flow_starts_inside_the_margin_and_the_run_defers_before_the_ceiling(rig, override, capsys):
    rig.mp.setenv("AQUEDUCT_BUDGET_MIN_OVERRIDE", override)
    names = rig.flows(30)
    ceiling_s, delay = 30 * 60.0, 60.0
    per_flow = delay + istat.RATE                     # one GET + the RATE pause, in fake seconds
    res, host, t0 = rig.run(delay=delay, ceiling_s=ceiling_s)

    starts = host.starts
    assert starts, "no flow ran at all"
    assert _late(starts, t0, ceiling_s, per_flow) == [], (
        f"flows started inside margin + max(2 x longest, {FLOOR_S:.0f} s) of the ceiling "
        f"(override {override} min must not lift it)")
    k = len(starts)
    stop = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("[istat] run ceiling:")]
    assert len(stop) == 1 and f"{k} flow(s) attempted, {k} merged, {len(names) - k} deferred" in stop[0], stop
    last = max(starts.values()) - t0
    assert last <= ceiling_s - MARGIN_S, "a flow started inside the margin before the ceiling"
    # It does not stop early either: the next flow would have started inside the bound.
    assert ceiling_s - (last + per_flow) < MARGIN_S + max(2 * per_flow, FLOOR_S)
    assert len(starts) < len(names), "the ceiling stopped nothing"
    assert list(starts) == names[:len(starts)], "flows were not walked in rotation order"

    assert res.status == "partial", res.error
    assert "deferred by budget, not reached in this pass" in res.error, res.error
    assert "transient-failed" not in res.error, "a ceiling stop is a deferral, not a failure"
    assert rig.clock.t - t0 < ceiling_s, "the run returned after the ceiling"
    assert rig.bookmark() == list(starts)[-1] + ".parquet", "the bookmark is not the last flow tried"

    # THE WINDOW MOVES (R784 (b): a budget and a rotation are one mechanism, test them over runs).
    res2, host2, t1 = rig.run(delay=delay, ceiling_s=ceiling_s)
    second = list(host2.starts)
    assert second and second[0] == names[len(starts)], (
        f"run 2 began at {second[:1]}, not after run 1's last flow {list(starts)[-1]}")
    assert not set(second) & set(starts), "run 2 walked flows run 1 had already tried"
    assert _late(host2.starts, t1, ceiling_s, per_flow) == []


@pytest.mark.parametrize("where", ["get", "rate", "store", "save"])
def test_the_two_x_longest_term_stops_a_flow_the_floor_alone_would_start(rig, where):
    """Every flow costs 501 s of fake time. Four flows run; a fifth would start with 1,250 s left:
    above margin + floor (300 + 912 = 1,212 s) but below margin + 2 x longest (300 + 1,002 =
    1,302 s). Only the 2 x longest term refuses it. The 501 s sits in the GET, in the RATE pause, in
    the store read before the fetch (on r2 that read downloads the flow's whole object), or in the
    bookmark save (an R2 PUT on r2), so the `longest` span must cover all four: from the flow's
    checks to after its bookmark."""
    names = rig.flows(8)
    delay = {"get": 500.0, "rate": 1.0, "store": 0.0, "save": 0.0}[where]
    if where == "rate":
        rig.mp.setattr(istat, "RATE", 500.0)
    if where == "store":
        real_max_obs = istat._max_obs
        rig.mp.setattr(istat, "_max_obs", lambda path: (rig.clock.sleep(500.0), real_max_obs(path))[1])
    if where == "save":
        real_save = istat.save_rotation
        rig.mp.setattr(istat, "save_rotation",
                       lambda out_dir, key: (rig.clock.sleep(500.0), real_save(out_dir, key))[1])
    per_flow = 501.0
    ceiling_s = 4 * per_flow + 1250.0
    assert MARGIN_S + FLOOR_S <= 1250.0 < MARGIN_S + 2 * per_flow      # the case is the one described
    res, host, t0 = rig.run(delay=delay, ceiling_s=ceiling_s)
    assert list(host.starts) == names[:4], list(host.starts)
    assert _late(host.starts, t0, ceiling_s, per_flow, 500.0 if where == "store" else 0.0) == []
    assert res.status == "partial" and "deferred by budget" in res.error, res.error


@pytest.mark.parametrize("odd", [True, False, float("nan"), float("inf"), "1800000000"])
def test_an_odd_ceiling_value_switches_the_check_off_as_in_census_and_wid(rig, odd):
    """CONTROL for the value guard: a bool, NaN, inf or a string in _RUN_DEADLINE_TS is not a
    ceiling, so every flow runs (True would otherwise read as epoch 1 s and stop at once). The
    same rule as census.py, wid.py and imf_imts_direct.py."""
    names = rig.flows(6)
    host = _Istat(rig.clock, None, 60.0)
    rig.mp.setattr(istat.requests, "Session", lambda: host)
    rig.mp.setattr(orchestrate, "_RUN_DEADLINE_TS", odd)
    res = istat.update(None, None)
    assert list(host.starts) == names and res.status == "ok", (list(host.starts), res.error)


@pytest.mark.parametrize("bad", ["abc", "-1", "nan"])
def test_a_bad_margin_value_falls_back_to_five_minutes(rig, bad, capsys):
    """A bad AQUEDUCT_ISTAT_MARGIN_MIN must not remove the margin: it falls back to 5 min and says
    so, and the run stops exactly where the 5-min default stops it (10 flows of 61 s, 30-min
    ceiling: see the first test)."""
    rig.mp.setenv("AQUEDUCT_ISTAT_MARGIN_MIN", bad)
    rig.flows(30)
    res, host, t0 = rig.run(delay=60.0, ceiling_s=30 * 60.0)
    out = capsys.readouterr().out
    assert f"AQUEDUCT_ISTAT_MARGIN_MIN={bad!r} is not a finite number >= 0 - using 5 min" in out, out
    assert _late(host.starts, t0, 30 * 60.0, 61.0) == []
    assert len(host.starts) == 10 and res.status == "partial", (len(host.starts), res.error)


def test_a_kill_in_flow_5_leaves_the_bookmark_at_flow_4_whatever_its_outcome(rig):
    """The bookmark names the last flow whose attempt FINISHED, on ANY outcome (R784: a bookmark
    that moves only on success lets one failing flow pin everything behind it). Flow 4 here is an
    EMPTY window and flow 2 a transient failure, so a bookmark that advances only on `ok` stops at
    flow 3. Flow 5 is cut mid-attempt: it is not named, so the next run starts AT it - the killed
    flow re-runs and nothing is skipped. The run starts mid-list (bookmark F03) so an unmoved
    bookmark cannot pass by accident."""
    rig.flows(12)
    _common.save_rotation(str(rig.out), "F03.parquet")
    plan = {"F04": "ok", "F05": "transient", "F06": "ok", "F07": "empty", "F08": "kill"}
    with pytest.raises(_Kill):
        rig.run(plan=plan, delay=1.0, ceiling_s=None)
    assert rig.bookmark() == "F07.parquet", (
        f"after a kill in flow 5 (F08) the bookmark reads {rig.bookmark()!r}: expected flow 4 (F07)")

    # The next run starts at the killed flow and walks on.
    res, host, _t0 = rig.run(delay=1.0, ceiling_s=None)
    assert list(host.starts)[:2] == ["F08", "F09"], list(host.starts)
    assert res.status == "ok", res.error


@pytest.mark.parametrize("ceiling_s", [None, 10 * 24 * 3600.0])
def test_control_without_a_near_ceiling_every_flow_runs_as_before(rig, ceiling_s):
    """NEGATIVE CONTROL for the ceiling check: no ceiling, or one ten days away. The run takes
    30 x 61 s = 30.5 min of fake time - longer than the 30-min ceiling of the first test - and
    walks every flow, ends `ok` with no deferral, and leaves the bookmark at the last flow. This
    passes on the fetcher before this change too."""
    names = rig.flows(30)
    res, host, t0 = rig.run(delay=60.0, ceiling_s=ceiling_s)
    assert list(host.starts) == names
    assert rig.clock.t - t0 > 30 * 60.0
    assert res.status == "ok", res.error
    assert "deferred" not in (res.error or "")
    assert rig.bookmark() == "F30.parquet"


def test_a_progress_line_every_five_minutes_and_every_fifty_flows(rig, capsys):
    """2026-11-22: 188 min and 451 merged flows with not one line, so a working sweep read as a hang.
    A: 12 flows at 61 s, no ceiling -> a line after flow 5 (305 s) and flow 10 (610 s).
    B: 55 flows at 1 s each (RATE only) -> 55 s in all, so only the 50-flow line."""
    rig.flows(12)
    rig.run(delay=60.0, ceiling_s=None)
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("[istat] progress:")]
    assert len(lines) == 2, lines
    assert "5/12 flow(s) attempted, 5 merged" in lines[0] and ", 7 left;" in lines[0], lines[0]
    assert "10/12 flow(s) attempted, 10 merged" in lines[1] and ", 2 left;" in lines[1], lines[1]

    for p in rig.out.glob("*.parquet"):
        p.unlink()
    _common.save_rotation(str(rig.out), "")
    rig.flows(55)
    rig.run(delay=0.0, ceiling_s=None)
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.startswith("[istat] progress:")]
    assert len(lines) == 1 and "50/55 flow(s) attempted, 50 merged" in lines[0], lines


@pytest.mark.parametrize("shape", ["sdmx_redirects_esploradati_tcp_dead", "both_tcp_dead"])
def test_the_flow_in_which_the_last_host_dies_is_not_named(rig, shape, capsys):
    """Review of PR #325, F1. The 2026-11-19/20 shape: sdmx.istat.it answers its one GET with a
    redirect loop and is marked dead, esploradati fails its TCP probe, and the run stops "after 1
    flow(s) attempted". No working host was asked about that flow (F04), so the bookmark must stay at
    F03 and the next run must start AT F04 - not pass it and leave it a whole rotation. The second
    shape asks no host at all (both TCP probes fail)."""
    rig.flows(6)
    _common.save_rotation(str(rig.out), "F03.parquet")
    reachable = {"sdmx_redirects_esploradati_tcp_dead": {SDMX}, "both_tcp_dead": set()}[shape]
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: base in reachable)
    res, host, _t0 = rig.run(delay=1.0, ceiling_s=None, sdmx="redirect")
    out = capsys.readouterr().out
    assert "stopping after 1 flow(s) attempted" in out, out
    assert host.gets == (1 if reachable else 0), f"premise: {host.gets} GET(s)"
    assert res.status == "partial", res.error
    assert rig.bookmark() == "F03.parquet", (
        f"the bookmark moved to {rig.bookmark()!r} on a night no working host was asked anything")

    # ISTAT is back: the next run starts at the flow the outage cut.
    rig.mp.setattr(istat, "_tcp_reachable", lambda base: True)
    res2, host2, _t1 = rig.run(delay=1.0, ceiling_s=None)
    assert list(host2.starts)[:2] == ["F04", "F05"], list(host2.starts)
    assert res2.status == "ok", res2.error


@pytest.mark.parametrize("outcome", ["transient", "structural", "merge_guard"])
def test_a_failing_last_flow_before_the_ceiling_stop_is_named_and_run_2_starts_after_it(rig, outcome):
    """Review of PR #325, F2; R784 (b): a budget and a rotation are one mechanism, so test a sub-unit
    that both FAILS and is the last before the stop, over two runs sharing one bookmark. With the
    first test's shape (61-s flows, a 30-min ceiling) flows F01-F10 start; here F10 is transient (503
    from both hosts on every attempt), structural (a 200 whose real body parses 0 rows) or trips the
    merge guard. The ceiling stop follows it. The bookmark must name F10 and run 2 must start at F11.
    A save skipped on that outcome leaves F09, and run 2 walks F10 again."""
    names = rig.flows(30)
    plan = {"F10": outcome} if outcome != "merge_guard" else {}
    if outcome == "merge_guard":
        real_merge = istat.merge.merge_and_write

        def merge_and_write(path, tbl, **kw):
            if path.endswith("F10.parquet"):
                raise RuntimeError("guard: would shrink")
            return real_merge(path, tbl, **kw)

        rig.mp.setattr(istat.merge, "merge_and_write", merge_and_write)
    if outcome == "structural":
        # finalize() raises on a structural flow (no Result); the bookmark was saved before that.
        with pytest.raises(DefinitiveError, match="F10"):
            rig.run(plan=plan, delay=60.0, ceiling_s=30 * 60.0)
    else:
        res, _host, _t0 = rig.run(plan=plan, delay=60.0, ceiling_s=30 * 60.0)
        assert res.status == "partial", res.error
    assert list(rig.last_host.starts) == names[:10], list(rig.last_host.starts)
    assert rig.bookmark() == "F10.parquet", (
        f"the last flow before the stop was {outcome} and the bookmark reads {rig.bookmark()!r}")
    _res2, host2, _t1 = rig.run(delay=60.0, ceiling_s=30 * 60.0)
    assert list(host2.starts)[:1] == ["F11"], list(host2.starts)[:3]


@pytest.mark.parametrize("extra, n_started", [(-0.5, 0), (0.5, 1)])
def test_the_floor_is_912_s_with_the_backoff(rig, extra, n_started):
    """The first flow of a run starts only with margin + FLOOR_S (300 + 912 s) left, where 912 s is
    RETRIES x 300 s PLUS the 4 s and 8 s backoff sleeps. Half a second under it: no flow starts and
    the run defers; half a second over: exactly one 61-s flow starts."""
    names = rig.flows(5)
    res, host, _t0 = rig.run(delay=60.0, ceiling_s=MARGIN_S + FLOOR_S + extra)
    assert list(host.starts) == names[:n_started], list(host.starts)
    assert res.status == "partial" and "deferred by budget" in res.error, res.error
