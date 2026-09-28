"""derive_and_put must END when the orchestrator's hard fence fires - and say honestly what it did (review R1246).

Before: SIGALRM raised UnitTimeout in the main thread, but (a) the thread pool's `with` exit waited for every running
worker AND ran the queued ids (a 1 s alarm surfaced at 12 s behind one wedged PUT), and (b) on the serial path
_put_with_retry's `except Exception` caught it, slept, retried, and derive returned normally - the handler never ran.

The alarm is EMULATED here (Windows has no SIGALRM): a SIGINT handler that raises the real orchestrate.UnitTimeout,
fired by _thread.interrupt_main() from a timer - the same delivery (main thread, between bytecodes) as SIGALRM.
"""
from __future__ import annotations
import _thread
import os
import signal
import sys
import threading
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from updater import derive, orchestrate  # noqa: E402


class _Blob:
    """put_atomic sleeps in 0.05 s slices (so the main thread can take the alarm) when told to wedge an id."""

    def __init__(self, wedge=(), wedge_s=10.0):
        self.wedge, self.wedge_s, self.put = set(wedge), wedge_s, []
        self.lock = threading.Lock()

    def put_atomic(self, key, body, **kw):
        if any(w in key for w in self.wedge):
            t = time.monotonic()
            while time.monotonic() - t < self.wedge_s:
                time.sleep(0.05)
        with self.lock:
            self.put.append(key)


@pytest.fixture
def alarm(monkeypatch):
    """alarm(seconds): after `seconds`, run the orchestrator's REAL handler body (_deliver_alarm) in the main thread -
    so inside a wait slice it is deferred exactly as SIGALRM's would be (review R1262)."""
    prev = signal.getsignal(signal.SIGINT)
    monkeypatch.setattr(orchestrate, "_DEFER_ALARM", False)
    monkeypatch.setattr(orchestrate, "_ALARM_PENDING", None)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)

    def _fire(signum, frame):
        orchestrate._deliver_alarm("emulated SIGALRM (test)", 0)
    signal.signal(signal.SIGINT, _fire)
    timers = []

    def arm(seconds):
        t = threading.Timer(seconds, _thread.interrupt_main)
        t.daemon = True
        t.start()
        timers.append(t)
    yield arm
    for t in timers:
        t.cancel()
    signal.signal(signal.SIGINT, prev)


@pytest.fixture(autouse=True)
def _fake_csv(monkeypatch):
    monkeypatch.setattr(derive, "_series_csv_bytes", lambda sid: f"series_id,obs_date,value\n{sid},2024-01-01,1\n".encode())
    monkeypatch.delenv("AQUEDUCT_DERIVE_BUDGET_MIN", raising=False)


def _heartbeats_alive():
    return [t for t in threading.enumerate()
            if getattr(getattr(t, "_target", None), "__name__", "") == "_heartbeat" and t.is_alive()]


def test_the_pool_path_ends_at_the_fence_not_behind_a_wedged_put(monkeypatch, alarm):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    blob = _Blob(wedge={"wedged"}, wedge_s=10.0)
    ids = ["zz:wedged"] + [f"zz:q{i}" for i in range(40)]      # more than the 4x-workers queue
    alarm(1.0)
    t0 = time.monotonic()
    with pytest.raises(orchestrate.UnitTimeout) as trip:
        derive.derive_and_put(ids, blob, budget_min=0)
    took = time.monotonic() - t0
    assert took < 4.0, f"the fence surfaced after {took:.1f} s - it waited for the wedged PUT"
    part = trip.value.derive_partial
    assert "zz:wedged" not in part["put_ids"], "a PUT that never finished was booked as derived"
    assert set(part["put_ids"]) <= set(ids)
    assert len(part["put_ids"]) < len(ids), "the queued ids ran after the trip"


def test_the_serial_path_lets_the_fence_through_instead_of_retrying(monkeypatch, alarm):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    blob = _Blob(wedge={"wedged"}, wedge_s=10.0)
    alarm(1.0)
    t0 = time.monotonic()
    with pytest.raises(orchestrate.UnitTimeout) as trip:
        derive.derive_and_put(["zz:wedged", "zz:after"], blob, budget_min=0)
    assert time.monotonic() - t0 < 4.0
    assert trip.value.derive_partial["put_ids"] == [], trip.value.derive_partial
    assert not any("after" in k for k in blob.put), "derive carried on after the fence"


def test_a_wedged_DERIVE_on_the_serial_path_lets_the_fence_through(monkeypatch, alarm):
    """The resolve/derive step has its own `except Exception` (a store-coverage gap is booked failed) - it must not
    book the fence as one more failed id and carry on."""
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")

    def slow_csv(sid):
        if "wedged" in sid:
            t = time.monotonic()
            while time.monotonic() - t < 10:
                time.sleep(0.05)
        return b"series_id,obs_date,value\n"
    monkeypatch.setattr(derive, "_series_csv_bytes", slow_csv)
    blob = _Blob()
    alarm(1.0)
    t0 = time.monotonic()
    with pytest.raises(orchestrate.UnitTimeout):
        derive.derive_and_put(["zz:wedged", "zz:after"], blob, budget_min=0)
    assert time.monotonic() - t0 < 4.0 and blob.put == [], blob.put


def test_after_a_trip_the_queued_ids_do_not_run_in_the_background(monkeypatch, alarm):
    """cancel_futures: without it the ~15 ids already queued in the pool keep running after the phase ended."""
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")

    class _SlowBlob(_Blob):
        def put_atomic(self, key, body, **kw):
            time.sleep(0.3)                      # every PUT takes a while, so the queue is still full at the trip
            super().put_atomic(key, body, **kw)
    blob = _SlowBlob()
    ids = [f"zz:s{i}" for i in range(60)]
    alarm(1.0)
    with pytest.raises(orchestrate.UnitTimeout):
        derive.derive_and_put(ids, blob, budget_min=0)
    at_trip = len(blob.put)
    time.sleep(2.0)
    assert len(blob.put) - at_trip <= 4, f"{len(blob.put) - at_trip} PUTs ran after the trip (the running workers are 4)"


def test_the_heartbeat_stops_with_the_phase(monkeypatch, alarm):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    before = len(_heartbeats_alive())
    alarm(0.5)
    with pytest.raises(orchestrate.UnitTimeout):
        derive.derive_and_put(["zz:wedged", "zz:b", "zz:c"], _Blob(wedge={"wedged"}), budget_min=0)
    time.sleep(0.3)
    assert len(_heartbeats_alive()) == before, "derive's heartbeat outlived the tripped phase"


def test_a_put_that_finished_before_the_fence_is_reported_as_put(monkeypatch, alarm):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    blob = _Blob(wedge={"wedged"}, wedge_s=10.0)
    alarm(1.0)
    with pytest.raises(orchestrate.UnitTimeout) as trip:
        derive.derive_and_put(["zz:first", "zz:wedged", "zz:third"], blob, budget_min=0)
    assert trip.value.derive_partial["put_ids"] == ["zz:first"], trip.value.derive_partial


def test_negative_control_without_a_fence_everything_is_derived(monkeypatch):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    ids = [f"zz:n{i}" for i in range(12)]
    out = derive.derive_and_put(ids, _Blob(), budget_min=0)
    assert out["put"] == 12 and sorted(out["put_ids"]) == sorted(ids) and out["failed"] == []


def test_the_heartbeat_stops_after_a_normal_run_too(monkeypatch):
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    before = len(_heartbeats_alive())
    derive.derive_and_put(["zz:a", "zz:b"], _Blob(), budget_min=0)
    time.sleep(0.3)
    assert len(_heartbeats_alive()) == before


# ---- review R1254: the alarm inside wait(), and the alarm in DuckDB's clothes ----------------------------------------

def test_an_alarm_inside_a_wait_slice_is_raised_just_after_it(monkeypatch):
    """R1254 item 2 / R1262: an alarm raised inside wait()'s lock-taking __enter__ left condition locks held and the
    process hung at exit. The handler body lands INSIDE wait here (deterministically): it must NOT raise there, and
    derive must raise the trip as soon as that wait returns - with its partial, and the protocol reset after."""
    import concurrent.futures as cf
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "4")
    monkeypatch.setattr(orchestrate, "_DEFER_ALARM", False)
    monkeypatch.setattr(orchestrate, "_ALARM_PENDING", None)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    real, calls, raised_inside = cf.wait, [0], []

    def landing(*a, **k):
        calls[0] += 1
        if calls[0] == 3:
            try:
                orchestrate._deliver_alarm("landed inside wait (test)", 0)
            except orchestrate.UnitTimeout:
                raised_inside.append(True)
                raise
        return real(*a, **k)
    monkeypatch.setattr(cf, "wait", landing)
    with pytest.raises(orchestrate.UnitTimeout, match="landed inside wait") as trip:
        derive.derive_and_put([f"zz:w{i}" for i in range(40)], _Blob(), budget_min=0)
    assert not raised_inside, "the alarm raised INSIDE wait() - the lock-taking window"
    assert calls[0] == 3, f"derive went on waiting after the trip ({calls[0]} waits)"
    assert trip.value.derive_partial is not None
    assert orchestrate._DEFER_ALARM is False and orchestrate._ALARM_PENDING is None


def test_negative_control_outside_a_wait_slice_the_alarm_raises_at_once(monkeypatch):
    monkeypatch.setattr(orchestrate, "_DEFER_ALARM", False)
    monkeypatch.setattr(orchestrate, "_ALARM_PENDING", None)
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)
    with pytest.raises(orchestrate.UnitTimeout):
        orchestrate._deliver_alarm("outside (test)", 0)
    assert orchestrate.UNIT_TIMEOUT_FIRED is True and orchestrate._ALARM_PENDING is None


def test_the_unit_deadline_resets_the_deferral_state(monkeypatch):
    """A trip held for one unit must never surface in the next one: reset on ENTRY and on EXIT."""
    monkeypatch.setattr(orchestrate, "_DEFER_ALARM", True)
    monkeypatch.setattr(orchestrate, "_ALARM_PENDING", orchestrate.UnitTimeout("stale (test)"))
    with orchestrate._unit_deadline("next unit (test)", 0):
        assert orchestrate._DEFER_ALARM is False and orchestrate._ALARM_PENDING is None      # entry
        orchestrate._DEFER_ALARM = True                                   # held INSIDE this unit ...
        orchestrate._ALARM_PENDING = orchestrate.UnitTimeout("held (test)")
    assert orchestrate._DEFER_ALARM is False and orchestrate._ALARM_PENDING is None          # ... gone at exit


_REAL_SIGNAL_PROBE = r'''
import concurrent.futures._base as B, os, signal, sys, time
sys.path.insert(0, sys.argv[1])
from updater import derive, orchestrate as O
derive._series_csv_bytes = lambda sid: b"series_id,obs_date,value\n"
CALLS = [0]
_orig = B._AcquireFutures.__enter__
def _enter(self):
    CALLS[0] += 1
    futs = list(self.futures)
    half = len(futs) // 2
    for f in futs[:half]:
        f._condition.acquire()
    if CALLS[0] == 3 and len(futs) >= 2:
        signal.setitimer(signal.ITIMER_REAL, 0.02)          # a REAL timer alarm, while half the locks are held
        end = time.monotonic() + 0.3
        while time.monotonic() < end:
            pass
    for f in futs[half:]:
        f._condition.acquire()
B._AcquireFutures.__enter__ = _enter
class Blob:
    def put_atomic(self, key, body, **kw):
        time.sleep(0.05)
os.environ["AQUEDUCT_DERIVE_WORKERS"] = "4"
with O._unit_deadline("probe", 60):                         # installs the REAL handler
    try:
        derive.derive_and_put([f"zz:q{i}" for i in range(60)], Blob(), budget_min=0)
        print("RETURNED")
    except O.UnitTimeout:
        print("TRIPPED")
'''


@pytest.mark.skipif(not hasattr(signal, "setitimer"), reason="POSIX only: no setitimer, no fence (CI runs it)")
@pytest.mark.parametrize("run", range(5))
def test_a_real_alarm_inside_wait_ends_the_process(tmp_path, run):
    """R1262: the real handler, a real setitimer alarm landing inside wait()'s __enter__ with half the locks held
    and a 300 ms window; five runs, each in its own process, each must END (the unmasked design hung at exit)."""
    import subprocess
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    r = subprocess.run([sys.executable, "-B", "-c", _REAL_SIGNAL_PROBE, root], capture_output=True, text=True,
                       timeout=60, stdin=subprocess.DEVNULL)
    assert "TRIPPED" in r.stdout, (r.returncode, r.stdout[-500:], r.stderr[-1500:])


def test_a_wait_slice_off_posix_or_off_the_main_thread_is_a_plain_wait(monkeypatch):
    """Where there is no SIGALRM (Windows) the slice is a plain 1 s wait - and it still returns what finished."""
    import concurrent.futures as cf
    with cf.ThreadPoolExecutor(1) as ex:
        f = ex.submit(lambda: 7)
        done, _ = derive._wait_slice({f: "x"}, 5.0)
    assert f in done and f.result() == 7


def _fire(monkeypatch):
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", True)


@pytest.mark.parametrize("where", ["series", "flow_rows", "flow_stream"])
def test_an_error_after_the_alarm_fired_is_the_fence(monkeypatch, where):
    """R1254 item 6: DuckDB takes the signal and raises RuntimeError('Query interrupted'); `except fence` missed it
    and the id was booked failed while the phase carried on. Once UNIT_TIMEOUT_FIRED is set, it is the fence."""
    import core.derive_csv as dc
    import econdl._resolve as rs
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)

    def interrupted(*a, **k):
        _fire(monkeypatch)
        raise RuntimeError("Query interrupted")
    monkeypatch.setattr(rs, "resolve", lambda sid: sid)
    monkeypatch.setattr(dc, "resolved_paths", interrupted if where == "flow_rows" else (lambda r: []))
    monkeypatch.setattr(dc, "_series_csv_to_file_sorted", interrupted)
    if where == "series":
        monkeypatch.setattr(derive, "_series_csv_bytes", interrupted)
    blob = _Blob()
    with pytest.raises(orchestrate.UnitTimeout, match="RuntimeError") as trip:
        derive.derive_and_put(["zz:x", "zz:after"], blob, budget_min=0, flow_grain=(where != "series"))
    assert trip.value.derive_partial["put_ids"] == [] and trip.value.derive_partial["failed"] == [], \
        trip.value.derive_partial
    assert blob.put == [], "derive carried on after the fence"


@pytest.mark.parametrize("flow", [False, True])
def test_negative_control_the_same_error_without_the_alarm_is_one_failed_id(monkeypatch, flow):
    import core.derive_csv as dc
    import econdl._resolve as rs
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(orchestrate, "UNIT_TIMEOUT_FIRED", False)

    def broken(*a, **k):
        raise RuntimeError("Query interrupted")
    monkeypatch.setattr(rs, "resolve", lambda sid: sid)
    monkeypatch.setattr(dc, "resolved_paths", lambda r: [])
    monkeypatch.setattr(dc, "_series_csv_to_file_sorted", broken)
    monkeypatch.setattr(derive, "_series_csv_bytes",
                        broken if not flow else (lambda sid: b"series_id,obs_date,value\n"))
    out = derive.derive_and_put(["zz:x"], _Blob(), budget_min=0, flow_grain=flow)
    assert out["failed"] == ["zz:x"] and out["put"] == 0, out



# ---- review R1271: #79's served-CSV merge must let #90's fence through too ------------------------------------------

@pytest.mark.parametrize("wedge", ["get", "merge"])
def test_the_served_csv_merge_lets_the_fence_through(monkeypatch, alarm, wedge):
    """ecb merges each upload with the served CSV (csv_merge_served, #79). Its two `except Exception` blocks - the
    served GET and the merge - swallowed the fence on the serial path: derive ran on past it (11.1 s, the next id
    uploaded). Both now re-raise it."""
    monkeypatch.setenv("AQUEDUCT_DERIVE_WORKERS", "1")
    monkeypatch.setattr(derive, "_merge_served_sources", lambda: {"zz"})

    def slow(*a, **k):
        t = time.monotonic()
        while time.monotonic() - t < 10:
            time.sleep(0.05)
        return b"series_id,obs_date,value\n"

    class _MergeBlob(_Blob):
        def get(self, key):
            return slow() if wedge == "get" else b"series_id,obs_date,value\nzz:a,2023-01-01,1\n"
    if wedge == "merge":
        monkeypatch.setattr(derive, "_merge_with_served", lambda body, served: slow())
    blob = _MergeBlob()
    alarm(1.0)
    t0 = time.monotonic()
    with pytest.raises(orchestrate.UnitTimeout):
        derive.derive_and_put(["zz:wedged", "zz:after"], blob, budget_min=0)
    assert time.monotonic() - t0 < 4.0 and blob.put == [], blob.put
